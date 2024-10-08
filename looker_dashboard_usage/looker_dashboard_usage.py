###################################################################
# Script Name   : looker_dashboard_usage.py
#
#
# Description : Microservice script to read table
#             : data from the looker internal database and
#             : save it to csv in S3.
#
# Requirements: You must set the following environment variables
#             : to establish credentials for the microservice user
#             : export lookeruser=<<looker user>>
#             : export lookerpass=<<looker_PASSWD>>
#
# Usage       : python looker_dashboard_usage.py looker_dashboard_usage.json


import logging
import lib.logs as log
from tzlocal import get_localzone
from pytz import timezone
import os  # to read environment variables
import os.path  # file handling
import sys  # to read command line parameters
import boto3  # s3 access
from botocore.exceptions import ClientError
import json  # to read json config files
from io import StringIO
import pandas as pd
import datetime
from lib.redshift import RedShift
from sqlalchemy import create_engine
import pymysql

# Get script start time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
  .normalize(datetime.datetime.now(local_tz)
  .astimezone(yvr_tz)))
    
logger = logging.getLogger(__name__)
log.setup()
logging.getLogger("RedShift").setLevel(logging.WARNING)


# Exit and return exit code, message
def clean_exit(code, message):
  """Exits with a logger message and code"""
  logger.info(f'Exiting with code {code} : {message}')
  sys.exit(code)# Exit and return exit code, message
  
# check that configuration file was passed as argument
if len(sys.argv) != 2:
  print('Usage: python looker_dashboard_usage.py config.json')
  clean_exit(1,'Bad command use.')
configfile = sys.argv[1]
# confirm that the file exists
if os.path.isfile(configfile) is False:
  print(f'Invalid file name {configfile}')
  clean_exit(1,'Bad file name.')
# open the confifile for reading
with open(configfile) as f:
  data = json.load(f)

# get variables from config file
bucket = data['bucket']
source = data['source']
directory = data['directory']

# MySQL DB variables for the Looker internal DB.
looker_database='looker'
looker_user=os.environ['lookeruser']
looker_passwd=os.environ['lookerpass']

prev_date=datetime.date.today() - datetime.timedelta(days=1)

# tables and queries
tables=[
  {'tablename':'dashboard','query': 
    '''SELECT dashboard.* 
      FROM looker.dashboard 
      WHERE id IN (
        SELECT dashboard_id 
        FROM looker.history 
        WHERE history.source LIKE 'dashboard');'''},
  {'tablename':'history','query':
    f'''SELECT *
    FROM looker.history
    LEFT JOIN looker.dashboard
    ON history.dashboard_id = dashboard.id
    WHERE history.COMPLETED_AT LIKE '{prev_date}%%'
    AND status NOT LIKE 'error'
    AND source like 'dashboard';'''},
  {'tablename':'user','query':'SELECT * FROM looker.user;'},
  {'tablename':'user_facts','query':'SELECT * FROM looker.user_facts;'}
]

# set up S3 connection
client = boto3.client('s3')  # low-level functional API
resource = boto3.resource('s3')  # high-level object-oriented API

# Redshift Database connection string
rs_conn_string = """
dbname='{dbname}' host='{host}' port='{port}' user='{user}' password={password}
""".format(dbname='snowplow',
           host='redshift.analytics.gov.bc.ca',
           port='5439',
           user=os.environ['pguser'],
           password=os.environ['pgpass'])

# START CHANGES - 2022/11/28 BEO GDXDSD-5398
def structure_output_item(item_name):
  """create a standard structure for reporting item folder"""
  if item_name== 'user_facts':
    item_name = 'looker_user_facts'
  report_line= f"{source}/{directory}/{item_name}.{prev_date}"
  return(report_line)
# END changes - 2022/11/28 BEO GDXDSD-5398

def report(data):
  """reports out the data from the main program loop
  if no objects were processed; do not print a report"""
  if data["objects"] == 0:
    return
  print(f'Report: {__file__}\n')
  print(f'Config: {configfile}\n')

  # START CHANGES - 2022/11/28 BEO GDXDSD-5398
  # print success result 
  suc_unsuc_message=('This microservice ran: ')

  # Determine if the run was successfull / unsuccessful / empty and append to message
  if data['bad_list']:
      suc_unsuc_message+=( 'unsuccessful\n')
  elif data['good_list']:
      suc_unsuc_message+=( 'successful\n')
  else:
      suc_unsuc_message+=( 'empty\n')
  print(f"{suc_unsuc_message}")
  # END changes - 2022/11/28 BEO GDXDSD-5398

  # get times from system and convert to Americas/Vancouver for printing
  yvr_dt_end = (yvr_tz
                .normalize(datetime.datetime.now(local_tz)
                           .astimezone(yvr_tz)))  
  print(
    'Microservice started at: '
    f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
    f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
    f'elapsing: {yvr_dt_end - yvr_dt_start}.\n')
  print(f'Objects to process: {data["objects"]}')
  print(f'Objects that failed to process: {data["failed"]}')
  print(f'Objects output to \'processed/good\': {data["good"]}')
  print(f'Objects output to \'processed/bad\': {data["bad"]}')
  # START CHANGES - 2022/11/28 BEO GDXDSD-5398
  if data['good_list']:
    print(
       # START CHANGES - 2024/10/08 VV GDXDSD-7111
      "\nList of objects successfully fully ingested from S3, processed, "
      "copied from Redshift, and loaded to S3 ('good'):")
      # END CHANGES - 2024/10/08 VV GDXDSD-7111
    for i, item in enumerate(data['good_list'], 1):
      print(f"{i}.",structure_output_item(item))
  if data['bad_list']:
    print('\nList of objects that failed to process:')
    for i, item in enumerate(data['bad_list'], 1):
      print(f"{i}.",structure_output_item(item))
  return()
  # END changes - 2022/11/28 BEO GDXDSD-5398


# Mysql Database connection string
def get_looker_db_connection():
  connection_string = "mysql+pymysql://{}:{}@{}:{}/{}".format(
    looker_user, 
    looker_passwd, 
    'looker-backend.cos2g85i8rfj.ca-central-1.rds.amazonaws.com',
    '3306', 
    looker_database
  )
  return create_engine(connection_string)


# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'objects': 4,
    'failed': 0,
    'good': 0,
    'bad': 0,
    'good_list': [],
    'bad_list': []
}

# Reads a query against a db table and returns a dataframe
def read_table_to_dataframe(table,mydb):
  try:
    df = pd.read_sql(table["query"],mydb)
  except Exception as err:
    logger.exception(f'Exception reading from Looker Internal Database: {err}')
    report_stats['failed'] += 1
    report_stats['bad_list'].append(table['tablename'])
    report(report_stats)
    clean_exit(1,'Reading from Looker Internal Database failed')
  if table['tablename'] == 'history':
    # treats the pattern as a literal string when regex=False
    df.message = df.message.str.replace('|','\|', regex=False)
  return df


# Takes a select query string and a mysql connection and
# returns a dataframe with the results of the select 
def query_mysql_db(table):
  try:
    mydb = get_looker_db_connection()
    result_dataframe = read_table_to_dataframe(table,mydb)
  except create_engine.OperationalError as err:
    logger.exception('Connection to Looker Internal DB failed.')
    logger.exception(f'mysql errno, sqlstate, msg: {err}')
    report_stats['failed'] += 1
    report(report_stats)
    clean_exit(1,'Connection to Looker Internal Database failed')
  return result_dataframe


# Takes a dataframe and writes it to the specified bucket in S3
def write_dataframe_as_csv_to_s3(df, filename):
  """writes a dataframe in CSV format to S3.
  Usage:
  df = dataframe
  filename is S3 filename target
  """
  # START CHANGES - 2022/11/28 BEO GDXDSD-5398 - commented out code and replaced with one line
  #if filename == 'user_facts':
  #  filename = 'looker_user_facts'
  #outfile=f'{filename}.{prev_date}'
  #object_key = f"{source}/{directory}/{outfile}"
  object_key = structure_output_item(filename)
  # END changes - 2022/11/28 BEO GDXDSD-5398
  csv_buffer = StringIO()
  df.to_csv(csv_buffer, header=True, index=False, sep="|")
  try:
    resource.Bucket(bucket).put_object(Key=object_key,
                                     Body=csv_buffer.getvalue())
  except ClientError:
    logger.exception(f'Failed to copy {filename} to {object_key} in S3.')
    report_stats['failed'] += 1
    report_stats['bad_list'].append(filename)
    report(report_stats)
    clean_exit(1,'Write to S3 failed')


for table in tables:
  # select from table into df
  try:
    df = query_mysql_db(table)
  except:
    logger.exception(f"Failed to query looker.{table['tablename']}.")
    report_stats['bad'] += 1
    report_stats['bad_list'].append(table['tablename'])
    report(report_stats)
    clean_exit(1,'Querying Looker Internal Database failed')

  # upload df to S3 microservices bucket
  try:
    write_dataframe_as_csv_to_s3(df,table['tablename'])
  except:
    logger.exception(f"Failed to write looker.{table['tablename']} dataframe to S3.")
    report_stats['bad'] += 1
    report_stats['bad_list'].append(table['tablename'])
    report(report_stats)
    clean_exit(1,'Writing dataframe as csv to S3 failed')
  report_stats['good'] += 1
  report_stats['good_list'].append(table['tablename'])

report(report_stats)
clean_exit(0, 'Finished all processing cleanly.')
