###################################################################
# Script Name   : sbc_dashboard_usage.py
#
#
# Description 
#
# Requirements
# 

import mysql.connector as connection
from mysql.connector.errors import Error
import logging
import lib.logs as log
from tzlocal import get_localzone
from pytz import timezone
import os  # to read environment variables
import sys  # to read command line parameters
import boto3  # s3 access
from botocore.exceptions import ClientError
import json  # to read json config files
from io import StringIO
import pandas as pd
import datetime
from lib.redshift import RedShift

# Get script start time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
  .normalize(datetime.datetime.now(local_tz)
  .astimezone(yvr_tz)))
    
logger = logging.getLogger(__name__)
log.setup()
logging.getLogger("RedShift").setLevel(logging.WARNING)

# check that configuration file was passed as argument
if len(sys.argv) != 2:
  print('Usage: python sbc_dashboard_usage.py config.json')
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

prev_date=datetime.datetime.today() - datetime.timedelta(days=1)


# tables and queries
tables=[
  {'tablename':'dashboard','query': 
    f'''SELECT * FROM looker.dashboard where id IN ('70');'''},
  {'tablename':'history','query':
    f'''SELECT * 
    FROM looker.history
    LEFT JOIN looker.dashboard
    ON history.dashboard_id = dashboard.id
    WHERE dashboard.id IN ('70')
    AND history.COMPLETED_AT = (TIMESTAMP({prev_date});'''},
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


# Exit and return exit code, message
def clean_exit(code, message):
  """Exits with a logger message and code"""
  logger.info(f'Exiting with code {code} : {message}')
  sys.exit(code)


# Mysql Database connection string
def get_looker_db_connection():
  return connection.connect(host='looker-backend.cos2g85i8rfj.ca-central-1.rds.amazonaws.com',
                            port='3306',
                            database=looker_database,
                            user=looker_user,
                            password=looker_passwd)


# Reads a query against a db table and returns a dataframe
def read_table_to_dataframe(query,mydb):
  try:
    df = pd.read_sql(query,mydb)
  except Exception as err:
    logger.exception(f'Exception reading from Looker Internal Database: {err}')
    clean_exit(1,'Reading from Looker Internal Database failed')
  return df


# Takes a select query string and a mysql connection and
# returns a dataframe with the results of the select 
def query_mysql_db(looker_query,get_db_connection):
  try:
    mydb = get_db_connection()
    result_dataframe = read_table_to_dataframe(looker_query,mydb)
    mydb.close() #close the connection
  except connection.Error as err:
    mydb.close()
    logger.exception('Connection to Looker Internal DB failed.')
    logger.exception(f'mysql errno, sqlstate, msg: {err}')
    clean_exit(1,'Connection to Looker Internal Database failed')
  return result_dataframe


# Takes a dataframe and writes it to the specified bucket in S3
def write_dataframe_as_csv_to_s3(df, filename):
  outfile=f'{filename}.{prev_date}.csv'
  object_key = f"{source}/{directory}/{outfile}"
  csv_buffer = StringIO()
  df.to_csv(csv_buffer, header=True, index=False, sep="|")
  try:
    resource.Bucket(bucket).put_object(Key=object_key,
                                     Body=csv_buffer.getvalue())
  except ClientError:
    logger.exception(f'Failed to copy {filename} to {object_key} in S3.')


for table in tables:
  # select from table into df
  df = query_mysql_db(table['query'],get_looker_db_connection())
  # upload df to S3 microservices bucket
  write_dataframe_as_csv_to_s3(df,table['tablename'])


clean_exit(0, 'Finished all processing cleanly.')
