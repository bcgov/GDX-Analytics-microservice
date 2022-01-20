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
import pandas as pd

# Get script start time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
    .normalize(datetime.now(local_tz)
    .astimezone(yvr_tz)))
    
logger = logging.getLogger(__name__)
log.setup()
logging.getLogger("RedShift").setLevel(logging.WARNING)

looker_database='looker'
looker_user=os.environ['lookeruser']
looker_passwd=os.environ['lookerpass']
history_table_query=""""""
dashboard_table_query=""""""


# Exit and return exit code, message
def clean_exit(code, message):
  """Exits with a logger message and code"""
  logger.info('Exiting with code %s : %s', str(code), message)
  sys.exit(code)


# Redshift Database connection string
rs_conn_string = """
dbname='{dbname}' host='{host}' port='{port}' user='{user}' password={password}
""".format(dbname='snowplow',
           host='redshift.analytics.gov.bc.ca',
           port='5439',
           user=os.environ['pguser'],
           password=os.environ['pgpass'])


# Mysql Database connection string
def get_looker_db_connection():
  return connection.connect(host='looker-backend.cos2g85i8rfj.ca-central-1.rds.amazonaws.com',
                            looker_database=looker_database,
                            looker_user=looker_user, 
                            looker_passwd=looker_passwd,
                            use_pure=True)


# Reads a query against a db table and returns a dataframe
def read_table_to_dataframe(query,mydb):
  try:
    df = pd.read_sql(query,mydb)
  except Exception as err:
    logger.exception("Exception reading from Looker Internal Database. %s", str(err))
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
    logger.exception("Connection to Looker Internal DB failed.")
    logger.exception("mysql errno, sqlstate, msg: %s", str(err))
    clean_exit(1,'Connection to Looker Internal Database failed')
  return result_dataframe



def copy_dataframe_to_redshift():
  return 0


def main():
  # select from history table into df
  history_df = query_mysql_db(history_table_query,get_looker_db_connection())

  # upload history df into redshift 
  copy_dataframe_to_redshift(history_df)

  # select from dashboard table into df
  dashboard_df = query_mysql_db(dashboard_table_query,get_looker_db_connection)

  # upload dashboard df into redshift 
  copy_dataframe_to_redshift(dashboard_df)


clean_exit(0, 'Finished all processing cleanly.')

