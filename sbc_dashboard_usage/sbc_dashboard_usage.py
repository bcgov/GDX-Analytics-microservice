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
def get_mysql_connection():
    return connection.connect(host='looker-backend.cos2g85i8rfj.ca-central-1.rds.amazonaws.com',
                              database = 'looker',
                              user=os.environ['lookeruser'], 
                              passwd=os.environ['lookerpass'],
                              use_pure=True)


def looker_query_builder():
    mysql_query=""""""


def read_table_to_dataframe(query,mydb):
    return pd.read_sql(query,mydb)


def query_looker_internal_db():
  try:
      mydb = get_mysql_connection()
      query = looker_query_builder()
      result_dataFrame = read_table_to_dataframe(query,mydb)
      mydb.close() #close the connection
  except mysql.connector.Error as err:
      mydb.close()
      logger.error("Reading from Looker Internal DB failed.")
      logger.error("msql errno, sqlstate, msg: ", err)