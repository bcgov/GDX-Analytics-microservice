###################################################################
# Script Name   : google_mybusiness_servicebc_derived.py
#
# Description   : Creates google_mybusiness_servicebc_derived, which is a
#               : persistent derived table (DT) joining google.locations
#               : with servicebc.office_info, and servicebc.datedimension
#
# Requirements  : You must set the following environment variable
#               : to establish credentials for the pgpass user microservice
#
#               : export pguser=<<database_username>>
#               : export pgpass=<<database_password>>
#
#
# Usage         : python google_mybusiness_servicebc_derived.py -c config.json
#
#               : the flags specified in the usage example above are:
#               : -c <Microservice configuration file>
#
import os
import sys
import logging
import psycopg2
import json
import argparse
import lib.logs as log

# Set up logging
logger = logging.getLogger(__name__)
log.setup()

# set up the Redshift connection
dbname = 'snowplow'
host = 'redshift.analytics.gov.bc.ca'
port = '5439'
user = os.environ['pguser']
password = os.environ['pgpass']
conn_string = (f"dbname='{dbname}' host='{host}' port='{port}' "
               f"user='{user}' password={password}")

parser = argparse.ArgumentParser(
    description='GDX Analytics utility for Google My Business Service BC.')
parser.add_argument('-c', '--conf', help='Microservice config file',)

flags = parser.parse_args()

CONFIG = flags.conf

with open(CONFIG) as f:
    config = json.load(f)

config_schema = config["schema"]
config_dbtable = config["dbtable"]
config_dml = config["dml"]

with open('dml/{}'.format(config_dml), 'r') as f:
    query = f.read()

query = query.format(
    schema=config_schema,
    dbtable=config_dbtable)


with psycopg2.connect(conn_string) as conn:
    with conn.cursor() as curs:
        try:
            curs.execute(query)
        except psycopg2.Error:
            logger.exception((
                'Error: failed to execute the transaction '
                'to prepare the google_mybusiness_servicebc_derived DT'))
            print(
                'Error: failed to execute the transaction '
                'to prepare the google_mybusiness_servicebc_derived DT')
            sys.exit(1)
        else:
            logger.info((
                'Success: executed the transaction '
                'to prepare the google_mybusiness_servicebc_derived DT'))
            print(
                'Success: executed the transaction '
                'to prepare the google_mybusiness_servicebc_derived DT')
            sys.exit(0)

