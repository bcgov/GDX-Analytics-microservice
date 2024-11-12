###################################################################
# Script Name   : build_derived_assets.py
#
# Description   : Creates asset_downloads_derived, which is a
#               : persistent derived table (PDT)
#
# Requirements  : You must set the following environment variable
#               : to establish credentials for the pgpass user microservice
#
#               : export pguser=<<database_username>>
#               : export pgpass=<<database_password>>
#
#
# Usage         : python build_derived_assets.py
#
#               : This microservice can be run after asset_data_to_redshift.py
#               : has run using the *_assets.json config file and updated the
#               : {{schema}}.asset_downloads table.
#

import os
import logging
import sys
import json  # to read json config files
from lib.redshift import RedShift
from datetime import datetime
from tzlocal import get_localzone
from pytz import timezone
import lib.logs as log

# Set local timezone and get time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
                .normalize(datetime.now(local_tz)
                           .astimezone(yvr_tz)))

logger = logging.getLogger(__name__)
log.setup()
logging.getLogger("RedShift").setLevel(logging.WARNING)


# Provides exit code and logs message
def clean_exit(code, message):
    """Exits with a logger message and code"""
    logger.info('Exiting with code %s : %s', str(code), message)
    sys.exit(code)


if not os.path.exists('logs'):
    os.makedirs('logs')

# check that configuration file was passed as argument
if len(sys.argv) != 2:
    print('Usage: python build_derived_assets.py config.json')
    clean_exit(1, 'Bad command use.')
configfile = sys.argv[1]
# confirm that the file exists
if os.path.isfile(configfile) is False:
    print("Invalid file name {}".format(configfile))
    clean_exit(1, 'Invalid file name.')
# open the confifile for reading
with open(configfile) as f:
    data = json.load(f)


def report(data):
    '''reports out the data from the main program loop'''
    # if no objects were processed; do not print a report
    if data["objects"] == 0:
        return
    print(f'\nReport {__file__}:')
    print(f'\nConfig: {configfile}')
    # get times from system and convert to Americas/Vancouver for printing
    yvr_dt_end = (yvr_tz
                  .normalize(datetime.now(local_tz)
                             .astimezone(yvr_tz)))
    print(
        '\nMicroservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.')
    print(f'\nObjects to process: {data["objects"]}')
    print(f'Objects that failed to process: {data["failed"]}')
    print(f'Objects loaded to Redshift: {data["loaded"]}')
    if data['good_list']:
        print(
        "\nList of tables successfully parsed, "
        "and copied to the derived table in Redshift:")
        [print(table) for table in data['good_list']]
    if data['bad_list']:
        print('\nList of tables that failed to process:')
        [print(table) for table in data['bad_list']]
    if data['incomplete_list']:
        print('\nList of tables that were not processed due to early exit:')
        [print(table) for table in data['incomplete_list']]
    print(f'\n-----------------\n')

schema_name = data['schema_name']
asset_host = data['asset_host']
asset_source = data['asset_source']
asset_scheme_and_authority = data['asset_scheme_and_authority']
dbtable = data['dbtable']
if 'truncate' in data:
    truncate = data['truncate']
else:
    truncate = False


truncate_intermediate_table = 'TRUNCATE TABLE ' + dbtable + ';'


with open('ddl/build_derived_assets.sql', 'r') as file:
    query = file.read()
query = query.format(schema_name=schema_name,
           asset_host=asset_host,
           asset_source=asset_source,
           asset_scheme_and_authority=asset_scheme_and_authority,
           truncate_intermediate_table=truncate_intermediate_table)

# Reporting variables
report_stats = {
    'objects': 1,
    'failed': 0,
    'loaded': 0,
    'good_list': [],
    'bad_list': [],
    'incomplete_list': []
}


# Execute the transaction against Redshift using local lib redshift module
table_name = dbtable
spdb = RedShift.snowplow(table_name)
if spdb.query(query):
    report_stats['loaded'] += 1
    report_stats['good_list'].append(table_name)
else:
    report_stats['failed'] += 1
    report_stats['bad_list'].append(table_name)
    report_stats['incomplete_list'].append(table_name)
    clean_exit(1, f'Query failed to load {table_name}, '
               'no further processing.')
spdb.close_connection()

report(report_stats)
clean_exit(0, 'Finished all processing cleanly.')
