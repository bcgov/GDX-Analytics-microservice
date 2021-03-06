"""Create an object in S3 containing the response of a query on Redshift"""
###################################################################
# Script Name   : redshift_to_s3.py
#
# Description   : Creates an object in S3, the result of a query on Redshift
#               : defined by the DML file referenced in the configuration file.
#
# Requirements  : You must set the following environment variable
#               : to establish credentials for the pgpass user microservice
#
#               : export pguser=<<database_username>>
#               : export pgpass=<<database_password>>
#
# Usage         : python redshift_to_s3.py -c config.d/config.json
#
import os
import logging
import argparse
import json
import sys
from datetime import datetime, date, timedelta
import psycopg2
import boto3
import lib.logs as log

logger = logging.getLogger(__name__)
log.setup()


def clean_exit(code, message):
    """Exits with a logger message and code"""
    logger.info('Exiting with code %s : %s', str(code), message)
    sys.exit(code)


# Get required environment variables
pguser = os.environ['pguser']
pgpass = os.environ['pgpass']

# AWS Redshift and S3 configuration
conn_string = """
dbname='{dbname}' host='{host}' port='{port}' user='{user}' password={password}
""".format(dbname='snowplow',
           host='redshift.analytics.gov.bc.ca',
           port='5439',
           user=pguser,
           password=pgpass)

# Command line arguments
parser = argparse.ArgumentParser(
    description='GDX Analytics ETL utility for PRMP.')
parser.add_argument('-c', '--conf', help='Microservice configuration file.',)
parser.add_argument('-d', '--debug', help='Run in debug mode.',
                    action='store_true')
flags = parser.parse_args()

config = flags.conf

# Load configuration json file as a dictionary
with open(config) as f:
    config = json.load(f)

object_prefix = config['object_prefix']
bucket = config['bucket']

source = config['source']
directory = config['directory']
source_prefix = f'{source}/{directory}'

destination = config['destination']
good_prefix = f"{destination}/good/{config['source']}/{config['directory']}"

dml_file = config['dml']
header = config['header']

sql_parse_key = \
    False if 'sql_parse_key' not in config else config['sql_parse_key']


def raise_(ex):
    '''to raise generic exceptions'''
    raise ex


# returns the pmrp_date_range SQL statement to return a BETWEEN clause on date
def pmrp_date_range():
    '''geneate a SQL DML sting for a date type BETWEEN clause'''
    between = "''{}'' AND ''{}''".format(start_date, end_date)
    logger.info('date clause will be between %s', between)
    return between


# IMPORTANT
# setup a list of known SQL Parse Keys; when adding new configs requiring
# SQL request queries that contain a unique sql_parse_key keywords to format,
# this dictionary must be updated to reference both the expected keyword and
# a function name which will return the value for that keyword when called.
SQLPARSE = {
    'pmrp_date_range': pmrp_date_range,
    }


def return_query(local_query):
    '''returns the response from a query on redshift'''
    with psycopg2.connect(conn_string) as local_conn:
        with local_conn.cursor() as local_curs:
            try:
                local_curs.execute(local_query)
            except psycopg2.Error:
                logger.exception("psycopg2.Error:")
                clean_exit(1, 'Failed psycopg2 query attempt.')
            else:
                response = local_curs.fetchone()[0]
                logger.info("returned: %s", response)
    return response


def last_modified_object_key(prefix):
    '''return last modified object key'''
    # set up S3 connection
    client = boto3.client('s3')
    # extract the list of objects
    list_objs = client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if list_objs['IsTruncated']:
        logger.warning('The list of objects in: %s/%s was truncated.',
                       bucket, prefix)
    if list_objs['KeyCount'] == 0:
        logger.warning('No objects found in %s/%s', bucket, prefix)
        return None

    # https://stackoverflow.com/questions/45375999/how-to-download-the-latest-file-of-an-s3-bucket-using-boto3
    def last_modified_sorter(obj):
        '''a sorter key method'''
        return int(obj['LastModified'].strftime('%s'))

    objs = list_objs['Contents']
    last_added = [obj['Key'] for obj in sorted(
        objs, key=last_modified_sorter, reverse=True)][0]
    return last_added


def unsent():
    '''determine the start date'''
    last_file = last_modified_object_key(good_prefix)
    # default start date to three days ago if no objects present
    if last_file is None:
        logger.info("No previous files to extract start date key from")
        return (date.today() - timedelta(days=3)).strftime('%Y%m%d')
    # extract a start date based on the end date of the last uploaded file
    logger.info("setting startdate to 1 after end date of last file: %s",
                 last_file)
    # 3rd from last index on split contains the file's end date as YYYYMMDD
    return (datetime.strptime(last_file.split("_")[-3], '%Y%m%d')
            + timedelta(days=1)).strftime('%Y%m%d')


def get_date(date_selector):
    '''return the SQL query for a date selector type'''
    select = (f"SELECT to_char({date_selector}(date), 'YYYYMMDD') FROM "
              "google.google_mybusiness_servicebc_derived")
    return return_query(select)


def object_key_builder(key_prefix, *args):
    """Construct an Object Key string based on a prefix followed by any number
    of optional positional arguemnts and suffixed by the localized timestamp.

    Args:
        key_prefix: A required prefix for this object's key
        *args: Variable length argument list.

    Returns:
        The complete object key string.
    """
    nowtime = datetime.now().strftime('%Y%m%dT%H%M%S')
    key_parts = [key_prefix]
    if args:
        key_parts.extend(list(args))
    key_parts.append(nowtime)
    object_key = '_'.join(str(part) for part in key_parts)
    return object_key

if 'start_date' in config and 'end_date' in config:
    # set start and end dates, defaulting to min/max if not defined
    start_date = config['start_date']
    end_date = config['end_date']

    # set start_date if not a YYYYMMDD value
    if any(start_date == pick for pick in ['min', 'max']):
        start_date = get_date(start_date)

    # determine unsent value for start date
    if start_date == 'unsent':
        start_date = unsent()
        logger.info("unsent start date set to: %s", start_date)

    # set end_date if not a YYYYMMDD value
    if any(end_date == pick for pick in ['min', 'max', 'unsent']):
        if end_date == 'unsent':
            end_date = 'max'
        end_date = get_date(end_date)

    if start_date > end_date:
        clean_exit(1, f'Start_date: {start_date} cannot be greater '
                   'than end_date: {end_date}.')

    object_key = object_key_builder(object_prefix,start_date,end_date)
else:
    object_key = object_key_builder(object_prefix)

# the _substantive_ query, one that users expect to see as output in S3.
request_query = open('dml/{}'.format(dml_file), 'r').read()

# If an SQL Parse Key was configured modify the request_query according to
# the value of the set parse
if sql_parse_key:
    # Check to see if the SQL Parse Key configured is known
    try:
        # derive the sql_parse_value based on the sql_parse_key
        sql_parse_value = SQLPARSE.get(
            sql_parse_key, lambda: raise_(Exception(LookupError)))()
    except KeyError:
        clean_exit(1,'The SQL Parse Key configured has not been implemented.')

    # Set the config defined sql_parse_key value as the key
    # in a dict with the computed sql_parse_value as that key's value
    keyword_dict = {config['sql_parse_key']: sql_parse_value}
    # pass the keyword_dict to the request query formatter
    request_query = request_query.format(**keyword_dict)


# The UNLOAD query to support S3 loading direct from a Redshift query
# ref: https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
# This UNLOAD inserts into the S3 SOURCE path. Use s3_to_sfts.py to move these
# SOURCE files into the SFTS, copying them to DESTINATION GOOD/BAD paths
log_query = '''
UNLOAD ('{request_query}')
TO 's3://{bucket}/{source_prefix}/{object_key}_part'
credentials 'aws_access_key_id={aws_access_key_id};\
aws_secret_access_key={aws_secret_access_key}'
PARALLEL OFF
{header}
'''.format(
    request_query=request_query,
    bucket=bucket,
    source_prefix=source_prefix,
    object_key=object_key,
    aws_access_key_id='{aws_access_key_id}',
    aws_secret_access_key='{aws_secret_access_key}',
    header='HEADER' if header else '')

query = log_query.format(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

with psycopg2.connect(conn_string) as conn:
    with conn.cursor() as curs:
        try:
            logger.info("executing query")
            curs.execute(query)
            logger.info(log_query)
        except psycopg2.Error:
            logger.exception("psycopg2.Error:")
            logger.error(('UNLOAD transaction on %s failed.'
                          'Quitting with error code 1'), dml_file)
            clean_exit(1,'Failed psycopg2 query attempt.')
        else:
            logger.info(
                'UNLOAD successful. Object prefix is %s/%s/%s',
                bucket, source_prefix, object_key)
            clean_exit(0,'Finished succesfully.')
