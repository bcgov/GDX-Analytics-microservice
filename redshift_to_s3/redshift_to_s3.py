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
from tzlocal import get_localzone
from pytz import timezone
import psycopg2
import boto3
from botocore.exceptions import ClientError
import lib.logs as log
import re

logger = logging.getLogger(__name__)
log.setup()

# Get script start time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
    .normalize(datetime.now(local_tz)
    .astimezone(yvr_tz)))

def clean_exit(code, message):
    """Exits with a logger message and code"""
    logger.info('Exiting with code %s : %s', str(code), message)
    sys.exit(code)


# Command line arguments
parser = argparse.ArgumentParser(
    description='GDX Analytics ETL utility for PRMP.')
parser.add_argument('-c', '--conf', help='Microservice configuration file.',)
parser.add_argument('-d', '--debug', help='Run in debug mode.',
                    action='store_true')
flags = parser.parse_args()

config = flags.conf
config_file = sys.argv[2]

# Load configuration json file as a dictionary
with open(config) as f:
    config = json.load(f)

bucket = config['bucket']
storage = config['storage']
archive = config['archive']
directory = config['directory']

object_prefix = config['object_prefix']

# creates the paths to the objects in s3 but does not have the object names
storage_prefix = f'{storage}/{directory}'
good_prefix = f"{archive}/good/{config['storage']}/{config['directory']}"
bad_prefix = f"{archive}/bad/{config['storage']}/{config['directory']}"
batch_prefix = f"{archive}/batch/{config['storage']}/{config['directory']}"

dml_file = config['dml']
header = config['header']

sql_parse_key = \
    False if 'sql_parse_key' not in config else config['sql_parse_key']

if 'date_list' in config:
    dates = config['date_list']

# Get required environment variables
pguser = os.environ['pguser']
pgpass = os.environ['pgpass']

# set up AWS Redshift connection
conn_string = """
dbname='{dbname}' host='{host}' port='{port}' user='{user}' password={password}
""".format(dbname='snowplow',
           host='redshift.analytics.gov.bc.ca',
           port='5439',
           user=pguser,
           password=pgpass)

# set up S3 connection
client = boto3.client('s3')  # low-level functional API
resource = boto3.resource('s3')  # high-level object-oriented API
res_bucket = resource.Bucket(bucket)  # resource bucket object
bucket_name = res_bucket.name

def raise_(ex):
    '''to raise generic exceptions'''
    raise ex


# returns the pmrp_date_range SQL statement to return a BETWEEN clause on date
def pmrp_date_range():
    '''generate a SQL DML string for a date type BETWEEN clause'''
    between = "''{}'' AND ''{}''".format(start_date, end_date)
    logger.info('date clause will be between %s', between)
    return between


def pmrp_qdata_dates():
    '''generate a SQL DML string for a date list'''
    date_list = ''
    query = ''
    for date in dates:
        query += date_list.join(
        "((cfms_poc.welcome_time >= (TIMESTAMP " + "''" + date + "''" + ")) AND "
        "(cfms_poc.welcome_time < ((DATEADD(day,1, TIMESTAMP " + "''" + date + "''" + "))))) OR ")
    last_or_index = query.rfind("OR")
    query_string = query[:last_or_index]
    return query_string


# IMPORTANT
# setup a list of known SQL Parse Keys; when adding new configs requiring
# SQL request queries that contain a unique sql_parse_key keywords to format,
# this dictionary must be updated to reference both the expected keyword and
# a function name which will return the value for that keyword when called.
SQLPARSE = {
    'pmrp_date_range': pmrp_date_range,
    'pmrp_qdata_dates': pmrp_qdata_dates,
    }


def return_query(local_query):
    '''returns the response from a query on redshift'''
    with psycopg2.connect(conn_string) as local_conn:
        with local_conn.cursor() as local_curs:
            try:
                local_curs.execute(local_query)
            except psycopg2.Error:
                logger.exception("psycopg2.Error:")
                report_stats['failed_redshift_queries'] += 1
                clean_exit(1, 'Failed psycopg2 query attempt.')
            else:
                response = local_curs.fetchone()[0]
                logger.info("returned: %s", response)
                report_stats['good_redshift_queries'] += 1
    return response


def last_modified_object_key(prefix):
    '''return last modified object key'''
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

# Will run at end of script to print out accumulated report_stats
def report(data):
    '''reports out the data from the main program loop'''
    if data['failed_redshift_queries'] or data['failed_unloads']:
        print(f'\n*** ATTN: The microservice ran unsuccessfully. Please investigate logs/{__file__} ***\n') 
    else:
        print(f'\n***The microservice ran successfully***\n')
    
    print(f'Report: {__file__}\n')
    print(f'Config: {config_file}\n')
    print(f'DML: {dml_file}\n')

    if 'start_date' and 'end_date' in config:
        print(f'Requested Dates: {start_date} to {end_date}\n')
    # Get times from system and convert to Americas/Vancouver for printing
    yvr_dt_end = (yvr_tz
        .normalize(datetime.now(local_tz)
        .astimezone(yvr_tz)))
    print(
    	f'Microservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.\n')

    print(f'\nObjects to process: {data["objects"]}')
    print(f'Objects loaded to S3: {data["sucessful_unloads"]}/{data["objects"]}')
    
    #Print additional messages to standardize reports
 
    if data["sucessful_unloads"]:
        print(f'Objects successful loaded to S3: {data["sucessful_unloads"]}')
        print("\nList of objects successfully loaded to S3")
        for i, item in enumerate(data['good_list'], 1):
            print(f"{i}.",f'{storage}/{directory}/{item}')
 
    if data["failed_unloads"]:
        print(f'Objects unsuccessful loaded to S3: {data["failed_unloads"]}')
        print('\nList of objects that failed to process:')
        for i, item in enumerate(data['bad_list'], 1):
             print(f"{i}.",f'{storage}/{directory}/{item}')

# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'objects':1,  #Script runs on a per object basis
    'unprocessed_objects': 0,
    'unprocessed_objects_list': [],
    'objects_processed': 0,
    'objects_not_processed': 0,
    'processed':0,
    'redshift_queries': 0,
    'failed_redshift_queries':0,
    'good_redshift_queries':0,
    'sucessful_unloads':0,
    'failed_unloads':0,
    'good_list': [],
    'bad_list' : [],
    's3_good_list': [],
    's3_bad_list' : []
}

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

elif 'date_list' in config:
    # set dates requested in date_list
    date_key = "_".join(dates)
    temp_key = object_key_builder(object_prefix, date_key)
    # restrict object name length
    object_key = temp_key[:255] if len(temp_key) > 255 else temp_key

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
# This UNLOAD inserts into the S3 BATCH path
log_query = '''
UNLOAD ('{request_query}')
TO 's3://{bucket}/{batch_prefix}/{object_key}_part'
credentials 'aws_access_key_id={aws_access_key_id};\
aws_secret_access_key={aws_secret_access_key}'
PARALLEL OFF
{header}
'''.format(
    request_query=request_query,
    bucket=bucket,
    batch_prefix=batch_prefix,
    object_key=object_key,
    aws_access_key_id='{aws_access_key_id}',
    aws_secret_access_key='{aws_secret_access_key}',
    header='HEADER' if header else '')

query = log_query.format(
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

def get_unprocessed_objects():
    # This bucket scan will find unprocessed objects matching on the object prefix
    # objects_to_process will contain zero or one objects if truncate = True
    # objects_to_process will contain zero or more objects if truncate = False
    filename_regex = fr'^{object_prefix}'
    objects_to_process = []
    for object_summary in res_bucket.objects.filter(Prefix=batch_prefix): # batch_prefix may need a trailing /
        key = object_summary.key
        filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')
        goodfile = f"{archive}/good/{key}"
        badfile = f"{archive}/bad/{key}"

        def is_processed():
            '''Check to see if the file has been processed already'''
            try:
                client.head_object(Bucket=bucket, Key=goodfile)
            except ClientError:
                pass  # this object does not exist under the good destination path
            else:
                logger.info("%s was processed as good already.", filename)
                return True
            try:
                client.head_object(Bucket=bucket, Key=badfile)
            except ClientError:
                pass  # this object does not exist under the bad destination path
            else:
                logger.info("%s was processed as bad already.", filename)
                return True
            logger.info("%s has not been processed.", filename)
            return False

        # skip to next object if already processed
        if is_processed():
            continue
        if re.search(filename_regex, filename):
            objects_to_process.append(object_summary)
            logger.info('added %a for processing', filename)
            report_stats['unprocessed_objects'] += 1
            report_stats['unprocessed_objects_list'].append(object_summary)
    return objects_to_process

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
            report_stats['failed_unloads'] += 1
            report_stats['bad_list'].append(object_key)
            report(report_stats)
            clean_exit(1,'Failed psycopg2 query attempt.')
        else:
            logger.info(
                'UNLOAD successful. Object prefix is %s/%s/%s',
                bucket, storage_prefix, object_key)
            report_stats['sucessful_unloads'] += 1

            # optionally add the file extension and transfer to storage folders
            objects = get_unprocessed_objects()
            for object in objects:
                key = object.key
                filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')
                copy_from_prefix = ''
                copy_to_prefix = ''
                copy_good_prefix = f"{good_prefix}/{filename}"
                copy_bad_prefix = f"{bad_prefix}/{filename}"
                if 'extension' in config:
                    extension = config['extension']
                    logger.info(
                        'File extension set in %s as %s',
                        config_file, extension)
                    copy_from_prefix = f"{batch_prefix}/{filename}"
                    copy_to_prefix = f"{storage_prefix}/{filename}{extension}"
                else:
                    logger.info(
                        'File extension not set in %s',
                        config_file)
                    copy_from_prefix = f"{batch_prefix}/{filename}"
                    copy_to_prefix = f"{storage_prefix}/{filename}"
                try:
                    logger.info(
                        'Copying from %s',
                        copy_from_prefix)
                    logger.info(
                        'Copying to %s',
                        copy_to_prefix)
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_to_prefix)
                except ClientError:
                    logger.exception('Exception copying object %s', copy_to_prefix)
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_bad_prefix)
                    report_stats['s3_bad_list'].append(object_key)
                    report_stats['objects_not_processed'] += 1
                else:
                    logger.info('copied %s to %s', object.key, copy_to_prefix)
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_good_prefix)
                    report_stats['objects_processed'] += 1
                    report_stats['s3_good_list'].append(object_key)

            report_stats['good_list'].append(object_key)
            report(report_stats)
            clean_exit(0,'Finished succesfully.')
