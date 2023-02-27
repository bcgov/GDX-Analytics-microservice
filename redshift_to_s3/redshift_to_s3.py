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
batch_prefix = f"{archive}/batch/{config['storage']}/{config['directory']}" # where the data is temporarly stored when unloaded from redshift
storage_prefix = f'{storage}/{directory}'                                   # where the final file for the client is stored
good_prefix = f"{archive}/good/{config['storage']}/{config['directory']}"   # where the unloaded data is archived if the storage is successful
bad_prefix = f"{archive}/bad/{config['storage']}/{config['directory']}"     # where the unloaded data is archived if the storage is unsuccessful

dml_file = config['dml']
header = config['header']

sql_parse_key = \
    False if 'sql_parse_key' not in config else config['sql_parse_key']

delimiter = False if 'delimiter' not in config else config['delimiter']

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
    if data['failed_redshift_queries'] or data['failed_unloads'] or data['unstored_objects'] or data['bad_objects']:
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

    print(f'\nObjects loaded to S3 /batch: {data["successful_unloads"]}/{data["successful_unloads"]+data["failed_unloads"]}')

    #Print additional messages to standardize reports

    if data["successful_unloads"]:
        print(f'Objects successfully loaded to S3 /batch: {data["successful_unloads"]}')
        print("\nList of objects successfully loaded to S3 /batch")
        for i, item in enumerate(data['successful_unloads_list'], 1):
            print(f"{i}.",f'{batch_prefix}/{item}')

    if data["failed_unloads"]:
        print(f'\nObjects unsuccessfully loaded to S3 /batch: {data["failed_unloads"]}')
        print("\nList of objects unsuccessfully loaded to S3 /batch:")
        for i, item in enumerate(data['failed_unloads_list'], 1):
             print(f"{i}.",f'{batch_prefix}/{item}')
    
    print(f'\n\nObjects to store: {data["unprocessed_objects"]}')

    # Print all objects loaded into s3/client
    if data["stored_objects"]:
        print(f'Objects stored to s3 /client: {data["stored_objects"]}')
        print(f'\nList of objects stored to S3 /client:')
        if data['stored_objects_list']:
            for i, item in enumerate(data['stored_objects_list'], 1):
                print(f"{i}: {storage_prefix}/{item}")

    # Print all objects not loaded into s3/client
    if data["unstored_objects"]:
        print(f'Objects not stored to s3 /client: {data["unstored_objects"]}')
        print(f'\nList of objects not stored to S3 /client:')
        if data['unstored_objects_list']:
            for i, item in enumerate(data['unstored_objects_list'], 1):
                print(f"{i}: {storage_prefix}/{item}")

    print(f'\n\nObjects to process: {data["unprocessed_objects"]}')

    # Print all objects loaded into s3/good
    if data["good_objects"]:
        print(f'Objects processed to s3 /good: {data["good_objects"]}')
        print(f'\nList of objects processed to S3 /good:')
        if data['good_objects_list']:
            for i, item in enumerate(data['good_objects_list'], 1):
                print(f"{i}: {good_prefix}/{item}")

    # Print all objects loaded into s3/bad
    if data["bad_objects"]:
        print(f'Objects processed to s3 /bad: {data["bad_objects"]}')
        print(f'\nList of objects processed to S3 /bad:')
        if data['bad_objects_list']:
            for i, item in enumerate(data['bad_objects_list'], 1):
                print(f"{i}: {bad_prefix}/{item}")

# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'redshift_queries': 0,
    'failed_redshift_queries':0,
    'good_redshift_queries':0,
    'successful_unloads':0,
    'successful_unloads_list': [],
    'failed_unloads':0,
    'failed_unloads_list': [],
    'unprocessed_objects': 0,
    'stored_objects': 0,
    'stored_objects_list': [],
    'unstored_objects': 0,
    'unstored_objects_list': [],
    'good_objects': 0,
    'good_objects_list': [],
    'bad_objects': 0,
    'bad_objects_list' : []
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

# If there is no delimiter specified in the config file
# build the delimiter string for the UNLOAD query, else leave it blank
if delimiter:
    delimiter_string = "delimiter '{delimiter}'".format(delimiter=delimiter)
else:
    delimiter_string = ""

# The UNLOAD query to support S3 loading direct from a Redshift query
# ref: https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
# This UNLOAD inserts into the S3 BATCH path
log_query = '''
UNLOAD ('{request_query}')
TO 's3://{bucket}/{batch_prefix}/{object_key}_part'
credentials 'aws_access_key_id={aws_access_key_id};\
aws_secret_access_key={aws_secret_access_key}'
{delimiter_string}
PARALLEL OFF
{header}
'''.format(
    request_query=request_query,
    bucket=bucket,
    batch_prefix=batch_prefix,
    object_key=object_key,
    aws_access_key_id='{aws_access_key_id}',
    aws_secret_access_key='{aws_secret_access_key}',
    delimiter_string=delimiter_string,
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
        goodfile = f"{good_prefix}/{filename}"
        badfile = f"{bad_prefix}/{filename}"

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
            report_stats['failed_unloads_list'].append(object_key)
            report(report_stats)
            clean_exit(1,'Failed psycopg2 query attempt.')
        else:
            logger.info(
                'UNLOAD successful. Object prefix is %s/%s/%s',
                bucket, storage_prefix, object_key)
            report_stats['successful_unloads'] += 1
            report_stats['successful_unloads_list'].append(object_key)

            # optionally add the file extension and transfer to storage folders
            objects = get_unprocessed_objects()
            for object in objects:
                key = object.key
                filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')

                # final paths that include the filenames
                copy_good_prefix = f"{good_prefix}/{filename}"
                copy_bad_prefix = f"{bad_prefix}/{filename}"
                copy_from_prefix = f"{batch_prefix}/{filename}"
                if 'extension' in config:
                    # if an extension was set in the config, add it to the end of the file
                    extension = config['extension']
                    filename_with_extension = f"{filename}{extension}"
                    logger.info('File extension set in %s as "%s"', config_file, extension)
                else:
                    filename_with_extension = filename
                    logger.info('File extension not set in %s', config_file)
                try:
                    # final storage path that includes the filename and optional extension
                    copy_to_prefix = f"{storage_prefix}/{filename_with_extension}"
                    logger.info('Copying to s3 /client ...')
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_to_prefix)
                except ClientError:
                    logger.exception('Exception copying from s3://%s/%s', bucket, copy_from_prefix)
                    logger.exception('to s3://%s/%s', bucket, copy_to_prefix)
                    report_stats['unstored_objects'] += 1
                    report_stats['unstored_objects_list'].append(filename_with_extension)
                    
                    logger.info('Copying to s3 /bad ...')
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_bad_prefix)
                    logger.info('Copied from s3://%s/%s', bucket, copy_from_prefix)
                    logger.info('Copied to s3://%s/%s', bucket, copy_bad_prefix)
                    report_stats['bad_objects_list'].append(filename)
                    report_stats['bad_objects'] += 1
                    
                    report(report_stats)
                    clean_exit(1,'Failed boto3 copy_object attempt.')
                else:
                    logger.info('Copied from s3://%s/%s', bucket, copy_from_prefix)
                    logger.info('Copied to s3://%s/%s', bucket, copy_to_prefix)
                    report_stats['stored_objects'] += 1
                    report_stats['stored_objects_list'].append(filename_with_extension)
                    
                    logger.info('Copying to s3 /good ...')
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_good_prefix)
                    logger.info('Copied from s3://%s/%s', bucket, copy_from_prefix)
                    logger.info('Copied to s3://%s/%s', bucket, copy_good_prefix)
                    report_stats['good_objects'] += 1
                    report_stats['good_objects_list'].append(filename)

            report(report_stats)
            clean_exit(0,'Finished succesfully.')
