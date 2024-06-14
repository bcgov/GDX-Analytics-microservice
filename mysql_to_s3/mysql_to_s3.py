"""Create an object in S3 containing the response of a query on MySQL"""
###################################################################
# Script Name   : mysql_to_s3.py
#
# Description   : Creates an object in S3, the result of a query on MySQL
#               : defined by the DML file referenced in the configuration file.
#
# Requirements  : You must set the following environment variable
#               : to establish credentials for the pgpass user microservice
#
#               : export mysqluser=<<database_username>>
#               : export mysqlpass=<<database_password>>
#
# Usage         : python mysql_to_s3.py -c config.d/config.json
#
import os
import logging
import argparse
import json
import sys
from datetime import datetime, date, timedelta
from tzlocal import get_localzone
from pytz import timezone
import pymysql
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
    description='GDX Analytics microservice to transfer data from MySQL to S3.')
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
batch_prefix = f"{archive}/batch/{config['storage']}/{config['directory']}" # where the data is temporarly stored when unloaded from mysql
storage_prefix = f'{storage}/{directory}'                                   # where the final file for the client is stored
good_prefix = f"{archive}/good/{config['storage']}/{config['directory']}"   # where the unloaded data is archived if the storage is successful
bad_prefix = f"{archive}/bad/{config['storage']}/{config['directory']}"     # where the unloaded data is archived if the storage is unsuccessful

dml_file = config['dml']
header = config['header']

# if escape option is missing from the config, set as disabled
escape = False if 'escape' not in config else config['escape']

# if delimiter option is missing from the config, set as disabled
delimiter = False if 'delimiter' not in config else config['delimiter']

# if addquotes option is missing from the config, set as enabled
# even with this option enabled Excel will break when in a value a " appears before a delimiter character
addquotes = True if 'addquotes' not in config else config['addquotes']

# Get required environment variables
mysqluser = os.environ['mysqluser']
mysqlpass = os.environ['mysqlpass']

#
# TODO: change connection string to PyMySQL
#
# set up AWS RDS MySQL connection
conn_string = """
dbname='{dbname}' host='{host}' port='{port}' user='{user}' password={password}
""".format(dbname='looker',
           host='looker-backend.cos2g85i8rfj.ca-central-1.rds.amazonaws.com',
           port='3306',
           user=mysqluser,
           password=mysqlpass)

# set up S3 connection
client = boto3.client('s3')  # low-level functional API
resource = boto3.resource('s3')  # high-level object-oriented API
res_bucket = resource.Bucket(bucket)  # resource bucket object
bucket_name = res_bucket.name

#
# TODO: rework to be an upload object function
#
def download_object(o):
    '''downloads object to a tmp directoy'''
    dl_name = o.replace(source_prefix, '')
    try:
        res_bucket.download_file(o, './tmp/{0}{1}'.format(dl_name, extension))
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object %s does not exist.", dl_name)
            logger.exception("ClientError 404:")
            clean_exit(1, 'Expected object is missing on S3.')
        else:
            raise

def raise_(ex):
    '''to raise generic exceptions'''
    raise ex

# TODO: change connection to PyMySQL
def return_query(local_query):
    '''returns the response from a query on mysql'''
    with pymysql.connect(conn_string) as local_conn:
        with local_conn.cursor() as local_curs:
            try:
                local_curs.execute(local_query)
            except pymysql.Error:
                logger.exception("pymysql.Error:")
                report_stats['failed_mysql_queries'] += 1
                clean_exit(1, 'Failed pymysql query attempt.')
            else:
                response = local_curs.fetchone()[0]
                logger.info("returned: %s", response)
                report_stats['good_mysql_queries'] += 1
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

#
# TODO: check to see if this is actually needed for PoC
#
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
    if data['failed_mysql_queries'] or data['failed_unloads'] or data['unstored_objects'] or data['bad_objects']:
        print(f'\n*** ATTN: The microservice ran unsuccessfully. Please investigate logs/{__file__} ***\n') 
    else:
        print(f'\n***The microservice ran successfully***\n')
 
    print(f'Report: {__file__}\n')
    print(f'Config: {config_file}\n')
    print(f'DML: {dml_file}\n')

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
                print(f"{i}: {item}")

    # Print all objects not loaded into s3/client
    if data["unstored_objects"]:
        print(f'Objects not stored to s3 /client: {data["unstored_objects"]}')
        print(f'\nList of objects not stored to S3 /client:')
        if data['unstored_objects_list']:
            for i, item in enumerate(data['unstored_objects_list'], 1):
                print(f"{i}: {item}")

    print(f'\n\nObjects to process: {data["unprocessed_objects"]}')

    # Print all objects loaded into s3/good
    if data["good_objects"]:
        print(f'Objects processed to s3 /good: {data["good_objects"]}')
        print(f'\nList of objects processed to S3 /good:')
        if data['good_objects_list']:
            for i, item in enumerate(data['good_objects_list'], 1):
                print(f"{i}: {item}")

    # Print all objects loaded into s3/bad
    if data["bad_objects"]:
        print(f'Objects processed to s3 /bad: {data["bad_objects"]}')
        print(f'\nList of objects processed to S3 /bad:')
        if data['bad_objects_list']:
            for i, item in enumerate(data['bad_objects_list'], 1):
                print(f"{i}: {item}")

# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'mysql_queries': 0,
    'failed_mysql_queries':0,
    'good_mysql_queries':0,
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


# build the object key
object_key = object_key_builder(object_prefix)

# the _substantive_ query, one that users expect to see as output in S3.
request_query = open('dml/{}'.format(dml_file), 'r').read()

# If escape was enabled, create string needed to be added to UNLOAD command 
# else adds a blank string
if escape:
    escape_string = "ESCAPE"
else:
    escape_string = ""

# If delimiter was enabled, create string needed to be added to UNLOAD command 
# else adds a blank string
if delimiter:
    delimiter_string = "delimiter '{delimiter}'".format(delimiter=delimiter)
else:
    delimiter_string = ""

# If addquotes was enabled, create string needed to be added to UNLOAD command 
# else adds a blank string
if addquotes:
    addquotes_string = "ADDQUOTES"
else:
    addquotes_string = ""
#
# TODO: UNLOAD code needs to be switched to SELECT ... INTO OUTFILE
#
# The UNLOAD query to support S3 loading direct from a Redshift query
# ref: https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html
# This UNLOAD inserts into the S3 BATCH path
log_query = '''
UNLOAD ('{request_query}')
TO 's3://{bucket}/{batch_prefix}/{object_key}_part'
credentials 'aws_access_key_id={aws_access_key_id};\
aws_secret_access_key={aws_secret_access_key}'
{escape_string}
{delimiter_string}
{addquotes_string}
PARALLEL OFF
{header}
'''.format(
    request_query=request_query,
    bucket=bucket,
    batch_prefix=batch_prefix,
    object_key=object_key,
    aws_access_key_id='{aws_access_key_id}',
    aws_secret_access_key='{aws_secret_access_key}',
    escape_string=escape_string,
    delimiter_string=delimiter_string,
    addquotes_string=addquotes_string,
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
    for object_summary in res_bucket.objects.filter(Prefix=f'{batch_prefix}/'):
        key = object_summary.key # aka the batch prefix of the object
        filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')
        
        # replaces the batch part of the key with good/bad
        goodfile = key.replace(f'{archive}/batch/', f'{archive}/good/', 1)
        badfile = key.replace(f'{archive}/batch/', f'{archive}/bad/', 1)

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
#
# TODO: change to PyMySQL connection
#
with pymysql.connect(conn_string) as conn:
    with conn.cursor() as curs:
        try:
            logger.info("executing query")
            curs.execute(query)
            logger.info(log_query)
        except pymysql.Error:
            logger.exception("pymysql.Error:")
            logger.error(('UNLOAD transaction on %s failed.'
                          'Quitting with error code 1'), dml_file)
            report_stats['failed_unloads'] += 1
            report_stats['failed_unloads_list'].append(object_key)
            report(report_stats)
            clean_exit(1,'Failed pymysql query attempt.')
        else:
            #
            # TODO: rework, need to upload instead of download
            #
            # downloads go to a temporary folder: ./tmp
            if not os.path.exists('./tmp'):
                os.makedirs('./tmp')
            for obj in objects_to_process:
                download_object(obj.key)

            logger.info(
                'UNLOAD successful. Object prefix is %s/%s/%s',
                bucket, storage_prefix, object_key)
            report_stats['successful_unloads'] += 1
            report_stats['successful_unloads_list'].append(object_key)

            # optionally add the file extension and transfer to storage folders
            objects = get_unprocessed_objects()
            for object in objects:
                key = object.key # aka the batch prefix of the object
                filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')

                # final paths that include the filenames
                copy_good_prefix = key.replace(f'{archive}/batch/', f'{archive}/good/', 1)
                copy_bad_prefix = key.replace(f'{archive}/batch/', f'{archive}/bad/', 1)
                copy_from_prefix = key 
                
                if 'extension' in config:
                    # if an extension was set in the config, add it to the end of the file
                    extension = config['extension']
                    filename_with_extension = f"{key}{extension}"
                    logger.info('File extension set in %s as "%s"', config_file, extension)
                else:
                    filename_with_extension = key 
                    logger.info('File extension not set in %s', config_file)
                try:
                    # final storage path that includes the filename and optional extension, removes the batch part of the prefix and leaves the client
                    copy_to_prefix = filename_with_extension.replace(f'{archive}/batch/', '', 1)
                    
                    logger.info('Copying to s3 /client ...')
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_to_prefix)
                except ClientError:
                    logger.exception('Exception copying from s3://%s/%s', bucket, copy_from_prefix)
                    logger.exception('to s3://%s/%s', bucket, copy_to_prefix)
                    report_stats['unstored_objects'] += 1
                    report_stats['unstored_objects_list'].append(copy_from_prefix)
                    
                    logger.info('Copying to s3 /bad ...')
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_bad_prefix)
                    logger.info('Copied from s3://%s/%s', bucket, copy_from_prefix)
                    logger.info('Copied to s3://%s/%s', bucket, copy_bad_prefix)
                    report_stats['bad_objects_list'].append(copy_bad_prefix)
                    report_stats['bad_objects'] += 1
                    
                    report(report_stats)
                    clean_exit(1,'Failed boto3 copy_object attempt.')
                else:
                    logger.info('Copied from s3://%s/%s', bucket, copy_from_prefix)
                    logger.info('Copied to s3://%s/%s', bucket, copy_to_prefix)
                    report_stats['stored_objects'] += 1
                    report_stats['stored_objects_list'].append(copy_to_prefix)
                    
                    logger.info('Copying to s3 /good ...')
                    client.copy_object(
                        Bucket=bucket,
                        CopySource='{}/{}'.format(bucket, copy_from_prefix),
                        Key=copy_good_prefix)
                    logger.info('Copied from s3://%s/%s', bucket, copy_from_prefix)
                    logger.info('Copied to s3://%s/%s', bucket, copy_good_prefix)
                    report_stats['good_objects'] += 1
                    report_stats['good_objects_list'].append(copy_good_prefix)

            report(report_stats)
            clean_exit(0,'Finished succesfully.')
