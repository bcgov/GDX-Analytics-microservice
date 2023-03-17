'''Microservice script to load a csv file from s3 and load it into Redshift'''
###################################################################
# Script Name   : s3_to_redshift.py
#
# Description   : Microservice script to load a csv file from s3
#               : and load it into Redshift
#
# Requirements  : You must set the following environment variables
#               : to establish credentials for the microservice user
#
#               : export AWS_ACCESS_KEY_ID=<<KEY>>
#               : export AWS_SECRET_ACCESS_KEY=<<SECRET_KEY>>
#               : export pgpass=<<DB_PASSWD>>
#
#
# Usage         : python s3_to_redshift.py configfile.json
#

import re  # regular expressions
from io import StringIO
import os  # to read environment variables
import os.path  # file handling
import json  # to read json config files
import sys  # to read command line parameters
import logging
import time
from datetime import datetime
from tzlocal import get_localzone
from pytz import timezone
import boto3  # s3 access
from botocore.exceptions import ClientError
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd  # data processing
import pandas.errors
from ua_parser import user_agent_parser
from referer_parser import Referer
from lib.redshift import RedShift
import lib.logs as log

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

# check that configuration file was passed as argument
if len(sys.argv) != 2:
    print('Usage: python s3_to_redshift.py config.json')
    clean_exit(1,'Bad command use.')
configfile = sys.argv[1]
# confirm that the file exists
if os.path.isfile(configfile) is False:
    print("Invalid file name {}".format(configfile))
    clean_exit(1,'Bad file name.')
# open the confifile for reading
with open(configfile) as f:
    data = json.load(f)

# get variables from config file
bucket = data['bucket']
source = data['source']
destination = data['destination']
directory = data['directory']
doc = data['doc']
if 'dbschema' in data:
    dbschema = data['dbschema']
else:
    dbschema = 'microservice'
dbtable = data['dbtable']
table_name = dbtable[dbtable.rfind(".") + 1:]
column_count = data['column_count']
columns = data['columns']
dtype_dic = {}
if 'dtype_dic_strings' in data:
    for fieldname in data['dtype_dic_strings']:
        dtype_dic[fieldname] = str
if 'dtype_dic_bools' in data:
    for fieldname in data['dtype_dic_bools']:
        dtype_dic[fieldname] = bool
if 'dtype_dic_ints' in data:
    for fieldname in data['dtype_dic_ints']:
        dtype_dic[fieldname] = pd.Int64Dtype()
if 'no_header' in data:
    no_header = data['no_header']
else:
    no_header = False
delim = data['delim']
truncate = data['truncate']
if 'drop_columns' in data:
    drop_columns = data['drop_columns']
else:
    drop_columns = {}
if 'add_columns' in data:
    add_columns = data['add_columns']
else:
    add_columns = {}
ldb_sku = False if 'ldb_sku' not in data else data['ldb_sku']
sql_query = False if 'sql_query' not in data else data['sql_query']
file_limit = False if truncate or 'file_limit' not in data else data['file_limit']

if 'strip_quotes' in data:
    strip_quotes = data['strip_quotes']
else:
    strip_quotes = False
if 'encoding' in data:
    encoding = data['encoding']
else:
    encoding = 'utf-8'

# set up S3 connection
client = boto3.client('s3')  # low-level functional API
resource = boto3.resource('s3')  # high-level object-oriented API
my_bucket = resource.Bucket(bucket)  # subsitute this for your s3 bucket name.
bucket_name = my_bucket.name

# Database connection string
conn_string = """
dbname='{dbname}' host='{host}' port='{port}' user='{user}' password={password}
""".format(dbname='snowplow',
           host='redshift.analytics.gov.bc.ca',
           port='5439',
           user=os.environ['pguser'],
           password=os.environ['pgpass'])


def copy_query(this_table, this_batchfile, this_log):
    '''Constructs the database copy query string'''
    if this_log:
        aws_key = 'AWS_ACCESS_KEY_ID'
        aws_secret_key = 'AWS_SECRET_ACCESS_KEY'
    else:
        aws_key = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    cp_query = """
COPY {0}\nFROM 's3://{1}/{2}'\n\
CREDENTIALS 'aws_access_key_id={3};aws_secret_access_key={4}'\n\
IGNOREHEADER AS 1 MAXERROR AS 0 DELIMITER '|' NULL AS '-' ESCAPE;\n
""".format(this_table, bucket_name, this_batchfile, aws_key, aws_secret_key)
    return cp_query


def is_processed(this_object_summary):
    '''Check to see if the file has been processed already'''
    this_key = this_object_summary.key
    # get the filename (after the last '/')
    this_filename = this_key[this_key.rfind('/') + 1:]
    this_goodfile = destination + "/good/" + this_key
    this_badfile = destination + "/bad/" + this_key
    try:
        client.head_object(Bucket=bucket, Key=this_goodfile)
    except ClientError:
        pass  # this object does not exist under the good destination path
    else:
        logger.info('%s was processed as good already.', this_filename)
        return True
    try:
        client.head_object(Bucket=bucket, Key=this_badfile)
    except ClientError:
        pass  # this object does not exist under the bad destination path
    else:
        logger.info('%s was processed as bad already.', this_filename)
        return True
    logger.info('%s has not been processed.', this_filename)
    return False


def report(data):
    '''reports out the data from the main program loop'''
    # if no objects were processed; do not print a report
    if data["objects"] == 0:
        return
    print(f'Report: {__file__}\n')
    print(f'Config: {configfile}\n')
    if data['failed'] or data['bad']:
        print(f'*** ATTN: A failure occurred. Please investigate logs/{__file__} ***\n')    
    # get times from system and convert to Americas/Vancouver for printing
    yvr_dt_end = (yvr_tz
        .normalize(datetime.now(local_tz)
        .astimezone(yvr_tz)))
    print(
        'Microservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.')
    print(f'\nObjects to process: {data["objects"]}')
    print(f'Objects successfully processed: {data["processed"]}')
    print(f'Objects that failed to process: {data["failed"]}')
    print(f'Objects output to \'processed/good\': {data["good"]}')
    print(f'Objects output to \'processed/bad\': {data["bad"]}')
    print(f'Objects loaded to Redshift: {data["loaded"]}')
    print(f'Empty Objects: {data["empty"]}\n')
    if data['good_list']:
        print(
        "\nList of objects successfully fully ingested from S3, processed, "
        "loaded to S3 ('good'), and copied to Redshift:")
        for i, meta in enumerate(data['good_list'], 1):
            print(f"{i}: {meta.key}")
    if data['bad_list']:
        print('\nList of objects that failed to process:')
        for i, meta in enumerate(data['bad_list'], 1):
            print(f"{i}: {meta.key}")
    if data['incomplete_list']:
        print('\nList of objects that were not processed due to early exit:')
        for i, meta in enumerate(data['incomplete_list'], 1):
            print(f"{i}: {meta.key}")
    if data['empty_list']:
        print('\nList of empty objects:')
        for i, meta in enumerate(data['empty_list'], 1):
            print(f"{i}: {meta.key}")


# This bucket scan will find unprocessed objects.
# objects_to_process will contain zero or one objects if truncate = True
# objects_to_process will contain zero or more objects if truncate = False
objects_to_process = []

# function to sort unsorted objects
def sortobjects_last_modified(o):
    return o.last_modified

# get all object references on the configured path, then sort by last_modified
unsorted_objects = my_bucket.objects.filter(Prefix=source +
                                            "/" + directory + "/")
sorted_objects = sorted(unsorted_objects, key=sortobjects_last_modified)

for object_summary in sorted_objects:
    # stop building list of files to process if provided file_limit is reached
    if file_limit and len(objects_to_process) == file_limit:
        logger.info('reached file limit of %s', file_limit)
        break
    key = object_summary.key
    # Ignore files in the "Archive" folder
    if re.search(doc + '$', key) and not (re.search('\/archive',key)):
        # skip to next object if already processed
        if is_processed(object_summary):
            continue
        # under truncate = True, we will keep list length to 1
        # only adding the most recently modified file to objects_to_process
        if truncate:
            if len(objects_to_process) == 0:
                objects_to_process.append(object_summary)
                continue
            # compare last modified dates of the latest and current obj
            if (object_summary.last_modified
                    > objects_to_process[0].last_modified):
                objects_to_process[0] = object_summary
        else:
            # no truncate, so the list may exceed 1 element
            objects_to_process.append(object_summary)

# an object exists to be processed as a truncate copy to the table
if truncate and len(objects_to_process) == 1:
    logger.info(
        'truncate is set. processing only one file: %s (modified %s)',
        objects_to_process[0].key, objects_to_process[0].last_modified)

# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'objects':0,
    'processed':0,
    'failed':0,
    'good': 0,
    'bad': 0,
    'loaded': 0,
    'empty': 0,
    'good_list':[],
    'bad_list':[],
    'empty_list': [],
    'incomplete_list':[]
}

report_stats['objects'] = len(objects_to_process)
report_stats['incomplete_list'] = objects_to_process.copy()

# process the objects that were found during the earlier directory pass
for object_summary in objects_to_process:
    batchfile = destination + "/batch/" + object_summary.key
    goodfile = destination + "/good/" + object_summary.key
    badfile = destination + "/bad/" + object_summary.key

    # get the object from S3 and take its contents as body
    obj = client.get_object(Bucket=bucket, Key=object_summary.key)
    body = obj['Body']

    # Create an object to hold the data while parsing
    csv_string = ''
    
    # The file is an empty upload. Key to badfile and stop processing further.
    if (obj['ContentLength'] == 0):
        logger.info('%s is empty and zero bytes in size, keying to badfile and no further processing.',
                     object_summary.key)
        outfile = badfile
        try:
            client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
        report_stats['failed'] += 1
        report_stats['empty'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['empty_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)

        report(report_stats)
        clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')

    # Read the S3 object body (bytes)
    csv_string = body.read()

    # Check that the file decodes as UTF-8. If it fails move to bad and end
    try:
        csv_string = csv_string.decode(encoding)
    except UnicodeDecodeError as _e:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        e_object = _e.object.splitlines()
        logger.exception(
            ''.join((
                "Decoding {0} failed for file {1}\n"
                .format(encoding, object_summary.key),
                "The input file stopped parsing after line {0}:\n{1}\n"
                .format(len(e_object), e_object[-1]),
                "Keying to badfile and stopping.\n")))
        try:
            client.copy_object(
                Bucket="sp-ca-bc-gov-131565110619-12-microservices",
                CopySource=(
                    "sp-ca-bc-gov-131565110619-12-microservices/"
                    f"{object_summary.key}"
                ),
                Key=badfile)
        except Exception as _e:
            logger.exception("S3 transfer failed. %s", str(_e))
        report(report_stats)
        clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')

    # If strip_quotes is set, remove all double quotes (") from the string
    if strip_quotes:
        csv_string = csv_string.replace('"', "")
        
    # Check for an empty file. If it's empty, accept it as bad
    try:
        if no_header:
            df = pd.read_csv(
                StringIO(csv_string),
                sep=delim,
                index_col=False,
                dtype=dtype_dic,
                usecols=range(column_count),
                header=None)
        else:
            df = pd.read_csv(
                StringIO(csv_string),
                sep=delim,
                index_col=False,
                dtype=dtype_dic,
                usecols=range(column_count))
    except pandas.errors.EmptyDataError as _e:
        logger.exception('exception reading %s', object_summary.key)
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['empty'] += 1       
        report_stats['empty_list'].append(object_summary)
        report_stats['bad_list'].append(object_summary)  
        report_stats['incomplete_list'].remove(object_summary)
        if str(_e) == "No columns to parse from file":
            logger.warning('%s is empty, keying to badfile and stopping.',
                           object_summary.key)
            outfile = badfile
        else:
            logger.warning('%s not empty, keying to badfile and stopping.',
                           object_summary.key)
            outfile = badfile
        try:
            client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
        report(report_stats)
        clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')
    except ValueError:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        logger.exception('ValueError exception reading %s', object_summary.key)
        logger.warning('Keying to badfile and proceeding.')
        outfile = badfile
        try:
            client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
        report(report_stats)
        clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')

    # map the dataframe column names to match the columns from the configuation
    df.columns = columns
    
    # Check for empty file that has zero data rows
    if len(df.index) == 0:
        logger.info('%s contains zero data rows, keying to badfile and no further processing.',
                     object_summary.key)
        outfile = badfile

        try:
            client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
        report_stats['failed'] += 1
        report_stats['empty'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['empty_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
       
        report(report_stats)
        clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')

    # Truncate strings according to config set column string length limits
    if 'column_string_limit' in data:
        for key, value in data['column_string_limit'].items():
            try:
                df[key] = df[key].str.slice(0, value)
            except AttributeError:
                report_stats['failed'] += 1
                report_stats['bad'] += 1
                report_stats['bad_list'].append(object_summary)
                report_stats['incomplete_list'].remove(object_summary) 
                report(report_stats)
                clean_exit(1, f'File {object_summary.key} not configured correctly, '
                          'column number mismatch - no further processing.')

    if 'drop_columns' in data:  # Drop any columns marked for dropping
        df = df.drop(columns=drop_columns)

    # Add columns from the config file into the dataframe
    if 'add_columns' in data:
        for key, value in data['add_columns'].items():
            df[key] = value
    
    # Run replace on some fields to clean the data up
    if 'replace' in data:
        for thisfield in data['replace']:
            df[thisfield['field']].replace(
                thisfield['old'], thisfield['new'])

    # Clean up date fields
    # for each field listed in the dateformat
    # array named "field" apply "format"
    if 'dateformat' in data:
        for thisfield in data['dateformat']:
            df[thisfield['field']] = \
                pd.to_datetime(df[thisfield['field']],
                               format=thisfield['format'])

    # Cast the config-defined dtype_dic_ints columns as Pandas Int64 types
    if 'dtype_dic_ints' in data:
        for thisfield in data['dtype_dic_ints']:
            try:
                df[thisfield] = df[thisfield].astype(pd.Int64Dtype())
            except TypeError:
                logger.exception('column %s cannot be cast as Integer type ',
                                 thisfield)
                report_stats['failed'] += 1
                report_stats['bad'] += 1
                report_stats['bad_list'].append(object_summary)
                report_stats['incomplete_list'].remove(object_summary)
                logger.warning('Keying to badfile and proceeding.')
                outfile = badfile
                try:
                    client.copy_object(
                        Bucket=f"{bucket}",
                        CopySource=f"{bucket}/{object_summary.key}",
                        Key=outfile)
                except ClientError:
                    logger.exception("S3 transfer failed")
                report(report_stats)
                clean_exit(
                    1,f'Bad file {object_summary.key} in objects to process, '
                    f'due to attempt to cast {thisfield} as an Integer type. '
                    'no further processing.')

    # escape valid pipes in object cols
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.replace('|','\|')

    # Put the full data set into a buffer and write it
    # to a "|" delimited file in the batch directory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, header=True, index=False, sep="|")
    resource.Bucket(bucket).put_object(Key=batchfile,
                                       Body=csv_buffer.getvalue())

    # prep database call to pull the batch file into redshift
    query = copy_query(dbtable, batchfile, this_log=False)
    logquery = copy_query(dbtable, batchfile, this_log=True)

    # if truncate is set to true, perform a transaction that will
    # replace the existing table data with the new data in one commit
    # if truncate is not true then the query remains as just the copy command
    if truncate:
        scratch_start = """
BEGIN;
-- Clean up from last run if necessary
DROP TABLE IF EXISTS {0}_scratch;
DROP TABLE IF EXISTS {0}_old;
-- Create scratch table to copy new data into
CREATE TABLE {0}_scratch (LIKE {0});
ALTER TABLE {0}_scratch OWNER TO microservice;
-- Grant access to Looker and to Snowplow pipeline users
GRANT SELECT ON {0}_scratch TO looker;\n
GRANT SELECT ON {0}_scratch TO datamodeling;\n
""".format(dbtable)

        scratch_copy = copy_query(
            dbtable + "_scratch", batchfile, this_log=False)
        scratch_copy_log = copy_query(
            dbtable + "_scratch", batchfile, this_log=True)

        scratch_cleanup = """
-- Replace main table with scratch table, clean up the old table
ALTER TABLE {0} RENAME TO {1}_old;
ALTER TABLE {0}_scratch RENAME TO {1};
DROP TABLE {0}_old;
COMMIT;
""".format(dbtable, table_name)

        query = scratch_start + scratch_copy + scratch_cleanup
        logquery = scratch_start + scratch_copy_log + scratch_cleanup

    # Execute the transaction against Redshift using local lib redshift module
    logger.info(logquery)
    spdb = RedShift.snowplow(batchfile)
    if spdb.query(query):
        outfile = goodfile
        report_stats['loaded'] += 1
    else:
        outfile = badfile

    # if ldb_sku is set to true, then this code is run to populate the microservice.ldb_sku table

    if ldb_sku and outfile == goodfile:
        with open(sql_query) as f:
            ldb_sku_query = f.read()

        with spdb.connection as conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(ldb_sku_query.strip())
                except Exception as err:
                    outfile = badfile
                    logger.error(
                        "Loading LDB data to RedShift failed.")
                    spdb.print_psycopg2_exception(err)
                else:
                    logger.info(
                        "Loaded LDB data to RedShift successfully")
                    outfile = goodfile

    spdb.close_connection()

    # copy the object to the S3 outfile (processed/good/ or processed/bad/)
    try:
        client.copy_object(
            Bucket="sp-ca-bc-gov-131565110619-12-microservices",
            CopySource=(
                "sp-ca-bc-gov-131565110619-12-microservices/"
                f"{object_summary.key}"
            ),
            Key=outfile)
    except ClientError:
        logger.exception("S3 transfer failed")

    if outfile == badfile:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        report(report_stats)
        clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')

    report_stats['processed'] += 1
    report_stats['good'] += 1
    report_stats['good_list'].append(object_summary)
    report_stats['incomplete_list'].remove(object_summary)
    logger.info("finished %s", object_summary.key)

report(report_stats)
clean_exit(0, 'Finished all processing cleanly.')
