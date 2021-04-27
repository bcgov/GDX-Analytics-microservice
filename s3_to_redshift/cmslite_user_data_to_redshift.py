###################################################################
# Script Name   : cmslite_user_data_to_redshift.py
#
# Description   :
#
# Requirements  : You must set the following environment variables
#               : to establish credentials for the microservice user
#
#               : export AWS_ACCESS_KEY_ID=<<KEY>>
#               : export AWS_SECRET_ACCESS_KEY=<<SECRET_KEY>>
#               : export pgpass=<<DB_PASSWD>>
#
#
# Usage         : python cmslite_user_data_to_redshift.py configfile.json
#

import boto3  # s3 access
from botocore.exceptions import ClientError
import pandas as pd  # data processing
import pandas.errors
import re  # regular expressions
from io import StringIO
import os  # to read environment variables
import json  # to read json config files
import sys  # to read command line parameters
from lib.redshift import RedShift
import os.path  # file handling
import shutil
import logging
from shutil import unpack_archive
from lib.redshift import RedShift
import lib.logs as log
from datetime import datetime
from tzlocal import get_localzone
from pytz import timezone

local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
    .normalize(datetime.now(local_tz)
    .astimezone(yvr_tz)))

# set up logging
logger = logging.getLogger(__name__)
log.setup()
logging.getLogger("RedShift").setLevel(logging.WARNING)

# Handle exit code
def clean_exit(code, message):
    """Exits with a logger message and code"""
    logger.debug('Exiting with code %s : %s', str(code), message)
    sys.exit(code)


# check that configuration file was passed as argument
if (len(sys.argv) != 2):
    print('Usage: python cmslite_user_data_to_redshift.py config.json')
    clean_exit(1, 'Bad command use.')
configfile = sys.argv[1]
# confirm that the file exists
if os.path.isfile(configfile) is False:
    print("Invalid file name {}".format(configfile))
    clean_exit(1, 'Bad file name.')
# open the confifile for reading
with open(configfile) as f:
    data = json.load(f)

bucket = data['bucket']
source = data['source']
destination = data['destination']
directory = data['directory']
prefix = source + "/" + directory + "/"
doc = data['doc']
dbschema = data['schema']
truncate = data['truncate']
delim = data['delim']

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


# Constructs the database copy query string
def copy_query(dbtable, batchfile, log):
    if log:
        aws_key = 'AWS_ACCESS_KEY_ID'
        aws_secret_key = 'AWS_SECRET_ACCESS_KEY'
    else:
        aws_key = os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
    query = """
COPY {0}\nFROM 's3://{1}/{2}'\n\
CREDENTIALS 'aws_access_key_id={3};aws_secret_access_key={4}'\n\
IGNOREHEADER AS 1 MAXERROR AS 0 DELIMITER '|' NULL AS '-' ESCAPE;\n
""".format(dbtable, bucket_name, batchfile, aws_key, aws_secret_key)
    return query


def download_object(file_object):
    '''downloads object to a tmp directory'''
    dl_name = file_object.replace(prefix, '')
    try:
        my_bucket.download_file(file_object, './tmp/{0}'.format(dl_name))
    except ClientError as e:
        logger.exception(f'Download {object_summary.key} from S3 failed')
        report_stats['failed'] += 1
        report(report_stats)
        clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')


# delete tmp directory and all its contents.  
def delete_tmp():
    if os.path.exists('./tmp'):
        shutil.rmtree('./tmp')


# Check to see if the file has been processed already
def is_processed(object_summary):
    key = object_summary.key
    filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')
    goodfile = destination + "/good/" + key
    badfile = destination + "/bad/" + key
    try:
        client.head_object(Bucket=bucket_name, Key=goodfile)
    except ClientError:
        pass  # this object does not exist under the good destination path
    else:
        logger.debug("{0} was processed as good already.".format(filename))
        return True
    try:
        client.head_object(Bucket=bucket_name, Key=badfile)
    except ClientError:
        pass  # this object does not exist under the bad destination path
    else:
        logger.debug("{0} was processed as bad already.".format(filename))
        return True
    logger.debug("{0} has not been processed.".format(filename))
    return False


def report(data):
    '''reports out the data from the main program loop'''
    # if no objects were processed; do not print a report
    if data["objects"] == 0:
        return
    print(f'report {__file__}:')
    print(f'\nObjects to process: {data["objects"]}')
    print(f'Objects successfully processed: {data["processed"]}')
    print(f'Objects that failed to process: {data["failed"]}')
    print(f'Objects output to \'processed/good\': {data["good"]}')
    print(f'Objects output to \'processed/bad\': {data["bad"]}')
    print(f'Tables loaded to Redshift: {data["loaded"]}/4')
    if data['good_list']:
        print(
            "\nList of objects successfully fully ingested from S3, processed, "
            "loaded to S3 ('good'), and copied to Redshift:")
        [print(meta.key) for meta in data['good_list']]
    if data['bad_list']:
        print('\nList of objects that failed to process:')
        [print(meta.key) for meta in data['bad_list']]
    if data['incomplete_list']:
        print('\nList of objects that were not processed due to early exit:')
        [print(meta.key) for meta in data['incomplete_list']]
    if data['tables_loaded']:
        print('\nList of tables that were successfully loaded into Redshift:')
        [print(table) for table in data['tables_loaded']]
    if data['table_loads_failed']:
        print('\nList of tables that failed to load into Redshift:')
        [print(table) for table in data['table_loads_failed']]
    # get times from system and convert to Americas/Vancouver for printing
    yvr_dt_end = (yvr_tz
        .normalize(datetime.now(local_tz)
        .astimezone(yvr_tz)))
    print(
        '\nMicroservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.')


objects_to_process = []

# This bucket scan will find unprocessed objects.
# objects_to_process will contain zero or one objects if truncate = True
# objects_to_process will contain zero or more objects if truncate = False
for object_summary in my_bucket.objects.filter(Prefix=source + "/"
                                            + directory + "/"):
    key = object_summary.key
    # skip to next object if already processed
    if is_processed(object_summary):
        continue
    # only review those matching our configued 'doc' regex pattern
    if re.search(doc + '$', key):
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
    logger.debug(
        'truncate is set. processing only one file: {0} (modified {1})'.format(
            objects_to_process[0].key, objects_to_process[0].last_modified))

# Process the objects that were found during the earlier directory pass.
# Download the tgz file, unpack it to a temp directory in the local working
# directory, process the files, and then shift the data to redshift. Finally,
# delete the temp directory.

if not os.path.exists('./tmp'):
    os.makedirs('./tmp')

# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'objects':0,
    'processed':0,
    'failed':0,
    'good': 0,
    'bad': 0,
    'loaded': 0,
    'good_list':[],
    'bad_list':[],
    'incomplete_list':[],
    'tables_loaded':[],
    'table_loads_failed':[]
}

report_stats['objects'] = len(objects_to_process)
report_stats['incomplete_list'] = objects_to_process.copy()

# process the objects that were found during the earlier directory pass
for object_summary in objects_to_process:

    # Download and unpack to a temporary folder: ./tmp
    download_object(object_summary.key)

    # Get the filename from the full path in the object summary key
    filename = re.search("(cms-analytics-csv)(.)*tgz$",
                         object_summary.key).group()

    # Unpack the object in the tmp directory
    unpack_archive('./tmp/' + filename, './tmp/' + filename.rstrip('.tgz'))


    for file in os.listdir('./tmp/' + filename.rstrip('.tgz')):
        if file.startswith("._"):
            os.remove('./tmp/' + filename.rstrip('.tgz') + '/' + file)

    # process files for upload to batch folder on S3
    for file in os.listdir('./tmp/' + filename.rstrip('.tgz')):
        batchfile = destination + "/batch/client/" + directory + '/' + file
        goodfile = destination + "/good/client/" + directory + '/' + file
        badfile = destination + "/bad/client/" + directory + '/' + file

        # Read config data for this file
        file_config = data['files'][file.split('.')[0]]
        dbtable = data['schema'] + '.' + file_config['dbtable']
        table_name = file_config['dbtable']

        file_obj = open('./tmp/' + filename.rstrip('.tgz') + '/' + file,
                        "r",
                        encoding="utf-8")

        # Read the file and build the parsed version
        try:
            df = pd.read_csv(
                file_obj,
                usecols=range(file_config['column_count']))
        except pandas.errors.EmptyDataError as e:
            logger.exception('exception reading %s', file)
            report_stats['failed'] += 1
            report_stats['bad'] += 1
            report_stats['bad_list'].append(object_summary)
            report_stats['incomplete_list'].remove(object_summary)
            if str(e) == "No columns to parse from file":
                logger.warning('%s is empty, keying to badfile '
                               'and proceeding.',
                               file)
                outfile = badfile
            else:
                logger.warning('%s not empty, keying to badfile '
                               'and proceeding.',
                               file)
                outfile = badfile
            try:
                client.copy_object(Bucket=f"{bucket}",
                                   CopySource=f"{bucket}/{object_summary.key}",
                                   Key=f"{destination}/bad/client/{directory}/{filename}")
            except ClientError:
                logger.exception("S3 transfer failed")
            report(report_stats)
            clean_exit(1, f'Bad file {object_summary.key} in objects to '
                           'process,no further processing.')
        except ValueError:
            report_stats['failed'] += 1
            report_stats['bad'] += 1
            report_stats['bad_list'].append(object_summary)
            report_stats['incomplete_list'].remove(object_summary)
            logger.exception('ValueError exception reading %s',
                             file)
            logger.warning('Keying to badfile and proceeding.')
            outfile = badfile
            try:
                client.copy_object(Bucket=f"{bucket}",
                                   CopySource=f"{bucket}/{object_summary.key}",
                                   Key=outfile)
            except ClientError:
                logger.exception("S3 transfer failed")
            report(report_stats)
            clean_exit(1, f'Bad file {object_summary.key} in objects to '
                           'process,no further processing.')

        # Map the dataframe column names to match the columns
        # from the configuration
        df.columns = file_config['columns']

        # Clean up date fields
        # for each field listed in the dateformat
        # array named "field" apply "format"
        if 'dateformat' in file_config:
            for thisfield in file_config['dateformat']:
                df[thisfield['field']] = \
                    pd.to_datetime(df[thisfield['field']],
                                   format=thisfield['format'])

        # Parse group name from memo field in user activity table
        if file_config['dbtable'] == 'user_activity':
            group_name = df.memo.str.split(' - ').str[1]
            df['group_name'] = group_name
        
        # Put the full data set into a buffer and write it
        # to a "|" delimited file in the batch directory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, header=True, index=False, sep="|")
        resource.Bucket(bucket_name).put_object(Key=batchfile,
                                                Body=csv_buffer.getvalue())
        # prep database call to pull the batch file into redshift
        query = copy_query(dbtable, batchfile, log=False)
        logquery = copy_query(dbtable, batchfile, log=True)

        # if truncate is set to true, perform a transaction that will
        # replace the existing table data with the new data in one commit
        # if truncate is not true then the query remains as just the copy command
        if (truncate):
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
                dbtable + "_scratch", batchfile, log=False)
            scratch_copy_log = copy_query(
                dbtable + "_scratch", batchfile, log=True)

            scratch_cleanup = """
-- Replace main table with scratch table, clean up the old table
ALTER TABLE {0} RENAME TO {1}_old;
ALTER TABLE {0}_scratch RENAME TO {1};
DROP TABLE {0}_old;
COMMIT;
""".format(dbtable, table_name)

            query = scratch_start + scratch_copy + scratch_cleanup
            logquery = scratch_start + scratch_copy_log + scratch_cleanup

        # Execute the transaction against Redshift using local lib
        # redshift module
        logger.debug(logquery)
        spdb = RedShift.snowplow(batchfile)
        if spdb.query(query):
            outfile = destination + "/good/" + object_summary.key
            report_stats['loaded'] += 1
            report_stats['tables_loaded'].append(dbtable)
        else:
            outfile = destination + "/bad/" + object_summary.key
            report_stats['failed'] += 1
            report_stats['bad'] += 1
            report_stats['bad_list'].append(object_summary)
            report_stats['incomplete_list'].remove(object_summary)
            report_stats['table_loads_failed'].append(dbtable)
            try:
                client.copy_object(Bucket=f"{bucket}",
                                   CopySource=f"{bucket}/{object_summary.key}",
                                   Key=f"{destination}/bad/client/{directory}/{filename}")
            except ClientError:
                logger.exception("S3 copy to processed folder failed")
            report(report_stats)
            clean_exit(1, f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')
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
        logger.exception("S3 copy to processed folder failed")

    if outfile == destination + "/bad/" + object_summary.key:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        report(report_stats)
        clean_exit(1, f'Bad file {object_summary.key} in objects to process, '
                   'no further processing.')

    report_stats['good'] += 1
    report_stats['processed'] += 1
    report_stats['good_list'].append(object_summary)
    report_stats['incomplete_list'].remove(object_summary)
    logger.debug("finished %s", object_summary.key)

# Delete tmp files and dir once microservice has finished processing
delete_tmp()

report(report_stats)
clean_exit(0, 'Finished all processing cleanly.')
