###################################################################
# Script Name   : asset_data_to_redshift.py
#
# Description   : Microservice script to load a apache access log
#               : files from s3 and load it into Redshift
#
# Requirements  : You must set the following environment variables
#               : to establish credentials for the microservice user
#
#               : export AWS_ACCESS_KEY_ID=<<KEY>>
#               : export AWS_SECRET_ACCESS_KEY=<<SECRET_KEY>>
#               : export pgpass=<<DB_PASSWD>>
#
#
# Usage         : python asset_data_to_redshift.py configfile.json
#

# Exit codes
EX_OK = 0          # successful termination
EX_USAGE = 64      # command line usage error
EX_DATAERR = 65    # data format error
EX_NOINPUT = 66    # cannot open input
EX_SOFTWARE = 70   # internal software error
EX_OSERR = 71      # system error (e.g., can't fork)
EX_IOERR = 74      # input/output error
EX_NOPERM = 77     # permission denied
EX_CONFIG = 78     # configuration error


import re  # regular expressions
from io import StringIO
import os  # to read environment variables
import json  # to read json config files
import sys  # to read command line parameters
import os.path  # file handling
import logging
import lib.logs as log
from datetime import datetime
from tzlocal import get_localzone
from pytz import timezone
import boto3  # s3 access
from botocore.exceptions import ClientError
import pandas as pd  # data processing
import pandas.errors
from lib.redshift import RedShift

from ua_parser import user_agent_parser
# ua_parser documentation: https://github.com/ua-parser/uap-python

from referer_parser import Referer
# referer_parser documentation:
# https://github.com/snowplow-referer-parser/referer-parser

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
if (len(sys.argv) != 2):
    print('Usage: python asset_data_to_redshift.py config.json')
    clean_exit(EX_USAGE, 'Bad command use.')# Exit with EX_USAGE for command line error
configfile = sys.argv[1]
# confirm that the file exists
if os.path.isfile(configfile) is False:
    print("Invalid file name {}".format(configfile))
    clean_exit(EX_NOINPUT, 'Bad file name.')  # Exit with EX_NOINPUT for missing input file
# open the confifile for reading
try:
    with open(configfile) as f:
        data = json.load(f)
except json.JSONDecodeError:
    print("Configuration file has an invalid format.")
    clean_exit(EX_SOFTWARE, 'Configuration file format error')  # Exit with EX_SOFTWARE for config issue


if 'empty_files_ok' in data:
    empty_files_ok = data['empty_files_ok']
else:
    empty_files_ok = False

# get variables from config file
if 'bucket' not in data:
    clean_exit(EX_CONFIG, "Missing required 'bucket' key in config.")
else:
    bucket = data['bucket']

if 'source' not in data:
    clean_exit(EX_CONFIG, "Missing required 'source' key in config.")
else:
    source = data['source']

if 'destination' not in data:
    clean_exit(EX_CONFIG, "Missing required 'destination' key in config.")
else:
    destination = data['destination']

directory = data['directory']
doc = data['doc']
if 'dbschema' in data:
    dbschema = data['dbschema']
else:
    dbschema = 'microservice'
dbtable = data['dbtable']
table_name = dbtable[dbtable.rfind(".")+1:]
column_count = data['column_count']
columns = data['columns']
dtype_dic = {}
if 'dtype_dic_strings' in data:
    for fieldname in data['dtype_dic_strings']:
        dtype_dic[fieldname] = str
if 'dtype_dic_bools' in data:
    for fieldname in data['dtype_dic_bools']:
        dtype_dic[fieldname] = bool
delim = data['delim']

if 'truncate' in data:
    truncate = data['truncate']
else:
    truncate = False

if 'drop_columns' in data:
    drop_columns = data['drop_columns']
else:
    drop_columns = {}
truncate_intermediate_table = 'TRUNCATE TABLE ' + dbtable + ';'
# set up S3 connection
client = boto3.client('s3')  # low-level functional API
resource = boto3.resource('s3')  # high-level object-oriented API
my_bucket = resource.Bucket(bucket)  # subsitute this for your s3 bucket name.
bucket_name = my_bucket.name


# Constructs the database copy query string
def copy_query(dbtable, batchfile, log):
    try:
        aws_key = 'AWS_ACCESS_KEY_ID' if log else os.environ['AWS_ACCESS_KEY_ID']
        aws_secret_key = 'AWS_SECRET_ACCESS_KEY' if log else os.environ['AWS_SECRET_ACCESS_KEY']
    except KeyError as e:
        clean_exit(EX_CONFIG, f"Missing AWS environment variable: {e}")
    query = """
COPY {0}\nFROM 's3://{1}/{2}'\n\
CREDENTIALS 'aws_access_key_id={3};aws_secret_access_key={4}'\n\
IGNOREHEADER AS 1 MAXERROR AS 0 DELIMITER '|' NULL AS '-' ESCAPE;\n
""".format(dbtable, bucket_name, batchfile, aws_key, aws_secret_key)
    return query


# Check to see if the file has been processed already
def is_processed(object_summary):
    key = object_summary.key
    filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')
    goodfile = destination + "/good/" + key
    badfile = destination + "/bad/" + key
    try:
        client.head_object(Bucket=bucket, Key=goodfile)
    except ClientError:
        pass  # this object does not exist under the good destination path
    else:
        logger.info("{0} was processed as good already.".format(filename))
        return True
    try:
        client.head_object(Bucket=bucket, Key=badfile)
    except ClientError:
        pass  # this object does not exist under the bad destination path
    else:
        logger.info("{0} was processed as bad already.".format(filename))
        return True
    logger.info("{0} has not been processed.".format(filename))
    return False


#delete the file from processed folder in s3
def cleanup(object_keys, path):
   for key in object_keys: 
        #store the filename that was processed       
        filename = f'{destination}/{path}/{key}'
        #deletes the processed file
        try:
            client.delete_object(Bucket=bucket, Key=filename)
        except ClientError as e:
            clean_exit(EX_IOERR, f"Failed to delete object {filename} from S3: {e}")


def report(data):
    '''reports out the data from the main program loop'''
    # if no objects were processed; do not print a report
    if data["objects"] == 0:
        clean_exit(EX_NOINPUT, "No objects to process.")
    print(f'Report: {__file__}\n')
    print(f'Config: {configfile}\n')
    # get times from system and convert to Americas/Vancouver for printing
    yvr_dt_end = (yvr_tz
                  .normalize(datetime.now(local_tz)
                             .astimezone(yvr_tz)))
    print(
        'Microservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.\n')
    print(f'Objects to process: {data["objects"]}')
    print(f'Objects successfully processed: {data["processed"]}')
    print(f'Objects that failed to process: {data["failed"]}')
    print(f'Objects output to \'processed/good\': {data["good"]}')
    print(f'Objects output to \'processed/bad\': {data["bad"]}')
    print(f'Objects loaded to Redshift: {data["loaded"]}')
    print(f'Empty Objects: {data["empty"]}\n')

    if data['good_list']:
        print(
        "List of objects successfully fully ingested from S3, processed, "
        "loaded to S3 ('good'), and copied to Redshift:")
        for i, meta in enumerate(data['good_list'], 1):
            print(f"{i}: {meta.key}")
    if data['bad_list']:
        print('\nList of objects that failed to process:')
        for i, meta in enumerate(data['bad_list']):
            print(f"{i}: {meta.key}")
    if data['incomplete_list']:
        print('\nList of objects that were not processed due to early exit:')
        for i, meta in enumerate(data['incomplete_list']):
            print(f"{i}: {meta.key}")
        clean_exit(EX_DATAERR, "Some objects were not processed due to early exit.")
    if data['empty_list']:
        print('\nList of empty objects:')
        for i, meta in enumerate(data['empty_list']):
            print(f"{i}: {meta.key}")
        clean_exit(EX_DATAERR, "Some objects were empty.")


# This bucket scan will find unprocessed objects.
# objects_to_process will contain zero or one objects if truncate = True
# objects_to_process will contain zero or more objects if truncate = False
objects_to_process = []
for object_summary in my_bucket.objects.filter(Prefix=source + "/"
                                               + directory + "/"):
    key = object_summary.key
    # skip to next object if already processed
    if is_processed(object_summary):
        continue
    else:
        # only review those matching our configued 'doc' regex pattern
        if re.search(doc + '$', key):
            # under truncate = True, we will keep list length to 1
            # only adding the most recently modified file to objects_to_process
            if truncate:
                if len(objects_to_process) == 0:
                    objects_to_process.append(object_summary)
                    continue
                else:
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
        'truncate is set. processing only one file: {0} (modified {1})'.format(
            objects_to_process[0].key, objects_to_process[0].last_modified))

# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'objects': 0,
    'processed': 0,
    'failed': 0,
    'good': 0,
    'bad': 0,
    'loaded': 0,
    'empty': 0,
    'good_list': [],
    'bad_list': [],
    'empty_list': [],
    'incomplete_list': []
}

report_stats['objects'] = len(objects_to_process)
report_stats['incomplete_list'] = objects_to_process.copy()
# list to keep track of objects
good_objects = []
path = ''


# clean up the intermediate table
bad_table_cleanup = r'''
BEGIN;
-- clean up the intermediate table with bad data
{truncate_intermediate_table}
COMMIT;
'''.format(truncate_intermediate_table=truncate_intermediate_table)

# process the objects that were found during the earlier directory pass
for object_summary in objects_to_process:
    batchfile = destination + "/batch/" + object_summary.key
    goodfile = destination + "/good/" + object_summary.key
    badfile = destination + "/bad/" + object_summary.key
    
    spdb = RedShift.snowplow(batchfile) 

    # get the object from S3 and take its contents as body
    obj = client.get_object(Bucket=bucket, Key=object_summary.key)

    # The file is an empty upload. Key to badfile and stop processing further.
    if ((obj['ContentLength'] == 0) and (not empty_files_ok)):
        logger.info('%s is empty, keying to badfile and no further processing.',
                     object_summary.key)
        outfile = badfile
        #if there are any files in processed/good folder that were processed
        #before this bad file was hit, then delete it
        try:
            if good_objects:
                cleanup(good_objects, path)
                report_stats['good'] = 0
                report_stats['loaded'] = 0
                report_stats['good_list'] = 0
        except ClientError:
            logger.exception("S3 delete failed")
            clean_exit(EX_SOFTWARE, "Failed to delete good objects due to ClientError.")
        
        try:
            client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
        report_stats['empty'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['empty_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)

        report(report_stats)
        #clean up the intermediate table if bad file is hit
        spdb.query(bad_table_cleanup)
        spdb.close_connection()
        clean_exit(EX_DATAERR, f'Bad file {object_summary.key} in objects to process, no further processing.')
    elif((obj['ContentLength'] == 0) and (empty_files_ok)):
        logger.info('%s is empty, but empty files are set to be ok.',
                     object_summary.key)
        report_stats['empty'] += 1
        report_stats['empty_list'].append(object_summary)

    body = obj['Body']

    # Create an object to hold the data while parsing
    csv_string = ''

    # Perform apache access log parsing according to config, if defined
    if 'access_log_parse' in data:
        linefeed = ''
        parsed_list = []
        if(data['access_log_parse']['string_repl']):
            inline_pattern = data['access_log_parse']['string_repl']['pattern']
            inline_replace = data['access_log_parse']['string_repl']['replace']
        body_stringified = body.read().decode('utf-8')
        # perform regex replacements by line
        for line in body_stringified.splitlines():
            # Replace pipe char with encoded version, %7C
            if(data['access_log_parse']['string_repl']):
                line = line.replace(inline_pattern, inline_replace)
            # The config contains regex's that correspond to the
            # number of columns in the log entry.
            # This is necessary because some log entries do not
            # have the tenth column for server response time in ms.
            # Check if there are 9 or 10 columns in access log entry by
            # attempting to apply these regex's until finding one that parses.
            for exp in data['access_log_parse']['regexs']:
                parsed_line, num_subs = re.subn(
                    exp['pattern'], exp['replace'], line)
                # If a match for the replacement pattern is found,
                # construct the parsed line
                if num_subs:
                    # Extract user_agent and referrer_url from log entry.
                    # The field names referenced here are only for
                    # use with the third party libraries. The field
                    # names for the table are set in the config.
                    user_agent = re.match(exp['pattern'], line).group(9)
                    referrer_url = re.match(exp['pattern'], line).group(8)

                    # Parse user_agent and referrer strings into lists
                    parsed_ua = user_agent_parser.Parse(user_agent)
                    parsed_referrer_url = Referer(referrer_url,
                                                  data['asset_scheme_and_authority'])

                    # Add OS family and version to user agent string
                    ua_string = '|' + parsed_ua['os']['family']
                    if parsed_ua['os']['major'] is not None:
                        ua_string += '|' + parsed_ua['os']['major']
                        if parsed_ua['os']['minor'] is not None:
                            ua_string += '.' + parsed_ua['os']['minor']
                        if parsed_ua['os']['patch'] is not None:
                            ua_string += '.' + parsed_ua['os']['patch']
                    else:
                        ua_string += '|'

                    # Add Browser family and version to user agent string
                    ua_string += '|' + parsed_ua['user_agent']['family']
                    if parsed_ua['user_agent']['major'] is not None:
                        ua_string += '|' + parsed_ua['user_agent']['major']
                    else:
                        ua_string += '|' + 'NULL'
                    if parsed_ua['user_agent']['minor'] is not None:
                        ua_string += '.' + parsed_ua['user_agent']['minor']
                    if parsed_ua['user_agent']['patch'] is not None:
                        ua_string += '.' + parsed_ua['user_agent']['patch']

                    # Add referrer term and medium to referrer string
                    referrer_string = ''
                    if parsed_referrer_url.referer is not None:
                        referrer_string += '|' + parsed_referrer_url.referer
                    else:
                        referrer_string += '|'
                    if parsed_referrer_url.medium is not None:
                        referrer_string += '|' + parsed_referrer_url.medium
                    else:
                        referrer_string += '|'

                    # Determine the end of line char:
                    # Use linefeed if defined in config, or default "/r/n"
                    if(data['access_log_parse']['linefeed']):
                        linefeed = data['access_log_parse']['linefeed']
                    else:
                        linefeed = '\r\n'

                    # Form the now parsed log entry line
                    parsed_line += ua_string + referrer_string

                    # Add the parsed log entry line to the list
                    parsed_list.append(parsed_line)

                    # Break after first match
                    break

        # Concatenate all the parsed lines together with the end of line char
        csv_string = linefeed.join(parsed_list)
        logger.info(object_summary.key + " parsed successfully")

    # This is not an apache access log
    if 'access_log_parse' not in data:
        csv_string = body.read()

    # Check that the file decodes as UTF-8. If it fails move to bad and end
    if not isinstance(csv_string, str):
        try:
            csv_string = csv_string.decode('utf-8')
        except UnicodeDecodeError as e:
            report_stats['failed'] += 1
            report_stats['bad'] += 1
            report_stats['bad_list'].append(object_summary)
            report_stats['incomplete_list'].remove(object_summary)
            e_object = e.object.splitlines()
            logger.exception(
                ''.join((
                    "Decoding UTF-8 failed for file {0}\n"
                    .format(object_summary.key),
                    "The input file stopped parsing after line {0}:\n{1}\n"
                    .format(len(e_object), e_object[-1]),
                    "Keying to badfile and skipping.\n")))
            #if there are any files in processed/good folder that were processed
            #before this bad file was hit, then delete it
            try:
                if good_objects:
                    cleanup(good_objects, path)
                    report_stats['good'] = 0
                    report_stats['loaded'] = 0
                    report_stats['good_list'] = 0
                    
            except ClientError:
                logger.exception("S3 delete failed")
                clean_exit(EX_SOFTWARE, "Failed to delete good objects due to ClientError.")

            try:
                client.copy_object(
                    Bucket="sp-ca-bc-gov-131565110619-12-microservices",
                    CopySource="sp-ca-bc-gov-131565110619-12-microservices/"
                    + object_summary.key, Key=badfile)
            except Exception as e:
                logger.exception("S3 transfer failed.\n{0}".format(e.message))
                clean_exit(EX_IOERR, "Failed to copy object to bad file.")
            report(report_stats)
            #clean up the intermediate table if bad file is hit
            spdb.query(bad_table_cleanup)
            spdb.close_connection()
            clean_exit(EX_DATAERR, f'Bad file {object_summary.key} in objects to process, no further processing.')
    else:
        report_stats['processed'] += 1

    # Check for an empty file. If it's empty, accept it as bad and skip
    # to the next object to process
    try:
        df = pd.read_csv(
            StringIO(csv_string),
            sep=delim,
            index_col=False,
            dtype=dtype_dic,
            usecols=range(column_count),
            names=columns)
    except pandas.errors.EmptyDataError as _e:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        logger.exception('exception reading {0}'.format(object_summary.key))
        if (str(_e) == "No columns to parse from file"):
            logger.warning('File is empty, keying to badfile \
                           and proceeding.')
            outfile = badfile
        else:
            logger.warning('File not empty, keying to badfile \
                           and proceeding.')
            outfile = badfile

        #if there are any files in processed/good folder that were processed
        #before this bad file was hit, then delete it
        try:
            if good_objects:
                cleanup(good_objects, path)
                report_stats['good'] = 0
                report_stats['loaded'] = 0
                report_stats['good_list'] = 0
        except ClientError:
            logger.exception("S3 delete failed")
            clean_exit(EX_SOFTWARE, "Failed to delete good objects due to ClientError.")

        try:
            client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
            clean_exit(EX_IOERR, "Failed to copy object")
        report(report_stats)
        #clean up the intermediate table if bad file is hit
        spdb.query(bad_table_cleanup)
        spdb.close_connection()
        clean_exit(EX_DATAERR, f'Bad file {object_summary.key} in objects to process, no further processing.')
    except ValueError:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        logger.exception('ValueError exception reading %s', object_summary.key)
        logger.warning('Keying to badfile and proceeding.')
        outfile = badfile
        #if there are any files in processed/good folder that were processed
        #before this bad file was hit, then delete it
        try:
            if good_objects:
                cleanup(good_objects, path)
                report_stats['good'] = 0
                report_stats['loaded'] = 0
                report_stats['good_list'] = 0
        except ClientError:
            logger.exception("S3 delete failed")
            clean_exit(EX_SOFTWARE, "Failed to delete good objects due to ClientError.")

        try:
            client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
            clean_exit(EX_IOERR, "Failed to copy object")
        report(report_stats)
        #clean up the intermediate table if bad file is hit
        spdb.query(bad_table_cleanup)
        spdb.close_connection()
        clean_exit(EX_DATAERR, f'Bad file {object_summary.key} in objects to process, no further processing.')

    # Truncate strings according to config set column string length limits
    if 'column_string_limit' in data:
        for key, value in data['column_string_limit'].items():
            try:
                df[key] = df[key].str.slice(0, value)
                logger.info(f'Truncated {key} column to {value} characters')
            except AttributeError:
                logger.debug(f'Could not enforce string limit on {df[key]} in {object_summary.key}.')

    if 'drop_columns' in data:  # Drop any columns marked for dropping
        df = df.drop(columns=drop_columns)

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

    # Put the full data set into a buffer and write it
    # to a "|" delimited file in the batch directory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, header=True, index=False, sep="|")
    resource.Bucket(bucket).put_object(Key=batchfile,
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



    # Execute the transaction against Redshift using the psycopg2 library
    logger.info(logquery)
    
    if spdb.query(query):
        outfile = goodfile
        report_stats['loaded'] += 1
    else:
        outfile = badfile


        
    # copy the object to the S3 outfile (processed/good/ or processed/bad/)
    try:
        client.copy_object(
            Bucket="sp-ca-bc-gov-131565110619-12-microservices",
            CopySource="sp-ca-bc-gov-131565110619-12-microservices/"
            + object_summary.key, Key=outfile)
    except ClientError:
        logger.exception("S3 transfer failed")
        clean_exit(EX_IOERR, "Failed to copy object")
    else:
        if outfile == goodfile:
            path = 'good'
            good_objects.append(object_summary.key)
    if outfile == badfile:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        report(report_stats)
        #if there are any files in processed/good folder that were processed
        #before this bad file was hit, then delete it
        try:
            if good_objects:
                cleanup(good_objects, path)
                report_stats['good'] = 0
                report_stats['loaded'] = 0
                report_stats['good_list'] = 0
        except ClientError:
            logger.exception("S3 delete failed")
            clean_exit(EX_SOFTWARE, "Failed to delete good objects due to ClientError.")
        #clean up the intermediate table if bad file is hit
        spdb.query(bad_table_cleanup)
        spdb.close_connection()
        clean_exit(EX_DATAERR, f'Bad file {object_summary.key} in objects to process, no further processing.')
    report_stats['good'] += 1
    report_stats['good_list'].append(object_summary)
    report_stats['incomplete_list'].remove(object_summary)
    logger.info("finished %s", object_summary.key)
    logger.info("finished %s", object_summary.key)



report(report_stats)
#this is to close spdb connection,if there are any objects were processed
#this connection is never opened if no ojects were processed.
if len(objects_to_process) >=1 :
    spdb.close_connection()
clean_exit(EX_OK, 'Finished all processing cleanly.')
