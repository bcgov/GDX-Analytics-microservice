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
ldb_sku = False if 'ldb_sku' not in data else data['ldb_sku']
file_limit = False if truncate or 'file_limit' not in data else data['file_limit']


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
        csv_string = csv_string.decode('utf-8')
    except UnicodeDecodeError as _e:
        report_stats['failed'] += 1
        report_stats['bad'] += 1
        report_stats['bad_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)
        e_object = _e.object.splitlines()
        logger.exception(
            ''.join((
                "Decoding UTF-8 failed for file {0}\n"
                .format(object_summary.key),
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

    # escape pipe symbol in limesurvey surveyls_title column
    if doc == "limesurvey-analytics.*":
        logger.info('Escaping pipe in limesurvey surveyls_title column')
        df['surveyls_title'] = df['surveyls_title'].str.replace('|','\|')
        logger.info('Finished excaping pipe in limesurvey surveyls_title column')

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
            df[key] = df[key].str.slice(0, value)

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

    # Logic for specific case handling for LDB files
    if ldb_sku and outfile == goodfile:
        # There are 2 tables: microservice.ldb_sku and microservice.ldb_sku_csv.
        # microservice.ldb_sku_csv is loaded with new data from latest CSV processed (in the steps above).
        # microservice.ldb_sku is the table for Looker read and has additional date_added and date_removed columns.
        # The logic to load ldb_sku is based on it's differences from ldb_sku_csv.
        #
        # ldb_sku is updated according to ldb_sku_csv. In a transaction:
        #  1 - SKU in ldb_sku and not in ldb_sku_csv: date_removed = today,
        #  2 - SKU match: update content from ldb_sku_csv (accounts for changes in info),
        #  3 - SKU in ldb_sku_csv and not in ldb_sku: append to ldb_sku and date_added = today.
        #
        # on failure: outfile = badfile, report(report_stats), and clean_exit(1).
        # on success, end loop.

        ldb_query = """

BEGIN;

-- When a SKU is not in ldb_sku_csv, if it's in the prod table set a date_removed as today
-- End result: same number of rows in ldb_sku. Only changes 'date_removed' column if the csv didn't have that sku.

UPDATE microservice.ldb_sku
    SET date_removed = CURRENT_DATE
WHERE
    sku NOT IN ( SELECT sku FROM microservice.ldb_sku_csv ) AND
    date_removed IS NULL;

-- when SKUs match between the two tables, update PROD to match new data from CSV
-- End result: no new rows, only updates the data fields on every matching SKU row, as copied from the csv.
--             if the sku found in the csv had previously been assigned a date_removed, that will revert to NULL.
--             the sku, and date_added fields are not affected, but every other field may be.

UPDATE microservice.ldb_sku SET
    product_name = ldb_sku_csv.product_name,
    image = ldb_sku_csv.image,
    body = ldb_sku_csv.body,
    volume = ldb_sku_csv.volume,
    bottles_per_pack = ldb_sku_csv.bottles_per_pack,
    regular_price = ldb_sku_csv.regular_price,
    lto_price = ldb_sku_csv.lto_price,
    lto_start = ldb_sku_csv.lto_start,
    lto_end = ldb_sku_csv.lto_end,
    price_override = ldb_sku_csv.price_override,
    store_count = ldb_sku_csv.store_count,
    inventory = ldb_sku_csv.inventory,
    availability_override = ldb_sku_csv.availability_override,
    whitelist = ldb_sku_csv.whitelist,
    blacklist = ldb_sku_csv.blacklist,
    upc = ldb_sku_csv.upc,
    all_upcs = ldb_sku_csv.all_upcs,
    alcohol = ldb_sku_csv.alcohol,
    kosher = ldb_sku_csv.kosher,
    organic = ldb_sku_csv.organic,
    sweetness = ldb_sku_csv.sweetness,
    vqa = ldb_sku_csv.vqa,
    craft_beer = ldb_sku_csv.craft_beer,
    bcl_select = ldb_sku_csv.bcl_select,
    new_flag = ldb_sku_csv.new_flag,
    rating = ldb_sku_csv.rating,
    votes = ldb_sku_csv.votes,
    product_type = ldb_sku_csv.product_type,
    category = ldb_sku_csv.category,
    sub_category = ldb_sku_csv.sub_category,
    country = ldb_sku_csv.country,
    region = ldb_sku_csv.region,
    sub_region = ldb_sku_csv.sub_region,
    grape_variety = ldb_sku_csv.grape_variety,
    restriction_code = ldb_sku_csv.restriction_code,
    status_code = ldb_sku_csv.status_code,
    inventory_code = ldb_sku_csv.inventory_code,
    date_removed = NULL
FROM microservice.ldb_sku_csv
WHERE
    ldb_sku.sku = ldb_sku_csv.sku;

-- When there is a new SKU add it into the prod table date_added = today
-- End result: new rows are inserted to the ldb_sku table if the csv had new SKUs.

INSERT INTO microservice.ldb_sku (
SELECT
    ldb_sku_csv.sku,
    ldb_sku_csv.product_name,
    ldb_sku_csv.image,
    ldb_sku_csv.body,
    ldb_sku_csv.volume,
    ldb_sku_csv.bottles_per_pack,
    ldb_sku_csv.regular_price,
    ldb_sku_csv.lto_price,
    ldb_sku_csv.lto_start,
    ldb_sku_csv.lto_end,
    ldb_sku_csv.price_override,
    ldb_sku_csv.store_count,
    ldb_sku_csv.inventory,
    ldb_sku_csv.availability_override,
    ldb_sku_csv.whitelist,
    ldb_sku_csv.blacklist,
    ldb_sku_csv.upc,
    ldb_sku_csv.all_upcs,
    ldb_sku_csv.alcohol,
    ldb_sku_csv.kosher,
    ldb_sku_csv.organic,
    ldb_sku_csv.sweetness,
    ldb_sku_csv.vqa,
    ldb_sku_csv.craft_beer,
    ldb_sku_csv.bcl_select,
    ldb_sku_csv.new_flag,
    ldb_sku_csv.rating,
    ldb_sku_csv.votes,
    ldb_sku_csv.product_type,
    ldb_sku_csv.category,
    ldb_sku_csv.sub_category,
    ldb_sku_csv.country,
    ldb_sku_csv.region,
    ldb_sku_csv.sub_region,
    ldb_sku_csv.grape_variety,
    ldb_sku_csv.restriction_code,
    ldb_sku_csv.status_code,
    ldb_sku_csv.inventory_code,
    CURRENT_DATE AS date_added,
    NULL AS date_removed
    
FROM microservice.ldb_sku_csv
WHERE ldb_sku_csv.sku NOT IN (
    SELECT sku FROM microservice.ldb_sku
));

COMMIT;
"""

        with spdb.connection as conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(ldb_query)
                except Exception as err:
                    outfile = badfile
                    logger.error(
                        "Loading LDB SKU to RedShift failed.")
                    spdb.print_psycopg2_exception(err)
                else:
                    logger.info(
                        "Loaded LDB SKU to RedShift successfully")
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

    report_stats['good'] += 1
    report_stats['good_list'].append(object_summary)
    report_stats['incomplete_list'].remove(object_summary)
    logger.info("finished %s", object_summary.key)

report(report_stats)
clean_exit(0, 'Finished all processing cleanly.')
