"""Uploads objects from S3 to the SFTS system."""
###################################################################
# Script Name   : s3_to_sfts.py
#
# Description   : Uploads previously un-loaded objects from a location on S3
#               : to a location on the SFTS system.
#
# Requirements  : the XFer java client tool must be extracted into a path
#               : available to the executing environment.
#               :
#               : You must set the following environment variables:
#
#               : export sfts_user=<<sfts_service_account_username>>
#               : export sfts_pass=<<sfts_service_account_password>>
#               : export xfer_path=<</path/to/xfer/jar/files/>>
#
# Usage         : python s3_to_sfts.py -c config.d/config.json
#
# XFer          : Download the XFer jar files as "Client Tools Zip" from:
# https://community.ipswitch.com/s/article/Direct-Download-Links-for-Transfer-and-Automation-2018

import os
import sys
import shutil
import re
import logging
import argparse
import json
import time
import datetime
from tzlocal import get_localzone
from pytz import timezone
import subprocess
import boto3
from botocore.exceptions import ClientError
import lib.logs as log

logger = logging.getLogger(__name__)
log.setup()

# Get script start time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
    .normalize(datetime.datetime.now(local_tz)
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

configfile = sys.argv[2]

# Parse the CONFIG file as a json object and load its elements as variables
with open(config) as f:
    config = json.load(f)

config_bucket = config['bucket']

source = config['source']
source_client = config['source_client']
source_directory = config['source_directory']
source_prefix = f'{source}/{source_client}/{source_directory}/'

archive = config['archive']
archive_client = config['archive_client']
archive_directory = config['archive_directory']
good_archive_prefix = f'{archive}/good/{source}/{archive_client}/{archive_directory}'
bad_archive_prefix = f'{archive}/bad/{source}/{archive_client}/{archive_directory}'

object_prefix = config['object_prefix']

sfts_path = config['sfts_path']
if 'extension' in config:
    extension = config['extension']
else:
    extension = ''

# Get required environment variables
sfts_user = os.environ['sfts_user']
sfts_pass = os.environ['sfts_pass']
xfer_path = os.environ['xfer_path']

# set up S3 connection
client = boto3.client('s3')  # low-level functional API
resource = boto3.resource('s3')  # high-level object-oriented API
res_bucket = resource.Bucket(config_bucket)  # resource bucket object
bucket_name = res_bucket.name


def download_object(o):
    '''downloads object to a tmp directoy'''
    dl_name = o.replace(source_prefix, '')
    #dl_name = o[o.rfind('/')+1:] # get the filename (after the last '/')
    try:
        res_bucket.download_file(o, './tmp/{0}{1}'.format(dl_name, extension))
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error("The object %s does not exist.", dl_name)
            logger.exception("ClientError 404:")
            clean_exit(1, 'Expected object is missing on S3.')
        else:
            raise


def is_processed():
    '''Check to see if the file has been processed already'''
    try:
        client.head_object(Bucket=config_bucket, Key=goodfile)
    except ClientError:
        pass  # this object does not exist under the good archive path
    else:
        logger.info("%s was processed as good already.", filename)
        return True
    try:
        client.head_object(Bucket=config_bucket, Key=badfile)
    except ClientError:
        pass  # this object does not exist under the bad archive path
    else:
        logger.info("%s was processed as bad already.", filename)
        return True
    logger.info("%s has not been processed.", filename)
    return False


# Will run at end of script to print out accumulated report_stats
def report(data):
    '''reports out cumulative script events'''
    print(f'Report: {__file__}\n')
    print(f'Config: {configfile}')
    if not data['objects_to_sfts'] or data['objects_not_processed']:
        print(f'*** ATTN: A failure occured ***')
    # Get script end time
    yvr_dt_end = (yvr_tz
        .normalize(datetime.datetime.now(local_tz)
        .astimezone(yvr_tz)))
    print(
    	'\nMicroservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.')
    print(f'\nItems to process: {data["objects"]}')
    print(f'Objects successfully processed to s3: {data["objects_processed"]}')
    print(f'Objects unsuccessfully processed to s3: {data["objects_not_processed"]}')
    print(f'Objects successfully processed to sfts: {len(data["s3_good_list"])}')

    # Print all objects loaded into s3/good
    if data['s3_good_list']:
        print(f'\nObjects loaded to S3 /good:')  
        for i, item in enumerate(data['s3_good_list'], 1):
            print(f"\n{i}: {item}")

    # Print all objects loaded into s3/bad
    if data['s3_bad_list']:
        print(f'\nObjects loaded to S3 /bad:')
        for i, item in enumerate(data['s3_bad_list'], 1):
            print(f"\n{i}: {item}")


# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'objects':0,
    'objects_processed':0,
    'objects_not_processed':0,
    'objects_to_sfts':False,
    'objects_list':[],
    's3_good_list':[], 
    's3_bad_list':[],
    'sfts_good_list':[],
    'sfts_bad_list':[]
}

# This bucket scan will find unprocessed objects matching on the object prefix
# objects_to_process will contain zero or one objects if truncate = True
# objects_to_process will contain zero or more objects if truncate = False
filename_regex = fr'^{object_prefix}'
objects_to_process = []
for object_summary in res_bucket.objects.filter(Prefix=source_prefix):
    key = object_summary.key
    
    # destination key removes the old client folder name and puts the new one in
    # this will have to be mirrored further down the code as I made the same change there
    archive_key = key.replace(f'{source}/{source_client}', f'{source}/{archive_client}', 1)
    #'client/google-mybusiness-sfts_sbc/pmrp_date_range/gdxdsd_3300/pmrp_20201103_20201114_20201126T183404_part000'

    filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')
    goodfile = f"{archive}/good/{archive_key}" # edit good_archive_prefix
    badfile = f"{archive}/bad/{archive_key}"
    # skip to next object if already processed
    if is_processed():
        continue
    if re.search(filename_regex, filename):
        # an s3 object needs to be added to the list as we use an s3 function to download the object later
        objects_to_process.append(object_summary)
        logger.info('added %a for processing', filename)
        report_stats['objects'] += 1
        report_stats['objects_list'].append(object_summary)


if not objects_to_process:
    clean_exit(1, 'Failing due to no files to process')

# downloads go to a temporary folder: ./tmp
if not os.path.exists('./tmp'):
    os.makedirs('./tmp')
for obj in objects_to_process:
    download_object(obj.key)

# Copy to SFTS
# write a file to use with the -s flag for the xfer.sh service
sfts_conf = './tmp/sfst_conf'
sf = open(sfts_conf, 'w')
sf_full_path = os.path.realpath(sf.name)
# switch to the writable directory
sf.write('cd {}\n'.format(sfts_path))
# write all file names downloaded in "A" in the objects_to_process list
for obj in objects_to_process:
    transfer_file = f"./tmp/{obj.key.replace(source_prefix, '')}{extension}"
    #transfer_file = f"./tmp/{obj.key[obj.key.rfind('/')+1:]}{extension}"
    sf.write('put {}\n'.format(transfer_file))
sf.write('quit\n')
sf.close()

logger.info('file for xfer -s call is %s', os.path.realpath(sf.name))
with open(sfts_conf, 'r') as sf:
    logger.info('Contents:\n%s', sf.read())
sf.close()

# as a subprocess pass the credentials and the sfile to run xfer in batch mode
# https://docs.ipswitch.com/MOVEit/Transfer2017Plus/FreelyXfer/MoveITXferManual.html
# TODO: do uploads one at a time and treat them on S3 one-by-one
try:
    xfer_jar = f"{xfer_path}/xfer.jar"
    jna_jar = f"{xfer_path}/jna.jar"
    logger.info(("trying to call subprocess:\nxfer.jar: "
           f"{xfer_jar}\njna.jar : {jna_jar}"))
    output = subprocess.check_output(
        ["java", "-classpath", f"{xfer_jar}:{jna_jar}",
         "xfer",
         f"-user:{sfts_user}",
         f"-password:{sfts_pass}",
         "-quiterror",
         f"-s:{sfts_conf}",
         "filetransfer.gov.bc.ca"])
    xfer_proc = True
    logger.info(output.decode("utf-8"))
except subprocess.CalledProcessError:
    logger.exception('Non-zero exit code calling XFer:')
    xfer_proc = False
else:
    report_stats['objects_to_sfts'] = True

# copy the processed files to their outfile archive path
for obj in objects_to_process:
    key = obj.key
    archive_key = key.replace(f'{client}/{source_client}', f'{client}/{archive_client}', 1)
    # TODO: check SFTS endpoint to determine which files reached SFTS
    # currently it's all based on whether or not the XFER call returned 0 or 1
    # append the file to the good or bad archive path
    if xfer_proc:
        outfile = f"{archive}/good/{archive_key}"
    else:
        outfile = f"{archive}/bad/{archive_key}"
    try:
        client.copy_object(
            Bucket=config_bucket,
            CopySource='{}/{}'.format(config_bucket, obj.key),
            Key=outfile)
    except ClientError:
        logger.exception('Exception copying object %s', obj.key)
        report_stats['s3_bad_list'].append(outfile)
        report_stats['objects_not_processed'] += 1
    else:
        logger.info('copied %s to %s', obj.key, outfile)
        report_stats['objects_processed'] += 1
        report_stats['s3_good_list'].append(outfile)


# Remove the temporary local files used to transfer
try:
    shutil.rmtree('./tmp')
except (os.error, OSError):
    logger.exception('Exception deleting temporary folder:')
    clean_exit(1,'Could not delete tmp folder')
else:
    logger.debug('Successfully deleted temporary folder:')

# run report output
report(report_stats)

if xfer_proc:
    clean_exit(0,'Finished successfully.')
clean_exit(1, 'Finished with a subroutine error.')
