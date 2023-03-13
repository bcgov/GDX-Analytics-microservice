"""Google My Business API Loader Script"""
###################################################################
# Script Name   : google_mybusiness.py
#
#
# Description   : A script to access the Google Locations/My Business
#               : api, download analytcs info and dump to a CSV in S3
#               : The resulting file is loaded to Redshift and then
#               : available to Looker through the project google_api.
#
# Requirements  : Install python libraries: httplib2, oauth2client
#               : google-api-python-client
#               :
#               : You will need API credensials set up. If you don't have
#               : a project yet, follow these instructions. Otherwise,
#               : place your credentials_mybusiness.json file in the location defined
#               : below.
#               :
#               : ------------------
#               : To set up the Google end of things, following this:
#   : https://developers.google.com/api-client-library/python/start/get_started
#               : the instructions are:
#               :
#               :
#               : Set up a Google account linked to an IDIR service account
#               : Create a new project at
#               : https://console.developers.google.com/projectcreate
#               :
#               : Under 'APIs & Services' Click 'Credentials':
#               :   Click on 'Create Credentials' at the top of the screen
#               :   to select that you want to create an 'OAuth client id'. 
#               :   You will have to configure a consent screen.
#               :   You must provide an Application name, and
#               :   under 'Scopes for Google APIs' add the scopes:
#               :   '../auth/business.manage'.
#               :
#               :   After you save, you will have to pick an application type.
#               :   Choose Other, and provide a name for this OAuth client ID.
#               :
#               :   Download the JSON file and place it in your working
#               :   directory as 'credentials_mybusiness.json'
#               :
#               :   When you first run it, it will ask you do do an OAUTH
#               :   validation, which will create a file
#               :   'credentials_mybusiness.dat', saving that auhtorization.
#               :
# Usage         : e.g.:
#               : $ python google_mybusiness.py -o credentials_mybusiness.json\
#               :  -a credentials_mybusiness.dat -c config_mybusiness.json
#               :
#               : the flags specified in the usage example above are:
#               : -o <OAuth Credentials JSON file>
#               : -a <Stored authorization dat file>
#               : -c <Microservice configuration file>

import os
import sys
import json
import boto3
import logging
import psycopg2
import argparse
import httplib2
import pandas as pd

from io import StringIO
import datetime
from pytz import timezone
import dateutil.relativedelta
from datetime import timedelta
from tzlocal import get_localzone
from time import sleep

import googleapiclient.errors
from googleapiclient.discovery import build
from oauth2client import tools
from oauth2client.file import Storage
from oauth2client.client import flow_from_clientsecrets
import lib.logs as log

# Get script start time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
    .normalize(datetime.datetime.now(local_tz)
    .astimezone(yvr_tz)))

# Ctrl+C
def signal_handler(signal, frame):
    logger.info('Ctrl+C pressed!')
    sys.exit(0)


# Set up logging
logger = logging.getLogger(__name__)
log.setup()


def clean_exit(code, message):
    """Exits with a logger message and code"""
    logger.info('Exiting with code %s : %s', str(code), message)
    sys.exit(code)


# Command line arguments
parser = argparse.ArgumentParser(
    parents=[tools.argparser],
    description='GDX Analytics ETL utility for Google My Business insights.')
parser.add_argument('-o', '--cred', help='OAuth Credentials JSON file.')
parser.add_argument('-a', '--auth', help='Stored authorization dat file.')
parser.add_argument('-c', '--conf', help='Microservice configuration file.',)
parser.add_argument('-d', '--debug', help='Run in debug mode.',
                    action='store_true')
flags = parser.parse_args()

CLIENT_SECRET = flags.cred  # credentials_mybusiness.json
AUTHORIZATION = flags.auth  # credentials_mybusiness.dat
CONFIG = flags.conf


# Parse the CONFIG file as a json object and load its elements as variables
with open(CONFIG) as f:
    config = json.load(f)

config_bucket = config['bucket']
config_source = config['source']
config_destination = config['destination']
config_directory = config['directory']
config_dbtable = config['dbtable']
config_metrics = config['metrics']
config_locations = config['locations']


# set up the S3 resource
s3 = boto3.client('s3')
resource = boto3.resource('s3')


# set up the Redshift connection
dbname = 'snowplow'
host = 'redshift.analytics.gov.bc.ca'
port = '5439'
user = os.environ['pguser']
password = os.environ['pgpass']
conn_string = (f"dbname='{dbname}' host='{host}' port='{port}' "
               f"user='{user}' password={password}")


# Google API Access requires a browser-based authentication step to create
# the stored authorization .dat file. Forcing noauth_local_webserver to True
# allows for authentication from an environment without a browser, such as EC2.
flags.noauth_local_webserver = True

'''
Initialize the OAuth2 authorization flow.
where CLIENT_SECRET is the OAuth Credentials JSON file script argument
       scope is  google APIs authorization web address
       redirect_uri specifies a loopback protocol 4201 selected as a random open port 
         -more information on loopback protocol: 
       https://developers.google.com/identity/protocols/oauth2/resources/loopback-migration
'''
flow_scope = 'https://www.googleapis.com/auth/business.manage'
flow = flow_from_clientsecrets(CLIENT_SECRET, scope=flow_scope,
                               redirect_uri='http://127.0.0.1:4201',
                               prompt='consent')


# Specify the storage path for the .dat authentication file
storage = Storage(AUTHORIZATION)
credentials = storage.get()

# Refresh the access token if it expired
if credentials is not None and credentials.access_token_expired:
    try:
        h = httplib2.Http()
        credentials.refresh(h)
    except Exception:
        pass

# Acquire credentials in a command-line application
if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)

# Apply credential headers to all requests made by an httplib2.Http instance
http = credentials.authorize(httplib2.Http())

# Build the Service Objects for the Google My Business APIs
# My Business Account Management API v1 provides: Accounts List
# https://mybusinessaccountmanagement.googleapis.com/$discovery/rest?version=v1
gmbAMso = build('mybusinessaccountmanagement', 'v1', http=http)
# My Business Business Information API v1 Provides: Accounts Locations List
# 'https://mybusinessbusinessinformation.googleapis.com/$discovery/rest?version=v1'
gmbBIso = build('mybusinessbusinessinformation', 'v1', http=http)
# https://businessprofileperformance.googleapis.com/$discovery/rest?version=v1
gmbv1 = build('businessprofileperformance', 'v1', http=http, static_discovery=False)

# Check for a last loaded date in Redshift
# Load the Redshift connection
def last_loaded(dbtable, account, location_id):
    last_loaded_date = None
    con = psycopg2.connect(conn_string)
    cursor = con.cursor()
    loc_id = str(account) + "/" + str(location_id)
    # query the latest date for any search data on this site loaded to redshift
    query = ("SELECT MAX(Date) "
             "FROM {0} "
             "WHERE location_id = '{1}'").format(dbtable, loc_id)
    cursor.execute(query)
    # get the last loaded date
    last_loaded_date = (cursor.fetchone())[0]
    # close the redshift connection
    cursor.close()
    con.commit()
    con.close()
    return last_loaded_date

def get_locations(gmbBIso, account_uri):
    """
    Used to request list of locations for current account.
    Will attempt 10 times to query Google API before
    reporting an error and exiting.
    """
    wait_time = 0.25
    error_count = 0
    while error_count < 11:
        try:
            locations = \
                gmbBIso.accounts().locations().list(
                parent=account_uri,pageSize=100,readMask='name,title').execute()
        except googleapiclient.errors.HttpError:
            if error_count == 10:
                logger.exception(
                    "Request hit 503 error. Exiting after 10th attempt."
                )
                clean_exit(1, "Request to API hit 503 error")
            error_count += 1
            sleep(wait_time)
            wait_time = wait_time *2
            logger.warning(
                "Retrying connection to Google Analytics with %s wait time", wait_time
            )
        else:
            error_count = 11

    logger.info("Retrieved list of locations.")
    return locations


# Will run at end of script to print out accumulated report_stats
def report(data):
    '''reports out the data from the main program loop'''
    if data['no_new_data'] == data['locations']:
        logger.info("No API response contained new data")
        return
    print(f'Report: {__file__}\n')
    print(f'Config: {CONFIG}')
    # Get times from system and convert to Americas/Vancouver for printing
    yvr_dt_end = (yvr_tz
        .normalize(datetime.datetime.now(local_tz)
        .astimezone(yvr_tz)))
    print(
        '\nMicroservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.')
    print(f'\nLocations to process: {data["locations"]}')
    print(f'Successful API calls: {data["retrieved"]}')
    print(f'Failed API calls: {data["not_retrieved"]}')
    print(f'Successful loads to RedShift: {data["loaded_to_rs"]}')
    print(f'Failed loads to RedShift: {data["failed_rs"]}')
    print(f'Files loads to S3 /good: {data["good"]}')
    print(f'Files loads to S3 /bad: {data["bad"]}')
    print(f'Sites failed due to hitting an error: {len(data["failed_process_list"])}\n')

    # Print all fully processed locations in good
    print(f'Objects loaded RedShift and to S3 /good:')
    if data['good_list']:
        for i, item in enumerate(data['good_list'], 1):
            #removing newline character per GDXDSD-5197
            print(f"{i}: {item}")

    # Print all fully processed locations in bad
    if data['bad_list']:
        print(f'\nObjects loaded RedShift and to S3 /bad:')
        for i, item in enumerate(data['bad_list'], 1):
            print(f"\n{i}: {item}")

    # Print failed to copy to RedShift
    if data['failed_rs_list']:
        print(f'\nList of objects that failed to copy to Redshift:')
        for i, item in enumerate(data['failed_rs_list'], 1):
            print(f'\n{i}: {item}')

    # Print unsuccessful API calls 
    if  data['not_retrieved_list']:
        print(f'List of sites that were not processed due to early exit:')
        for i, site in enumerate(data['not_retrieved_list'], 1):
            print(f'\n{i}: {site}')

    #print any  failed sites
    if data['failed_process_list']:
        print(f'List of sites that were skipped due to hitting an error:')
        for i, site in enumerate(data['failed_process_list'], 1):
            print(f'\n{i}: {site}')

# Reporting variables. Accumulates as the the loop below is traversed
report_stats = {
    'locations':0,
    'no_new_data':0,
    'retrieved':0,
    'not_retrieved':0,
    'processed':0,
    'good': 0,
    'bad': 0,
    'loaded_to_rs': 0,
    'failed_rs':0,
    'locations_list':[],
    'retrieved_list':[],
    'not_retrieved_list':[],
    'failed_s3_list':[],
    'good_rs_list':[],
    'failed_rs_list':[],
    'good_list':[],  # Made it all the way through
    'bad_list':[],
    #this will be used to capture any and all sites that are skipped over in the loop 
    'failed_process_list':[]
}

# Location Check

# Create a list of accounts using My Business Account Management API
# (ignoring the first one, since it is the 'self' reference account).
accounts = gmbAMso.accounts().list().execute()['accounts'][1:]

# check that all locations defined in the configuration file are available
# to the authencitad account being used to access the MyBusiness API, and
# append those accounts information into a 'validated_locations' list.
validated_accounts = []
for loc in config_locations:
    try:
        validated_accounts.append(
            next({
                'uri': item['name'],
                'name': item['accountName'],
                'id': item['accountNumber'],
                'client_shortname': loc['client_shortname'],
                'start_date': loc['start_date'],
                'end_date': loc['end_date']}
                 for item
                 in accounts
                 if item['accountNumber'] == str(loc['id'])))
    except StopIteration:
        logger.exception(
            'No API access to %s. Excluding from insights query.', loc['name'])
        continue

# iterate over ever location of every account
for account in validated_accounts:
    # Create a dataframe with dates as rows and columns according to the table
    df = pd.DataFrame()
    account_uri = account['uri']
    # Create a list of locations in this account
    locations = get_locations(gmbBIso, account_uri)
    
    # we will handle each location separately
    for loc in locations['locations']:

        logger.info("Begin processing on location: %s", loc['title'])

        # encode as ASCII for dataframe
        location_uri = loc['name']
        location_name = loc['title']

        # Report out locations names and count
        report_stats['locations_list'].append(location_name)
        report_stats['locations'] += 1

        # if a start_date is defined in the config file, use that date
        start_date = account['start_date']
        if start_date == '':
            # The earliest available data is from 18 months ago from the
            # query day. Adding a day accounts for possible timezone offset.
            # timedelta does not support months, dateutil.relativedelta does.
            # reference: https://stackoverflow.com/a/14459459/5431461
            start_date = (
                datetime.datetime.today().date()
                - dateutil.relativedelta.relativedelta(months=18)
                + timedelta(days=1)
                ).isoformat()

        # query RedShift to see if there is a date already loaded
        last_loaded_date = last_loaded(config_dbtable, account_uri, location_uri)
        if last_loaded_date is None:
            logger.info("first time loading %s: %s",
                        account['name'], loc['name'])

        # If it is loaded with some data for this ID, use that date plus
        # one day as the start_date.
        if (last_loaded_date is not None
                and last_loaded_date.isoformat() >= start_date):
            start_date = last_loaded_date + timedelta(days=1)
            start_date = start_date.isoformat()

        start_time = start_date + 'T01:00:00Z'
        start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")

        # the most recently available data from the Google API is 2 days before
        # the query time. More details in the API reference at:
        # https://developers.google.com/my-business/reference/performance/rest/v1/locations/getDailyMetricsTimeSeries
        date_api_upper_limit = (
            datetime.datetime.today().date() - timedelta(days=2)).isoformat()
        # if an end_date is defined in the config file, use that date
        end_date = account['end_date']
        if end_date == '':
            end_date = date_api_upper_limit
        if end_date > date_api_upper_limit:
            logger.warning(
                "The end_date for location %s is more recent than 2 days ago.",
                location_name)

        end_time = end_date + 'T01:00:00Z'
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")
        # if start and end times are same, then there's no new data
        if start_time == end_time:
            logger.info(
                "Redshift already contains the latest avaialble data for %s.",
                location_name)
            report_stats['no_new_data'] += 1
            continue

        logger.info("Querying range from %s to %s", start_date, end_date)

        #defining dict to store incoming data and processed into dict objects
        daily_data = {}

        """
        
        for metric in config_metrics:
            #defining the API call using necessary parameters
            logger.info("Processing metric: %s", metric)
            daily_m = gmbv1.locations().getDailyMetricsTimeSeries(
                name=location_uri,
                dailyMetric=metric,
                dailyRange_endDate_day=end_date.day,
                dailyRange_endDate_month=end_date.month,
                dailyRange_endDate_year=end_date.year,
                dailyRange_startDate_day=start_date.day,
                dailyRange_startDate_month=start_date.month,
                dailyRange_startDate_year=start_date.year
            )
            try:
                #executing the call
                dailyMetric = daily_m.execute()
            except error:
                logger.exception(
                    "Error trying to collect ", metric, " for location: ", loc['title'] , " with error:"
                )
                logger.exception(error)
                report_stats['failed_process_list'].append(location_name)
                continue
            
            #pulling out the necessary data
            daily_metrics = dailyMetric['timeSeries']['datedValues']

            for date_value in daily_metrics:
                year = date_value['date']['year']
                month = date_value['date']['month']
                day = date_value['date']['day']
                date = datetime.datetime(year, month, day).date()
                
                if 'value' in date_value:
                    metric_val = int(date_value['value'])
                else:
                    metric_val = 0

                if date not in daily_data:
                    daily_data[date] = {metric: metric_val}
                elif metric not in daily_data[date]:
                    daily_data[date][metric] = metric_val
                else:
                    logger.error("Hit duplicate: ", date, ", ", metric)
                
        for date_value in daily_data:
            #turn data into rows for the dataframe. 
            #this script is cateres to 9 metric points, if that changes errors may ensue
            row = pd.DataFrame([{
                'date': date_value.strftime('%Y-%m-%d'),
                'client': account['client_shortname'],
                'location': location_name,
                'location_id': f'{account_uri}/{location_uri}',
                config_metrics[0].lower(): daily_data[date_value][config_metrics[0]],
                config_metrics[1].lower(): daily_data[date_value][config_metrics[1]],
                config_metrics[2].lower(): daily_data[date_value][config_metrics[2]],
                config_metrics[3].lower(): daily_data[date_value][config_metrics[3]],
                config_metrics[4].lower(): daily_data[date_value][config_metrics[4]],
                config_metrics[5].lower(): daily_data[date_value][config_metrics[5]], 
                config_metrics[6].lower(): daily_data[date_value][config_metrics[6]],
                config_metrics[7].lower(): daily_data[date_value][config_metrics[7]],
                config_metrics[8].lower(): daily_data[date_value][config_metrics[8]]
            }])
            
            df = pd.concat([df, row], sort=False)

        # sort the dataframe by date
        df.sort_values('date')
       
        # collapse rows on the groupers columns, which will remove all NaNs.
        # reference: https://stackoverflow.com/a/36746793/5431461
        groupers = ['date', 'client', 'location', 'location_id']
        groupees = [e.lower() for e in config_metrics]
        df = df.groupby(groupers).apply(lambda g: g[groupees].ffill().iloc[-1])
 
        # prepare csv buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=True, header=True, sep='|')

        # Set up the S3 path to write the csv buffer to
        object_key_path = (f"{config_source}/"
                           f"{config_directory}/"
                           f"{account['client_shortname']}/")

        outfile = (f"gmb_"
                   f"{location_name.replace(' ', '-')}_"
                   f"{start_date.strftime('%Y-%m-%d')}_"
                   f"{end_date.strftime('%Y-%m-%d')}"
                   f".csv")

        object_key = object_key_path + outfile

        resource.Bucket(config_bucket).put_object(
            Key=object_key,
            Body=csv_buffer.getvalue())
        logger.info('PUT_OBJECT: %s:%s', outfile, config_bucket)
        object_summary = resource.ObjectSummary(config_bucket, object_key)
        logger.info('OBJECT LOADED ON: %s OBJECT SIZE: %s',
                     object_summary.last_modified, object_summary.size)

        # Prepare the Redshift COPY command.
        logquery = (
            (f"copy {config_dbtable} FROM 's3://{config_bucket}/{object_key}' "
             "CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};"
             "aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' "
             "IGNOREHEADER AS 1 MAXERROR AS 0 DELIMITER '|' "
             "NULL AS '-' ESCAPE;"))
        query = logquery.format(
            AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
            AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'])
        logger.info(logquery)

        # Define s3 bucket paths
        goodfile = f"{config_destination}/good/{object_key}"
        badfile = f"{config_destination}/bad/{object_key}"

        # Connect to Redshift and execute the query.
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as curs:
                try:
                    curs.execute(query)
                except psycopg2.Error as e:
                    logger.error("".join((
                        "Loading failed {0} with error:\n{1}"
                        .format(location_name, e.pgerror),
                        " Object key: {0}".format(object_key.split('/')[-1]))))
                    outfile = badfile
                    report_stats['failed_rs_list'].append(outfile)
                    report_stats['failed_rs'] += 1
                else:
                    logger.info("".join((
                        "Loaded {0} successfully."
                        .format(location_name),
                        ' Object key: {0}'.format(object_key.split('/')[-1]))))
                    outfile = goodfile
                    report_stats['good_rs_list'].append(outfile)
                    report_stats['loaded_to_rs'] += 1
                    report_stats['processed'] += 1
                    
        # copy the object to the S3 outfile (processed/good/ or processed/bad/)
        try:
            s3.copy_object(
                Bucket="sp-ca-bc-gov-131565110619-12-microservices",
                CopySource="sp-ca-bc-gov-131565110619-12-microservices/{}"
                .format(object_key), Key=outfile)
        except boto3.exceptions.ClientError:
            logger.exception("S3 transfer to %s failed", outfile)
            report_stats['failed_s3_list'].append(outfile)
            clean_exit(1,f'S3 transfer of {object_key} to {outfile} failed.')
        else:
            if outfile == goodfile:
                report_stats['good_list'].append(outfile)
                report_stats['good'] += 1
            else:
                report_stats['bad_list'].append(outfile)
                report_stats['bad'] += 1


report(report_stats)
clean_exit(0,'Ran without errors.')
"""
