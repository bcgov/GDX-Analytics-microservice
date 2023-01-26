###################################################################
# Script Name   : google_directions.py
#
#
# Description   : A script to access the Google Locations/My Business
#               : api, download driving insights for locations in a location
#               : group, then dump the output per location into an S3 Bucket
#               : where that file is then loaded to Redshift.
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
#               : $ python google_directions.py -o credentials_mybusiness.json\
#               :   -a credentials_mybusiness.dat -c config_directions.json
#
#               : the flags specified in the usage example above are:
#               : -o <OAuth Credentials JSON file>
#               : -a <Stored authorization dat file>
#               : -c <Microservice configuration file>
#               :
# References    :
# https://developers.google.com/my-business/reference/rest/v4/accounts.locations/reportInsights

import os
import sys
import json
import boto3
import logging
import time
import datetime
import psycopg2
import argparse
import httplib2
import googleapiclient.errors
import pandas as pd
import lib.logs as log
from time import sleep
from io import StringIO
from pytz import timezone
from oauth2client import tools
from tzlocal import get_localzone
from pandas import json_normalize
from oauth2client.file import Storage
from googleapiclient.discovery import build
from botocore.exceptions import ClientError
from oauth2client.client import flow_from_clientsecrets


AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
CLIENT_SECRET = ''
AUTHORIZATION = ''
CONFIG = ''

# Set up logging
logger = logging.getLogger(__name__)
log.setup() 

# Get script start time
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz
    .normalize(datetime.datetime.now(local_tz)
    .astimezone(yvr_tz)))


"""Used to handle a forced exit"""
def signal_handler(signal, frame):
    logger.info('Ctrl+C pressed!')
    sys.exit(0)


"""Used for exiting code with a message and code"""
def clean_exit(code, message):
    """Exits with a logger message and code"""
    logger.info('Exiting with code %s : %s', str(code), message)
    sys.exit(code)  


"""Creates and returns a string that 
can establish a connection to Redshift

"""
def redshift_connection():
    dbname = 'snowplow'
    host = 'redshift.analytics.gov.bc.ca'
    port = '5439'
    user = os.environ['pguser']
    password = os.environ['pgpass']
    conn_string = (f"dbname='{dbname}' host='{host}' port='{port}' "
                f"user='{user}' password={password}")

    return conn_string


""" Initialize the OAuth2 authorization flow. 
where CLIENT_SECRET is the OAuth Credentials JSON file script argument
    scope is  google APIs authorization web address
    redirect_uri specifies a loopback protocol 4202 selected as a random open port 
       -more information on loopback protocol: 
       https://developers.google.com/identity/protocols/oauth2/resources/loopback-migration
returns valid credentials
"""
def google_auth(CLIENT_SECRET, AUTHORIZATION, flags):
    flow_scope = 'https://www.googleapis.com/auth/business.manage'
    flow = flow_from_clientsecrets(CLIENT_SECRET, scope=flow_scope,
                                redirect_uri='http://127.0.0.1:4202',
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

    return credentials

""" Check to see if the file has been processed already"""
def is_processed(key, config_destination, config_bucket, client):
    filename = key[key.rfind('/')+1:]  # get the filename (after the last '/')
    goodfile = config_destination + "/good/" + key
    badfile = config_destination + "/bad/" + key
    try:
        client.head_object(Bucket=config_bucket, Key=goodfile)
    except ClientError:
        pass  # this object does not exist under the good destination path
    else:
        logger.info("%s was processed as good already.", filename)
        return True
    try:
        client.head_object(Bucket=config_bucket, Key=badfile)
    except ClientError:
        pass  # this object does not exist under the bad destination path
    else:
        logger.info("%s was processed as bad already.", filename)
        return True
    logger.info("%s has not been processed.", filename)
    return False


"""access each location listed in the json config file
and try to add information for each to validated_accounts
"""
def get_locations(accounts, config_locationGroups, validated_accounts):
    for loc in config_locationGroups:
        # access the environment variable that sets the Account ID for this
        # Location Group, which is to be passed to the validated accounts list
        accountNumber = os.environ[f"{loc['clientShortname']}_accountid"]
        try:
            validated_accounts.append(
                next({
                    'name': item['name'],
                    'clientShortname': loc['clientShortname'],
                    'aggregate_days': loc['aggregate_days'],
                    'accountNumber': accountNumber}
                    for item
                    in accounts
                    if item['accountNumber'] == accountNumber))
        except StopIteration:
            logger.warning('No access to %s: %s. Skipping.',
                        loc['clientShortname'], accountNumber)
            continue

"""check the aggregate_days validity"""
def check_days(account):
    return_val = True
    if 1 <= len(account["aggregate_days"]) <= 3:
        for i in account["aggregate_days"]:
            if not any(i == s for s in ["SEVEN", "THIRTY", "NINETY"]):
                logger.error(
                    "%s is an invalid aggregate option. Skipping %s.",
                    i, account['clientShortname'])
                return_val = False
    else:
        logger.error(
            "aggregate_days on %s is invalid due to size. Skipping.",
            account['clientShortname'])
        return_val = False
    return return_val


"""Posts the API request"""
def post_api(gmbv49so, bodyvar, report_stats, account):
    name = account['name']
    error_count = 0
    wait_time = 0.25
    while error_count < 11:
        try:
            response = \
                gmbv49so.accounts().locations().\
                reportInsights(body=bodyvar, name=name).execute()
        except googleapiclient.errors.HttpError:
            if error_count == 10:
                logger.exception(
                    "Request contains an invalid argument. Skipping.")
                report_stats['not_retrieved'] += 1
                clean_exit(1,'Request to API caused an Error.')
            error_count += 1
            sleep(wait_time)
            wait_time = wait_time * 2
            logger.warning("retrying connection to Google Analytics with wait time %s", wait_time)
        else:
            break

    # If retreived, report it
    logger.info(f"{account['clientShortname']} Retrieved.")
    report_stats['retrieved'] += 1
    return response


"""Create a query and execute it in Redshift
Return the location that the file is placed in S3
"""
def execute_query(config_dbtable, config_bucket, conn_string, account, object_key, badfile, goodfile, report_stats):
    logquery = (
            f"COPY {config_dbtable} ("
            "client_shortname,"
            "days_aggregated,"
            "location_label,"
            "location_locality,"
            "location_name,"
            "location_postal_code,"
            "location_time_zone,"
            "rank_on_query,"
            "region_label,"
            "region_latitude,"
            "region_longitude,"
            "utc_query_date,"
            "region_count_seven_days,"
            "region_count_ninety_days,"
            "region_count_thirty_days"
            f") FROM 's3://{config_bucket}/{object_key}' CREDENTIALS '"
            "aws_access_key_id={AWS_ACCESS_KEY_ID};"
            "aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' "
            "IGNOREHEADER AS 1 MAXERROR AS 0 DELIMITER '|' NULL AS '-' ESCAPE;")
    query = logquery.format(
                AWS_ACCESS_KEY_ID=AWS_ACCESS_KEY_ID,
                AWS_SECRET_ACCESS_KEY=AWS_SECRET_ACCESS_KEY)
    logger.info(logquery)
    
    # Connect to Redshift and execute the query.
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor() as curs:
            try:
                curs.execute(query)
            except psycopg2.Error:
                logger.exception(
                    ("Loading driving directions for failed %s "
                    "on Object key: %s"),
                    account['clientShortname'],object_key.split('/')[-1])
                outfile = badfile
                print('outfile in except: ', outfile)
                report_stats['failed_rs_list'].append(outfile)
                report_stats['failed_rs'] += 1
            else:
                logger.info(
                    ("Loaded %s driving directions successfully. "
                    "Object key %s."),
                    account['clientShortname'], object_key.split('/')[-1])
                outfile = goodfile
                report_stats['good_rs_list'].append(outfile)
                report_stats['loaded_to_rs'] += 1
    return outfile


""" Copy the processed file to the outfile destination path"""
def copy_file(client, object_summary, outfile, goodfile, report_stats, badfile):
    try:
        client.copy_object(
            Bucket="sp-ca-bc-gov-131565110619-12-microservices",
            CopySource="sp-ca-bc-gov-131565110619-12-microservices/"
            + object_summary.key, Key=outfile)
    except boto3.exceptions.ClientError:
        logger.exception("S3 copy %s to %s location failed.",
                        object_summary.key, outfile=outfile)
        clean_exit(1,'S3 transfer failed.')
    else:
        if outfile == goodfile:
            report_stats['good_list'].append(outfile)
            report_stats['good'] += 1
        else:
            report_stats['bad_list'].append(outfile)
            report_stats['bad'] += 1
    if outfile == badfile:
        clean_exit(1,'The output file was bad.')


""" iterate over the top 10 driving direction requests for this
location. The order of these is desending by number of requests
"""
def iterate_top_ten(location, query_date, account, label_lookup, location_region_rows):
    source = location['topDirectionSources'][0]
    regions = source['regionCounts']
    
    for order, region in enumerate(regions):
        row = {
            'utc_query_date': query_date,
            'client_shortname': account['clientShortname'],
            'location_label':
                label_lookup[location['locationName']]['title'],
            'location_locality':
                label_lookup[location['locationName']]['locality'],
            'location_postal_code':
                label_lookup[location['locationName']]['postalCode'],
            'location_name': location['locationName'],
            'days_aggregated': source['dayCount'],
            'rank_on_query': order + 1,  # rank is from 1 to 10
            'region_count': region['count'],
            'region_latitude': region['latlng']['latitude'],
            'region_longitude': region['latlng']['longitude'],
            'region_label': region['label'],
            'location_time_zone': location['timeZone']
            }
        location_region_rows.append(row)


"""Write csv to s3
Returns information on the file so that it can be copied to s3
"""
def write_to_s3(resource, config_bucket, object_key, csv_stream, outfile):
    resource.Bucket(config_bucket).put_object(
        Key=object_key,
        Body=csv_stream.getvalue())
    logger.info('S3 PUT_OBJECT: %s:%s', outfile, config_bucket)
    object_summary = resource.ObjectSummary(config_bucket, object_key)
    logger.info('S3 OBJECT LOADED ON: %s OBJECT SIZE: %s',
                object_summary.last_modified, object_summary.size)
    return object_summary


def print_list(report_string, report_list):
    print('\n', report_string)
    for i, item in enumerate(report_list, 1):
            print(f"\n{i}: {item}")

""" Will run at end of script to print out accumulated report_stats
reports out the data from the main program loop
"""
def report(data):
    if data['no_new_data'] == True:
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
    print(f'Number of files to process: {data["items"]}')
    print(f'Successful loads to RedShift: {data["loaded_to_rs"]}')
    print(f'Failed loads to RedShift: {data["failed_rs"]}')
    print(f'Objects output to processed/good: {data["good"]}')
    print(f'Objects output to processed/bad: {data["bad"]}\n')

    # Print all fully processed locations in good
    if data['good_list']:
        print_list('Objects loaded RedShift and to S3 /good:', data['good_list'])

    # Print all fully processed locations in bad
    if data['bad_list']:
        print_list('Objects loaded RedShift and to S3 /bad:', data['bad_list'])

    # Print failed load to RedShift
    if data['failed_rs_list']:
        print_list('List of objects that failed to copy to Redshift:', data['failed_rs_list'])

    # Print unsuccessful API calls 
    if data['not_retrieved_list']:
        print_list('List of sites that were not processed do to Google API Error:', data['not_retrieved_list'])


def main():
    # Command line arguments
    parser = argparse.ArgumentParser(
        parents=[tools.argparser],
        description='GDX Analytics ETL utility for Google My Business insights.')
    parser.add_argument('-o', '--cred', help='OAuth Credentials JSON file.')
    parser.add_argument('-a', '--auth', help='Stored authorization dat file')
    parser.add_argument('-c', '--conf', help='Microservice configuration file.',)
    parser.add_argument('-d', '--debug', help='Run in debug mode.',
                        action='store_true')
    flags = parser.parse_args()

    global CLIENT_SECRET
    global AUTHORIZATION
    global CONFIG
    CLIENT_SECRET = flags.cred
    AUTHORIZATION = flags.auth
    CONFIG = flags.conf

    # Parse the CONFIG file as a json object and load its elements as variables
    with open(CONFIG) as f:
        config = json.load(f)

    config_bucket = config['bucket']
    config_dbtable = config['dbtable']
    config_destination = config['destination']
    config_locationGroups = config['locationGroups']
    config_prefix = config['prefix']

    conn_string  = redshift_connection()
 
    # set the query date as now in UTC
    query_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    # Google API Access requires a browser-based authentication step to create
    # the stored authorization .dat file. Forcing noauth_local_webserver to True
    # allows for authentication from an environment without a browser, such as EC2.
    flags.noauth_local_webserver = True

    credentials = google_auth(CLIENT_SECRET, AUTHORIZATION, flags)

    # Apply credential headers to all requests made by an httplib2.Http instance
    http = credentials.authorize(httplib2.Http())

    # Build the Service Objects for the Google My Business APIs
    # My Business Account Management API v1 provides: Accounts List
    # https://mybusinessaccountmanagement.googleapis.com/$discovery/rest?version=v1
    gmbAMso = build('mybusinessaccountmanagement', 'v1', http=http)

    # My Business Business Information API v1 Provides: Accounts Locations List
    # 'https://mybusinessbusinessinformation.googleapis.com/$discovery/rest?version=v1'
    gmbBIso = build('mybusinessbusinessinformation', 'v1', http=http)

    # My Business API v4.9 provides: Accounts Locations reportInsights
    DISCOVERY_URI_v4_9_gmb = 'https://developers.google.com/my-business/samples/mybusiness_google_rest_v4p9.json'
    gmbv49so = build(
                 'mybusiness','v4',http=http,
                 discoveryServiceUrl=DISCOVERY_URI_v4_9_gmb
                 )

    # set up the S3 resource
    client = boto3.client('s3')
    resource = boto3.resource('s3')

    # Reporting variables. Accumulates as the the loop below is traversed
    report_stats = {
        'locations':0,
        'items':0,
        'no_new_data':False,
        'retrieved':0,
        'not_retrieved':0,
        'processed':0,
        'good':0,
        'bad':0,
        'loaded_to_rs': 0,
        'failed_rs':0,
        'locations_list':[],
        'retrieved_list':[],
        'not_retrieved_list':[],
        'failed_s3_list':[],
        'good_rs_list':[],
        'failed_rs_list':[],
        'good_list':[],  # Made it all the way through
        'bad_list':[]
    }

    # Location Check
    # check that all locations defined in the configuration file are available
    # to the authencitad account being used to access the MyBusiness API, and
    # append those accounts information into a 'validated_locations' list.
    validated_accounts = []
    
    # Get the list of accounts that the Google account being used to access
    # the My Business API has rights to read location insights from
    # (ignoring the first one, since it is the 'self' reference account).
    accounts = gmbAMso.accounts().list().execute()['accounts'][1:]
    get_locations(accounts, config_locationGroups, validated_accounts)

    # iterate over ever validated account
    report_stats["items"]  = len(validated_accounts)
    for account in validated_accounts:
        if not check_days(account):
            continue

        # Set up the S3 path to write the csv buffer to
        object_key_path = f"client/{config_prefix}_{account['clientShortname']}/"
        outfile = f"gmb_directions_{account['clientShortname']}_{query_date}.csv"
        object_key = object_key_path + outfile

        if is_processed(object_key, config_destination, config_bucket, client):
            logger.info(
                ("The file: %s has already been generated "
                "and processed by this script today."), object_key)
            report_stats['no_new_data'] = True
            continue

        goodfile = f"{config_destination}/good/{object_key}"
        badfile = f"{config_destination}/bad/{object_key}"

        # Create a dataframe with dates as rows and columns according to the table
        df = pd.DataFrame()
        # Get account/accountId
        account_uri = account['name']
        name = account_uri  # done for readability
        locations = (
                    gmbBIso.accounts().locations().list(
                       parent=name,
                       pageSize=100,
                       readMask='name,title,storefrontAddress'
                       ).execute()
                    )

        # Google's MyBusiness API supports querying for 10 locations at a time, so
        # here we batch locations into a list-of-lists of size batch_size (max=10).
        batch_size = 10
        location_names = [i['name'] for i in locations['locations']]
        
        # Add account_uri prefix to location 
        location_names_list = [f'{account_uri}/{i}' for i in location_names]
        
        # construct the label lookup and apply formatting if any
        # if not present, locality and postalCode will default to none
        label_lookup = {
            f'{account_uri}/'+ i['name']: {
                'title': i['title'],
                'locality': i.get('storefrontAddress', {}).get('locality'),
                'postalCode': i.get('storefrontAddress', {}).get('postalCode')
                } for i in locations['locations']}

        # batched_location_names is a list of lists
        # each list within batched_location_names contains up to 10 location names
        # each list of 10 will added pre API request, which can support responsese
        # of up to 10 locations at a time. The purpose of this is to reduce calls.
        batched_location_names = [
            location_names_list[i * batch_size:(i + 1) * batch_size] for i in
            range((len(location_names_list) + batch_size - 1) // batch_size)]

        # Iterate over each list of locations, calling the API for each that batch
        # stitching the responses into a single list to process after the API calls
        stitched_responses = {'locationDrivingDirectionMetrics': []}
        for key, batch in enumerate(batched_location_names):
            logger.info("Begin processing on locations batch %s of %s",
                        str(key + 1), 
                        len(batched_location_names)
                        )
            for days in account['aggregate_days']:
                logger.info("Begin processing on %s day aggregate", days)
                bodyvar = {
                    'locationNames': batch,
                    # https://developers.google.com/my-business/reference/rest/v4/accounts.locations/reportInsights#DrivingDirectionMetricsRequest
                    'drivingDirectionsRequest': {
                        'numDays': f'{days}',
                        'languageCode': 'en-US'
                        }
                    }
                report_stats['locations'] += 1
                logger.info("Request JSON -- \n%s", json.dumps(bodyvar, indent=2))

                response = post_api(gmbv49so, bodyvar, report_stats, account)
                
                # stitch all responses responses for later iterative processing
                stitched_responses['locationDrivingDirectionMetrics'] += \
                    response['locationDrivingDirectionMetrics']

        # The stiched_responses now contains all location driving direction data
        # as a list of dictionaries keyed to 'locationDrivingDirectionMetrics'.
        # The next block will build a dataframe from this list for CSV ouput to S3

        # location_region_rows will collect elements from the API response
        # JSON into a list of dicts, to normalize into a DataFrame later
        location_region_rows = []
        location_directions = stitched_responses['locationDrivingDirectionMetrics']
        
        for location in location_directions:
            # The case where no driving directions were queried over this time
            # these records will be omitted, since there is nothing to report
            if 'topDirectionSources' not in location:
                continue

            iterate_top_ten(location, query_date, account, label_lookup, location_region_rows)

        # normalizing the list of dicts to a dataframe
        df = json_normalize(location_region_rows)

        # build three columns: region_count_seven_days, region_count_thirty_days
        # and region_count_ninety_days to replace region_count column.
        new_cols = {
            'region_count_seven_days': 7,
            'region_count_thirty_days': 30,
            'region_count_ninety_days': 90
            }
        for key, value in new_cols.items():
            def alert(c):
                if c['days_aggregated'] == value:
                    return c['region_count']
                else:
                    return 0

            df[key] = df.apply(alert, axis=1)

        df.drop(columns='region_count', inplace=True)

        # output csv formatted dataframe to stream
        csv_stream = StringIO()
        # set order of columns for S3 file in order to facilitate table COPY
        column_names = [
            "client_shortname", "days_aggregated", "location_label",
            "location_locality", "location_name", "location_postal_code",
            "location_time_zone", "rank_on_query", "region_label",
            "region_latitude", "region_longitude", "utc_query_date",
            "region_count_seven_days", "region_count_ninety_days",
            "region_count_thirty_days"]
        df = df.reindex(columns=column_names)
        df.to_csv(csv_stream, sep='|', encoding='utf-8', index=False)

        object_summary = write_to_s3(resource, config_bucket, object_key, csv_stream, outfile)

        outfile = execute_query(config_dbtable, config_bucket, conn_string, account, object_key, badfile, goodfile, report_stats)

        copy_file(client, object_summary, outfile, goodfile, report_stats, badfile)

    report(report_stats)
    clean_exit(0,'Finished without errors.')

if __name__ == '__main__':
    main()
