"""Google Search Console API Loader Script"""
###################################################################
# Script Name   : google_search.py
#
#
# Description   : A script to access the Google Search Console
#               : api, download analytcs info and dump to a CSV in S3
#               : The resulting file is loaded to Redshift and then
#               : available to Looker through the project google_api.
#               : Calls span 30 days or less, and calls begin from
#               : the day after the latest data loaded into Redshift,
#               : or 16 months ago, or on the date specified in the
#               : file referenced by the GOOGLE_MICROSERVICE_CONFIG
#               : environment variable. The config JSONschema is
#               : defined in the google-api README.md file.
#
# Requirements  : Install python libraries: httplib2, oauth2client
#               : google-api-python-client
#               :
#               : You will need API credensials set up. If you don't have
#               : a project yet, follow these instructions. Otherwise,
#               : place your credentials_search.json file in the location
#               : defined below.
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
#               : Enable the API via "+ Enable APIs and Services" by choosing:
#               :      "Google Search Console API"
#               :
#               : Under 'APIs & Services' Click 'Credentials':
#               :   Click on 'Create Credentials' at the top of the screen
#               :   to select that you want to create an 'OAuth client id'. 
#               :   You will have to configure a consent screen.
#               :   You must provide an Application name, and
#               :   under "Scopes for Google APIs" add the scopes:
#               :   "../auth/webmasters" and "../auth/webmasters.readonly".
#               :
#               :   After you save, you will have to pick an application type.
#               :   Choose Other, and provide a name for this OAuth client ID.
#               :
#               :   Download the JSON file and place it in your directory as
#               :   "credentials_search.json" as described by the variable below
#               :
#               :   When you first run it, it will ask you do do an OAUTH
#               :   validation, which will create a file
#               :   "credentials_search.dat", saving that auhtorization.


import re
from datetime import date, datetime, timedelta
from time import sleep
import json
import argparse
import sys       # to read command line parameters
import os.path   # file handling
import io        # file and stream handling
import logging
from tzlocal import get_localzone
from pytz import timezone
import signal
import backoff
import boto3     # For Amazon S3 IO
import httplib2
from oauth2client.client import HttpAccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError as GoogleHttpError
import psycopg2  # For Amazon Redshift IO
import lib.logs as log

# Get script start times
local_tz = get_localzone()
yvr_tz = timezone('America/Vancouver')
yvr_dt_start = (yvr_tz.normalize(
    datetime.now(local_tz)
            .astimezone(yvr_tz)))


# Ctrl+C
def signal_handler(sig, frame):
    '''Ctrl+C signal handler'''
    logger.info('singal handler sig: %s frame: %s', sig, frame)
    logger.info('Ctrl+C pressed!')
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


logger = logging.getLogger(__name__)
log.setup()


def clean_exit(code, message):
    """Exits with a logger message and code"""
    logger.info('Exiting with code %s : %s', str(code), message)
    sys.exit(code)


# Custom backoff logging handlers
def backoff_hdlr(details):
    """Event handler for use in backoff decorators on_backoff kwarg"""
    msg = "Backing off %s(...) for %.1fs after try %i"
    log_args = [details['target'].__name__, details['wait'], details['tries']]
    logger.info(msg, *log_args)


def giveup_hdlr(details):
    """Event handler for for use backoff decorators on_giveup kwarg"""
    msg = "Give up calling %s(...) after %.1fs elapsed over %i tries"
    log_args = [details['target'].__name__, details['elapsed'],
                details['tries']]
    logger.error(msg, *log_args)
    clean_exit(1, 'could not reach Google Search Console after backoff.')


def last_loaded(_s):
    """Check for a sites last loaded date in Redshift"""
    # Load the Redshift connection
    con = psycopg2.connect(conn_string)
    cursor = con.cursor()
    # query the latest date for any search data on this site loaded to redshift
    _q = f"SELECT MAX(DATE) FROM {config_dbtable} WHERE site = '{_s}'"
    cursor.execute(_q)
    # get the last loaded date
    lld = (cursor.fetchall())[0][0]
    # close the redshift connection
    cursor.close()
    con.commit()
    con.close()
    return lld


# the latest available Google API data is two less than the query date (today)
latest_date = date.today() - timedelta(days=2)

# Command line arguments
parser = argparse.ArgumentParser(
    parents=[tools.argparser],
    description='GDX Analytics ETL utility for Google search.')
parser.add_argument('-o', '--cred', help='OAuth Credentials JSON file.')
parser.add_argument('-a', '--auth', help='Stored authorization dat file.')
parser.add_argument('-c', '--conf', help='Microservice configuration file.')
flags = parser.parse_args()
flags.noauth_local_webserver = True

CLIENT_SECRET = flags.cred
AUTHORIZATION = flags.auth
CONFIG = flags.conf

if CLIENT_SECRET is None or AUTHORIZATION is None or CONFIG is None:
    logger.error('Missing one or more requied arguments.')
    sys.exit(1)

# calling the Google API. If credentials_search.dat is not yet generated
# then brower based Google Account validation will be required
API_NAME = 'searchconsole'
API_VERSION = 'v1'
DISCOVERY_URI = 'https://searchconsole.googleapis.com/$discovery/rest'

flow_scope = 'https://www.googleapis.com/auth/webmasters.readonly'
'''
connect to Google Analytics Business account to pull data 
where CLIENT_SECRET is the OAuth Credentials JSON file script argument
       scope is  google APIs authorization web address
       redirect_uri specifies a loopback protocol 4200 selected as a random open port 
       -more information on loopback protocol: 
       https://developers.google.com/identity/protocols/oauth2/resources/loopback-migration
'''
flow = flow_from_clientsecrets(
    CLIENT_SECRET,
    scope=flow_scope,
    redirect_uri='http://127.0.0.1:4200')

flow.params['access_type'] = 'offline'
flow.params['approval_prompt'] = 'force'

storage = Storage(AUTHORIZATION)
credentials = storage.get()

if credentials is not None and credentials.access_token_expired:
    try:
        h = httplib2.Http()
        credentials.refresh(h)
    except HttpAccessTokenRefreshError:
        pass

if credentials is None or credentials.invalid:
    credentials = tools.run_flow(flow, storage, flags)

http = credentials.authorize(httplib2.Http())


# discoveryServiceUrl can become unavailable: use backoff
@backoff.on_exception(backoff.expo, GoogleHttpError,
                      on_backoff=backoff_hdlr, on_giveup=giveup_hdlr,
                      factor=0.5, max_tries=10, logger=None)
def build_service():
    """Consruct a resource to interact with the Search Console API service"""
    # disabling cache-discovery to suppress warnings on:
    # ImportError: file_cache is unavailable when using oauth2client >= 4.0.0
    # https://stackoverflow.com/questions/40154672/importerror-file-cache-is-unavailable-when-using-python-client-for-google-ser
    svc = build(API_NAME,
                API_VERSION,
                http=http,
                discoveryServiceUrl=DISCOVERY_URI,
                cache_discovery=False)
    return svc


service = build_service()

# Read configuration file from env parameter
with open(CONFIG) as f:
    config = json.load(f)

config_sites = config['sites']
config_bucket = config['bucket']
config_dbtable = config['dbtable']
config_source = config['source']
config_directory = config['directory']
dml_file = config['dml']

# Create S3 client and resouce
client = boto3.client('s3')
resource = boto3.resource('s3')

# Set up the Redshift connection
dbname = 'snowplow'
host = 'redshift.analytics.gov.bc.ca'
port = 5439
pguser = os.environ['pguser']
pgpass = os.environ['pgpass']
conn_string = (
    f"dbname='{dbname}' "
    f"host='{host}' "
    f"port='{port}' "
    f"user='{pguser}' "
    f"password={pgpass}")

""" Used to clean report list printing"""
def print_list(report_string, report_list):
    print('\n' + report_string)
    for i, site in enumerate(report_list, 1):
        print(f"{i}: {site}")

# Will run at end of script to print out accumulated report_stats
def report(data):
    '''Reports out the data from the main program loop'''
    # Get script end time and convert to Americas/Vancouver for printing
    yvr_dt_end = (yvr_tz.normalize(
        datetime.now(local_tz)
                .astimezone(yvr_tz)))
    # If no objects were processed; do not print a report
    if data['no_new_data'] == data['sites']:
        logger.info("No API response contained new data")
        return
    print(f'Report: {__file__}\n')
    print(f'Config: {CONFIG}\n')
    
    if (report_stats['dt_build_success'] == None):
        #variable not changed build not attempted
        print('Google Search derived table build not attempted')
    elif (report_stats['dt_build_success'] == True):
        #variable changed to True build successful
        print(
            'Derived table build started at: '
            f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
            f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
            f'elapsing: {yvr_dt_end - yvr_dt_start}.')
    else:
        #variable changed to False build failed
        print('Google Search derived table build failed\n')
    
    print(
        'Microservice started at: '
        f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
        f'elapsing: {yvr_dt_end - yvr_dt_start}.')
    print(f'\nSites to process: {data["sites"]}')
    print(f'Successful API calls: {data["retrieved"]}')
    print(f'Failed API calls: {data["failed_api"]}')
    print(f'Failed loads to RedShift: {data["failed_rs"]}\n')

    # Print all processed sites
    if data['processed']:
        print_list('Objects loaded to S3 and copied to RedShift:', data['processed'])

    # If anything failed to copy to RedShift, print it.
    if data['failed_to_rs']:
        print_list('List of objects that failed to copy to Redshift:', data['failed_to_rs'])

    # If anything failed do to early exit, print it
    if data['failed_api_call']:
        print_list('List of sites that were not processed due to early exit:', data['failed_api_call'])

    if data['failed_verification']:
        print_list('List of sites that were not processed as they are not verified:', data['failed_verification'])


# Reporting variables. Accumulates as the the sites lare looped over
report_stats = {
    'sites': 0,  # Number of sites in config_search.json
    'retrieved': 0,  # Successful API calls
    'no_new_data': 0,  # Sites where last_loaded_date < 2 days
    'failed_api': 0,
    'failed_rs': 0,
    'processed': [],  # API call, load to S3, and copy to Redshift all OK
    'failed_to_rs': [],  # Objects that failed to copy to Redshift
    'failed_api_call': [],  # Objects not processed due to early exit
    'dt_build_success': None,  # True if successfull, False if failed
    'failed_verification':  [] # Holds non-verified site names
}

report_stats['sites'] = len(config_sites)
report_stats['retrieved'] = len(config_sites)  # Minus 1 if failure occurs

for site_item in config_sites:
    report_stats['failed_api_call'].append(site_item['name'])

# each site in the config list of sites gets processed in this loop
for site_item in config_sites:  # noqa: C901
    # read the config for the site name and default start date if specified
    site_name = site_item['name']
    site_okay = True
    
    # get the last loaded date.
    # may be None if this site has not previously been loaded into Redshift
    last_loaded_date = last_loaded(site_name)

    # if the last load is 2 days old, there will be no new data in Google
    if last_loaded_date is not None and last_loaded_date >= latest_date:
        report_stats['failed_api_call'].remove(site_name)
        report_stats['no_new_data'] += 1
        continue

    # determine default start
    start_date_default = site_item.get("start_date_default")
    # if no start date is specified in the config, set it to 16 months ago
    if start_date_default is None:
        start_date_default = date.today() - timedelta(days=480)
    # if a start date was specified, it has to be formatted into a date type
    else:
        start_date_default = datetime.strptime(
            start_date_default, '%Y-%m-%d').date()

    # Load 30 days at a time until the data in Redshift has
    # caught up to the most recently available data from Google
    while last_loaded_date is None or last_loaded_date <= latest_date:
        if not site_okay:
            break

        # if there isn't data in Redshift for this site,
        # start at the start_date_default set earlier
        if last_loaded_date is None:
            start_dt = start_date_default
        # offset start_dt one day ahead of last Redshift-loaded data
        else:
            start_dt = last_loaded_date + timedelta(days=1)

        # the start_dt cannot exceed the latest date
        if start_dt > latest_date:
            break

        # end_dt will be the lesser of:
        # (up to) 1 month ahead of start_dt OR (up to) two days before now.
        end_dt = min(start_dt + timedelta(days=30), latest_date)

        # prepare stream with header
        stream = io.StringIO("site|date|query|country|device|"
                             "page|position|clicks|ctr|impressions\n")

        # Limit each query to 20000 rows. If there are more than 20000 rows
        # in a given day, it will split the query up.
        rowlimit = 20000
        index = 0

        def daterange(start_date, end_date):
            """yields a generator of all dates from startDate to endDate"""
            logger.info("daterange called with startDate: %s and endDate: %s",
                        start_date, end_date)
            assert end_date >= start_date, (f'start_date: {start_date} '
                                            'cannot exceed end_date: '
                                            f'{end_date} in '
                                            'daterange generator')
            for _n in range(int((end_date - start_date).days) + 1):
                yield start_date + timedelta(_n)

        search_analytics_response = ''

        # loops on each date from start date to the end date, inclusive
        # initializing date_in_range avoids pylint [undefined-loop-variable]
        date_in_range = ()
        max_date_in_data = '0'
        for date_in_range in daterange(start_dt, end_dt):
            if not site_okay:
                break
            # A wait time of 250ms each query reduces chance of HTTP 429 error
            # "Rate Limit Exceeded", handled below
            wait_time = 0.25
            sleep(wait_time)

            index = 0
            while (index == 0 or ('rows' in search_analytics_response)):
                logger.info('%s %s', str(date_in_range), index)

                # The order of the values in the dimensions[] block of this
                #  Google Search API query determines the order of the keys[]
                #  values in the response body.
                # IMPORTANT: logic is tied to the current order; any change
                #  to the current order will require refactoring this script.
                bodyvar = {
                    "aggregationType": 'auto',
                    "startDate": str(date_in_range),
                    "endDate": str(date_in_range),
                    "dimensions": [
                        "date",
                        "query",
                        "country",
                        "device",
                        "page"],
                    "rowLimit": rowlimit,
                    "startRow": index * rowlimit}

                # This query to the Google Search API may eventually yield an
                # HTTP response code of 429, "Rate Limit Exceeded".
                # The handling attempt below will increase the wait time on
                # each re-attempt up to 10 times.

                # As a scheduled job, ths microservice will restart after the
                # last successful load to Redshift.
                retry = 1
                while True:
                    try:
                        search_analytics_response = service.searchanalytics()\
                            .query(siteUrl=site_name, body=bodyvar).execute()
                    except GoogleHttpError:
                        if retry == 11:
                            logger.error(("Failing with HTTP error after 10 "
                                          "retries with query time easening."))
                            report_stats['failed_verification'].append(site_name)
                            report_stats['failed_api'] += 1
                            report_stats['retrieved'] -= 1
                            # Break out of this loop
                            break  
                        
                        wait_time = wait_time * 2
                        logger.warning(
                            "retrying site %s: %s with wait time %s",
                            site_name, retry, wait_time)
                        retry = retry + 1
                        sleep(wait_time)
                    else:
                        break

                # Check to see if we were able to reach the site
                if retry ==  11:
                    # if not go to the next site.
                    logger.info('Site: %s  not verified. Skipping to next site.', site_name)
                    site_okay = False
                    index = index + 1
                    break

                index = index + 1

                if 'rows' in search_analytics_response:
                    for row in search_analytics_response['rows']:
                        outrow = site_name + "|"
                        for i, key in enumerate(row['keys']):
                            # keys[0] contains the date value.
                            if i == 0:
                                # Find max date in data to use in filename.
                                max_date_in_data = max(max_date_in_data, key)
                            # for now, we strip | from searches
                            key = re.sub(r'\|', '', key)
                            # for now, we strip \\ from searches
                            key = re.sub('\\\\', '', key)
                            outrow = outrow + key + "|"
                        outrow = \
                            outrow + str(row['position']) + "|" + \
                            re.sub(r'\.0', '', str(row['clicks'])) + "|" + \
                            str(row['ctr']) + "|" + \
                            re.sub(r'\.0', '', str(row['impressions'])) + "\n"
                        stream.write(outrow)

        if max_date_in_data < str(end_dt):
            logger.info('The date range in the request spanned %s - %s, '
                        'but the max date in the data retrieved was: %s',
                        str(start_dt), str(end_dt), str(max_date_in_data))

        # check if the stream contains no data
        if stream.tell() == 0:
            logger.warning('No data retrieved for %s over date request range '
                           '%s - %s. Skipping s3 object creation and '
                           'Redshift load steps.',
                           site_name, start_dt, end_dt)
            report_stats['no_new_data'] += 1
        else:

            # set the file name that will be written to S3

            site_fqdn = re.sub(
                r'^https?:\/\/', '', re.sub(r'\/$', '', site_name))
            outfile = ("googlesearch-"
                       f"{site_fqdn}-{start_dt}-{max_date_in_data}.csv")
            object_key = f"{config_source}/{config_directory}/{outfile}"

            # Write the stream to an outfile in the S3 bucket with naming
            # like "googlesearch-sitename-startdate-enddate.csv"
            resource.Bucket(config_bucket).put_object(Key=object_key,
                                                      Body=stream.getvalue())
            logger.info('PUT_OBJECT: %s:%s', outfile, config_bucket)
            object_summary = resource.ObjectSummary(config_bucket, object_key)
            logger.info('OBJECT LOADED ON: %s, OBJECT SIZE: %s',
                        object_summary.last_modified, object_summary.size)

            # S3 file path for report_stats
            s3_file_path = f's3://{config_bucket}/{object_key}'
            report_stats['failed_to_rs'].append(s3_file_path)

            # Prepare the Redshift COPY command.
            logquery = (
                f"copy {config_dbtable} "
                f"FROM 's3://{config_bucket}/{object_key}' "
                "CREDENTIALS 'aws_access_key_id={AWS_ACCESS_KEY_ID};"
                "aws_secret_access_key={AWS_SECRET_ACCESS_KEY}' "
                "IGNOREHEADER AS 1 MAXERROR AS 0 DELIMITER '|' "
                "NULL AS '-' ESCAPE TRUNCATECOLUMNS;")
            query = logquery.format(
                AWS_ACCESS_KEY_ID=os.environ['AWS_ACCESS_KEY_ID'],
                AWS_SECRET_ACCESS_KEY=os.environ['AWS_SECRET_ACCESS_KEY'])
            logger.info(logquery)

            # Load into Redshift
            with psycopg2.connect(conn_string) as conn:
                with conn.cursor() as curs:
                    try:
                        curs.execute(query)
                    # if the DB call fails, print error and place file in /bad
                    except psycopg2.Error:
                        logger.exception(
                            "FAILURE loading %s (%s index) over date range "
                            "%s to %s into %s. Object key %s.", site_name,
                            str(index), str(start_dt), str(end_dt),
                            config_dbtable, object_key.split('/')[-1])
                        report_stats['failed_rs'] += 1
                        clean_exit(1, 'Could not load to redshift.')
                    else:
                        report_stats['failed_to_rs'].remove(s3_file_path)
                        if max_date_in_data != str(0):
                            report_stats['processed'].append(s3_file_path)
                            logger.info(
                                "SUCCESS loading %s (%s index) over date "
                                "range %s to %s into %s. Object key %s.",
                                site_name, str(index), str(start_dt),
                                str(end_dt), config_dbtable,
                                object_key.split('/')[-1])
                        else:
                            # The s3 object is 68B and max_Date_in data == 0
                            logger.info(
                                "%s max_date_in_data == 0"
                                "Nothing copied to RedShift", site_name)
                            report_stats['no_new_data'] += 1

        # set last_loaded_date to end_dt to iterate through the next month
        last_loaded_date = end_dt

    # Remove site_name from failed lists
    report_stats['failed_api_call'].remove(site_name)

# Count all failed loads to RedShift
report_stats['failed_rs'] = len(report_stats['failed_to_rs'])
# Get DT build start time
yvr_dt_start = (yvr_tz
                    .normalize(
                        datetime.now(local_tz)
                        .astimezone(yvr_tz)))

# This query will INSERT INTO cmslite.google_dt
# cmslite.google_dt, a derived table built from google.googlesearch
# Get sql for dt build form dml folder
query = open('dml/{}'.format(dml_file), 'r').read()

# Execute the query and log the outcome
logger.info(query)
with psycopg2.connect(conn_string) as conn:
    with conn.cursor() as curs:
        try:
            curs.execute(query)
        except psycopg2.Error:
            logger.exception("Google Search DT loading failed")
            report_stats['dt_build_success'] = False
            report(report_stats)
            clean_exit(1, 'Could not rebuild DT in Redshift.')
        else:
            report_stats['dt_build_success'] = True
            logger.info("Google Search DT loaded successfully")
            report(report_stats)
            clean_exit(0, 'Finished successfully.')
