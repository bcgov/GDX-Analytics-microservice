# Google API microservices

This directory contains scripts, configs, and DDL files describing the Google Business Profile Performance API calling microservice implemented on the GDX-Analytics platform. 

Information on the shared approached to [credentials and authentication](#credentials-and-authentication) can be found below.

### Google My Business API Loader microservice

The `google_mybusiness.py` script pulling the Google Business Profile Performance API data for locations according to the accounts specified in `google_mybusiness.json`. The metrics from each location are consecutively recorded as `.csv` files in S3 and then copied to Redshift.

Google makes location insights data available for a time range spanning 18 months ago to 3 days ago (as tests have determined to be a reliable "*to date*"). From the Google Business Profile Performance API [BasicMetricsRequest reference guide](https://developers.google.com/my-business/reference/performance/rest/v1/locations/getDailyMetricsTimeSeries):
> The maximum range is 18 months from the request date. In some cases, the data may still be missing for days close to the request date. Missing data will be specified in the metricValues in the response.

The script iterates each location for the date range specified on the date range specified by config keys `start_date` and `end_date`. If no range is set (those key values are left as blank strings), then the script attempts to query for the full range of data availability.

Log files are appended at the debug level into file called `google_mybusiness.log` under a `logs/` folder which much be created manually. Info level logs are output to stdout. In the log file, events are logged with the format showing the log level, the function name, the timestamp with milliseconds, and the message: `INFO:__main__:2010-10-10 10:00:00,000:<log message here>`.

#### Table

The `google.mybusiness` schema is defined by the google.mybusiness.sql file.

#### Configuration

##### Environment Variables

The Google Search API loader microservice requires the following environment variables be set to run correctly.

- `pgpass`: the database password for the microservice user;
- `AWS_ACCESS_KEY_ID`: the AWS access key for the account authorized to perform COPY commands from S3 to Redshift; and,
- `AWS_SECRET_ACCESS_KEY`: the AWS secret access key for the account authorized to perform COPY commands from S3 to Redshift.

##### Command Line Arguments

- `-o` or `--oauth`: the OAuth Credentials JSON file;
- `-a` or `--auth`: the stored authorization dat file;
- `-c` or `--conf`: the microservice configuration file;
- `-d` or `--debug`: runs the microservice in debug mode (currently unsupported).

##### Configuration File

The JSON configuration is required, following a `-c` or `--conf` flag when running the `google_mybusiness.py` script. It follows this structure:

- `"bucket"`: a string to define the S3 bucket where CSV Google My Business API query responses are stored.
- `"dbtable"`: a string to define the Redshift table where the S3 stored CSV files are copied to after their creation.
- `"metrics"`: an list containing the list of metrics to pull from Google My Business
- `"locations"`: an object that annotates account information from clients that have provided us access”
  - `"client_shortname"`: the client name to be recorded in the client column of the table for filtering. This shortname will also map the path where the `.csv` files loaded into AWS S3 as `'client/google_mybusiness_<client_shortname>/'`.
  - `"name"`: The account Name label
  - `"names_replacement"`: a list to replace matched values from the locations under this account as suggested by: `['find','replace']`. For example, in the case of Service BC, all locations are prefixed with "Service BC Centre". We replace this with nothing in order to get _just_ the unique names (the locations' community names).
  - `"id"`: the location group, used for validation when querying the API.
  - `"start_date"`: the query start date as `"YYYY-MM-DD"`, leave as `""` to get the oldest data possible (defaults to the longest the API accepts, 18 months ago)
  - `"end_date"`: the query end date as `"YYYY-MM-DD"`, leave as `""` to get the most recent data possible (defaults to the most recent that the API has been tested to provide, 3 days ago)


### Credentials and Authentication
All three scripts use [Google OAuth 2.0 for Installed Applications](https://googleapis.github.io/google-api-python-client/docs/oauth-installed.html) and the `flow_from_clientsecrets` library. Credentials configuration file `'credentials_mybusiness.json'` is required to run the script. 

Go to your Google Console at https://console.cloud.google.com/. 

If you do not have a project, create a new project with these steps:

- Navigate to https://console.developers.google.com/projectcreate
- Give your project a name and location, and select 'CREATE'. This will open a new page showing your Console with your new project in a Project info info-box.

Follow these steps to setup credentials and authentication:

1. In the left-hand column, select the option "APIs & Services" and then "Credentials".
  You should see a "Remember to configure the OAuth consent screen with information about your application" alert.
    - Note! If you ever navigate away from the wizard described in the steps below, click on the hamburger/Navigation menu beside the Google Cloud and use the Recent tool to return to your previous location.

1. Select the CONFIGURE CONSENT SCREEN button on the right to activate the set-up wizard. 
    1. OAuth consent screen - The information you enter here will be shown on a consent screen to a user. The only information necessary is the App Name, user support email, and developer contact email.
        1. Select "External" and "CREATE".
        3. (Required) Add an App name
        4. (Required) Add a User support email
        5. (Required) Add Developer contact information
        6. Select "SAVE AND CONTINUE"
    2. Scopes - The set-up wizard will go to Scopes next. Scopes express the permissions that you request users to authorise for your app and allow your project to access specific types of private user data from their Google Account. You will add 3 scopes linked to 2 APIs in this step.
        1. Before adding scopes, make sure the 2 APIs are enabled:
             1. Right-click on "Enabled APIs and services", and open in a new tab.
             2. In the "Search for resources, docs, products and more" enter "My Business Account Management API" and select Search. Select and ENABLE.
             3. Now search for "Google Search Console API". Select and ENABLE.
             4. You can close this tab.
        2. On your wizard tab, in the Scopes section, select ADD OR REMOVE SCOPES and an 'Update elected scopes' dialogue will open on the right. 
        3. Search and enable scopes using these steps:
           1. Search for "auth/business.manage". Select with the checkbox. Select UPDATE at the bottom.
           2. Then search for "/auth/webmasters". Select with the checkbox. Select UPDATE at the bottom.
           3. Then search for "auth/webmasters.readonly". Select with the checkbox. Select UPDATE at the bottom.
        4. Select SAVE AND CONTINUE at the bottom of Scopes.
    3. Test Users - Next in the wizard, you will add test users, users that can use your app when publishing status is set to testing.
        1. Add a test user by entering an email address. Email addresses must be associated with an active Google Account, Google Workspace account or Cloud Identity account.
        2. Select "SAVE AND CONTINUE"
    4. Summary - You will then be taken to a summary screen showing all the parameters you have set. 
    - If anything is set incorrectly you can select "EDIT" and redo the options.
    - If everything is correct select "BACK TO DASHBOARD" at the bottom of the screen.
  
2. Follow the steps below to generate a client ID:
    1. In the Credentials tab on the left-side, select "+ CREATE CREDENTIALS" in the top options bar, and choose "OAuth client ID" from the drop down.
    2. Specify what type of application you are creating credentials for.
    3. Select Application type = "Desktop app" 
    4. Change the name of the client ID so that it is recognizable from other IDs you may create.
    5. Select "CREATE"
  - This will produce a Client ID and Client Secret. It is recommended that you download the json file as 'credentials_<<application_name>>.json where <<application_name>> is replaced with the Google Miccroservice you are trying to run. Save this file to your current working directory.
  - When you first run the program with this file it will ask you to do an OAuth validation, which will create a dat credential file for authorization.
  - Note that OAuth access is restricted to the test users listed on your OAuth consent screen
 
1. Set up the authentication in each script
- In each script, the `flow_from_clientsecrets` process initializes the OAuth2 authorization flow. It takes the following arguments:
- `CLIENT_SECRET`: the OAuth Credentials JSON file script argument (eg. `'credentials.dat'`)
- `scope` is Google APIs authorization web address (eg. `'https://www.googleapis.com/auth/webmasters.readonly'`)
- `redirect_uri` specifies a loopback address. As per [Google's documentation](https://developers.google.com/identity/protocols/oauth2/resources/loopback-migration), this can be any open port. The `google_mybusiness.py` scipt has been coded to use port 4201   

## Project Status

As clients provide GDX Analytics with access to their My Business profiles, they will be added to the configuration file to be handled by the microservice.

## Getting Help

Please Contact the GDX Service desk for any analytics service help. For inquiries about Google My Business API integration or for inquiries about starting a new analytics account for Government, please contact The GDX Analytics team.

## Contributors

The GDX analytics team will be the main contributors to this project currently and will maintain the code.

## License

Copyright 2015 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
