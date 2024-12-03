# Google Search Console API microservices

This directory contains scripts, configs, and DDL files describing the Google Search console API calling microservice implemented on the GDX-Analytics platform. 

Information on the shared approached to [credentials and authentication](#credentials-and-authentication) can be found below.

### Google Search Console API Loader Microservice

The `google_search.py` script automates the loading of Google Search data into S3 (as `.csv`) and Redshift (the `google.googlesearch` schema as defined by `google.googlesearch.sql`) via the Google Search Console API.

The Google Search Console API documentation is here: https://developers.google.com/webmaster-tools/search-console-api-original. The latest data available from the Google Search API is from two days ago (relative to "_now_"). The Search Console API provides programmatic access to most of the functionality of Google Search Console.

The Google Search Console is here: https://search.google.com/search-console. This console helps to visually identify which Properties you have verified owner access to and allows manual querying of slightly more recent data than the API provides programmatic access to (you can see data from 1 day ago, instead of from 2 days ago).

To illustrate: on a Friday, the Search Console web interface would show search data on your property from a maximum range of search data from 18 months ago up until Thursday. However, the Search Console API would only be able to collect a maximum range of search data from 18 months ago up until Wednesday.

The accompanying `google_search.json` configuration file specifies:
 * the S3 bucket where the responses to API queries will be loaded into for storage;
 * the database table where the files stored into S3 will later be loaded to;
 * the Site URLs per property as defined in Search Console, such as "http://www.example.com/" (for a URL-prefix property) or "sc-domain:example.com" (for a Domain property); and
 * optional query start dates per property.

As mentioned, property types may be either _URL-prefixed_ or _Domain properties_. Domain properties delivery search query results for all subdomains and pages contained in that domain. URL-prefixed properties will only return search query results for pages prefixed with that URL. At the Domain property level, data aggregation performed at Google's end may have less detail than the equivalent URL-prefixed data, and as a result, you may observe data discrepancies when comparing one to the other.

We adjust for this difference by preferentially loading data from the URL-prefixed property instead of the Domain property (where both exist) into a persistent derived table generated at the end of the `google_search.py` script.

The microservice will begin loading Google Search data from the date specified in the configuration as `"start_date_default"`. If that is unspecified in the configuration, the script will attempt to load data from 18 months ago relative to the date when the script is run. If more recent data already exists in Redshift; it will load data from _the day after_ the most recent date that has already been loaded into Redshift up to a latest date of two-days ago (relative to the date on which the script is being run).

When run, the script collects property data in batches of 30 days at a time before posting a data file into the S3 bucket specified in the config. If querying recent data, the file will contain 30 days or fewer. For instance: If you set this job up as a cron task, the data file for a given property will typically contain only one day worth of data.

Log files are appended at the debug level into file called `google_search.log` under a `logs/` folder which must be created manually. Info level logs are output to stdout. In the log file, events are logged with the format showing the log level, the function name, the timestamp with milliseconds, and the message: `INFO:__main__:2010-10-10 10:00:00,000:<log message here>`.

#### Configuration

##### Environment Variables

The Google Search API loader microservice requires the following environment variables be set to run correctly.

- `GOOGLE_MICROSERVICE_CONFIG`: the path to the json configuration file, e.g.: `\path\to\google_search.json`;
- `pguser`: the database username for the microservice user;
- `pgpass`: the database password for the microservice user;
- `AWS_ACCESS_KEY_ID`: the AWS access key for the account authorized to perform COPY commands from S3 to Redshift; and,
- `AWS_SECRET_ACCESS_KEY`: the AWS secret access key for the account authorized to perform COPY commands from S3 to Redshift.

##### Configuration File

The JSON configuration is loaded as an environmental variable defined as `GOOGLE_MICROSERVICE_CONFIG`. It follows this structure:

- `"bucket"`: a string to define the S3 bucket where CSV Google Search API query responses are stored.
- `"dbtable"`: a string to define the Redshift table where the S3 stored CSV files are inserted to to after their creation.
- `"sites"`: a JSON array containing objects defining a `"name"` and an optional `"start_date_default"`.
  - `"name"`: the property URL-prefixed or Domain to query the Google Search API on.
  - `"start_date_default"`: an _optional_ key identifying where to begin queries from as a YYYY-MM-DD string. If excluded, the default behaviour is to look back to the earliest date that the Google Search API exposes, which is 16 months (scripted as 480 days).

```
{
    "bucket": string,
    "dbtable": "google.googlesearch",
    "sites":[
        {
        "name":"https://www2.gov.bc.ca/"
        },
        {
        "name":"sc-domain:gov.bc.ca",
        "start_date_default":"2020-01-01"
        }
    ]
}
```

##### Command Line Arguments

- `-c` or `--conf`: the microservice configuration file;

### Credentials and Authentication
The `google_search.py` script uses [Google OAuth 2.0 for Installed Applications](https://googleapis.github.io/google-api-python-client/docs/oauth-installed.html) and the `flow_from_clientsecrets` library. Credentials configuration file `'credentials_search.json'` is required to run the script. 

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
- `redirect_uri` specifies a loopback address. As per [Google's documentation](https://developers.google.com/identity/protocols/oauth2/resources/loopback-migration), this can be any open port. The `google_search.py` scipt have been coded to use port 4200   

## Project Status

As clients provide GDX Analytics with access to their Google Search profiles, they will be added to the configuration file to be handled by the microservice.

## Getting Help

Please Contact the GDX Service desk for any analytics service help. For inquiries about Google Search API integration or for inquiries about starting a new analytics account for Government, please contact The GDX Analytics team.

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
