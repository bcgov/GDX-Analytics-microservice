# Google API microservices

This directory contains the script, configs, and DDL files describing the Google My Business Service BC Derived API calling microservice implemented on the GDX-Analytics platform.

### Google My Business Service BC Derived
The script `google_my_business_servicebc_derived.py` automates the build of `google_mybusiness_servicebc_derived` which is a compilation of 3 tables within Redshift. The three tables to be combined are `google.locations`, `servicebc.datedimension`, and `servicebc.office_info`. These table names are predefined in the SQL query. The resulting table will be named based off of the config variables (noted below.) With the default json file as is the file will be named `google.google_mybusiness_servicebc_derived`.

#### Configuration 

The JSON configuration is required, following a `-c` or `--conf` flag when running the `config_servicebc.json` script. It follows this structure:   
- `"schema"`: a string to define the Redshift schema that will contain the resulting table
- `"database table"`: a string to define the name of the resulting Redshift table from the build
- `"dml"`: a string to define the name of a file containing a SQL query. 

##### Environment Variables

The Google My Business Service BC Derived microservice requires the following environment variables be set to be run correctly. 

- `pguser`: the database username for the microservice user;
- `pgpass`: the database password for the microservice user;

##### Command Line Arguments

- `-c` or `--conf`: the microservice configuration file;

## Project Status

As clients provide GDX Analytics with access to their Google Search of My Business profiles, they will be added to the configuration file to be handled by the microservice.

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
