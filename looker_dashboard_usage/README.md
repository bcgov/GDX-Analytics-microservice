# Looker Dashboard Usage Microservice

This folder contains scripts and configuration files that enable the the Looker Dashboard Usage microservice implemented on the GDX-Analytics platform.

## `looker_dashboard_usage.py`

The Looker Dashboard Usage microservice is invoked through pipenv and requires a `json` configuration file passed as the second command line argument to run. Table data is queried from the GDX Analytics Looker internal database and saved to files in S3.


The configuration file format is described in more detail below.

looker_dashboard_usage usage:

```
pipenv run python looker_dashboard_usage.py looker_dashboard_usage.json
```

### Overview

This script connects to the Looker internal RDS (mysql) and downlaods four separate tables related to dashsbaorod history and usage: user, user_facts, dashboard, history.

 The data contained in the input file is read into memory as a [Pandas dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html).
 
 The final dataframe is written out to S3 as `<bucket_name>/looker_dashboard_usage/table_name.csv`, where:
- the bucket is specified from the configuration file,
- the table_name corresponds to the table name in the RDS


Log files are appended at the debug level into file called `looker_dashbaord_usage.log` under a `logs/` folder which much be created manually. Info level logs are output to stdout. In the log file, events are logged with the format showing the log level, the function name, the timestamp with milliseconds, and the message: `INFO:__main__:2010-10-10 10:00:00,000:<log message here>`.

### Configuration

#### Environment Variables

The Looker Dashboard Usage microservice requires the following environment variables be set to run correctly.

- `pgpass`: the database password for the looker user;
- `lookeruser`: the databasse username for the looekr RDS


#### Configuration File

The JSON configuration is required as a second argument when running the `looker_dashboard_usage.py` script. It follows this structure:

- `"bucket"`: the label defining the S3 bucket that the microservice will reference.
- `"source"`: the top level S3 prefix for source objects after the bucket label, as in: `<bucket>/<source>/<client>/<doc>`.
- `"destination"`: the top level S3 prefix for processed objects to be stored, as in: `<bucket>/<destination>/<client>/<doc>`.
- `"directory"`: the S3 prefix to follow source or destination and before the `<doc>` objects.


The structure of the config file should resemble the following:

```
{
  "bucket": String,
  "source": String,
  "destination": String,
  "directory": String
}
```


## Project Status

As new data sources become available, new configuration files will be prepared to support the consumption of those data sources. This project is ongoing.

## Getting Help

Please Contact the GDX Service desk for any analytics service help. 

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
