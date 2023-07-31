# Redshift to S3 microservice

This folder contains the script, configuration files (within the [config.d](./config.d/) folder), and SQL formatted DML files (within the [dml](./dml/) folder) that enable the the Redshift to S3 microservice implemented on the GDX-Analytics platform.


## Overview

The [redshift_to_s3.py](./redshift_to_s3.py) script performs the function of copying data from Redshift into S3 and EC2. This script can be used preceeding the [S3 to SFTS microservice](../s3_to_sfts/) to store files in S3 and EC2 as an intermediary service (EC2 to execute the scripts and store and modify temporary files, and S3 to store objects unloaded from Redshift). A example below demonstrates the use of the `redshift_to_s3.py` script to perform an unload of data from Redshift into a file stored in S3 ([skip to Usage Example section](#usage-example)).  

### Setup

The Pipfiles included in this repository instructs pipenv on what dependencies are required for the virtual environment used to invoke these scripts. It must be installed using the following command:

```
pipenv install
```

The Redshift to S3 microservice requires:

 - a `json` configuration file passed as a second command line argument to run.

 - a `dml` SQL formatted query file to perform inside a nesting `UNLOAD` command.

- The following environment variables must be set:

  - `$pguser`

    the user with query access to the Redshift database.

  - `$pgpass`

    the password for the `$pguser`.

The configuration file format is described in more detail below. Usage is like:

```
pipenv run python redshift_to_s3.py -c config.d/config.json
```

#### Output File

The output object will be under the configured S3 bucket and path. The key for this object will resemble one like: "`prefix_20200101_20200131_20200325T153025_part000`" (if `"start_date"` and `"end_date"` were set in the config file) or "`prefix_20200325T153025_part000`" (if `"start_date"` and `"end_date"` were not included in the config file). The components of the underscore separated parts to that key are:
 - object prefix (`prefix`): set in the config as `"object_prefix"` a static string;
 - query start (`20200101`): set in the config by `"start_date"` as a `YYYYMMDD` formatted date value or a string (`min`,`max`,`unsent`) to be computed by a query (more details in the configuration file section);
 - query end (`20200131`): set in the config as a static `YYYYMMDD` formatted date value or a string (`min`,`max`,`unsent`) to be computed by a query (more details in the configuration file section);
 - runtime timestamp (`20200325T153025`): used to identify independent runs of the same query.
 - `part000` an unavoidable artifact of the RedShift `UNLOAD` processing. See the section on `PARALLEL` in https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html for more detail.


## Configuration

### Environment Variables

The Redshift to S3 microservice requires the following environment variables be set to run correctly.

- `sfts_user`: the SFTS username used with `sfts_pass` to access the SFTS database;
- `sfts_pass`: the SFTS password used with `sfts_user` to access the SFTS database;
- `AWS_ACCESS_KEY_ID`: the AWS access key for the account authorized to perform COPY commands from Redshift to S3; and,
- `AWS_SECRET_ACCESS_KEY`: the AWS secret access key for the account authorized to perform COPY commands from Redshift to S3.

### Configuration File

Store configuration files in in [config.d/](./config.d/).

The JSON configuration is required as an argument proceeding the `-c` flag when running the `redshift_to_s3.py` script.

The structure of the config file should resemble the following:

```
{
  "bucket": String,
  "source": String,
  "archive": String,
  "directory": String,
  "object_prefix": String,
  "dml": String,
  "header": Boolean,
  "sql_parse_key": String,
  "start_date": String,
  "end_date": String,
  "date_list": String,
  "extension": String,
  "escape": Boolean,
  "delimiter": String,
  "addquotes": Boolean
}
```

The keys in the config file are defined as follows. All parameters are required in order to use one configuration file for both scripts (which is recommended for service encapsulation and ease of maintenance):

- `"bucket"`: the label defining the S3 bucket that the microservice will reference.
- `"storage"`: the first prefix of where the objects will be stored for the client, as in: `"s3://<bucket>/<storage>/.../<object>"`.
- `"archive"`: the first prefix of where processed objects are archived, as in `"s3://<bucket>/<archive>/<good|bad|batch>"`.
- `"directory"`: the last path prefix before the object itself: `"s3://<bucket>/<storage>/<directory>/<object>"` or `"s3://<bucket>/<archive>/<good|bad|batch>/<storage>/<directory>/<object>"`
- `"object_prefix"`: The final prefix of the object; treat this as a prefix on the filename itself.
- `"dml"`: The filename under the [`./dml`](./dml/) directory in this repository that contains the SQL statement to run as an UNLOAD command.
- `"header"`: Setting this to true will write a first row of column header values; setting as false will omit that row.
- `"sql_parse_key"`: [OPTIONAL] if the file referenced by `"dml"` contains a tag to be completed through some value that must be computed at runtime, reference that value here. It must be a tag that the `redshift_to_s3.py` script knows how to process (it must exist in that scripts `SQLPARSE` dictionary and have a function defined for it). Valid strings for this are:
  - `"pmrp_date_range"`: referenced in the dml SQL file as "`{pmrp_date_range}`". This populates a start and end date range for the select query based on the configured `"start_date"` and `"end_date"` values. If setting `"sql_parse_key"` to `"pmrp_date_range"` then you _MUST_ set a `"start_date"` _and_ an `"end_date"`.
  - `"pmrp_qdata_dates"`: referenced in the dml SQL file as "`{pmrp_qdata_dates}`". This populates the list of requested dates as chained OR conditions in the select query based on the configured `"date_list"` values. If setting `"sql_parse_key"` to `"pmrp_qdata_dates"` then you _MUST_ set a `"date_list"`.
- `"start_date"`: [Only use `"start_date"` if also setting `'slq_parse_key'`, otherwise exclude `"start_date"` from config file] Will be used to populate part of the resultant file name and may be used to determine query logic. It must be set to one of:
  - `"YYYYMMDD"` value where `YYYY` is a 4-digit year value, `MM` is a 2-digit month value, and `DD` is a two digit day value. For example: `"20200220"` would represent a start date of February 20th, 2020.
  - `"min"` where that is determined by the `MIN` value of the `date` column in `google.google_mybusiness_servicebc_derived`.
  - `"max"` where that is determined by the `MAX` value of the `date` column in `google.google_mybusiness_servicebc_derived`.
  - `"unsent"`, where it will determine the next start date based on the end date of the last successfully file sent by `s3_to_sfts.py`.
- `"end_date"`: [Only use `"end_date"` if also setting `'slq_parse_key'`, otherwise exclude `"end_date"` from config file] Will be used to populate part of the resultant file name and may be used to determine query logic. It must be set to one of:
  - `"YYYYMMDD"` value where `YYYY` is a 4-digit year value, `MM` is a 2-digit month value, and `DD` is a two digit day value. For example: `"20200220"` would represent a start date of February 20th, 2020.
  - `"min"` where that is determined by the `MIN` value of the `date` column in `google.google_mybusiness_servicebc_derived`.
  - `"max"` where that is determined by the `MAX` value of the `date` column in `google.google_mybusiness_servicebc_derived`.
  - `"unsent"` as an alias for `"max"`.
- `"date_list"`: [Only use `"date_list"` if also setting `'slq_parse_key'`, otherwise exclude `"date_list"` from config file] A list of arbitrary dates. Will be used to populate part of the resultant file name and may be used to determine query logic. It must be set as:
  - `["YYYYMMDD","YYYMMDD","YYYMMDD"]` value where `YYYY` is a 4-digit year value, `MM` is a 2-digit month value, and `DD` is a two digit day value. For example: `"20200220"` would represent a start date of February 20th, 2020. You may request one or multiple dates.
- `"extension"`: [OPTIONAL] specify a file extension that you would like to append to the object. If no extension is set, no extension will be appended to the object. Needs to have a period included in the value specified. For example: `".csv"`
- `"escape"`: [OPTIONAL] setting this to true will escape linefeeds `\n`, carrage returns `\r`, the escape character `\`, quotation mark characters `'` or `"` (if both ESCAPE and ADDQUOTES are specified in the UNLOAD command), or the delimiter character `|` pipe (default) or the character specified in `"delimiter"`, with a backslash `\`, defaults to `False`
- `"delimiter"`: [OPTIONAL] specify a single ASCII character that is used to separate fields in the output file, such as a pipe character `|`, a comma `,`, or a tab `\t`. If the delimiter is not set, it will default to use the pipe character `|` as the delimiter.
- `"addquotes"`: [OPTIONAL] setting this to true will surround all values in the file with double quotes `"`, defaults to `True`

### DML File

Store these in [dml/](./dml/).

This is simply a Redshift `UNLOAD` compatible query. The Redshift documentation for `UNLOAD` specifies how the `'select-statement'` must appear.

Note that there are some particular issues that are likely to cause problems:
- you may not use single quotes; since `UNLOAD` itself wraps the select-statement with single quotes. If you require single quotes in your statement, use a pair of single quotes instead
- The `SELECT` query can't use a `LIMIT` clause in the outer `SELECT`. Instead, use a nested `LIMIT` clause.

For more information see the Amazon Redshift documentation for UNLOAD: https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html

## Usage example
This example supposes that a client desires an "Example" service to transfer content from Redshift to S3 as a pipe delimited file.

The configuration file for this example service is created as: [`config.d/example.json`](./config.d/example.json); and the DML file that stores the SQL statement selecting the data they wish to copy from Redshift is created as: [`dml/pmrp_date_range.sql`](./dml/pmrp_date_range.sql).

The example service may be run once as follows:

```
$ pipenv run python redshift_to_s3.py -c config.d/example.json
```

This creates an object in S3, specifically into `S3://sp-ca-bc-gov-131565110619-12-microservices/client/pmrp_gdx/example`, which stores delimited content emitted from Redshift, based on the results of the configured `"dml"` value: [`"example.sql"`](./dml/example.json). The key of the object created under that path will resemble: `pmrp_YYYYMMDD_YYYYMMDD_YYYYMMDDTHH_part000` (where the dates in the key name are computed values).

## Project Status

As new projects require loading modeled data into S3, new configuration files will be prepared to support the consumption of those data sources.

This project is ongoing.

## Getting Help

For any questions regarding this project, please contact the GDX Analytics Team.

## Contributors

The GDX Analytics Team will be the main contributors to this project currently. They will maintain the code as well.

## License

```
Copyright 2015 Province of British Columbia

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
```
