# MySQL to S3 microservice

This folder contains the script, configuration files (within the [config.d](./config.d/) folder), and SQL formatted DML files (within the [dml](./dml/) folder) that enable the the MySQL to S3 microservice implemented on the GDX-Analytics platform.


## Overview

The [mysql_to_s3.py](./mysql_to_s3.py) script performs the function of copying data from MySQL into S3 and EC2. A example below demonstrates the use of the `mysql_to_s3.py` script to perform an unload of data from MySQL into a file stored in S3 ([skip to Usage Example section](#usage-example)).  

### Setup

The Pipfiles included in this repository instructs pipenv on what dependencies are required for the virtual environment used to invoke these scripts. It must be installed using the following command:

```
pipenv install
```

The MySQL to S3 microservice requires:

 - a `json` configuration file passed as a second command line argument to run.

 - a `dml` SQL formatted query file to perform a `SELECT` command.

The configuration file format is described in more detail below. Usage is like:

```
pipenv run python mysql_to_s3.py -c config.d/config.json
```

#### Output File

The output object will be under the configured S3 bucket and path. The key for this object will resemble one like: "`data-type_client-name_data-feed_schedule_%Y%m%dT%H%M%S.extension`." The components of the underscore separated parts to that key are:

 - `data-type`: the source of data used by the feeds. eg: webdata, theq
 - `client-name`: the short name of the client who owns the data, commonly the ministry abbreviation
 - `data-feed`: a short description of the data contained in the file
 - `schedule`: the frequency that the file is delivered. eg: daily, weekly, monthly
 - `%Y%m%dT%H%M%S`: the time when the file was produced and not the contents of the file, specified by the mysql_to_s3 python script
 - `.extension` (OPTIONAL): the extension of the file, eg: .csv, .txt


## Configuration

### Environment Variables

The MySQL to S3 microservice requires the following environment variables be set to run correctly.

- `mysqluser`: the user with query access to the MySQL database; and,
- `mysqlpass`: the password for the `mysqluser`.
- `AWS_ACCESS_KEY_ID`: the AWS access key for the account authorized to perform COPY commands from MySQL to S3; and,
- `AWS_SECRET_ACCESS_KEY`: the AWS secret access key for the account authorized to perform COPY commands from MySQL to S3.

### Configuration File

Store configuration files in in [config.d/](./config.d/).

The JSON configuration is required as an argument proceeding the `-c` flag when running the `mysql_to_s3.py` script.

The structure of the config file should resemble the following:

```
{
  "bucket": String,
  "storage": String,
  "archive": String,
  "directory": String,
  "object_prefix": String,
  "dml": String,
  "database": String,
  "header": Boolean,
  "extension": String,
  "escapechar": String,
  "sep": String,
  "quoting": Number,
  "quotechar": String
}
```

The keys in the config file are defined as follows. All parameters are required in order to use one configuration file for both scripts (which is recommended for service encapsulation and ease of maintenance):

- `"bucket"`: the label defining the S3 bucket that the microservice will reference.
- `"storage"`: the first prefix of where the objects will be stored for the client, as in: `"s3://<bucket>/<storage>/.../<object>"`.
- `"archive"`: the first prefix of where processed objects are archived, as in `"s3://<bucket>/<archive>/<good|bad|batch>"`.
- `"directory"`: the last path prefix before the object itself: `"s3://<bucket>/<storage>/<directory>/<object>"` or `"s3://<bucket>/<archive>/<good|bad|batch>/<storage>/<directory>/<object>"`
- `"object_prefix"`: The final prefix of the object; treat this as a prefix on the filename itself.
- `"dml"`: The filename under the [`./dml`](./dml/) directory in this repository that contains the SQL statement to run.
- `"database"`: The MySQL database that will be used for the query.
- `"header"`: Setting this to true will write a first row of column header values; setting as false will omit that row.
- `"extension"`: [OPTIONAL] specify a file extension that you would like to append to the object. If no extension is set, no extension will be appended to the object. Needs to have a period included in the value specified. For example: `".csv"`
- `"escapechar"`: [OPTIONAL] Character used to escape sep and quotechar when appropriate, defaults to `null`
- `"sep"`: [OPTIONAL] specify a single ASCII character that is used to separate fields in the output file, such as a pipe character `|`, a comma `,`, or a tab `\t`. If `sep` is not set, it will default to use the pipe character `|` as the delimiter.
- `"quoting"`: [OPTIONAL] specify how quotes are used in the file. Use one of `0` (QUOTE_MINIMAL), `1` (QUOTE_ALL), `2` (QUOTE_NONNUMERIC) or `3` (QUOTE_NONE). Click [here](https://docs.python.org/3/library/csv.html) for more details on what the options do. Defaults to `0`
- `"quotechar"`: [OPTIONAL] character used to quote fields, defaults to `"`

### DML File

Store these in [dml/](./dml/).

This is a MySQL `SELECT` compatible query. The MySQL documentation for `SELECT` specifies how the `'select-statement'` must appear.

Note that there are some particular issues that are likely to cause problems:

For more information see the MySQL documentation for `SELECT`: https://dev.mysql.com/doc/refman/8.0/en/select.html

## Usage example
This example supposes that a client desires an "Example" service to transfer content from MySQL to S3 as a pipe delimited file.

The configuration file for this example service is created as: [`config.d/example.json`](./config.d/example.json); and the DML file that stores the SQL statement selecting the data they wish to copy from MySQL is created as: [`dml/example.sql`](./dml/example.sql).

The example service may be run once as follows:

```
$ pipenv run python mysql_to_s3.py -c config.d/example.json
```

This creates an object in S3, specifically into `S3://sp-ca-bc-gov-131565110619-12-microservices/client/test_microservice-iam-key/mysql_to_s3/`, which stores delimited content emitted from MySQL, based on the results of the configured `"dml"` value: [`"example.sql"`](./dml/example.json). The key of the object created under that path will resemble: `test_mysql-to-s3_%Y%m%dT%H%M%S.txt` (where the dates in the key name are computed values).

## Project Status

This project contains the minimal features to be used as a Proof of Concept for transfering data from a MySQL database to a S3 bucket.

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
