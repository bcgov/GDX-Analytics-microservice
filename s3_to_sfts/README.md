# S3 to SFTS microservice

This folder contains scripts, configuration files (within the [config.d](./config.d/) folder) that enable the the S3 to SFTS microservice implemented on the GDX-Analytics platform. 


## Overview

The [s3_to_sfts.py](./s3_to_sfts.py) script performs the function of copying objects from S3 into SFTS. This script can be used following the [Redshift to S3 microservice](../redshift_to_s3/) to holistically transfer data from our Redshift database and store it in STFS for access by clients (EC2 to execute the scripts and store and modify temporary files, S3 to store objects unloaded from Redshift, and SFTS to store data files). An example below demonstrates the use of the `s3_to_sfts.py` script to perform the file transfer from S3 and into SFTS ([skip to Usage Example section](#usage-example)). 

### Setup

The Pipfile included in this repository instructs pipenv on what dependencies are required for the virtual environment used to invoke the script. It must be installed using the following command:

```
pipenv install
```

### `s3_to_sfts.py`

The S3 to SFTS microservice requires:

 - A Java runtime environment that can be invoked with the command line `java`

 - a `json` configuration file passed as a second command line argument to run.

 - The following environment variables must be set:

  - `$xfer_path`

    the path to the folder containing the MOVEit XFer Client Tools. The XFer Client Tools can be downloaded from the link labelled "Client Tools Zip (EZ, Freely, Xfer)" at  https://community.ipswitch.com/s/article/Direct-Download-Links-for-Transfer-and-Automation-2018).

    For example:
    ```
    export xfer_path=/path/to/xfer/jar/files/
    ```
    the folder _must_ contain `jna.jar` and `xfer.jar`

  - `$sfts_user`

    the service account user configured for access to the Secure File Transfer system, and with write access to SFTS path defined by the value for `sfts_path` in the json configuration file.

    ```
    export sfts_user=username  ## do not include an IDIR/ prefix in the username
    ```

  - `$sfts_pass`

    the password associated with the `$sfts_user` service account.

The configuration file format is described in more detail below. Usage is like:

```
pipenv run python s3_to_sfts.py -c config.d/config.json
```

## Configuration

### Environment Variables

The S3 to SFTS microservice requires the following environment variables be set to run correctly.

- `sfts_user`: the SFTS username used with `sfts_pass` to access the SFTS database;
- `sfts_pass`: the SFTS password used with `sfts_user` to access the SFTS database;
- `AWS_ACCESS_KEY_ID`: the AWS access key for the account authorized to perform COPY commands from S3 to Redshift; and,
- `AWS_SECRET_ACCESS_KEY`: the AWS secret access key for the account authorized to perform COPY commands from S3 to Redshift.

### Configuration File

Store configuration file in in [config.d/](./config.d/).

The JSON configuration is required as an argument proceeding the `-c` flag when running the `s3_to_sfts.py` script.

The structure of the config file should resemble the following:

```
{
  "bucket": String,
  "source": String,
  "directory": String,
  "destination": String,
  "object_prefix": String,
  "dml": String,
  "header": Boolean,
  "sfts_path": String,
  "extension": String,
  "sql_parse_key": String,
  "start_date": String,
  "end_date": String,
  "date_list": [String]
}
```

The keys in the config file are defined as follows. All parameters are required in order to use one configuration file for both scripts (which is recommended for service encapsulation and ease of maintenance):

- `"bucket"`: the label defining the S3 bucket that the microservice will reference.
- `"source"`: the first prefix of source objects, as in: `"s3://<bucket>/<source>/.../<object>"`.
- `"directory"`: the last path prefix before the object itself: `"s3://<bucket>/<source>/<directory>/<object>"` or `"s3://<bucket>/<destination>/<good|bad|batch>/<source>/<directory>/<object>"`
- `"destination"`: the first prefix of processed objects, as in `"s3://<bucket>/<destination>/<good|bad|batch>"`.
- `"object_prefix"`: The final prefix of the object; treat this as a prefix on the filename itself.
- `"dml"`: The filename under the [`./dml`](./dml/) directory in this repository that contains the SQL statement to run as an UNLOAD command.
- `"header"`: Setting this to true will write a first row of column header values; setting as false will omit that row.
- `"sfts_path"`: The folder path in SFTS where the objects retrieved from S3 will be uploaded to.
- `"extension"`: A postfix to the file name. As an extension, it must include the "`.`" character before the extension type, such as: `".csv"`. If no extension is needed then the value should be an empty string, like `""`. The extension is applied to the file created by `s3_to_redshift.py` at the time of downloading the source object from S3 to the local filesystem where the script is running. The extension is never applied to the source object key on S3 (that key is defined by the Redshift UNLOAD function used in `redshift_to_s3`, which does not support custom object key extensions).
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

## Usage example
This example supposes that a client desires an "Example" service to transfer content from S3 to SFTS as a pipe delimited file.

The configuration file for this example service is created as: [`config.d/example.json`](./config.d/example.json).

The example service may be run once as follows:

```
$ pipenv run python s3_to_sfts.py -c config.d/example.json
```

This transferred that file (and any other files matching the configured `"object_prefix"` value if they had not already been transferred) from S3 to the BC Government SFTS endpoint. The script may also modify the filename before transfer by appending the value of the configured `"extension"` parameter.

## Project Status

As new projects require loading modeled data from S3 into SFTS, new configuration files will be prepared to support the consumption of those data sources.

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
