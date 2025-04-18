# Derived Assets to Redshift Microservice

This folder contains scripts and configuration files (within the [config.d](./config.d/) folder) that enable the the Derived-Assets-to-Redshift microservice implemented on the GDX-Analytics platform.

## `asset_data_to_redshift.py`

The Asset Data to Redshift microservice is invoked through pipenv and requires a `json` configuration file passed as the second command line argument to run. The configuration file format is described in more detail below. The approach to loading data from s3 to redshift is the same as in s3_to_redshift, but the access log files require additional processing. If truncate is set to `false`, the script will run multiple files at a time. Empty files are treated as bad by default and processing will stop if script hits any empty file. 

asset_data_to_redshift usage:

```
pipenv run python asset_data_to_redshift.py config.d/configfile.json
```

## `build_derived_assets.py`

The Build Derived Gov Assets microservice is run on the table generated by asset_data_to_redshift.py and requires a `json` configuration file passed as a second command line argument to run. It generates a derived table through the ddl/build_derived_assets.sql file after performing the required processing. This script is to be run after asset_data_to_redshift.py has successfully ran. If s3_to_redshift.py and asset_data_to_redshift.py converge in the future then this script will run after a successful completion of that script. 

build_derived_gov_assets usage:

```
python build_derived_gov_assets.py config.d/configfile.json
```

### Overview

This script reads an input `csv` file one at a time from a list of one or more files. The data contained in the input file is read into memory as a [Pandas dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) which is manipulated according to options set in the config file. The final dataframe is written out to S3 as `<bucket_name>/batch/<path-to-file>/<object_summary.key>.csv`, where:
- the bucket is specified from the configuration file,
- the path to file matches the path to the input file (conventionally, we use `/client/service_name/<object_summary.key>.csv`), and
- the object summary key can be thought of as the filename (S3 stores data as objects, not files).

Next, a Redshift transaction attempts to `COPY` that file into Redshift as a single `COMMIT` event (this is done in order to fail gracefully with rollback if the transaction cannot be completed successfully).

Finally, if the transaction failed then the input is copied from `<bucket_name>/client/<path-to-file>/<object_summary.key>.csv` to the "`bad`" folder at `<bucket_name>/processed/bad/<path-to-file>/<object_summary.key>.csv`. Otherwise the successful transaction will result in the input file being copied to the "`good`" folder: `<bucket_name>/processed/good/<path-to-file>/<object_summary.key>.csv`.

Log files are appended at the debug level into file called `asset_data_to_redshift.log` or `build_derived_asset.log` under a `logs/` folder which much be created manually. Info level logs are output to stdout. In the log file, events are logged with the format showing the log level, the function name, the timestamp with milliseconds, and the message: `INFO:__main__:2010-10-10 10:00:00,000:<log message here>`.

### Configuration

#### Environment Variables

The S3 to Redshift microservice requires the following environment variables be set to run correctly.

- `pgpass`: the database password for the microservice user;
- `AWS_ACCESS_KEY_ID`: the AWS access key for the account authorized to perform COPY commands from S3 to Redshift; and,
- `AWS_SECRET_ACCESS_KEY`: the AWS secret access key for the account authorized to perform COPY commands from S3 to Redshift.

#### Configuration File

The JSON configuration is required as a second argument when running the `asset_data_to_redshift.py` and `build_derived_assets.py` scripts. The two scripts share on config file that follows this structure:

- `"bucket"`: the label defining the S3 bucket that the microservice will reference.
- `"source"`: the top level S3 prefix for source objects after the bucket label, as in: `<bucket>/<source>/<client>/<doc>`.
- `"destination"`: the top level S3 prefix for processed objects to be stored, as in: `<bucket>/<destination>/<client>/<doc>`.
- `"directory"`: the S3 prefix to follow source or destination and before the `<doc>` objects.
- `"doc"`: a regex pattern representing the final object after all directory prefixes, as in: `<bucket>/<source>/<client>/<doc>`.
- `"dbschema"`: An optional String defaulting to `'microservice'` _(currently unused by `asset_data_to_redshift.py` and `build_derived_assets.py`)_.
- `"dbtable"`: The table to `COPY` the processed data into _with the schema_, as in: `<schema>.<table>`.
- `"column_count"`: The number of columns the processed dataframe should contain.
- `"columns"`: A list containing the column names of the input file.
- `"column_string_limit"`: A dictionary where keys are names of string type column to truncate, and values are integers indicating the length to truncate to.  
- `"dtype_dic_strings"`: A list where keys are the names of columns in the input data whose data will be formatted as strings.
- `"dtype_dic_bools"`: A list where keys are the names of columns in the input data whose data will be formatted as boolean
- `"delim"`: specify the character that deliminates data in the input `csv`.
- `"truncate"`: boolean (`true` or `false`) that determines if the Redshift table will be truncated before inserting data, or instead if the table will be extended with the inserted data. When `true` only the most 
recently modified file in S3 will be processed.
- `"dateformat"` a list of dictionaries containing keys: `field` and `format`
  - `"field"`: a column name containing datetime format data.
  - `"format"`: strftime to parse time. See [strftime documentation](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior) for more information on choices.
- `"schema_name"`: specify the target table schema,
- `"asset_host"`: the host domain for the assets,
- `"asset_source"`: the group/project name,
- `"asset_scheme_and_authority"`: the protocol scheme and the asset host
- `"empty_files_ok"`: Default is `false` but can be set to `true` for cases where empty files are determined ok to process. This is helpful to process multiple files at a time without stopping the script due to empty files being hit.
  

The structure of the config file should resemble the following:

```
{
  "bucket": String,
  "source": String,
  "destination": String,
  "directory": String,
  "doc": String,
  "dbschema": String,
  "dbtable": String,
  "columns": [String],
  "column_count": Integer,
  "replace": [
    {
      "field": String,
      "old": String,
      "new": String
    }
  ],
  "column_string_limit":{
    "<column_name_1>": Integer
  }
  "dateformat": [
    {
      "field": String,
      "format": String
    }
  ],
  "dtype_dic_strings": [String],
  "dtype_dic_bools": [String],
  "delim": String,
  "truncate": Boolean,
  "schema_name": String,
  "asset_host": String,
  "asset_source": String,
  "asset_scheme_and_authority": String,
  "empty_files_ok": Boolean
}
```


## Project Status

As new data sources become available, new configuration files will be prepared to support the consumption of those data sources. This project is ongoing.

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
