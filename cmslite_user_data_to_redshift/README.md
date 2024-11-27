# CMSLite User Data to Redshift Microservice

This folder contains scripts and configuration files (within the [config.d](./config.d/) folder) that enable the the CMSLite User Data to Redshift microservice implemented on the GDX-Analytics platform.

## `cmslite_user_data_to_redshift.py`

The CMSLite User Data to Redshift microservice is invoked through pipenv and requires a `json` configuration file passed as the second command line argument to run. File processing is sequenced in the order of the last modified date of the files found for processing in S3. This microservice is similar to the `s3_to_redshift.py` microservice, but it is specifically tailored to process CMSLite User Data.

"Empty files" will be treated as bad files and processing will stop if script encounters one. We define empty files as any file where the:
* File has no data in it, no rows, no headers but not zero byte in size.

The configuration file format is described in more detail below.

cmslite_user_data_to_redshift usage:

```
pipenv run python cmslite_user_data_to_redshift.py config.d/configfile.json
```

### Overview

This script reads a compressed input `tgz` file one at a time from a list of one or more files. The compressed file is then unpacked into 4 uncompressed files. The data contained in these unpacked files is read into memory as a [Pandas dataframe](https://pandas.pydata.org/pandas-docs/stable/reference/frame.html) which is manipulated according to options set in the config file. The final dataframe is written out to S3 as `<bucket_name>/batch/<path-to-file>/<unpacked_file>.csv.key`, where:
- the bucket is specified from the configuration file,
- the path to file matches the path to the input file (conventionally, we use `/client/service_name/<object_summary.key>.tgz`), and
- the object summary key can be thought of as the filename (S3 stores data as objects, not files).

Next, a Redshift transaction attempts to `COPY` that file into Redshift as a single `COMMIT` event (this is done in order to fail gracefully with rollback if the transaction cannot be completed successfully).

Finally, if the transaction failed then the input is copied from `<bucket_name>/client/<path-to-file>/<object_summary.key>.tgz` to the "`bad`" folder at `<bucket_name>/processed/bad/<path-to-file>/<object_summary.key>.tgz`. Otherwise the successful transaction will result in the input file being copied to the "`good`" folder: `<bucket_name>/processed/good/<path-to-file>/<object_summary.key>.tgz`.

Log files are appended at the debug level into file called `cmslite_user_data_to_redshift.log` under a `logs/` folder which much be created manually. Info level logs are output to stdout. In the log file, events are logged with the format showing the log level, the function name, the timestamp with milliseconds, and the message: `INFO:__main__:2010-10-10 10:00:00,000:<log message here>`.

### Configuration

#### Environment Variables

The CMSLite User Data to Redshift microservice requires the following environment variables be set to run correctly.

- `pgpass`: the database password for the microservice user;
- `AWS_ACCESS_KEY_ID`: the AWS access key for the account authorized to perform COPY commands from CMSLite User Data to Redshift; and,
- `AWS_SECRET_ACCESS_KEY`: the AWS secret access key for the account authorized to perform COPY commands from CMSLite User Data to Redshift.

#### Configuration File

The JSON configuration is required as a second argument when running the `cmslite_user_data_to_redshift.py` script. It follows this structure:

- `"bucket"`: the label defining the S3 bucket that the microservice will reference.
- `"source"`: the top level S3 prefix for source objects after the bucket label, as in: `<bucket>/<source>/<client>/<doc>`.
- `"destination"`: the top level S3 prefix for processed objects to be stored, as in: `<bucket>/<destination>/<client>/<doc>`.
- `"directory"`: the S3 prefix to follow source or destination and before the `<doc>` objects.
- `"doc"`: a regex pattern representing the final object after all directory prefixes, as in: `<bucket>/<source>/<client>/<doc>`.
- `"schema"`: The schema to `COPY` the processed data into _with the table_, as in: `<schema>.<table>`.
- `"truncate"`: boolean (`true` or `false`) that determines if the Redshift table will be truncated before inserting data, or instead if the table will be extended with the inserted data. When `true` only the most recently modified file in S3 will be processed.
- `"delim"`: specify the character that deliminates data in the input `csv`.
- `"files"`: dictionary for each file found in the uncompressed `"doc"`  file which specifies the details of the destination tables
  - `"dbtable"`: The table to `COPY` the processed data into _with the schema_, as in: `<schema>.<table>`.
  - `"column_count"`: The number of columns the processed dataframe should contain.
  - `"columns"`: A list containing the column names of the input file.
  - `"dateformat"` a list of dictionaries containing keys: `field` and `format`
    - `"field"`: a column name containing datetime format data.
    - `"format"`: strftime to parse time. See [strftime documentation](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior) for more information on choices.


The structure of the config file should resemble the following:

```
{
  "bucket": String,
  "source": String,
  "destination": String,
  "directory": String,
  "doc": String,
  "schema": String,
  "truncate": Boolean,
  "delim": String,
  "files": {
    String: {
      "dbtable": String,
      "column_count": Integer,
      "columns": [String],
      "dateformat": [
        {
          "field": String,
          "format": String
        }
      ],
    }
  }
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
