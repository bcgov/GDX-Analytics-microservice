"""parse a cmslite csv file from s3 and load it into Redshift"""
###################################################################
# Script Name   : cmslitemetadata_to_redshift.py
#
# Description   : Microservice script to load a cmslite csv file from s3
#               : and load it into Redshift
#
# Requirements  : You must set the following environment variables
#               : to establish credentials for the microservice user
#
#               : export AWS_ACCESS_KEY_ID=<<KEY>>
#               : export AWS_SECRET_ACCESS_KEY=<<SECRET_KEY>>
#               : export pgpass=<<DB_PASSWD>>
#
#
# Usage         : pip install -r requirements.txt
#               : python cmslitemetadata_to_redshift.py configfile.json
#

import re  # regular expressions
from io import StringIO
import os  # to read environment variables
import json  # to read json config files
import sys  # to read command line parameters
import itertools  # functional tools for creating and using iterators
from datetime import datetime
import logging
import boto3  # s3 access
from botocore.exceptions import ClientError
import pandas as pd  # data processing
import pandas.errors
import numpy as np
import psycopg2  # to connect to Redshift
from lib.redshift import RedShift
import lib.logs as log
from tzlocal import get_localzone
from pytz import timezone


def main():
    """Process S3 loaded CMS Lite Metadata file to Redshift"""

    local_tz = get_localzone()
    yvr_tz = timezone('America/Vancouver')
    yvr_dt_start = (yvr_tz
        .normalize(datetime.now(local_tz)
        .astimezone(yvr_tz)))

    logger = logging.getLogger(__name__)
    log.setup()


    def clean_exit(code, message):
        """Exits with a logger message and code"""
        logger.info('Exiting with code %s : %s', str(code), message)
        sys.exit(code)

    # we will use this timestamp to write to the cmslite.microservice_log table
    # changes to that table trigger Looker cacheing.
    # As a result, Looker refreshes its cmslite metadata cache
    # each time this microservice completes
    starttime = str(datetime.now())

    # Read configuration file
    if len(sys.argv) != 2:  # will be 1 if no arguments, 2 if one argument
        logger.error(
            "Usage: python27 cmslitemetadata_to_redshift.py configfile.json")
        clean_exit(1, 'bad configuration')
    configfile = sys.argv[1]
    if os.path.isfile(configfile) is False:  # confirm that the file exists
        logger.error("Invalid file name %s", configfile)
        clean_exit(1, 'bad configuration')
    with open(configfile) as _f:
        data = json.load(_f)

    # Set up variables from config file
    bucket = data['bucket']
    source = data['source']
    destination = data['destination']
    directory = data['directory']
    doc = data['doc']
    if 'dbschema' in data:
        dbschema = data['dbschema']
    else:
        dbschema = 'microservice'
    dbtable = data['dbtable']

    column_count = data['column_count']
    columns_metadata = data['columns_metadata']
    columns_lookup = data['columns_lookup']
    dbtables_dictionaries = data['dbtables_dictionaries']
    dbtables_metadata = data['dbtables_metadata']
    nested_delim = data['nested_delim']
    columns = data['columns']
    if 'sql_query' not in data:
        clean_exit(1, "bad configuration, set ddl location in json config file")
    ddl_file = data['sql_query']
    dtype_dic = {}
    if 'dtype_dic_strings' in data:
        for fieldname in data['dtype_dic_strings']:
            dtype_dic[fieldname] = str
    delim = data['delim']
    truncate = data['truncate']

    # set up S3 connection
    client = boto3.client('s3')  # low-level functional API
    resource = boto3.resource('s3')  # high-level object-oriented API
    # subsitute this for your s3 bucket name.
    my_bucket = resource.Bucket(bucket)
    bucket_name = my_bucket.name

    aws_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']


    # bucket = the S3 bucket
    # filename = the name of the original file being processed
    # (eg. example.csv)
    # batchfile = the name of the batch file. This will be appended to the
    # original filename path (eg. part01.csv -> "example.csv/part01.csv")
    # df = the dataframe to write out
    # columnlist = a list of columns to use from the dataframe.
    # Must be the same order as the SQL table.
    # If null (eg None in Python), will write all columns in order.
    # index = if not Null, add an index column with this label
    def to_s3(loc_batchfile, filename, loc_df, loc_columnlist, loc_index):
        """Funcion to write a CSV to S3"""
        # Put the full data set into a buffer and write it
        # to a "   " delimited file in the batch directory
        csv_buffer = StringIO()
        if loc_columnlist is None:  # no column list, no index
            if loc_index is None:
                loc_df.to_csv(csv_buffer,
                              header=True,
                              index=False,
                              sep="	",
                              encoding='utf-8')
            else:  # no column list, include index
                loc_df.to_csv(csv_buffer,
                              header=True,
                              index=True,
                              sep="	",
                              index_label=loc_index,
                              encoding='utf-8')
        else:
            if loc_index is None:  # column list, no index
                loc_df.to_csv(csv_buffer,
                              header=True,
                              index=False,
                              sep="	",
                              columns=loc_columnlist,
                              encoding='utf-8')
            # column list, include index
            else:
                loc_df.to_csv(csv_buffer,
                              header=True,
                              index=True,
                              sep="	",
                              columns=loc_columnlist,
                              index_label=loc_index,
                              encoding='utf-8')

        logger.info("Writing " + filename + " to " + loc_batchfile)
        resource.Bucket(bucket).put_object(Key=loc_batchfile + "/" + filename,
                                           Body=csv_buffer.getvalue())

        
    # Create a dictionary dataframe based on a column
    def to_dict(loc_df, section):
        '''build a dictionary type dataframe for a column with nested \
        delimeters'''
        # drop any nulls and wrapping delimeters, split and flatten:
        clean = loc_df.copy().dropna(
            subset=[section])[section].str[1:-1].str.split(
                nested_delim).values.flatten()
        # set to exlude duplicates
        _l = list(set(itertools.chain.from_iterable(clean)))
        # make a dataframe of the list
        return pd.DataFrame({section: sorted(_l)})

    
    # Check to see if the file has been processed already
    def is_processed(loc_object_summary):
        '''check S3 for objects already processed'''
        # Check to see if the file has been processed already
        loc_key = loc_object_summary.key
        filename = loc_key[loc_key.rfind('/') + 1:]  # get the filename string
        loc_goodfile = destination + "/good/" + key
        loc_badfile = destination + "/bad/" + key
        try:
            client.head_object(Bucket=bucket, Key=loc_goodfile)
        except ClientError:
            pass  # this object does not exist under the good destination path
        else:
            logger.info('%s was processed as good already.', filename)
            return True
        try:
            client.head_object(Bucket=bucket, Key=loc_badfile)
        except ClientError:
            pass  # this object does not exist under the bad destination path
        else:
            return True
        logger.info("%s has not been processed.", filename)
        return False

    
    def report(data):
        '''reports out the data from the main program loop'''
        # if no objects were processed; do not print a report
        if data["objects"] == 0:
            return
        print(f'Report {__file__}:')
        print(f'\nConfig: {configfile}')
        # Get times from system and convert to Americas/Vancouver for printing
        yvr_dt_end = (yvr_tz
            .normalize(datetime.now(local_tz)
            .astimezone(yvr_tz)))
        print(
            '\nMicroservice started at: '
            f'{yvr_dt_start.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
            f'ended at: {yvr_dt_end.strftime("%Y-%m-%d %H:%M:%S%z (%Z)")}, '
            f'elapsing: {yvr_dt_end - yvr_dt_start}.')
        print(f'\nObjects to process: {data["objects"]}')
        print(f'Objects successfully processed: {data["processed"]}')
        print(f'Objects that failed to process: {data["failed"]}')
        print(f'Objects output to \'processed/good\': {data["good"]}')
        print(f'Objects output to \'processed/bad\': {data["bad"]}')
        print(f'Objects loaded to Redshift: {data["loaded"]}')
        if data['good_list']:
            print(
            "\nList of objects successfully fully ingested from S3, processed, "
            "loaded to S3 ('good'), and copied to Redshift:")
            for i, meta in enumerate(data['good_list'], 1):
                print(f"{i}: {meta.key}")
        if data['bad_list']:
            print('\nList of objects that failed to process:')
            for i, meta in enumerate(data['bad_list'], 1):
                print(f"{i}: {meta.key}")
        if data['incomplete_list']:
            print('\nList of objects that were not processed due to early exit:')
            for i, meta in enumerate(data['incomplete_list'], 1):
                print(f"{i}: {meta.key}")
        if data['tables_loaded']:
            print('\nList of tables that were successfully loaded into Redshift:')
            [print(table) for table in data['tables_loaded']]
        if data['table_loads_failed']:
            print('\nList of tables that failed to load into Redshift:')
            [print(table) for table in data['table_loads_failed']]


              
    # This bucket scan will find unprocessed objects.
    # objects_to_process will contain zero or one objects if truncate=True;
    # objects_to_process will contain zero or more objects if truncate=False.
    objects_to_process = []
    for object_summary in my_bucket.objects.filter(Prefix=source + "/"
                                                   + directory + "/"):
        key = object_summary.key
        # skip to next object if already processed
        if is_processed(object_summary):
            continue

        logger.info("Processing %s", object_summary)
        # only review those matching our configued 'doc' regex pattern
        if re.search(doc + '$', key):
            # under truncate, we will keep list length to 1
            # only adding the most recently modified file to objects_to_process
            if truncate:
                if len(objects_to_process) == 0:
                    objects_to_process.append(object_summary)
                    continue
                # compare last modified dates of the latest and current obj
                if (object_summary.last_modified
                        > objects_to_process[0].last_modified):
                    objects_to_process[0] = object_summary
                else:
                    logger.info(
                        "skipping %s; less recent than %s", key,
                        object_summary.last_modified)
            else:
                # no truncate, so the list may exceed 1 element
                objects_to_process.append(object_summary)

    if truncate and len(objects_to_process) == 1:
        logger.info(('truncate is set. processing most recent file match: '
                     '%s (modified %s)'), objects_to_process[0].key,
                    objects_to_process[0].last_modified)

    # Reporting variables. Accumulates as the the loop below is traversed
    report_stats = {
        'objects':0,
        'processed':0,
        'failed':0,
        'good': 0,
        'bad': 0,
        'loaded': 0,
        'good_list':[],
        'bad_list':[],
        'incomplete_list':[],
        'tables_loaded':[],
        'table_loads_failed':[]
    }

    report_stats['objects'] = len(objects_to_process)
    report_stats['incomplete_list'] = objects_to_process.copy()

    # process the objects that were found during the earlier directory pass
    for object_summary in objects_to_process:
        # Check to see if the file has been processed already
        batchfile = destination + "/batch/" + object_summary.key
        goodfile = destination + "/good/" + object_summary.key
        badfile = destination + "/bad/" + object_summary.key

        # Load the object from S3 using Boto and set body to be its contents
        obj = client.get_object(Bucket=bucket, Key=object_summary.key)
        body = obj['Body']

        csv_string = body.read().decode('utf-8')

        # XX  temporary fix while we figure out better delimiter handling
        csv_string = csv_string.replace('	', ' ')

        # Check for an empty file. If it's empty, accept it as good and move on
        _df = None
        try:
            _df = pd.read_csv(StringIO(csv_string), 
                              sep=delim, 
                              index_col=False,
                              dtype=dtype_dic, 
                              usecols=range(column_count))
        except pandas.errors.EmptyDataError as _e:
            logger.exception('Exception reading %s', object_summary.key)
            report_stats['failed'] += 1
            report_stats['bad'] += 1
            report_stats['bad_list'].append(object_summary)
            report_stats['incomplete_list'].remove(object_summary)
            if str(_e) == "No columns to parse from file":
                logger.warning('%s is empty, keying to badfile and stopping.',
                           object_summary.key)
                outfile = badfile
            else:
                logger.warning('%s not empty, keying to badfile and stopping.',
                           object_summary.key)
                outfile = badfile
            try:
                client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
            except ClientError:
                logger.exception("S3 transfer failed")
            report(report_stats)
            clean_exit(1, f'{object_summary.key} was empty and was tagged as bad.')
        except ValueError:
            report_stats['failed'] += 1
            report_stats['bad'] += 1
            report_stats['bad_list'].append(object_summary)
            report_stats['incomplete_list'].remove(object_summary)
            logger.exception('ValueError exception reading %s', object_summary.key)
            logger.warning('Keying to badfile and proceeding.')
            outfile = badfile
            try:
                client.copy_object(Bucket=f"{bucket}",
                                   CopySource=f"{bucket}/{object_summary.key}",
                                   Key=outfile)
            except ClientError:
                logger.exception("S3 transfer failed")
            report(report_stats)
            clean_exit(1,f'Bad file {object_summary.key} in objects to process, '
                       'no further processing.')

        # set the data frame to use the columns listed in the .conf file.
        # Note that this overrides the columns in the file, and will give an
        # error if the wrong number of columns is present.
        # It will not validate the existing column names.
        
        try:
          _df.columns = columns

          # Run rename to change column names
          if 'rename' in data:
              for thisfield in data['rename']:
                  if thisfield['old'] in _df.columns:
                      _df.rename(columns={thisfield['old']: thisfield['new']},
                                inplace=True)

          # Run replace on some fields to clean the data up
          if 'replace' in data:
              for thisfield in data['replace']:
                  _df[thisfield['field']].str.replace(thisfield['old'],
                                                      thisfield['new'])

          # Clean up date fields, for each field listed in the dateformat array
          # named "field" apply "format". Leaves null entries as blanks instead
          # of NaT.
          if 'dateformat' in data:
              for thisfield in data['dateformat']:
                  _df[thisfield['field']] = pd.to_datetime(
                      _df[thisfield['field']]).apply(
                          lambda x: x.strftime(
                              thisfield['format']) if not pd.isnull(x) else '')                      
        except ValueError as _e:
          print(f'\n**An Error Occurred**\n{str(_e)}\n')
          outfile = badfile
          logger.exception('Exception parsing %s', object_summary.key)
          report_stats['failed'] += 1
          report_stats['bad'] += 1
          report_stats['bad_list'].append(object_summary)
          report_stats['incomplete_list'].remove(object_summary)
          try:
                client.copy_object(Bucket=f"{bucket}",
                               CopySource=f"{bucket}/{object_summary.key}",
                               Key=outfile)
          except ClientError:
            logger.exception("S3 transfer to processed/bad has failed")
          report(report_stats)
          clean_exit(1, f'{object_summary.key} not parsable. Check data format.')
          

        # We loop over the columns listed in the JSON configuration file.
        # There are three sets of values that should match to consider:
        # - columns_lookup
        # - dbtables_dictionaries
        # - dbtables_metadata

        # The table is built in the same way as the others, but this allows us
        # to resuse the code below in the loop to write the batch file and run
        # the SQL command.

        dictionary_dfs = {}  # keep the dictionaries in storage
        # loop starts at index -1 to process the main metadata table.

        # Build an aggregate query which will be used to make one transaction.
        # To add a new fields to be parsed, you must add both a lookup table
        # and a dictionary table. These can be joined in the LookML to 
        # allow querying the parsed out values in the lookup columns.
        copy_queries = {}
        for i in range(-1, len(columns_lookup)*2):
            # the main metadata table is built on the first iteration
            if i == -1:
                column = "metadata"
                dbtable = "metadata"
                key = None
                columnlist = columns_metadata
                df_new = _df.copy()
                df_new = df_new.reindex(columns = columnlist)   
            # The columns_lookup tables are built in the iterations 
            # for i < len(columns_lookup).
            # The columns_lookup tables contain key-value pairs.
            # The key is the node_id from the metadata.
            # The value is a number assigned to each unique parsed-out value from
            # the pipe-separated column from the metadata.
            elif i < len(columns_lookup):
                key = "key"
                column = columns_lookup[i]
                columnlist = [columns_lookup[i]]
                dbtable = dbtables_dictionaries[i]
                df_new = to_dict(_df, column)  # make dict a df of this column
                dictionary_dfs[columns_lookup[i]] = df_new
            # The metadata tables are built in the i - len(columns_lookup) iterations.
            # The metadata dictionary tables contain key value pairs. 
            # The key is the value assigned to the values in the lookup table,
            # The value is the unique, parsed out values from the pipe-separated 
            # column from the metadata.
            else:
                i_off = i - len(columns_lookup)
                key = None
                column = columns_lookup[i_off]
                columnlist = ['node_id', 'lookup_id']
                dbtable = dbtables_metadata[i_off]

                # retrieve the dict in mem
                df_dictionary = dictionary_dfs[column]

                # for each row in df
                df_new = pd.DataFrame(columns=columnlist)
                for iterrows_tuple in _df.copy().iterrows():
                    row = iterrows_tuple[1]
                    # iterate over the list of delimited terms
                    if row[column] is not np.nan:
                        # get the full string of delimited
                        # values to be looked up
                        entry = row[column]
                        # remove wrapping delimeters
                        entry = entry[1:-1]
                        if entry:  # skip empties
                            # split on delimiter and iterate on resultant list
                            for lookup_entry in entry.split(nested_delim):
                                node_id = row.node_id
                                # its dictionary index
                                lookup_id = df_dictionary.loc[
                                    df_dictionary[
                                        column] == lookup_entry].index[0]
                                # create the data frame to concat
                                _d = pd.DataFrame(
                                    [[node_id, lookup_id]], columns=columnlist)
                                df_new = pd.concat(
                                    [df_new, _d], ignore_index=True)

            # output the the dataframe as a csv
            to_s3(batchfile, dbtable + '.csv', df_new, columnlist, key)

            # append the formatted copy query to the copy_queries dictionary
            copy_queries[dbtable] = (
                f"COPY {dbtable}_scratch FROM \n"
                f"'s3://{bucket_name}/{batchfile}/{dbtable}.csv' \n"
                f"CREDENTIALS 'aws_access_key_id={aws_access_key_id};"
                f"aws_secret_access_key={aws_secret_access_key}' \n"
                "IGNOREHEADER AS 1 MAXERROR AS 0 \n"
                "DELIMITER '	' NULL AS '-' ESCAPE;\n")

        # prepare the single-transaction query
        query = f'BEGIN; \nSET search_path TO {dbschema};'
        for table, copy_query in copy_queries.items():
            start_query = (
                f'DROP TABLE IF EXISTS {table}_scratch;\n'
                f'DROP TABLE IF EXISTS {table}_old;\n'
                f'CREATE TABLE {table}_scratch (LIKE {table});\n'
                f'ALTER TABLE {table}_scratch OWNER TO microservice;\n'
                f'GRANT SELECT ON {table}_scratch TO looker;\n')
            end_query = (
                f'ALTER TABLE {table} RENAME TO {table}_old;\n'
                f'ALTER TABLE {table}_scratch RENAME TO {table};\n'
                f'DROP TABLE {table}_old;\n')
            query = query + start_query + copy_query + end_query
        query = query + 'COMMIT;\n'
        logquery = (
            query.replace
            (os.environ['AWS_ACCESS_KEY_ID'], 'AWS_ACCESS_KEY_ID').replace
            (os.environ['AWS_SECRET_ACCESS_KEY'], 'AWS_SECRET_ACCESS_KEY'))

        # Execute the transaction against Redshift using 
        # local lib redshift module.
        logger.info(logquery)
        spdb = RedShift.snowplow(batchfile)
        if spdb.query(query):
            outfile = goodfile
            report_stats['loaded'] += 1
            report_stats['tables_loaded'].append(dbschema + '.metadata')
        else:
            outfile = badfile
            report_stats['table_loads_failed'].append(dbschema + '.metadata')
        spdb.close_connection()

        # Copies the uploaded file from client into processed/good or /bad
        try:
            client.copy_object(
                Bucket=bucket,
                CopySource=bucket + '/' + object_summary.key,
                Key=outfile)
        except ClientError:
            logger.exception("S3 transfer failed")
            report(report_stats)
            clean_exit(
                1,
                f'S3 transfer of {object_summary.key} to {outfile} failed.')

        # exit with non-zero code if the file was keyed to bad
        if outfile == badfile:
            report_stats['failed'] += 1
            report_stats['bad'] += 1
            report_stats['bad_list'].append(object_summary)
            report_stats['incomplete_list'].remove(object_summary)
            report(report_stats)
            clean_exit(1,f'{object_summary.key} was processed as bad.')

        report_stats['processed']  += 1
        report_stats['good'] += 1
        report_stats['good_list'].append(object_summary)
        report_stats['incomplete_list'].remove(object_summary)

    # now we run the single-time load on the cmslite.themes
    with open('ddl/{}'.format(ddl_file), 'r') as file:
        query = file.read()
    query = query.format(dbschema=dbschema)

    if(len(objects_to_process) > 0):
        # Execute the query using local lib redshift module and log the outcome
        logger.info('Executing query:\n%s', query)
        spdb = RedShift.snowplow(batchfile)
        if spdb.query(query):
            outfile = goodfile
            report_stats['loaded'] += 1
            report_stats['tables_loaded'].append(dbschema + '.themes')

            # if the job was succesful, write to cmslite.microservice_log
            endtime = str(datetime.now())
            query = (f"SET search_path TO {dbschema}; "
                     "INSERT INTO microservice_log VALUES "
                     f"('{starttime}', '{endtime}');")
            if spdb.query(query):
                logger.info("timestamp row added to microservice_log "
                                "table")
                logger.info("start time: %s -- end time: %s",
                                 starttime, endtime)
            else:
                logger.exception(
                        "Failed to write to %s.microservice_log", dbschema)
                logger.info("To manually update, use: "
                                 "start time: %s -- end time: %s",
                                 starttime, endtime)
                clean_exit(1,'microservice_log load failed.')
        else:
            outfile = badfile
        spdb.close_connection()
        logger.info("finished %s", object_summary.key)
    
    report(report_stats)

    clean_exit(0,'Succesfully finished cmslitemetadata_to_redshift.')


if __name__ == '__main__':
    main()
