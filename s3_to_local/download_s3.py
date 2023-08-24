# Script version 1.0.0 
import os
import boto3
import json

def download_s3_folder(bucket_name, prefix, local_directory, aws_access_key_id, aws_secret_access_key, aws_default_region, destination_key):
    # Create S3 clients and resources using provided credentials and region
    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_default_region)
    s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=aws_default_region)
    
    # Create a paginator to list objects in the S3 bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket_name, 'Prefix': prefix}

    # Iterate through each page of objects in the bucket
    for page in paginator.paginate(**operation_parameters):
        if 'Contents' in page:
            for s3_object in page['Contents']:
                # Get the file name and construct the local file path
                file_name = s3_object['Key']
                local_file_path = os.path.join(local_directory, os.path.basename(file_name))

                # Skip directories
                if file_name.endswith('/'):
                    continue

                # Create the directory structure if it doesn't exist
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                try:
                    # Download the file from S3 to the local file path
                    s3_client.download_file(bucket_name, file_name, local_file_path)
                    print(f"Downloaded {file_name} to {local_file_path}")

                    # Move the file to the new destination directory in S3
                    destination_path = os.path.join(destination_key, os.path.basename(file_name))
                    s3_resource.Object(bucket_name, destination_path).copy_from(
                        CopySource={'Bucket': bucket_name, 'Key': file_name}
                    )
                    s3_resource.Object(bucket_name, file_name).delete()
                    print(f"Moved {file_name} to {destination_path}")

                except Exception as e:
                    print(f"Error downloading {file_name}: {e}")

# Load configuration from JSON file
with open('config.json') as config_file:
    config = json.load(config_file)

# Extract configuration values
bucket_name = config['bucket_name']
prefix = config['prefix']
local_directory = config['local_directory']
aws_access_key_id = config['aws_access_key_id']
aws_secret_access_key = config['aws_secret_access_key']
aws_default_region = config['aws_default_region']
destination_key = config['destination_key']

# Call the main function
download_s3_folder(bucket_name, prefix, local_directory, aws_access_key_id, aws_secret_access_key, aws_default_region, destination_key)
