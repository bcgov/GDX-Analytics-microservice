# S3 File Downloader Script

This script allows you to download files from an Amazon S3 bucket to a local directory, while also moving the downloaded files to a new destination within the S3 bucket.

## Prerequisites

- Python 3.x installed on your system.
- An Amazon Web Services (AWS) account with access to the S3 service.
- AWS access key ID and secret access key with appropriate permissions to read and write to the specified bucket.

## Usage

1. Clone or download this repository to your local machine.

2. Install required Python packages using pip or pip3:
- pip install boto3 
- pip3 install boto3

3. Create a `config.json` file in the same directory using the config_sample.json script provided and replace the placeholders with your actual values.

```json
{
  "bucket_name": "YOUR_BUCKET_NAME",
  "prefix": "YOUR_PREFIX",
  "local_directory": "YOUR_LOCAL_DIRECTORY",
  "aws_access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
  "aws_secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY",
  "aws_default_region": "YOUR_AWS_REGION",
  "destination_key": "YOUR_DESTINATION_KEY"
}

```
## Run the script using the following command:

python3 download_s3.py

## Configuration file explained.

```json
{
  "bucket_name": "The name of the S3 bucket you want to download files from",
  "prefix": "The prefix of the files you want to download within the bucket",
  "local_directory": "The local directory where downloaded files will be saved",
  "aws_access_key_id": "Your AWS access key ID",
  "aws_secret_access_key": "Your AWS secret access key",
  "aws_default_region": "The AWS region of your S3 bucket",
  "destination_key": "The destination key within the bucket where downloaded files will be moved"
} 
```
