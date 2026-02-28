import boto3
import hashlib

from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

def upload_file_to_s3(file_path, bucket_name, s3_key):
    """upload a file to an S3 bucket and store its MD5 hash in metadata for
        integrity check.

    Args:
        file_path (str): The path to the file to be uploaded.
        bucket_name (str): The name of the S3 bucket to upload the file to.
        s3_key (str): The key under which the file will be stored in S3.
    """
    try:
        s3_client = boto3.client('s3')

        print(f'Uploading {file_path} to bucket {bucket_name}...')

        local_md5 = calculate_md5(file_path)

        s3_client.upload_file(
            file_path,
            bucket_name,
            s3_key,
            ExtraArgs={
                'Metadata': {
                    'md5': local_md5
                }
            }
        )

        print('File uploaded successfully.')

    except FileNotFoundError:
        print(f'File {file_path} not found.')

    except S3UploadFailedError as e:
        print(f'S3 Upload Failed: {e}')

    except ClientError as e:
        print(f'AWS ClientError: {e}')

    except Exception as e:
        print(f'Unexpected error: {e}')

def calculate_md5(file_path):
    """Calculate MD5 hash of a file.

    Args:
        file_path (str): The path to the file for which to calculate the MD5 hash.

    Returns:
        str: The MD5 hash of the file.
    """
    hash_md5 = hashlib.md5()

    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()

def main():
    file_path = 'data/covid_data.csv'
    bucket_name = 'vilas-aws-storage-project'
    s3_key = 'covid_data.csv'
    upload_file_to_s3(file_path, bucket_name, s3_key)

if __name__ == '__main__':
    main()