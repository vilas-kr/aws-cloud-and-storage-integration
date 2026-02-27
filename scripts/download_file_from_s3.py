import boto3
import os
import hashlib

from botocore.exceptions import ClientError

def download_file_from_s3(bucket_name, s3_key, download_dir):
    """Downloads a file from an S3 bucket."""
    try:
        os.makedirs(download_dir, exist_ok=True)
        download_path = os.path.join(download_dir, s3_key)
        s3_client = boto3.client('s3')
        
        # Check bucket if exist or not
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if  error_code in ["404", "NoSuchBucket"]:
                print("Bucket does not exist.")
                return
            elif error_code in ["403", "AccessDenied"]:
                print("Access denied to bucket.")
                return
            else:
                raise e

        try:        
            print(f"Downloading {s3_key} from bucket {bucket_name}...")
            s3_client.download_file(bucket_name, s3_key, download_path)
            print("File downloaded successfully")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ["404", "NoSuchKey"]:
                print("File does not exist in S3 bucket.")
            else:
                raise e

    except Exception as e:
        print(f"Unexpected error: {e}")

def calculate_md5(file_path):
    """Calculate MD5"""
    hash_md5 = hashlib.md5()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()

def check__integrity(file_path, bucket_name, s3_key):
    """Check integrity of the downloaded file by comparing MD5 hash."""
    local_md5 = calculate_md5(file_path)
    
    s3_client = boto3.client('s3')
    response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
    remote_md5 = response["Metadata"]["md5"]
    
    if local_md5 == remote_md5:
        print("file integrity verified successfully.")
    else:
        print("file integrity verification failed. The downloaded file may be corrupted")

        
def list_files_in_s3_bucket(bucket_name):
    """Lists files in an S3 bucket."""
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator("list_objects_v2")

        found = False
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get("Contents", []):
                found = True
                print(f'{obj["Key"]} \t {obj["Size"]} bytes \t Last Modified: {obj["LastModified"]}')
        
        if not found:
            print(f'Bucket is empty.')
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ["404", "NoSuchBucket"]:
            print("Bucket does not exist.")
        elif error_code in ["403", "AccessDenied"]:
            print("Access denied to bucket.")
        else:
            print(f"Client error: {e}")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        
def main():
    bucket_name = 'vilas-aws-storage-project'
    s3_key = 'covid_data.csv'
    download_dir = 'download'
    download_file_from_s3(bucket_name, s3_key, download_dir)
    check__integrity(os.path.join(download_dir, s3_key), bucket_name, s3_key)
    list_files_in_s3_bucket(bucket_name)    
    
if __name__ == '__main__':
    main()