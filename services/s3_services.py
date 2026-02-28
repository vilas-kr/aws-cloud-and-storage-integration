import logging
import os
import hashlib

import boto3
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError

class S3FileService:
    
    def __init__(self, aws_region: str):
        self.s3_client = boto3.client('s3', region_name=aws_region)

    def upload_file_to_s3(self, 
        file_path: str, 
        bucket_name: str, 
        s3_key: str
    ) -> None:
        """Upload a file to an S3 bucket.

        Args:
            file_path (str): The path to the file to be uploaded.
            bucket_name (str): The name of the S3 bucket to upload 
                the file to.
            s3_key (str): The key of the file in the S3 bucket.
            
        Raises:
            FileNotFoundError: If the specified file does not exist.
            S3UploadFailedError: If the upload to S3 fails.
            Exception: For any other unexpected errors.
        """
        try:
            logging.info(f'Uploading {file_path} to bucket {bucket_name}.')
            local_md5 = self.calculate_md5(file_path)
            self.s3_client.upload_file(
                file_path, 
                bucket_name, s3_key, 
                ExtraArgs={
                    'Metadata': {'md5': local_md5}
                }
            ) 
            logging.info('File uploaded successfully.')
            
        except FileNotFoundError:
            logging.error(f'File {file_path} not found.')
            raise
            
        except S3UploadFailedError as e:
            logging.error(f'S3 upload failed: {e}')
            raise
            
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            raise
        
    def download_file_from_s3(
        self, 
        s3_key: str, 
        bucket_name: str, 
        download_dir: str
    ) -> None:
        """Download a file from an S3 bucket.

        Args:
            s3_key (str): The key of the file in the S3 bucket.
            bucket_name (str): The name of the S3 bucket.
            download_dir (str): The local directory where the file will be 
                downloaded.

        Raises:
            ClientError: If there is an error with the AWS S3 client.
            Exception: For any other unexpected errors.
        """
        try:
            os.makedirs(download_dir, exist_ok=True)
            
            # Check bucket if exist or not
            try:
                self.s3_client.head_bucket(Bucket=bucket_name)
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['404', 'NoSuchBucket']:
                    logging.error('Bucket does not exist.')
                    raise
                elif error_code in ['403', 'AccessDenied']:
                    logging.error('Access denied to bucket.')
                    raise
                else:
                    raise 

            try:        
                logging.info(
                    f'Downloading {s3_key} from bucket '
                    f'{bucket_name}...'
                )
                self.s3_client.download_file(
                    bucket_name, 
                    s3_key, 
                    os.path.join(download_dir, os.path.basename(s3_key))
                )
                logging.info('File downloaded successfully')
                
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code in ['404', 'NoSuchKey']:
                    logging.error('File does not exist in S3 bucket.')
                    raise
                else:
                    raise 

        except Exception as e:
            logging.error(f'Unexpected error: {e}')
            raise
            
    def list_files_in_s3_bucket(self, bucket_name: str) -> None:
        """Lists files in an S3 bucket.
        
        Args:
            bucket_name (str): The name of the S3 bucket.
            
        raises:
            ClientError: If there is an error with the AWS S3 client.
            Exception: For any other unexpected errors.
            
        """
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')

            found = False
            for page in paginator.paginate(Bucket=bucket_name):
                for obj in page.get('Contents', []):
                    found = True
                    logging.info(
                        f'{obj['Key']} \t {obj['Size']} bytes \t '
                        f'Last Modified: {obj['LastModified']}'
                    )
            
            if not found:
                logging.info(f'Bucket is empty.')
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['404', 'NoSuchBucket']:
                logging.error('Bucket does not exist.')
                raise
            elif error_code in ['403', 'AccessDenied']:
                logging.error('Access denied to bucket.')
                raise
            else:
                logging.error(f'Client error: {e}')
                raise
                
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            raise
    
    def calculate_md5(self, file_path: str) -> str:
        """Calculate MD5 hash of a file.

        Args:
            file_path (str): The path to the file for which to calculate 
                the MD5 hash.

        Returns:
            str: The MD5 hash of the file.
        """
        hash_md5 = hashlib.md5()

        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                hash_md5.update(chunk)

        return hash_md5.hexdigest()
    
    def check__integrity(
        self, 
        file_path: str, 
        bucket_name: str, 
        s3_key: str
    ) -> None:
        """Check integrity of the downloaded file by comparing MD5 hash.
        
        Args:
            file_path (str): The path to the downloaded file.
            bucket_name (str): The name of the S3 bucket.
            s3_key (str): The key of the file in the S3 bucket.

        Raises:
            ClientError: If there is an error retrieving metadata from S3.
            KeyError: If the MD5 metadata is not found in the S3 object.
            Exception: For any other unexpected errors.
        """
        local_md5 = self.calculate_md5(file_path)
        
        s3_client = boto3.client('s3')
        response = s3_client.head_object(Bucket=bucket_name, Key=s3_key)
        remote_md5 = response['Metadata']['md5']
        
        if local_md5 == remote_md5:
            print('file integrity verified successfully.')
        else:
            print(f'file integrity verification failed. ' 
                  f'The downloaded file may be corrupted')