import logging

from config import settings
from services.s3_services import S3FileService

logging.basicConfig(level=logging.INFO)

def main():
    s3_client = S3FileService(settings.AWS_REGION)
    
    # upload file to s3 
    s3_client.upload_file_to_s3(
        settings.FILE_PATH,
        settings.BUCKET_NAME,
        settings.S3_KEY
    )
    
    # download file from s3
    s3_client.download_file_from_s3(
        settings.S3_KEY,
        settings.BUCKET_NAME,
        settings.DOWNLOAD_DIR
    )
    
    # List all files from s3
    s3_client.list_files_in_s3_bucket(
        settings.BUCKET_NAME
    )
    
    # file integrity check
    s3_client.check__integrity(
        settings.FILE_PATH,
        settings.BUCKET_NAME,
        settings.S3_KEY
    )
    
if __name__ == '__main__':
    main()
