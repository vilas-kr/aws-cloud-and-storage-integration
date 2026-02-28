import os
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
BUCKET_NAME = os.getenv("BUCKET_NAME")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
FILE_PATH = os.getenv("UPLOAD_FILE")
S3_KEY = os.getenv("S3_KEY")