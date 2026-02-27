import boto3
import hashlib
from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError

def upload_file_to_s3(file_path, bucket_name, s3_key):
    try:
        s3_client = boto3.client('s3')

        print(f"Uploading {file_path} to bucket {bucket_name}...")

        local_md5 = calculate_md5(file_path)

        s3_client.upload_file(
            file_path,
            bucket_name,
            s3_key,
            ExtraArgs={
                "Metadata": {
                    "md5": local_md5
                }
            }
        )

        print("File uploaded successfully.")

    except FileNotFoundError:
        print(f"File {file_path} not found.")

    except S3UploadFailedError as e:
        print(f"S3 Upload Failed: {e}")

    except ClientError as e:
        print(f"AWS ClientError: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")


def calculate_md5(file_path):
    hash_md5 = hashlib.md5()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    return hash_md5.hexdigest()

def main():
    file_path = 'data/covid_data.csv'
    bucket_name = 'vilas-aws-storage-project'
    s3_key = 'covid_data.csv'
    upload_file_to_s3(file_path, bucket_name, s3_key)

if __name__ == '__main__':
    main()