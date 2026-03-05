
# AWS Cloud & Storage Integration Using Python

----
## AWS Services Used
- VPC: To create private, isolated network for our instance.
- EC2: For optional execution environment.
- S3: For file storage.
- IAM: For access management.

## Script Flow
1.  Task 3:
    1. `upload_file_to_s3.py` - Upload a local CSV to S3.
    2. `download_file_from_s3.py` - Download from S3 and verify integrity and list files in S3.

2. Task 4:
    1. `settings.py` - Configures the variables.
    2. `s3_automation.py` - Upload a file to S3, download a file from S3, list files and verify integrity.
    3. `s3_services.py` - S3FileService class which is used to create s3 client and manage file operations

----
## Assumptions
- Bucket exists and is in region `ap-south-1`
- Sample CSV located at `data/covid_data.csv`
- AWS credentials configured via environment variables(local machine) or IAM role(EC2 Instance)
----
