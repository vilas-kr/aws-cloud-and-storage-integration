# Storage and Data Engineering with AWS RDS
## Project Description
This project demonstrates a cloud-based data engineering pipeline using AWS services and Python. The pipeline is designed to ingest raw data, store it in a cloud data lake, process and transform the data, and finally store structured data in a relational database for analysis.

The system uses Amazon S3 for storage, Python with Pandas for data processing, and Amazon RDS (Microsoft SQL Server) for structured data storage and analytics.

## Dataset Information
The dataset used in this project is a Sales dataset, which contains information about product sales, customers, orders, and order date.

Dataset Source: 
https://github.com/vilas-kr/aws-cloud-and-storage-integration/blob/main/storage_and_data_engineering_with_RDS/data/sales_dataset.csv

##  Technologies Used

### Cloud Platform
- AWS (Amazon Web Services)

### AWS Services
- Amazon EC2 – Linux server used to run the data pipeline
- Amazon S3 – Object storage used as a data lake
- Amazon RDS – Managed relational database (Microsoft SQL Server)

### Programming Language
- Python 3

## System Architecture / Data Pipeline
The project follows a simple ETL-style data pipeline:
1. The raw dataset is uploaded from the local system.
2. The raw data is stored in Amazon S3 as a data lake.
3. Python scripts running on EC2 read the dataset.
4. Data cleaning and transformation are performed using Pandas.
5. The processed data is stored again in Amazon S3.
6. The cleaned dataset is loaded into Amazon RDS (SQL Server).
7. SQL queries are executed on the database for data analysis.

## Data Flow Diagram
```
Local System
     |
     v
Upload Raw Dataset
     |
     v
Amazon S3 (Raw Data Storage)
     |
     v
Python Data Processing (Pandas on EC2)
     |
     v
Amazon S3 (Processed Data)
     |
     v
Amazon RDS (SQL Server Database)
     |
     v
SQL Queries and Data Analytics
```

## How to Run

### Install Required System Packages (Linux)
```
sudo yum install gcc gcc-c++ python3-devel unixODBC-devel -y
```

### Clone the Project Repository
```
git clone https://github.com/vilas-kr/aws-cloud-and-storage-integration.git
```

### Navigate to the Project Directory
```
cd aws-cloud-and-storage-integration
```

### Create Python Virtual Environment
```
python3 -m venv venv
source venv/bin/activate
```

### Install Python Dependencies
```
pip install -r requirements.txt
```

### Run the Application
Execute the Python scripts to start the data ingestion and processing pipeline

## Key Features of the Project
- Cloud-based data ingestion pipeline
- Use of Amazon S3 as a data lake
- Data cleaning and transformation using Pandas
- Storage of structured data in Amazon RDS
- Execution of SQL analytics queries
- Scalable cloud architecture

## Author
```
Name: Vilas K R
GitHub: https://github.com/vilas-kr
```