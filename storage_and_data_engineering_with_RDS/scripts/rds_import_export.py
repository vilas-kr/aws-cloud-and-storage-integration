# Part 7: CSV Import & Export with RDS
# Tasks
# Import CSV data into AWS RDS tables
# Export query results from RDS back to CSV
# Explain different methods for bulk import/export

import logging
import pandas as pd

from config import settings
from config.db_connection import get_connection

logging.basicConfig(level=logging.INFO)

# -------------------------------------------------------------------------
# Establish a connection to RDS SQL server database using pyodbc
# -------------------------------------------------------------------------
conn = get_connection()
logging.info('Connected to RDS SQL server database')

cursor = conn.cursor()

# -------------------------------------------------------------------------
# 1. Import CSV data into RDS table
# -------------------------------------------------------------------------
logging.info('Importing CSV data into RDS...')
# Read cleaned data from S3
data = pd.read_parquet(f's3://{settings.BUCKET_NAME}/processed/'
                   f'sales_dataset_cleaned.parquet')

# Delete existing records in RDS table to avoid duplicates
delete_query = 'DELETE FROM orders.sales;'
cursor.execute(delete_query)
conn.commit()

# Insert data into RDS table
cursor.fast_executemany = True
insert_query = '''
INSERT INTO orders.sales (transaction_id, customer_id, product_name,
    category, region, quantity, price, total_amount, order_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
'''
try:
    cursor.executemany(insert_query, data.values.tolist())
    conn.commit()
    logging.info('Parquet data imported successfully')
except Exception as e:
    logging.error('Error occurred while importing Parquet data: %s', e)
    raise e

# -------------------------------------------------------------------------
# 2. Export query results from RDS back to CSV
# -------------------------------------------------------------------------
logging.info('Exporting query results from RDS to CSV...')
export_query = '''
SELECT *
FROM orders.sales
WHERE order_date >= '2023-01-01' AND order_date < '2024-01-01';
'''
try:    
    exported_df = pd.read_sql(export_query, conn)

    # Save exported data to CSV
    exported_df.to_csv('data/imported_sales_data.csv', index=False)
    logging.info('Query results exported to CSV successfully')
    
except Exception as e:
    logging.error('Error occurred while exporting query results: %s', e)
    raise e
# -------------------------------------------------------------------------
# Explaination
# 3. Explain different methods for bulk import/export
# -------------------------------------------------------------------------
# There are several methods for bulk importing and exporting data with RDS:

# 1. Using SQL Server Management Studio (SSMS) for SQL Server: 
#   This tool provides a graphical interface for importing and exporting 
#   data using the Import and Export Wizard.

# 2. Using BCP (Bulk Copy Program): This is a command-line tool that allows 
#   for fast import and export of data between SQL Server and other data 
#   sources.

# 3. Using Python libraries like pandas and pyodbc: This method allows for 
#   programmatic control over the import and export process, making it 
#   suitable for automation and integration into data pipelines.
