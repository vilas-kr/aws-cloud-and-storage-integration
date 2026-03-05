# Part 5: AWS RDS – Database Operations

# Table Design
# Design an appropriate relational schema based on the dataset
# Create tables programmatically or via client tools

# CRUD Operations
# Perform the following using SQL:
# Insert records into RDS tables
# Read and filter records
# Update selected records
# Delete records based on conditions

import logging
import pandas as pd

from config import settings
from config.db_connection import get_connection

logging.basicConfig(level=logging.INFO)

# -------------------------------------------------------------------------
# Establish a connection to RDS SQL server database using pyodbc
# -------------------------------------------------------------------------
conn = get_connection()

# -------------------------------------------------------------------------
# 1. Create a table in RDS
# -------------------------------------------------------------------------
logging.info('Creating table in RDS...')
create_table_query = '''
IF NOT EXISTS (
    SELECT * 
    FROM sys.tables 
    WHERE name = 'sales'
    AND schema_id = SCHEMA_ID('orders')
)
BEGIN
    CREATE TABLE orders.sales (
        transaction_id CHAR(36) PRIMARY KEY,
        customer_id CHAR(8),
        product_name VARCHAR(255),
        category VARCHAR(255),
        region VARCHAR(255),
        quantity INT,
        price DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        order_date DATE
    );
END
'''
cursor = conn.cursor()
try:
    cursor.execute(create_table_query)
    conn.commit()
    logging.info('Table created successfully')
except Exception as e:
    logging.error('Error occurred while creating table: %s', e)
    raise e      

# -------------------------------------------------------------------------
# 2. Insert records into RDS table
# -------------------------------------------------------------------------
# Download cleaned data from S3 to local directory
try:
    data = pd.read_parquet(f's3://{settings.BUCKET_NAME}/processed/'
                   f'sales_dataset_cleaned.parquet')
    logging.info('Cleaned data loaded successfully')
except Exception as e:
    logging.error('Error occurred while loading cleaned data: %s', e)
    raise e
    
logging.info('Inserting records into RDS table...')
insert_query = '''
INSERT INTO orders.sales (transaction_id, customer_id, product_name, 
    category, region, quantity, price, total_amount, order_date)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
'''
try:
    # delete existing records to avoid duplicates
    cursor.execute('DELETE FROM orders.sales')

    # Iterate through the DataFrame and insert records into RDS
    cursor.fast_executemany = True
    cursor.executemany(insert_query, data.values.tolist())
    conn.commit()  
except Exception as e:
    logging.error('Error occurred while inserting records: %s', e)
    raise e

logging.info('Records inserted successfully')

# -------------------------------------------------------------------------
# 3. Read and filter records
# -------------------------------------------------------------------------
logging.info('Reading and filtering records from RDS...')
select_query = '''
SELECT * FROM orders.sales
WHERE region = 'North' AND category = 'Electronics';
'''
try:
    cursor.execute(select_query)
    filtered_records = cursor.fetchall()    
    logging.info(f'Number of records retrieved: {len(filtered_records)}')
except Exception as e:
    logging.error('Error occurred while reading and filtering records: %s', e)
    raise e


# -------------------------------------------------------------------------
# 4. Update selected records
# -------------------------------------------------------------------------
logging.info('Updating records in RDS...')
update_query = '''
UPDATE orders.sales
SET product_name = 'Mobile Phone'
WHERE category = 'Gadgets' AND region = 'North' AND product_name = 'Mobile';
'''
try:
    cursor.execute(update_query)
    conn.commit()
    logging.info('Records updated successfully')
except Exception as e:
    logging.error('Error occurred while updating records: %s', e)
    raise e

# -------------------------------------------------------------------------
# 5. Delete records based on conditions
# -------------------------------------------------------------------------
logging.info('Deleting records from RDS...')
delete_query = '''
DELETE FROM orders.sales
WHERE region = 'North' AND category = 'Electronics' AND price < 100;
'''
try:
    cursor.execute(delete_query)
    conn.commit()
    logging.info('Records deleted successfully')
except Exception as e:
    logging.error('Error occurred while deleting records: %s', e)
    raise e

# Close the database connection
conn.close()
logging.info('Database connection closed')

