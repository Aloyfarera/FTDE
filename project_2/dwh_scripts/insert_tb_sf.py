import pandas as pd
import snowflake.connector
from config import *
from snowflake.connector.cursor import SnowflakeCursor
import os
from creds import *

ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    role = ROLE
    )

def load_csv_to_snowflake_staging():
    cursor = ctx.cursor(SnowflakeCursor)  # Specify SnowflakeCursor type
    cursor.execute(f'''use database DWH_DBT_PROJECT''')
    # Get list of CSV files in the directory
    data_dir = "csv/"
    csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]  #

    for file_name in csv_files:
       
        file_path = os.path.join(data_dir, file_name)

        # Upload data to temporary location
        cursor.execute(f"PUT 'file://{file_path}' @csv_local auto_compress=true;")
        print(f"Uploaded {file_name} to Snowflake staging area.")

    # Close the cursor
    cursor.close()
    print("All CSV files processed.")

def load_staged_data_to_tables():
    cursor = ctx.cursor(SnowflakeCursor)  # Specify SnowflakeCursor type

    # Define a list of dictionaries containing table names and file patterns
    tables = [
        {"table_name": "DIM_CATEGORIES", "pattern": ".*categories.*"},
        {"table_name": "DIM_EMPLOYEES", "pattern": ".*employees.*"},
        {"table_name": "DIM_PRODUCTS", "pattern": ".*products.*"},
        {"table_name": "DIM_SUPPLIERS", "pattern": ".*suppliers.*"},
        {"table_name": "FACT_ORDERS", "pattern": ".*orders.*"},
        {"table_name": "FACT_ORDER_DETAILS", "pattern": ".*order_details.*"},
    ]
    # Use database
    cursor.execute(f'''use database DWH_DBT_PROJECT''')

    # Loop through each table dictionary
    for table in tables:
        table_name = table["table_name"]
        pattern = table["pattern"]

        # Load data using COPY INTO
        try:
            cursor.execute(f"""
                           copy into {table_name} 
                           from (select distinct * from @csv_local t) 
                           file_format = allow_duplicate_ff 
                           on_error = 'CONTINUE' 
                           pattern='{pattern}'
                           force = false;
                           """) 
            
            print(f"Loaded data into table: {table_name}")
        except snowflake.connector.Error as err:
            print(f"Error encountered for table {table_name}: {err}")

    # Close the cursor
    cursor.close()
    print("All CSV files processed.")

load_csv_to_snowflake_staging()
load_staged_data_to_tables()