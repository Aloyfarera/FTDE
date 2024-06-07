import pandas as pd
import snowflake.connector
from config import *
from creds import *



# Snowflake connection details
conn = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
    )
# Establish the connection
try:
    print("Connection successful!")
    # Optionally, run a test query
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    row = cursor.fetchone()
    print("Snowflake version:", row[0])
    
except snowflake.connector.errors.Error as e:
    print("Connection failed!")
    print(e)

def create_tables():
    """Create tables in the Snowflake data warehouse if they do not exist."""
    try:
        cursor = conn.cursor()
        for ddl in ddl_statements.values():
            cursor.execute(ddl)
            print(f"Executed DDL: {ddl.strip().splitlines()[0]}...")  # Print the first line of each DDL for confirmation
        print("All tables created successfully.")
    except snowflake.connector.errors.Error as e:
        print("Error creating tables:", e)
    finally:
        if conn:
            conn.close()

# Call the function to create tables
create_tables()
