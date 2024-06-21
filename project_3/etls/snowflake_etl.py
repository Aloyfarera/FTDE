import pandas as pd
from sqlalchemy import create_engine
from snowflake.connector.cursor import SnowflakeCursor,ProgrammingError
import snowflake.connector
import os 
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
dag_file_path = os.path.dirname(os.path.abspath(__file__))
from config import *
from utils.creds_sf import *

# PostgreSQL connection setup
pg_engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5431/airflow_dskola_db')

ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT,
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA,
    role = ROLE
    )

def transform_dm_supplier_revenue():
    # Extract data from PostgreSQL
    query = """
    WITH supplier_revenue AS (
      SELECT
        s.COMPANYNAME AS company_name,
        TO_DATE(CONCAT(EXTRACT(MONTH FROM o.ORDERDATE), '-', EXTRACT(YEAR FROM o.ORDERDATE)), 'MM-YYYY') AS month_order,
        SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS gross_revenue
      FROM
        fact_order_details od
        JOIN fact_orders o ON od.ORDERID = o.ORDERID
        JOIN dim_products p ON od.PRODUCTID = p.PRODUCTID
        JOIN dim_suppliers s ON p.SUPPLIERID = s.SUPPLIERID
      GROUP BY
        company_name, month_order
    )

    SELECT
      company_name,
      month_order,
      gross_revenue
    FROM
      supplier_revenue;
    """
    df = pd.read_sql_query(query, pg_engine)
    df.to_csv('data/supplier_revenue.csv', index=False)

def transform_dm_top_employee_revenue():
    # Extract data from PostgreSQL
    query ="""
    WITH employee_revenue AS (
      SELECT
        e.FIRSTNAME || ' ' || e.LASTNAME AS employee_name,
        TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
        SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS gross_revenue
      FROM
        fact_order_details od
        JOIN fact_orders o ON od.ORDERID = o.ORDERID
        JOIN dim_employees e ON o.EMPLOYEEID = e.EMPLOYEEID
      GROUP BY
        employee_name, month_order
    ),
    ranked_employees AS (
      SELECT
        employee_name,
        month_order,
        gross_revenue,
        ROW_NUMBER() OVER (PARTITION BY month_order ORDER BY gross_revenue DESC) AS rank
      FROM
        employee_revenue
    )

    SELECT
      employee_name,
      month_order,
      gross_revenue
    FROM
      ranked_employees
    WHERE
      rank = 1;
    """

    df = pd.read_sql_query(query, pg_engine)
    df.to_csv('data/top_employee_revenue.csv', index=False)

def transform_dm_top_category_sales():
    # Extract data from PostgreSQL
    query = """
    WITH category_sales AS (
      SELECT
        c.CATEGORYNAME AS category_name,
        TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
        SUM(od.QUANTITY) AS total_sold
      FROM
        fact_order_details od
        JOIN fact_orders o ON od.ORDERID = o.ORDERID
        JOIN dim_products p ON od.PRODUCTID = p.PRODUCTID
        JOIN dim_categories c ON p.CATEGORYID = c.CATEGORYID
      GROUP BY
        category_name, month_order
    ),
    ranked_categories AS (
      SELECT
        category_name,
        month_order,
        total_sold,
        ROW_NUMBER() OVER (PARTITION BY month_order ORDER BY total_sold DESC) AS rank
      FROM
        category_sales
    )
    SELECT
      category_name,
      month_order,
      total_sold
    FROM
      ranked_categories
    WHERE
      rank = 1;
    """

    df = pd.read_sql_query(query, pg_engine)
    df.to_csv('data/top_category_sales.csv', index=False)

def load_to_snowflake():

    # Create a Snowflake cursor
    cursor = ctx.cursor(SnowflakeCursor)
    # Paths to CSV files for each data mart
    csv_files = {
        'supplier_revenue': 'data/supplier_revenue.csv',
        'top_employee_revenue': 'data/top_employee_revenue.csv',
        'top_category_sales': 'data/top_category_sales.csv'
    }

    try:
        # Iterate through each data mart
        for table_name, csv_file_path in csv_files.items():
            # Read CSV into DataFrame
            df = pd.read_csv(csv_file_path)
            
            # Ensure column names match Snowflake table
            df.columns = df.columns.str.upper()  # Make column names uppercase to match table schema

            # Create a Snowflake cursor
            cursor = ctx.cursor(SnowflakeCursor)
            
            # Create the target table in Snowflake
            create_table_query = table_creation_queries[table_name]
            cursor.execute(create_table_query)

            # Put the CSV data into a stage
            stage_name = f"stg_{table_name}"
            stage_location = f"@%{stage_name}"
            cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")
            cursor.execute(f"PUT file://{os.path.abspath(csv_file_path)} {stage_location}")

            # Copy data from stage to Snowflake table
            copy_into_query = f"""
            COPY INTO DM_{table_name.upper()}
            FROM {stage_location}
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
            """
            cursor.execute(copy_into_query)

            # Close the cursor
            cursor.close()
            print(f"Data loaded to Snowflake table DM_{table_name.upper()} successfully.")

    except ProgrammingError as e:
        print(f"Snowflake error: {e}")

    finally:
        # Close Snowflake connection
        cursor.close()
    
    

# def load_to_snowflake():
#     # Convert DataFrame to CSV
#     csv_file_path = "data/supplier_revenue.csv"

#     # Create a Snowflake cursor
#     cursor = ctx.cursor(SnowflakeCursor) 
    
#     # Create the target table in Snowflake
#     cursor.execute("""
#     CREATE OR REPLACE TABLE supplier_revenue (
#         company_name STRING,
#         month_order DATE,
#         gross_revenue FLOAT
#     )
#     """)
    
#     # Put the CSV data into a stage
#     cursor.execute(f"PUT file://{csv_file_path} @%supplier_revenue")
    
#     # Copy data from stage to Snowflake table
#     cursor.execute("""
#     COPY INTO supplier_revenue
#     FROM @%supplier_revenue
#     FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
#     """)
    
#     # Close the cursor
#     cursor.close()
#     print("Data loaded to Snowflake successfully.")





