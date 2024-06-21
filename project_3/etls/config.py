table_creation_queries = {
    'supplier_revenue': """
        CREATE OR REPLACE TABLE DM_MONTHLY_SUPPLIER_GROSS_REVENUE (
            COMPANY_NAME VARCHAR(16777216),
            MONTH_ORDER VARCHAR(16777216),
            GROSS_REVENUE NUMBER(38,2)
        )
    """,
    'top_employee_revenue': """
        CREATE OR REPLACE TABLE DM_TOP_EMPLOYEE_REVENUE (
            EMPLOYEE_NAME VARCHAR(16777216),
            MONTH_ORDER VARCHAR(16777216),
            GROSS_REVENUE NUMBER(38,2)
        )
    """,
    'top_category_sales': """
        CREATE OR REPLACE TABLE DM_TOP_CATEGORY_SALES (
            CATEGORY_NAME VARCHAR(16777216),
            MONTH_ORDER VARCHAR(16777216),
            TOTAL_SOLD NUMBER(38,2)
        )
    """
}

# -- DIM_CATEGORIES table
# CREATE TABLE IF NOT EXISTS DWH_DBT_PROJECT.PUBLIC.DIM_CATEGORIES (
#     CATEGORYID BIGINT,
#     CATEGORYNAME TEXT,
#     DESCRIPTION TEXT,
#     PICTURE FLOAT,
#     PRIMARY KEY (CATEGORYID)
# );

# -- DIM_EMPLOYEES table
# CREATE TABLE IF NOT EXISTS DWH_DBT_PROJECT.PUBLIC.DIM_EMPLOYEES (
#     EMPLOYEEID BIGINT,
#     LASTNAME TEXT,
#     FIRSTNAME TEXT,
#     TITLE TEXT,
#     TITLEOFCOURTESY TEXT,
#     BIRTHDATE TIMESTAMP,
#     HIREDATE TIMESTAMP,
#     ADDRESS TEXT,
#     CITY TEXT,
#     REGION TEXT,
#     POSTALCODE TEXT,
#     COUNTRY TEXT,
#     HOMEPHONE TEXT,
#     EXTENSION BIGINT,
#     PHOTO FLOAT,
#     NOTES TEXT,
#     REPORTSTO TEXT,
#     PHOTOPATH TEXT,
#     PRIMARY KEY (EMPLOYEEID)
# );

# -- DIM_SUPPLIERS table
# CREATE TABLE IF NOT EXISTS DWH_DBT_PROJECT.PUBLIC.DIM_SUPPLIERS (
#     SUPPLIERID BIGINT,
#     COMPANYNAME TEXT,
#     CONTACTNAME TEXT,
#     CONTACTTITLE TEXT,
#     ADDRESS TEXT,
#     CITY TEXT,
#     REGION TEXT,
#     POSTALCODE TEXT,
#     COUNTRY TEXT,
#     PHONE TEXT,
#     FAX TEXT,
#     HOMEPAGE TEXT,
#     PRIMARY KEY (SUPPLIERID)
# );

# -- FACT_ORDERS table
# CREATE TABLE IF NOT EXISTS DWH_DBT_PROJECT.PUBLIC.FACT_ORDERS (
#     ORDERID BIGINT,
#     CUSTOMERID TEXT,
#     EMPLOYEEID BIGINT,
#     ORDERDATE TIMESTAMP,
#     REQUIREDDATE TIMESTAMP,
#     SHIPPEDDATE TEXT,
#     SHIPVIA BIGINT,
#     FREIGHT NUMERIC(38,2),
#     SHIPNAME TEXT,
#     SHIPADDRESS TEXT,
#     SHIPCITY TEXT,
#     SHIPREGION TEXT,
#     SHIPPOSTALCODE TEXT,
#     SHIPCOUNTRY TEXT,
#     PRIMARY KEY (ORDERID),
#     CONSTRAINT FK_ORDERS_EMPLOYEES FOREIGN KEY (EMPLOYEEID) REFERENCES DWH_DBT_PROJECT.PUBLIC.DIM_EMPLOYEES(EMPLOYEEID)
# );

# -- DIM_PRODUCTS table
# CREATE TABLE IF NOT EXISTS DWH_DBT_PROJECT.PUBLIC.DIM_PRODUCTS (
#     PRODUCTID BIGINT,
#     PRODUCTNAME TEXT,
#     SUPPLIERID BIGINT,
#     CATEGORYID BIGINT,
#     QUANTITYPERUNIT TEXT,
#     UNITPRICE NUMERIC(38,2),
#     UNITSINSTOCK BIGINT,
#     UNITSONORDER BIGINT,
#     REORDERLEVEL BIGINT,
#     DISCONTINUED BIGINT,
#     CONSTRAINT FK_PRODUCTS_CATEGORY FOREIGN KEY (CATEGORYID) REFERENCES DWH_DBT_PROJECT.PUBLIC.DIM_CATEGORIES(CATEGORYID),
#     PRIMARY KEY (PRODUCTID),
#     CONSTRAINT FK_PRODUCTS_SUPPLIERS FOREIGN KEY (SUPPLIERID) REFERENCES DWH_DBT_PROJECT.PUBLIC.DIM_SUPPLIERS(SUPPLIERID)
# );

# -- FACT_ORDER_DETAILS table
# CREATE TABLE IF NOT EXISTS DWH_DBT_PROJECT.PUBLIC.FACT_ORDER_DETAILS (
#     ORDERID BIGINT,
#     PRODUCTID BIGINT,
#     UNITPRICE NUMERIC(38,2),
#     QUANTITY BIGINT,
#     DISCOUNT NUMERIC(38,2),
#     CONSTRAINT FK_OD_ORDER FOREIGN KEY (ORDERID) REFERENCES DWH_DBT_PROJECT.PUBLIC.FACT_ORDERS(ORDERID),
#     CONSTRAINT FK_OD_PRODUCTS FOREIGN KEY (PRODUCTID) REFERENCES DWH_DBT_PROJECT.PUBLIC.DIM_PRODUCTS(PRODUCTID)
# );