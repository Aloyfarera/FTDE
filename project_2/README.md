# DIGITAL SKOLA DBT-SNOWFLAKE 

## Overview
This project aims to transform and load raw data into a data warehouse using Snowflake as the target database. The transformation and data warehouse creation will be implemented using dbt (Data Build Tool). Data marts will be created for reporting purposes.
### Source Data
The source data for this project can be found at:
[Northwind CSV Data](https://github.com/graphql-compose/graphql-compose-examples/tree/master/examples/northwind/data/csv)

### Objectives
1. Create a data warehouse and load raw data into Snowflake using Python.
2. Perform transformations using dbt.
3. Generate data marts to produce the following dashboard reports:
   - Monthly gross revenue per supplier.
   - Monthly most sold product category.
   - Best employee based on total monthly gross revenue.

### Calculations
- **Gross Revenue**: \((\text{price} - (\text{price} \times \text{discount})) \times \text{quantity}\)


### Prerequisites
- Docker and Docker Compose installed on your machine
- A Snowflake account

### Installation

1. Clone this repository
2. Set up your database credentials: 
   - Edit the dwh_scripts/config.py and dwh_scripts/creds.py files to include your Snowflake account details.
   - Ensure that dwh_scripts/config.py has the necessary configurations for your Snowflake account:
       - SNOWFLAKE_ACCOUNT = '<your_snowflake_account>'
       - SNOWFLAKE_USER = '<your_snowflake_user>'
       - SNOWFLAKE_PASSWORD = '<your_snowflake_password>'
       - SNOWFLAKE_WAREHOUSE = '<your_snowflake_warehouse>'
       - SNOWFLAKE_DATABASE = '<your_snowflake_database>'
       - SNOWFLAKE_SCHEMA = '<your_snowflake_schema>'
   - Alternatively, you can set those values in a .env file in the root directory
3. Build and run the Docker containers: 
   docker-compose up --build
