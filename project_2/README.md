# DIGITAL SKOLA DBT-SNWOFLAKE 

## Overview
This project aims to transform and load raw data into a data warehouse using Snowflake as the target database. The transformation and data warehouse creation will be implemented using dbt (Data Build Tool). Additionally, data marts will be created for reporting purposes.
### Source Data
The source data for this project can be found at:
[Northwind CSV Data](https://github.com/graphql-compose/graphql-compose-examples/tree/master/examples/northwind/data/csv)

### Objectives
1. Load raw data into PostgreSQL or Snowflake using Python.
2. Create a data warehouse and perform transformations using dbt.
3. Generate data marts to produce the following dashboard reports:
   - Monthly gross revenue per supplier.
   - Monthly most sold product category.
   - Best employee based on total monthly gross revenue.

### Calculations
- **Gross Revenue**: \((\text{price} - (\text{price} \times \text{discount})) \times \text{quantity}\)


