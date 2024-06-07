{{
  config(
    materialized='view'
  )
}}

SELECT
  *
FROM
  {{ source('dwh_dbt_project', 'fact_orders') }}