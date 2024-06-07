-- models/marts/top_category_sales.sql

WITH category_sales AS (
  SELECT
    c.CATEGORYNAME AS category_name,
    TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
    SUM(od.QUANTITY) AS total_sold
  FROM
    {{ ref('stg_fact_order_details') }} od
    JOIN {{ ref('stg_fact_orders') }} o ON od.ORDERID = o.ORDERID
    JOIN {{ ref('stg_dim_products') }} p ON od.PRODUCTID = p.PRODUCTID
    JOIN {{ ref('stg_dim_categories') }} c ON p.CATEGORYID = c.CATEGORYID
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
  rank = 1
