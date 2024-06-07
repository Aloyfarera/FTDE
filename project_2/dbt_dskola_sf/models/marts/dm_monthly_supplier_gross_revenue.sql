
WITH supplier_revenue AS (
  SELECT
    s.COMPANYNAME AS company_name,
    TO_CHAR(DATE_TRUNC('month', o.ORDERDATE), 'YYYY-MM') AS month_order,
    SUM((od.UNITPRICE - (od.UNITPRICE * od.DISCOUNT)) * od.QUANTITY) AS gross_revenue
  FROM
    {{ ref('stg_fact_order_details') }} od
    JOIN {{ ref('stg_fact_orders') }} o ON od.ORDERID = o.ORDERID
    JOIN {{ ref('stg_dim_products') }} p ON od.PRODUCTID = p.PRODUCTID
    JOIN {{ ref('stg_dim_suppliers') }} s ON p.SUPPLIERID = s.SUPPLIERID
  GROUP BY
    company_name, month_order
)

SELECT
  company_name,
  month_order,
  gross_revenue
FROM
  supplier_revenue
ORDER BY
  company_name, month_order
