-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Delta Live Tables

-- COMMAND ----------

CREATE SCHEMA hankehly;
USE SCHEMA hankehly;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## orders_raw

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/demo-datasets/bookstore

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files(
  "dbfs:/mnt/demo-datasets/bookstore/orders-raw",
  "parquet",
  map("schema", "order_id STRING, order_timestamp LONG, customer_id STRING, quantity LONG")
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### customers

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`dbfs:/mnt/demo-datasets/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver Layer Tables
-- MAGIC ### orders_cleaned

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order id"
AS
SELECT
  order_id,
  quantity,
  o.customer_id,
  c.profile:first_name as f_name,
  c.profile:last_name as l_name,
  cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') as timestamp) order_timestamp,
  c.profile:address:country as country
FROM
  STREAM(LIVE.orders_raw) o
LEFT JOIN
  LIVE.customers c
USING
  (customer_id)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Tables
-- MAGIC ### cn_daily_customer_books

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
SELECT
  customer_id,
  f_name,
  l_name,
  date_trunc("DD", order_timestamp) order_date,
  sum(quantity) book_counts
FROM
  LIVE.orders_cleaned
WHERE
  country = "China"
GROUP BY
  customer_id,
  f_name,
  l_name,
  date_trunc("DD", order_timestamp)
