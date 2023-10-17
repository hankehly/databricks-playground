-- Databricks notebook source
CREATE TABLE managed_default
  (width INT, length INT, height INT);

INSERT INTO managed_default
VALUES (3 INT, 2 INT, 1 INT);

-- COMMAND ----------

SHOW TABLES IN default;
