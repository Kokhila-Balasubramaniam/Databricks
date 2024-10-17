# Databricks notebook source
# MAGIC %run '/Workspace/Users/kokhilamuthukumar@gmail.com/Day1/includes'

# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}/sales.csv")

# COMMAND ----------

add_ingestion("df")
