# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

input_path="/Volumes/kokhila_databricks/default/raw/"

# COMMAND ----------

def add_ingestion(df):
    i=df.withColumn("ingestion_date",current_timestamp())
    return i
