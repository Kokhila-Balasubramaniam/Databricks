# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_customer=spark.table("customers")
df_sales=spark.table("sales")

# COMMAND ----------

df_joined=df_sales.join(df_customer, df_sales["customer_id"]==df_customer["customer_id"])
df_joined=df_sales.join(df_customer,"customer_id")

# COMMAND ----------

df_customer.where("customer_id>2 or customer_city='New Michaelview'").display()

# COMMAND ----------

df_customer.where(col("customer_id")>2).display()

# COMMAND ----------

df_customer.sort("customer_city").display()

# COMMAND ----------

df_customer.sort(col("customer_city").desc()).display()

# COMMAND ----------

df_joined.groupBy("customer_id").count().orderBy("customer_id").display()

# COMMAND ----------


