# Databricks notebook source
df=spark.read.csv("/Volumes/kokhila_databricks/default/raw/sales.csv",header=True,inferSchema=True)
df.display()

# COMMAND ----------

df_json=spark.read.json("/Volumes/kokhila_databricks/default/raw/customers.json")

# COMMAND ----------

df_json.display()

# COMMAND ----------

df_json.write.saveAsTable("customers")
df.write.mode("overwrite").saveAsTable("sales")

# COMMAND ----------

df_join
