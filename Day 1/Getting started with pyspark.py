# Databricks notebook source
# MAGIC %md
# MAGIC ####RDD DataFrame

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
#schema=["id","name","age"] <- this also works without datatypes
schema="id int, name string, age int"
df=spark.createDataFrame(data,schema)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Dataframe function
# MAGIC - .select
# MAGIC - .alias (rename)
# MAGIC - .col (return col type of the column)
# MAGIC - .withColumnRenamed (rename a single column)
# MAGIC - .withColumnsRenamed( rename multiple column at a time)
# MAGIC - .help
# MAGIC - .withColumn (replace existing column or create new column if column doesn't exist)
# MAGIC - .drop

# COMMAND ----------

df1= df.select("id","name")
#df.select("id".alias("emp_id")) <- error as alias works only on col and not on string
df2=df.select(col("id").alias("emp_id"))

# COMMAND ----------

df2.display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

help(df.withColumnRenamed("id","emp_id"))

# COMMAND ----------

#rename all column in one go
df.withColumnsRenamed({"id":"emp_id","name":"emp_name","age":"emp_age"}).display()

# COMMAND ----------

df.withColumn("current_date",current_date()).withColumn("age").display()

# COMMAND ----------


