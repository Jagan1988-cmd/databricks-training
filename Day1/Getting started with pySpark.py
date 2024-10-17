# Databricks notebook source
print("hello")

# COMMAND ----------

# MAGIC %md
# MAGIC SparkCore

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema=["id","name","age"]
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

data=[(1,'a',20),(2,'b',30)]
schema='id int,name string,age int'
df=spark.createDataFrame(data,schema)
df.display()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("id").alias("emp_id"))

# COMMAND ----------

df.select(col("id").alias("emp_id")).display()

# COMMAND ----------

df.select("*").display()

# COMMAND ----------

test=df.withColumnRenamed("id","emp_id").display()

# COMMAND ----------

df.withColumnRenamed("id","emp_id").withColumnRenamed("name","emp_name").withColumnRenamed("age","emp_age").display()
