# Databricks notebook source
# MAGIC %run /Workspace/Users/madhajagan1988@gmail.com/Day1/Includes
## ETL
# COMMAND ----------

df_sales=spark.read.csv(f"{input_path}sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df=add_ingestion(df_sales)
