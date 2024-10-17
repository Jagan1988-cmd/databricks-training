# Databricks notebook source
df=spark.read.csv("/Volumes/jagan_databricks/default/raw/sales.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df_customer=spark.read.json("/Volumes/jagan_databricks/default/raw/customers.json")

# COMMAND ----------

df_customer.display()

# COMMAND ----------

df_customer.write.saveAsTable("customer")

# COMMAND ----------

df.write.saveAsTable("sales")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("sales")
