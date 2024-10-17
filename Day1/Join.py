# Databricks notebook source
df_sales=spark.table("sales")

# COMMAND ----------

df_customers=spark.table("customers")

# COMMAND ----------

df_joined=df_sales.join(df_customers, df_sales["customer_id"]==df_customers["customer_id"], "inner")

# COMMAND ----------

df_joined.display()

# COMMAND ----------

df_customers.where("customer_id>2 or customer_city = 'New Michaelview'").display()

# COMMAND ----------

df_customers.sort("customer_name").display()

# COMMAND ----------

df_joined.count()

# COMMAND ----------

# MAGIC %md
# MAGIC GIT Test
