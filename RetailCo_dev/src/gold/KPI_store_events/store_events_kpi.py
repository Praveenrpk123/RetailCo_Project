# Databricks notebook source
# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","RetailCo_Dev_Catalog")
dbutils.widgets.text("schema_name", "RetailCo_Dev_schema")

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total events stored

# COMMAND ----------

total_events_stored = spark.sql(f"""select count(*) as total_events from {catalog_name}.{schema_name}.fact_store_events""")

total_events_stored.\
    write.\
    mode('overwrite').\
    saveAsTable(f"{catalog_name}.{schema_name}.total_events_stored")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Platform with more customers

# COMMAND ----------

more_customers_platform = spark.sql(f"""select platform, count(distinct *) as total_customers from {catalog_name}.{schema_name}.fact_store_events
group by platform
order by total_customers desc;""")

more_customers_platform.\
    write.\
    mode('overwrite').\
    saveAsTable(f"{catalog_name}.{schema_name}.platform_with_more_customers")