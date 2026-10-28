# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
dbutils.widgets.text('Job_id', '','')
dbutils.widgets.text('Run_id', '','')

# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")


job_id = dbutils.widgets.get('Job_id')
run_id = dbutils.widgets.get('Run_id')

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
# print(job_id, run_id)

# COMMAND ----------

spark.sql(f"""insert into {catalog_name}.{schema_name}.job_id 
          (job_id, run_id, updated_timestamp)
          values('{job_id}', '{run_id}', current_timestamp());""")