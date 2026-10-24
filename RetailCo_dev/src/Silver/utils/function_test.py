# Databricks notebook source
from retailco import retailCo_pipeline
from datetime import datetime
# datas
src = 'src'
tgt = 'tgt'
table_name = 'table_name'
max_dt = datetime.now()

#object_creation
rtc_pipeline = retailCo_pipeline(spark)
rtc_pipeline.control_table(src,tgt, table_name,max_dt)
# rtc_pipeline.job_logs()




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retailco.retailco_schema.control_table;