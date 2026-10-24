# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing required packages

# COMMAND ----------

import json
import time
import os
import requests
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Creating widgets and get value from widgets 
# We get values from the pipeline run and getting that values. 

# COMMAND ----------

dbutils.widgets.text('Run_id', '')
dbutils.widgets.text('Job_id', '')
dbutils.widgets.text('Job_name', '')
dbutils.widgets.text('No_of_retries', '')
dbutils.widgets.text('Job_datetime', '')
dbutils.widgets.text('Trigger_type', '')
dbutils.widgets.text('Trigger_datetime', '')

# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","RetailCo_Dev_Catalog")
dbutils.widgets.text("schema_name", "RetailCo_Dev_schema")
# # print(dbutils.jobs.taskValues)

# COMMAND ----------

job_id = dbutils.widgets.get('Job_id')
job_datetime = dbutils.widgets.get('Job_datetime')
job_name = dbutils.widgets.get('Job_name')
no_of_retries = dbutils.widgets.get('No_of_retries')
run_id = dbutils.widgets.get('Run_id')
trigger_datetime = dbutils.widgets.get('Trigger_datetime')
trigger_type = dbutils.widgets.get('Trigger_type')

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ingestion and loading

# COMMAND ----------

#Providing catalog_name based on environments
catalog_name = ''
schema_name = ''
# src_path and getting the config files
src_loc = "/Volumes/{catalog_name}/{schema_name}/raw_data/"
path = os.path.abspath('../../config_files/raw_configs/configs.json')
# print(path)

# Using autoloader for the ingesting and loading in bronze layer.
# defined Products_schema because this products has json which is not working in autoloader.
products_schema = StructType([
    StructField('product_id', StringType()),
    StructField('product_name', StringType()),
    StructField('category', StringType()),
    StructField('sub_category', StringType()),
    StructField('brand', StringType()),
    StructField('cost_price', IntegerType()),
    StructField('selling_price', DecimalType()),
    StructField('stock_qty', IntegerType()),
    StructField('supplier_id', StringType()),
])

#Opening the config file and getting the config data which has file name and the data type of the file.
with open(path) as f:
    config = json.load(f)
    for i in config['config']:
        filename = i['filename']
        fileformat = i['fileformat']
        start_time = time.time() #start time
        if filename == 'products':
            df = spark.readStream.\
                format('cloudFiles').\
                option('cloudFiles.format', f"{fileformat}").\
                option('cloudFiles.schemaLocation', f'/Volumes/{catalog_name}/{schema_name}/bronze/{filename}/schema').\
                option('cloudFiles.schemaHints', "product_id string, product_name string, category string, sub_category string, brand string, cost_price int, selling_price decimal(10,2), stock_qty int, supplier_id string").\
                option('cloudFiles.schemaEvolutionMode', 'addNewColumns').\
                option("multiLine", "true").\
                load(f'/Volumes/{catalog_name}/{schema_name}/raw_data/{filename}')
            
            # adding few columns to the dataframe for basic details of the bronze file.
            df = df.\
                withColumn('Ingested_timestamp', lit(datetime.now())).\
                withColumn('file_name', col('_metadata.file_name')).\
                withColumn('Job_Id', lit(job_id)).\
                withColumn('Run_id', lit(run_id)).\
                withColumn('folder_path', concat_ws(
                    "/",
                    slice(
                        split(col("_metadata.file_path"), "/"),
                        1,
                        size(split(col("_metadata.file_path"), "/")) - 1
                    )
                ))
                # .\                withColumn('record_hash', sha2(concat_ws("||",*df.columns),256))
            
            #writing data into the bronze layer
            df.writeStream.\
                format('delta').\
                outputMode('append').\
                option('mergeSchema', 'true').\
                option('checkpointLocation',f'/Volumes/{catalog_name}/{schema_name}/bronze/{filename}/checkpoint').\
                option('path',f"/Volumes/{catalog_name}/{schema_name}/bronze/{filename}/").\
                trigger(availableNow = True).\
                start()
        else:#if not products then it comes here
            df = spark.readStream.\
                format('cloudFiles').\
                option('cloudFiles.format', f"{fileformat}").\
                option('cloudFiles.inferSchema', 'true').\
                option('cloudFiles.schemaLocation', f'/Volumes/{catalog_name}/{schema_name}/bronze/{filename}/schema').\
                load(f'/Volumes/{catalog_name}/{schema_name}/raw_data/{filename}')
            
            # adding few columns to the dataframe for basic details of the bronze file.
            df = df.\
                withColumn('Ingested_timestamp', lit(datetime.now())).\
                withColumn('file_name', col('_metadata.file_name')).\
                withColumn('Job_Id', lit(dbutils.widgets.get('Job_id'))).\
                withColumn('Run_id', lit(dbutils.widgets.get('Run_id'))).\
                withColumn('folder_path', concat_ws(
                    "/",
                    slice(
                        split(col("_metadata.file_path"), "/"),
                        1,
                        size(split(col("_metadata.file_path"), "/")) - 1
                    )
                )).\
                withColumn('record_hash', sha2(concat_ws("||",*df.columns),256))

            #writing data into the bronze layer
            df.writeStream.\
                format('delta').\
                outputMode('append').\
                option('mergeSchema', 'true').\
                option('checkpointLocation',f'/Volumes/{catalog_name}/{schema_name}/bronze/{filename}/checkpoint').\
                option('path',f"/Volumes/{catalog_name}/{schema_name}/bronze/{filename}/").\
                trigger(availableNow = True).\
                start()

        #total_time_taken
        timetaken = time.time() - start_time

        #Inserting the metadata into the job_log table
        spark.sql(f"""insert into retailco.retailco_schema.job_logs(job_id , job_name, run_id , table_name,  timetaken, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) values ('{job_id}', '{job_name}', '{run_id}' ,'{filename}', '{timetaken}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")