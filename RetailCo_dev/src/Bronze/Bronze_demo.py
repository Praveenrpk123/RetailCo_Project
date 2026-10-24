# Databricks notebook source
import json
import os
src_loc = "/Volumes/retailco/retailco_schema/raw_data/"
# file_path = os.path.join('../../../config_files/configs.json')
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(nb_path)
print(os.path.abspath(''))
path = os.path.abspath('../../config_files/raw_configs/configs.json')
print(path)

with open(path) as f:
    config = json.load(f)
    # print(config)
    for i in config['config']:
        # print(i['filename'], i['fileformat'])
        if i['fileformat'] == 'parquet':
            df = spark.read.parquet(f"{src_loc}{i['filename']}")
            df.limit(10).display()
# df = spark.read.csv("")
# /Workspace/Users/mallikaravipp11@gmail.com/retailco-pipeline/



# COMMAND ----------

# src_loc = "/Volumes/retailco/retailco_schema/raw_data/"
# path = os.path.abspath('../../config_files/raw_configs/configs.json')
# # print(path)

# products_schema = StructType([
#     StructField('product_id', StringType()),
#     StructField('product_name', StringType()),
#     StructField('category', StringType()),
#     StructField('sub_category', StringType()),
#     StructField('brand', StringType()),
#     StructField('cost_price', IntegerType()),
#     StructField('selling_price', DecimalType()),
#     StructField('stock_qty', IntegerType()),
#     StructField('supplier_id', StringType()),
# ])

# with open(path) as f:
#     config = json.load(f)
#     for i in config['config']:
#         filename = i['filename']
#         fileformat = i['fileformat'])
#         start_time = time.time()
#         if i['filename'] == 'products':
#             df = spark.readStream.\
#                 format('cloudFiles').\
#                 schema(products_schema).\
#                 option('cloudFiles.format', f"{i['fileformat']}").\
#                 option('cloudFiles.schemaEvolutionMode', 'addNewColumns').\
#                 option('cloudFiles.schemaLocation', f'/Volumes/retailco/retailco_schema/bronze/{i['filename']}/schema').\
#                 load(f'/Volumes/retailco/retailco_schema/raw_data/{i['filename']}')

#             df = df.\
#                 withColumn('Ingested_timestamp', lit(datetime.now())).\
#                 withColumn('file_name', col('_metadata.file_name')).\
#                 withColumn('Job_Id', lit(job_id)).\
#                 withColumn('Run_id', lit(run_id)).\
#                 withColumn('folder_path', concat_ws(
#                     "/",
#                     slice(
#                         split(col("_metadata.file_path"), "/"),
#                         1,
#                         size(split(col("_metadata.file_path"), "/")) - 1
#                     )
#                 ))
#                 # .\                withColumn('record_hash', sha2(concat_ws("||",*df.columns),256))

#             df.writeStream.\
#                 format('delta').\
#                 outputMode('append').\
#                 option('mergeSchema', 'true').\
#                 option('checkpointLocation',f'/Volumes/retailco/retailco_schema/bronze/{i['filename']}/checkpoint').\
#                 option('path',f"/Volumes/retailco/retailco_schema/bronze/{i['filename']}/").\
#                 trigger(availableNow = True).\
#                 start()
#         else:
#             df = spark.readStream.\
#                 format('cloudFiles').\
#                 option('cloudFiles.format', f"{i['fileformat']}").\
#                 option('cloudFiles.schemaEvolutionMode', 'addNewColumns').\
#                 option('cloudFiles.inferSchema', 'true').\
#                 option('cloudFiles.schemaLocation', f'/Volumes/retailco/retailco_schema/bronze/{i['filename']}/schema').\
#                 load(f'/Volumes/retailco/retailco_schema/raw_data/{i['filename']}')

#             df = df.\
#                 withColumn('Ingested_timestamp', lit(datetime.now())).\
#                 withColumn('file_name', col('_metadata.file_name')).\
#                 withColumn('Job_Id', lit(dbutils.widgets.get('Job_id'))).\
#                 withColumn('Run_id', lit(dbutils.widgets.get('Run_id'))).\
#                 withColumn('folder_path', concat_ws(
#                     "/",
#                     slice(
#                         split(col("_metadata.file_path"), "/"),
#                         1,
#                         size(split(col("_metadata.file_path"), "/")) - 1
#                     )
#                 )).\
#                 withColumn('record_hash', sha2(concat_ws("||",*df.columns),256))

#             df.writeStream.\
#                 format('delta').\
#                 outputMode('append').\
#                 option('mergeSchema', 'true').\
#                 option('checkpointLocation',f'/Volumes/retailco/retailco_schema/bronze/{i['filename']}/checkpoint').\
#                 option('path',f"/Volumes/retailco/retailco_schema/bronze/{i['filename']}/").\
#                 trigger(availableNow = True).\
#                 start()
#         timetaken = time.time() - start_time
#         spark.sql(f"""insert into retailco.retailco_schema.job_logs(job_id , job_name, run_id , filename, timetaken, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) values ('{job_id}', '{job_name}', '{run_id}' ,'{filename}','{timetaken}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")

# COMMAND ----------

#  databricks secrets put-secret Retailco_scope job_token --string-value=dapi032a7c93aee1125f9fe0d0ce7e6742eb


# COMMAND ----------

import os
print('nb_path',nb_path)
notebook_dir = os.path.dirname(nb_path)
print('n_d',notebook_dir)
config_path = os.path.join(
    notebook_dir,
    "..",
    "..",
    "config_files",
    "raw_configs",
    "configs.json"
)
print(config_path)
config_path = os.path.normpath(config_path)

print(config_path)

# COMMAND ----------

df_products = spark.read.format('json').option('multiline',True).load("/Volumes/retailco/retailco_schema/raw_data/products/")
df_store_events = spark.read.format('json').load("/Volumes/retailco/retailco_schema/raw_data/store_events/")


# COMMAND ----------

# spark.sql(f"""insert into retailco.retailco_schema.job_logs(job_id , job_name, run_id , job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) values ('{job_id}', '{job_name}', '{run_id}' , '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")

# COMMAND ----------

# df = dbutils.secrets.get("Retailco_scope","job_token")
# print(df)

# COMMAND ----------

# df = dbutils.fs.ls('dbfs:/Volumes/retailco/retailco_schema/run_logs/261214072708056_05_07_2026_04_29.json')
# path = '/Volumes/retailco/retailco_schema/run_logs/261214072708056_05_07_2026_04_29.json'
# with open(path, 'r') as f:
#     data = json.load(f)
#     print(data)


# COMMAND ----------

df_store_events.limit(10).display()

# COMMAND ----------

# try:
# df = spark.readStream.format('cloudFiles').option('cloudFiles.format', 'parquet').option('cloudFiles.schemaLocation', '/Volumes/retailco/retailco_schema/raw_data/dummy/schema').load('/Volumes/retailco/retailco_schema/raw_data/orders')
from datetime import datetime
from pyspark.sql.functions import *


# from pyspark.sql.functions import split, slice, concat_ws, col

# df = df.withColumn(
#     "folder_path",
#     concat_ws(
#         "/",
#         slice(
#             split(col("_metadata.file_path"), "/"),
#             1,
#             size(split(col("_metadata.file_path"), "/")) - 1
#         )
#     )
# )

# withColumn('folder_path', concat_ws("//", (col('_metadata.file_path').split('/'))))
dbutils.widgets.text('Run_id', '', 'Databrick_Run_Id')
dbutils.widgets.text('Job_id', '', 'Databrick_Job_Id')


# run_id = dbutils.widgets.get('Run_id')
# job_id = dbutils.widgets.get('Job_id')


df = df_store_events.\
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
    withColumn('record_hash', sha2(concat_ws("||",*df_store_events.columns),256))

df.display()

# df = df.withColumn('')


# sp_df = spark.read.format('parquet').load('/Volumes/retailco/retailco_schema/raw_data/orders')

#     print(df)
# except Exception:
#     import pandas as pd
#     pd_df = pd.read_parquet('/Volumes/retailco/retailco_schema/raw_data/orders')
#     spark_df = spark.createDataFrame(pd_df)
    # print(spark_df)


# COMMAND ----------

parquet_file = "/Volumes/retailco/retailco_schema/raw_data/orders/orders_2026-06-28_10:51/year=2023/month=12/1c5e1e1a52a94523863a3677b8ed67be-0.parquet"
from pyspark.sql.functions import *
from pyspark.sql.types import *

# dbutils.fs.head(parquet_file)
# df.to_parquet(parquet_file)


import pandas as pd

# Read a local Parquet file
df = pd.read_parquet(parquet_file)
sp_df = spark.createDataFrame(df)
# sp_df = sp_df.withColumn("order_date", to_date(date_format(col('order_date'), 'yyyy-mm-dd')))
# df['order_data'] = df['order_date'].astype(date)

# View the first few rows
# print(df.display())
# print(df.dtypes)
# sp_df.display()
sp_df.write.format('parquet').mode('append').save('/Volumes/retailco/retailco_schema/raw_data/dummy')

# COMMAND ----------

# -- select 1 from qs.sql_practise;
df = spark.read.parquet('/Volumes/retailco/retailco_schema/raw_data/dummy')
df.display()

# COMMAND ----------

import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

src_loc = "/Volumes/retailco/retailco_schema/raw_data/"
path = os.path.abspath('../../config_files/raw_configs/configs.json')
# print(path)

with open(path) as f:
    config = json.load(f)
    for i in config['config']:
        print(i['filename'], i['fileformat'])
        df = spark.readStream.\
            format('cloudFiles').\
            option('cloudFiles.format', f"{i['fileformat']}").\
            option('cloudFiles.schemaLocation', f'/Volumes/retailco/retailco_schema/bronze/{i['filename']}/schema').\
            load(f'/Volumes/retailco/retailco_schema/raw_data/{i['filename']}')

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

        df.writeStream.\
            format('delta').\
            outputMode('append').\
            option('checkpointLocation',f'/Volumes/retailco/retailco_schema/bronze/{i['filename']}/checkpoint').\
            option('path',f"/Volumes/retailco/retailco_schema/bronze/{i['filename']}/schema").\
            trigger(availableNow = True).\
            start()
            

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

fileformat = 'json'
filename = 'products'

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

df = spark.readStream.\
    format('cloudFiles').\
    schema(products_schema).\
    option('cloudFiles.format', f"{fileformat}").\
    option('cloudFiles.schemaLocation', f'/Volumes/retailco/retailco_schema/bronze/{filename}/schema').\
    load(f'/Volumes/retailco/retailco_schema/raw_data/{filename}')

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

df.writeStream.\
    format('delta').\
    outputMode('append').\
    option('checkpointLocation',f'/Volumes/retailco/retailco_schema/bronze/{filename}/checkpoint').\
    option('path',f"/Volumes/retailco/retailco_schema/bronze/{filename}/").\
    trigger(availableNow = True).\
    start()

# COMMAND ----------



df = spark.readStream.\
    format('cloudFiles').\
    option('cloudFiles.format', 'parquet').\
    option('cloudFiles.schemaLocation', f'/Volumes/retailco/retailco_schema/bronze/{}/schema').\
    load('/Volumes/retailco/retailco_schema/raw_data/orders')

df = df.\
    withColumn('Ingested_timestamp', lit(datetime.now())).\
    withColumn('file_name', col('_metadata.file_name')).\
    withColumn('Job_Id', lit(dbutils.widgets.get('Job_id'))).\
    withColumn('Run_id', lit(dbutils.widgets.get('Run_id'))).\
    withColumn('folder_path', concat_ws(
;        "/",
        slice(
            split(col("_metadata.file_path"), "/"),
            1,
            size(split(col("_metadata.file_path"), "/")) - 1
        )
    )).\
    withColumn('record_hash', sha2(concat_ws("||",*df_store_events.columns),256))

df.writeStream.\
    format('delta').\
    outputMode('append').\
    option('checkpointLocation','').\
    option('path','').\
    trigger(availableNow = True).\
    start()


# COMMAND ----------

# df = spark.read.json('/Volumes/retailco/retailco_schema/run_logs/540980654998278_05_07_2026_07_48.json')
import json
with open('/Volumes/retailco/retailco_schema/run_logs/540980654998278_05_07_2026_07_48.json', 'r') as f:
    df = json.load(f)
    print(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retailco.retailco_schema.control_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct run_id from retailco.retailco_schema.job_logs;

# COMMAND ----------

# token = dbutils.secrets.get("Retailco_scope","job_token")
# print(token)
print(dbutils.secrets.help())

# COMMAND ----------

print(dbutils.secrets.get("retailco"))

# COMMAND ----------

import requests, json
from datetime import datetime
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
# token = dbutils.secrets.get("my_scope", "databricks_pat")
# token = "dapi529473ccc08129973435d5fe8a62bdb8"
token = dbutils.secrets.get("Retailco_scope","job_token")
headers = {
    "Authorization": f"Bearer {token}"
}
# run_id = 562182904517179
run_id = dbutils.widgets.get('Run_id')
print(run_id)
url = f"https://{workspace_url}/api/2.1/jobs/runs/get?run_id={run_id}"
response = requests.get(url, headers = headers)
print(type(response.json()))
response_val = response.json()
run_details = json.dumps(response_val, indent = 4)
# json_run = json.(dump)
print(run_details)
print(type(run_details))
# print(type(json_run))

# print(type(json.dumps(run_details, indent=4)))        # Pretty print JSON
# print(json.dumps(run_details, sort_keys=True))  # Sorted keys
# print(json.dumps(run_details, ensure_ascii=False))  # Non-ASCII encoding
# print(json.dumps([{k: run_details[k]} for k in run_details]))  # Convert to JSON array format

# p_df = pd.DataFrame(df)
# spark_df = spark.createDataFrame([df])
# df = json.load(df)
# json_df = spark.createDataFrame(df)
# display(spark_df)
# display(json_df)
# for i,j in df.items():
#     print(i, j)
date_val = datetime.strftime(datetime.now(), '%d_%m_%Y_%H_%M')
print(date_val)
run_log_path = '/Volumes/retailco/retailco_schema/run_logs'

# with open(f"{run_log_path}/{run_id}_{date_val}.json","w") as f:
#     json.dump(run_details, f, indent = 4)