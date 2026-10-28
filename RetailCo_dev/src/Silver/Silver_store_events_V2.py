# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing required libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import os, json
from utils.retailco_dq import retailco_dq

# COMMAND ----------

# MAGIC %md
# MAGIC ###Getting pipeline data using widgets

# COMMAND ----------

dbutils.widgets.text('Run_id', 'None')
dbutils.widgets.text('Job_id', 'None')
dbutils.widgets.text('Job_name', 'None')
dbutils.widgets.text('No_of_retries', 'None')
dbutils.widgets.text('Job_datetime', 'None')
dbutils.widgets.text('Trigger_type', 'None')
dbutils.widgets.text('Trigger_datetime', 'None')
# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","")
dbutils.widgets.text("schema_name", "")

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
# MAGIC ###Incremental loading

# COMMAND ----------

src = 'bronze'
tgt = 'silver'
tn = 'fact_store_events'
max_timestamp = ''

ts_val = spark.sql(f"""select coalesce(max_timestamp,timestamp('9999-12-31')) as max_dt from {catalog_name}.{schema_name}.control_table where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")

#ts.collect() will return a list. if len of list is not equal to zero, gets from control table else default value
if len(ts_val.collect()) != 0:
    max_timestamp = ts_val.collect()[0]['max_dt']
else:
    max_timestamp = datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
# display(max_timestamp)

# COMMAND ----------

path = '/Volumes/{catalog_name}/{schema_name}/bronze/store_events/'

##Data should be greater than last batch run - Incremental loading
df_store_events = spark.read.format('delta').load(path).filter(col('ingested_timestamp') > lit(max_timestamp))

#if count of our data is zero, Need to get out from this notebook(It will skip it)
# and inserting in our job logs with value as 0, else continue..
if df_store_events.count() == 0:
    init_cnt = 0
    final_cnt = 0
    spark.sql(f"""insert into {catalog_name}.{schema_name}.job_logs
              (job_id , job_name, run_id, table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}', '{tn}' ,'{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
    # logger.info("No records found. Skipping processing.")
    dbutils.notebook.exit("NO_DATA")

#initial Records count
init_cnt = df_store_events.count()


# COMMAND ----------

# MAGIC %md
# MAGIC ###DEDUP

# COMMAND ----------

window_val = Window.partitionBy(col('row_hash')).orderBy(col('ingested_timestamp'))

un_col = ['_rescued_data', 'Ingested_timestamp','file_name', 'Job_id', 'Run_id', 'folder_path', 'record_hash']
cols = []
print(df_store_events.count())
for i in df_store_events.columns:
    if i not in un_col:
        cols.append(i)

df_hash = df_store_events.withColumn('row_hash', sha2(
    concat_ws(
        "-", *cols), 256))

df = df_hash.withColumn("rn", 
                   row_number().over(window_val)).filter(col('rn') == 1).drop(col('rn'))
# display(df.limit(10))
# print(df.count())



# COMMAND ----------

# MAGIC %md
# MAGIC ###Trim and Standardize

# COMMAND ----------

df_standardize = df.\
    withColumn('customer_id', upper(trim(col('customer_id')))).\
    withColumn('device', initcap(trim(col('device')))).\
    withColumn('event_id', trim(col('event_id'))).\
    withColumn('event_type',trim(col('event_type'))).\
    withColumn('order_id', upper(trim(col('order_id')))).\
    withColumn('platform', initcap(trim(col('platform')))).\
    withColumn('timestamp', trim(col('timestamp')))

display(df_standardize.limit(10))

# display(df_standardize.select(col('device')).distinct())

# ----Event_type
# order_placed
# checkout_start
# payment_initiated
# add_to_cart
# page_view
# order_cancelled


# ----device
# Web
# Ios
# Android
# Tablet



# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA QUALITY CHECKS

# COMMAND ----------

import os
path = os.path.abspath('../config_files/silver_configs/silver_store_events_dq.json')
with open(path, 'r') as f:
    config = json.load(f)
    rc_dq = retailco_dq(spark)
    for i in config['configs']:
        # print(i['check'])
        # #Not Null checks
        if i["check"] == 'Not_Null' and i["enabled"]  == True:
            df, msg = rc_dq.not_null_check( job_id = job_id, run_id = run_id, 
                                 table_name = tn, check = i["check"],
                                  df = df, col_name = i["col_name"], catalog_name = catalog_name, schema_name = schema_name )
            print(msg)

        #Date DataType check
        if i["check"] == 'date_dataType_check' and i["check_type"] != None  and i["enabled"]  == True:
            df, msg = rc_dq.datatype_check(job_id = job_id, run_id = run_id, table_name = tn, 
                check = i["check"], check_type = i["check_type"], df = df, col_name = i["col_name"], catalog_name = catalog_name, schema_name = schema_name)
            print(msg)

        #sequence check
        if i["check"] == 'Sequence_ck'and i["check_list"] != None  and i["enabled"]  == True:
            old_cnt = df.count()
            if type(i["check_list"]) == list:
                df, msg = rc_dq.sequence_check(job_id = job_id, run_id = run_id, table_name = tn, 
                check = i["check"], df =df, col_name = i["col_name"], 
                check_list = i["check_list"], catalog_name = catalog_name, schema_name = schema_name)
                print(msg)
df_dq = df
# #Not Null Checks
# if check == 'Not_Null':
#     older_count = df.count()
#     df = df.filter(col(f'{col_name}').isNotNull())
#     new_count = df.count()
#     print(f"No of records dropped due to {check} check is {older_count - new_count}")

# #Date DataType_check
# if check == 'date_dataType_Check' and check_type != None:
#     old_cnt = df.count()
#     df = df.\
#         filter(col(f'{col_name}_check').try_cast(f'{check_type}').isNotNull()).\
#         withColumn(f'{col_name}', col(f'{col_name}').cast(f'{check_type}'))
#     new_count = df.count()
#     print(f"No of records dropped due to {check} check is {old_cnt - new_count}")

# #sequence_check
# if check == 'Sequence_ck'and check_list != None:
#     if type(check_list) == list:
#         df = df.filter(col(f'{col_name}').isin(f'{check_list}'))
#         new_count = df.count()
#         print(f"No of records dropped due to {check} check is {old_cnt - new_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drop unwanted cols and renaming cols

# COMMAND ----------

df_dq = df_dq.drop('Run_id', 'Job_Id', 'folder_path', 'record_hash', 'file_name').\
    withColumnRenamed('timestamp', 'created_ts').\
    withColumnRenamed('Ingested_timestamp', 'ingested_timestamp')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Join columns to filter out only macthing records of the table

# COMMAND ----------

#dim customers
df_cust = spark.read.table(f'{catalog_name}.{schema_name}.dim_customers').filter(col('is_active') == True)

#fact_orders
df_ord = spark.read.table(f'{catalog_name}.{schema_name}.fact_orders')

#dim Return events
df_return_ents = spark.read.table(f'{catalog_name}.{schema_name}.dim_return_events')

#Joining final results based on dim customer table and exchanging customer_id with customer_sk
df_cust_join = df_dq.join(df_cust, 'customer_id', 'inner').\
    select(df_dq['*'], df_cust['customer_sk']).\
    drop('customer_id')

#Joining customer results based on  fact orders table and exchanging order_id with order_sk
df_ord_join = df_cust_join.join(df_ord, 'order_id', 'inner').\
    select(df_cust_join['*'], df_ord['order_sk']).\
    drop('order_id')

#Joining orders results based on dim return events table and exchanging event_type with event_type_id
df_return_events_join = df_ord_join.join(df_return_ents, 'event_type', 'inner').\
    select(df_ord_join['*'], df_return_ents['event_type_id']).\
    withColumnRenamed('Ingested_timestamp', 'ingested_timestamp').\
    drop('event_type')

#Selecting the final values to be displayed in a store events table.
df_final = df_return_events_join.\
    select('event_id', 
           'session_id',
           'event_type_id',
           'order_sk',
           'customer_sk',
           'platform',
           'device',
           'row_hash',
           'created_ts',
           'ingested_timestamp'
           )

# display(df_final.limit(1))
final_cnt = df_final.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD-1

# COMMAND ----------

#storing final data as a view
df_final.createOrReplaceTempView('src_store_events')

#storing maximum ingested timestamp in a max_dt variable
max_ts = df_final.select(max(col('ingested_timestamp')).alias('max_dt'))
max_dt =  max_ts.collect()[0]['max_dt']

#If a particular table exists
if spark.catalog.tableExists(f'{catalog_name}.{schema_name}.fact_store_events'):
    #Getting Target Delta table
    target = DeltaTable.forName(spark, f'{catalog_name}.{schema_name}.fact_store_events')
    target_df = target.toDF()

    #if target is not an empty table
    if target_df.count() > 0:
        spark.sql(f"""
                  merge into {catalog_name}.{schema_name}.fact_store_events as target
                  using src_store_events as source
                  on target.event_id = source.event_id
                  when matched and source.row_hash != target.row_hash
                  then update set 
                    target.event_id = source.event_id,
                    target.session_id = source.session_id,
                    target.event_type_id = source.event_type_id,
                    target.order_sk = source.order_sk,
                    target.customer_sk = source.customer_sk,
                    target.platform = source.platform,
                    target.device = source.device,
                    target.row_hash = source.row_hash,
                    target.created_ts = source.created_ts,
                    target.ingested_timestamp = source.ingested_timestamp
                  when not matched then insert 
                  (
                    event_id,
                    session_id,
                    event_type_id,
                    order_sk,
                    customer_sk,
                    platform,
                    device,
                    row_hash,
                    created_ts,
                    ingested_timestamp
                  )
                  values
                  (
                    event_id,
                    session_id,
                    event_type_id,
                    order_sk,
                    customer_sk,
                    platform,
                    device,
                    row_hash,
                    created_ts,
                    ingested_timestamp
                  )
                  """)

        #Removing record for this table and adding a new record for the table
        spark.sql(f"""
            delete from {catalog_name}.{schema_name}.control_table
            where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';
            """)
    
        spark.sql(f"""insert into {catalog_name}.{schema_name}.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{tn}', '{max_dt}', true, current_timestamp());""")
    
        #Updating Job logs
        spark.sql(f"""insert into {catalog_name}.{schema_name}.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}' , '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
    else:
        #If it is an empty table
        spark.sql(f"""
              insert into {catalog_name}.{schema_name}.fact_store_events
              (
                  event_id,
                  session_id,
                  event_type_id,
                  order_sk,
                  customer_sk,
                  platform,
                  device,
                  row_hash,
                  created_ts,
                  ingested_timestamp
              )
              select
                  event_id,
                  session_id,
                  event_type_id,
                  order_sk,
                  customer_sk,
                  platform,
                  device,
                  row_hash,
                  created_ts,
                  ingested_timestamp
              from src_store_events
              """)

        #Removing record for this table and adding a new record for the table
        spark.sql(f"""delete from {catalog_name}.{schema_name}.control_table 
              where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")
    
        spark.sql(f"""insert into {catalog_name}.{schema_name}.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{tn}', '{max_dt}', true, current_timestamp());""")
        
        #Updating Job logs
        spark.sql(f"""insert into {catalog_name}.{schema_name}.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
        
else:
    #if such no table exists
    #create a table
    spark.sql(f"""
              create table if not exists {catalog_name}.{schema_name}.fact_store_events (
              event_sk bigint generated by default as identity primary key, 
              event_id string,
              session_id string,
              event_type_id string,
              order_sk string,
              customer_sk string,
              platform string,
              device string,
              row_hash string,
              created_ts timestamp, 
              ingested_timestamp timestamp)
              using delta
              partitioned by (ingested_timestamp)""")
    
    spark.sql(f"""
              insert into {catalog_name}.{schema_name}.fact_store_events
              (
                  event_id,
                  session_id,
                  event_type_id,
                  order_sk,
                  customer_sk,
                  platform,
                  device,
                  row_hash,
                  created_ts,
                  ingested_timestamp
              )
              select 
                  event_id,
                  session_id,
                  event_type_id,
                  order_sk,
                  customer_sk,
                  platform,
                  device,
                  row_hash,
                  created_ts,
                  ingested_timestamp
              from src_store_events
              """)
    
    #Removing record for this table and adding a new record for the table
    spark.sql(f"""
            delete from {catalog_name}.{schema_name}.control_table
            where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';
            """)
    
    spark.sql(f"""
            insert into {catalog_name}.{schema_name}.control_table
            (
            src_layer, 
            tgt_layer, 
            table_name,
            max_timestamp,
            is_active,
            ct_ingested_timestamp
            )
            values
            (
                '{src}' ,
                '{tgt}' ,
                '{tn}' ,
                '{max_dt}' ,
                True,
                current_timestamp()
            ) 
            """)
    
    #Updating Job logs
    spark.sql(f"""insert into {catalog_name}.{schema_name}.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")