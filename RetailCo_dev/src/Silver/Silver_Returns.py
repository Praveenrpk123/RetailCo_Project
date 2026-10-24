# Databricks notebook source
# MAGIC %md
# MAGIC ###Importing required libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
import os, json
from datetime import datetime , timedelta
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting pipeline data using widgets

# COMMAND ----------

dbutils.widgets.text('Run_id', 'None')
dbutils.widgets.text('Job_id', 'None')
dbutils.widgets.text('Job_name', 'None')
dbutils.widgets.text('No_of_retries', 'None')
dbutils.widgets.text('Job_datetime', 'None')
dbutils.widgets.text('Trigger_type', 'None')
dbutils.widgets.text('Trigger_datetime', 'None')

job_id = dbutils.widgets.get('Job_id')
job_datetime = dbutils.widgets.get('Job_datetime')
job_name = dbutils.widgets.get('Job_name')
no_of_retries = dbutils.widgets.get('No_of_retries')
run_id = dbutils.widgets.get('Run_id')
trigger_datetime = dbutils.widgets.get('Trigger_datetime')
trigger_type = dbutils.widgets.get('Trigger_type')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Incremental Loading

# COMMAND ----------

#getting max timestamp for this table from control table.
src = 'bronze'
tgt = 'silver'
tn = 'fact_returns'
max_timestamp = ''

ts_val = spark.sql(f"""select coalesce(max_timestamp,timestamp('9999-12-31')) as max_dt from retailco.retailco_schema.control_table where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")

print(len(ts_val.collect()))
#ts.collect() will return a list. if len of list is not equal to zero, gets from control table else default value

if len(ts_val.collect()) != 0:
    max_timestamp = ts_val.collect()[0]['max_dt']
else:
    max_timestamp = datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
display(max_timestamp)

# COMMAND ----------

path = '/Volumes/retailco/retailco_schema/bronze/returns/'

#Data should be greater than last batch run - Incremental loading
df_returns = spark.read.format('delta').load(path).filter(col('ingested_timestamp') > lit(max_timestamp))

#if count of our data is zero, Need to get out from this notebook(It will skip it)
# and inserting in our job logs with value as 0, else continue..
if df_returns.count() == 0:
    init_cnt = 0
    final_cnt = 0
    spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id, table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}', '{tn}' ,'{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
    # logger.info("No records found. Skipping processing.")
    dbutils.notebook.exit("NO_DATA")

init_cnt = df_returns.count()
# display(df_returns.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trim and standardize columns

# COMMAND ----------

df_trim = df_returns.\
    withColumn('return_id', upper(trim(col('return_id')))).\
    withColumn('order_id', upper(trim(col('order_id')))).\
    withColumn('customer_id', upper(trim(col('customer_id')))).\
    withColumn('product_id', upper(trim(col('product_id')))).\
    withColumn('return_reason', initcap(col('return_reason')))
   

df_standardize = df_trim.\
    withColumn('refund_amount', round(col('refund_amount').cast('double'), 2)).\
    drop('file_name', 'Job_Id', 'Run_id', 'folder_path')

# COMMAND ----------

# MAGIC %md
# MAGIC ###DEDUP

# COMMAND ----------

window_val = Window.partitionBy(col('primary_record_hash')).orderBy(col('customer_id'))

df = df_standardize.\
    withColumn('primary_record_hash', 
               sha2(concat_ws("-", 
                              coalesce(col("return_id"), lit("")), 
                              coalesce(col("order_id"), lit("")), 
                              coalesce(col("customer_id"), lit("")),
                              coalesce(col("product_id"), lit("")), 
                              coalesce(col("return_date"), lit("")), 
                              coalesce(col("return_reason"), lit("")), 
                              coalesce(col("refund_amount"), lit(""))), 256)).\
    withColumn("row_number", 
               row_number().over(window_val)).\
    filter(col('row_number') ==  1).\
    drop('row_number')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Quality Check

# COMMAND ----------

path = os.path.abspath('../../config_files/silver_configs/silver_return_dq.json')
with open(path, 'r') as f:
    configs = json.load(f)
    config_val = configs['configs']
    for i in config_val:
        total_checks = 0
        perc = 0
        #Not Null check
        if i["check"] == 'Not_Null':
            older_count = df.count()
            df = df.filter(col(f'{i["col_name"]}').isNotNull())
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')

        #Date DataType_check
        if i["check"] == 'date_dataType_check' and i["check_type"] != None:
            old_cnt = df.count()
            df = df.\
                filter(col(f'{i["col_name"]}').try_cast(f'{i["check_type"]}').isNotNull()).\
                withColumn(f'{i["col_name"]}', col(f'{i["col_name"]}').cast(f'{i["check_type"]}'))
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {old_cnt - new_count}')

        #Greater than some value
        if i["check"] == 'greater_than' and i["value_to_ck"] != None:
            older_count = df.count()
            df = df.filter(col(f'{i["col_name"]}') > i["value_to_ck"])
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')
    print("Data Quality Perc is : " , (perc/total_checks)*100)

#sequence_check
# if check == 'Sequence_ck'and check_list != None:
#     if type(check_list) == list:
#         df = df.withColumn(f'{col_name}', 
#                 when(col(f'{col_name}').\
#                             isin(f'{check_list}'), col('{col_name}')).\
#                 otherwise(lit('Unknown')))
df_dq = df
display(df_dq.limit(1))
print(df_dq.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Join columns to filter out only macthing records of the table

# COMMAND ----------

#dim customers
df_cust_join = spark.read.table('retailco.retailco_schema.dim_customers').\
    filter(col('is_active') == True)

#dim products
df_prod_join = spark.read.table('retailco.retailco_schema.dim_products').\
    filter(col('is_active') == True)

#fact_orders
df_order_join =  spark.read.table('retailco.retailco_schema.fact_orders')
# df_order_join.count()

#Joining final results based on dim customer table and exchanging customer_id with customer_sk
df_cust = df_dq.\
    join(df_cust_join, 
         ((df_dq['customer_id'] == df_cust_join['customer_id'])) , 'inner').\
             select(df_dq['*'], df_cust_join['customer_sk']).\
    drop('customer_id')
print(df_cust.count())

#Joining customer results based on dim products table and exchanging product_id with product_sk
df_prod = df_cust.\
    join(df_prod_join, 
         df_prod_join['product_id'] == df_cust['product_id'] , 'inner').\
             select(df_cust['*'], df_prod_join['product_sk']).\
    drop('product_id')
print('before_cnt',df_prod.count())

#Joining products results based on fact orders table and exchanging order_id with order_sk and getting few more columns
df_orders_stg = df_order_join.\
    join(df_prod,
         ((df_order_join['order_id'] == df_prod['order_id']) & (df_order_join['status'] == 'Returned')),
         'inner').select(df_prod['*'], df_order_join['order_sk'], df_order_join['order_date'],  df_order_join['delivery_date'], df_order_join['net_amount'], df_order_join['gross_amount']).\
    drop('order_id')
# print('after_cnt',df_orders_stg.count())

#Selecting the final values to be displayed in a returns table.
df_final = df_orders_stg.\
    withColumnRenamed('Ingested_timestamp', 'ingested_timestamp').\
    select('return_id', 
           'order_sk',
           'product_sk',
           'customer_sk',
           'order_date',
           'delivery_date',
           'return_date',
           'return_reason',
           'refund_amount',
           'gross_amount',
           'net_amount',
           'primary_record_hash',
           'ingested_timestamp'
           )
final_cnt = df_final.count()
display(df_final.limit(1))

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD-1

# COMMAND ----------

#storing final data as a view
df_final.createOrReplaceTempView('returns_view')

#storing maximum ingested timestamp in a max_dt variable
max_ts = df_final.select(max(col('ingested_timestamp')).alias('max_dt'))
max_dt =  max_ts.collect()[0]['max_dt']

#If a particular table exists
if spark.catalog.tableExists('retailco.retailco_schema.fact_returns'):

    #Getting Target Delta table
    target = DeltaTable.forName(spark, 'retailco.retailco_schema.fact_returns')
    target_df = target.toDF()

    #if target is not an empty table
    if target_df.count() > 0:
        spark.sql("""
                  merge into retailco.retailco_schema.fact_returns as target
                  using returns_view as source
                  on target.return_id = source.return_id
                  when matched and source.primary_record_hash != target.primary_record_hash then update set 
                    target.return_id =  source.return_id,
                    target.order_sk = source.order_sk,
                    target.product_sk = source.product_sk,
                    target.customer_sk = source.customer_sk,
                    target.order_date = source.order_date,
                    target.delivery_date = source.delivery_date,
                    target.return_date = source.return_date,
                    target.return_reason = source.return_reason,
                    target.refund_amount = source.refund_amount,
                    target.gross_amount = source.gross_amount,
                    target.net_amount = source.net_amount,
                    target.primary_record_hash = source.primary_record_hash,
                    target.ingested_timestamp = source.ingested_timestamp
                  when not matched then insert 
                  (
                      return_id, 
                      order_sk,
                      product_sk,
                      customer_sk,
                      order_date,
                      delivery_date,
                      return_date,
                      return_reason,
                      refund_amount,
                      gross_amount,
                      net_amount,
                      primary_record_hash,
                      ingested_timestamp
                  ) 
                  values 
                  (
                      return_id, 
                      order_sk,
                      product_sk,
                      customer_sk,
                      order_date,
                      delivery_date,
                      return_date,
                      return_reason,
                      refund_amount,
                      gross_amount,
                      net_amount,
                      primary_record_hash,
                      ingested_timestamp
                  )
                  """)
        
        #Removing record for this table and adding a new record for the table
        spark.sql(f"""delete from retailco.retailco_schema.control_table 
              where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")
    
        spark.sql(f"""insert into retailco.retailco_schema.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{tn}', '{max_dt}', true, current_timestamp());""")
      
        #Updating Job logs      
        spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
    else:
        #If it is an empty table
        spark.sql("""
               Insert into retailco.retailco_schema.fact_returns(
                    return_id, 
                    order_sk,
                    product_sk,
                    customer_sk,
                    order_date,
                    delivery_date,
                    return_date,
                    return_reason,
                    refund_amount,
                    gross_amount,
                    net_amount,
                    primary_record_hash,
                    ingested_timestamp
               ) 
               SELECT
                    return_id, 
                    order_sk,
                    product_sk,
                    customer_sk,
                    order_date,
                    delivery_date,
                    return_date,
                    return_reason,
                    refund_amount,
                    gross_amount,
                    net_amount,
                    primary_record_hash,
                    ingested_timestamp
               FROM returns_view
               """)
        
        #Removing record for this table and adding a new record for the table
        spark.sql(f"""
            delete from retailco.retailco_schema.control_table
            where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';
            """)
    
        spark.sql(f"""
            insert into retailco.retailco_schema.control_table
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
    spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")

else:
    #if such no table exists
    #create a table and insert records in it.
    spark.sql("""
              CREATE TABLE IF NOT EXISTS retailco.retailco_schema.fact_returns (
                return_sk bigint GENERATED BY DEFAULT AS IDENTITY primary key,
                return_id string,
                order_sk long,
                product_sk long,
                customer_sk long,
                order_date date,
                delivery_date date,
                return_date date,
                return_reason string,
                refund_amount double,
                gross_amount double,
                net_amount double,
                primary_record_hash string,
                ingested_timestamp timestamp)
                using delta 
                partitioned by(ingested_timestamp);
            """)
    
    spark.sql("""
               Insert into retailco.retailco_schema.fact_returns(
                    return_id, 
                    order_sk,
                    product_sk,
                    customer_sk,
                    order_date,
                    delivery_date,
                    return_date,
                    return_reason,
                    refund_amount,
                    gross_amount,
                    net_amount,
                    primary_record_hash,
                    ingested_timestamp
               )
               SELECT
                    return_id, 
                    order_sk,
                    product_sk,
                    customer_sk,
                    order_date,
                    delivery_date,
                    return_date,
                    return_reason,
                    refund_amount,
                    gross_amount,
                    net_amount,
                    primary_record_hash,
                    ingested_timestamp
               FROM returns_view;
               """)
    
    #Removing record for this table and adding a new record for the table
    spark.sql(f"""
            delete from retailco.retailco_schema.control_table
            where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';
            """)
    
    spark.sql(f"""
            insert into retailco.retailco_schema.control_table
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
    spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retailco.retailco_schema.fact_returns;