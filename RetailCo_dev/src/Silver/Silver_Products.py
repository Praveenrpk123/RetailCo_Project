# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing required libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, timedelta, date
from pyspark.sql.window import Window
from functools import reduce
from delta.tables import DeltaTable
import os, json
import random

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
tn = 'dim_products'
max_timestamp = ''

ts_val = spark.sql(f"""select max_timestamp as max_dt from retailco.retailco_schema.control_table where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")

# print(len(ts_val.collect()))
#ts.collect() will return a list. if len of list is not equal to zero, gets from control table else default value
if len(ts_val.collect()) != 0:
    max_timestamp = ts_val.collect()[0]['max_dt']
else:
    max_timestamp = datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
# display(max_timestamp)


# COMMAND ----------

path = '/Volumes/retailco/retailco_schema/bronze/products/'
#Data should be greater than last batch run - Incremental loading
df = spark.read.format('delta').load(path).filter(col('ingested_timestamp') > lit(max_timestamp))

#if count of our data is zero, Need to get out from this notebook(It will skip it)
# and inserting in our job logs with value as 0, else continue..
if df.count() == 0:
    init_cnt = 0
    final_cnt = 0
    spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id, table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}', '{tn}' ,'{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
    # logger.info("No records found. Skipping processing.")
    dbutils.notebook.exit("NO_DATA")
init_cnt = df.count()
# display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dropping unwanted columns

# COMMAND ----------

df = df.\
    drop('file_name',
         'Job_Id',
         'Run_id',
         'folder_path')

# display(df.limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trim and Standardize

# COMMAND ----------

df = df.\
    withColumn("category", initcap(trim(col('category')))).\
    withColumn("cost_price", trim(col('cost_price'))).\
    withColumn("product_id", upper(trim(col('product_id')))).\
    withColumn("product_name", initcap(trim(col('product_name')))).\
    withColumn("selling_price", round(trim(col('selling_price')),2)).\
    withColumn("sub_category", initcap(trim(col('sub_category')))).\
    withColumn("supplier_id", upper(col('supplier_id')))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Duplicate Checks

# COMMAND ----------

window_val = Window.partitionBy(col('record_hash')).orderBy(col('ingested_timestamp'))
df = df.withColumn("record_hash", sha2(
    concat_ws('-', 
              coalesce(col('brand'), lit('')), 
              coalesce(col('category'), lit('')),
              coalesce(col('cost_price'), lit('')),
              coalesce(col('product_id'), lit('')), 
              coalesce(col('product_name'), lit('')),
              coalesce(col('selling_price'), lit('')),
              coalesce(col('stock_qty'), lit('')),
              coalesce(col('sub_category'), lit('')),
              coalesce(col('_rescued_data'), lit('')),
              coalesce(col('Ingested_timestamp'), lit(''))
),256))

df = df.\
    withColumn('rn', 
               row_number().over(window_val)).\
    filter(col('rn') == 1).\
    drop('rn')

# display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ##DATA QUALITY CHECKS

# COMMAND ----------

path = os.path.abspath('../../config_files/silver_configs/silver_prod_dq.json')
with open(path, 'r') as f:
    configs = json.load(f)
    # print(configs['configs'])
    for i in configs['configs']:
        total_checks = 1
        perc = 1
        #NOT NULL i["check"]
        if i["check"] == 'Not_Null' and i["enabled"] == True:
            older_count = df.count()
            df = df.filter(col(f'{i["col_name"]}').isNotNull())
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            # perc += 1 - (final_cnt/older_count)
            print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}")

        if i["check"] == 'gt_zero' and i["enabled"] == True:
            older_count = df.count()
            df = df.filter(col(f'{i["col_name"]}')> 0)
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            # perc += 1 - (final_cnt/older_count)
            print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}")

        #starts with i["check"] for Customer_Id etc..
        if i["check"] == 'pattern' and i["starts_with"] != None and i["ends_with"] == 'none' and i["enabled"] == True:
            older_count = df.count()
            if type(i["starts_with"]) == str:
                # df = df.filter(col(f'{i["col_name"]}').startswith(f'{i["starts_with"]}'))
                df = df.filter(col(f'{i["col_name"]}').rlike(f'^{i["starts_with"]}[0-9]+$'))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                # perc += 1 - (final_cnt/older_count)
                print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}")
            elif type(i["starts_with"]) == list:
                df = df.filter(
                    reduce(lambda x, y : x | y , [col(f'{i["col_name"]}').startswith(sw) for sw in i["starts_with"]])
                    )
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                # perc += 1 - (final_cnt/older_count)
                print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}")


        #Handling other datatypes
        if i["check"] == 'datatype_handling' and i["check_type"] != None and i["enabled"] == True:
            old_cnt = df.count()
            df = df.filter(col(f'{i["col_name"]}').cast(f'{i["check_type"]}').isNotNull()).\
                withColumn(f'{i["col_name"]}', col(f'{i["col_name"]}').cast(f'{i["check_type"]}'))
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            # perc += 1 - (final_cnt/older_count)
            print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}")
    # Data quality for this pipeline
    print("Data Quality Perc is : " , (perc/total_checks)*100)
df_dq = df

# COMMAND ----------

#Defining is_active column
window_val = Window.partitionBy(col('product_id')).orderBy(desc(col('effective_from')))

start_date = date(2016, 1, 1)
end_date = date(2018, 1, 1)
time_between_dates = end_date - start_date
days_between_dates = time_between_dates.days
print(days_between_dates)
rand_days = random.randrange(days_between_dates)

# date_add(start_date, rand_days)
#effective_from
# the issue we faced is rand() * days_between_dates will give decimal once floor operation applied 
df_eff_from =  df_dq.\
    withColumn('rand_days', floor(rand() * days_between_dates).cast('int')).\
    withColumn('effective_from', lit(date_add(lit(start_date), col('rand_days'))))

df_eff_to = df_eff_from.\
    withColumn('rn', row_number().over(window_val)).\
    withColumn('lag_val', lag(col('effective_from')).over(window_val)).\
    withColumn('effective_to', when(col('lag_val').isNull(), lit((date(9999,1,1)))).\
        otherwise(date_add(col('lag_val'), -1))).\
    withColumn('is_active', when(col('rn') == 1, True).otherwise(False)).\
    withColumnRenamed('Ingested_timestamp', 'ingested_timestamp').\
    drop('rn', 'lag_val', 'rand_days', '_rescued_date')
    # withColumn('effective_to', when(col('lag_val').isNotNull(), col('lag_val')).otherwise(lit(date(9999,1,1))))
    # withColumn('effective_from', lit(start_date + timedelta(days=col('rand_days'))))

#Getting the final columns to be updated in the table
df_final = df_eff_to.select('product_id',
                            'product_name' , 
                            'category' , 
                            'sub_category' , 
                            'brand' , 
                            'cost_price' , 
                            'selling_price' , 
                            'stock_qty', 
                            'supplier_id' , 
                            'record_hash' ,
                            'is_active' , 
                            'effective_from' , 
                            'effective_to' ,
                            'ingested_timestamp') 

# COMMAND ----------

#Source
df_final.createOrReplaceTempView('product_view')
final_cnt = df_final.count()

#Getting maximum ingested timestamp to store in the control table - for incremental loading
max_ts = df_final.select(max(col('ingested_timestamp')).alias('max_dt'))
max_dt =  max_ts.collect()[0]['max_dt']

#If target table exists
if spark.catalog.tableExists('retailco.retailco_schema.dim_products'):
  #target
    target = DeltaTable.forName(spark, 'retailco.retailco_schema.dim_products')
    target_df =target.toDF()
    # print(target_df.count())

    #Target values greater than zero
    if target_df.count() > 0:
      # Step 1: Doing left outer join with source and target table based on customer_id column
      lo_join = df_final.\
        join(target_df, ((target_df['product_id'] == df_final['product_id']) & (target_df['is_active'] == True)) , 'left_outer').\
          select(df_final['*'],
                target_df['product_id'].alias('tgt_product_id'),
                target_df['product_name'].alias('tgt_product_name'), 
                target_df['category'].alias('tgt_category'), 
                target_df['sub_category'].alias('tgt_sub_category'), 
                target_df['sub_category'].alias('tgt_sub_category'), 
                target_df['cost_price'].alias('tgt_cost_price'), 
                target_df['selling_price'].alias('tgt_selling_price'), 
                target_df['stock_qty'].alias('tgt_stock_qty'), 
                target_df['supplier_id'].alias('tgt_supplier_id'),
                target_df['record_hash'].alias('tgt_record_hash'), 
                target_df['is_active'].alias('tgt_is_active'), 
                target_df['effective_from'].alias('tgt_effective_from'), 
                target_df['effective_to'].alias('tgt_effective_to'),
                target_df['ingested_timestamp'].alias('tgt_ingested_timestamp')
                 )
      # Step 2: Getting only the records whose effective from is greater than targets effective from    
      lo_join = lo_join.filter((col('effective_from') > col('tgt_effective_from')) | (col('tgt_effective_from').isNull()))
          
      #find records to be updated - if target record hash is not null it means source and target is matching by key column
      filter_df = lo_join.\
        filter(
          (col('tgt_record_hash').isNotNull())
          ).\
        withColumn('merge_key', lit(None))
      # display(filter_df)
      
      # for insert all rows - filter the rows whose 
       # target record hash is null
       # target record hash will be null only when source doesn't matches 
       # target and source record hash != meaning no EXACT Matching records.
      match_df = lo_join.\
        filter((col('tgt_record_hash') != col('record_hash')) | (col('tgt_record_hash').isNull())).\
        withColumn('merge_key' , concat_ws("||", col('product_id'), col('product_id')))
      # display(match_df)

      #Union on both
      union_df = filter_df.union(match_df)
      # display(union_df)
      union_df.createOrReplaceTempView('source_view')
      
      #SCD2
      spark.sql("""
                merge into retailco.retailco_schema.dim_products as target
                using source_view as source
                on  source.merge_key == concat_ws("||", target.product_id, target.product_id) 
                when matched 
                and target.is_active = True 
                and target.record_hash <> source.record_hash
                then 
                update set
                target.is_active = False,
                target.effective_to =  date_sub(source.effective_from, 1)
                when not matched then
                insert (
                  product_id ,
                  product_name , 
                  category , 
                  sub_category , 
                  brand , 
                  cost_price  , 
                  selling_price , 
                  stock_qty , 
                  supplier_id , 
                  record_hash ,
                  is_active , 
                  effective_from , 
                  effective_to ,
                  ingested_timestamp
                )
                values
                (
                  source.product_id ,
                  source.product_name , 
                  source.category , 
                  source.sub_category , 
                  source.brand , 
                  source.cost_price  , 
                  source.selling_price , 
                  source.stock_qty , 
                  source.supplier_id , 
                  source.record_hash ,
                  True , 
                  source.effective_from , 
                  source.effective_to ,
                  source.ingested_timestamp
                )
                """)
      
      #Removing record for this table and adding a new record for the table
      spark.sql(f"""delete from retailco.retailco_schema.control_table 
              where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")
    
      spark.sql(f"""insert into retailco.retailco_schema.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{tn}', '{max_dt}', true, current_timestamp());""")
      
      #Updating job logs
      spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id, table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}', '{tn}' ,'{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")

    else:
      #If there is no record in a table.
      spark.sql("""
              insert into retailco.retailco_schema.dim_products
              (
                product_id ,
                product_name , 
                category , 
                sub_category , 
                brand , 
                cost_price  , 
                selling_price , 
                stock_qty , 
                supplier_id , 
                record_hash ,
                is_active , 
                effective_from , 
                effective_to ,
                ingested_timestamp 
              ) 
              select
              product_id ,
              product_name , 
              category , 
              sub_category , 
              brand , 
              cost_price  , 
              selling_price , 
              stock_qty , 
              supplier_id , 
              record_hash ,
              True , 
              effective_from , 
              effective_to ,
              ingested_timestamp 
              from product_view """)
      
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
      
      #Updating job logs
      spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}', '{tn}', '{init_cnt}' , '{final_cnt}' , '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
      
else:
  #If there is no table
  #creating table and inserting records into i.
    spark.sql("""
              create or replace table retailco.retailco_schema.dim_products(
              product_sk bigint generated by default as identity primary key,
              product_id string,
              product_name string, 
              category string, 
              sub_category string, 
              brand string, 
              cost_price int , 
              selling_price int, 
              stock_qty int, 
              supplier_id string,
              record_hash string, 
              is_active boolean, 
              effective_from date, 
              effective_to date,
              ingested_timestamp timestamp
              )
              using delta
              partitioned by (ingested_timestamp);
              """)

    spark.sql("""
              insert into retailco.retailco_schema.dim_products
              (
                product_id ,
                product_name , 
                category , 
                sub_category , 
                brand , 
                cost_price  , 
                selling_price , 
                stock_qty , 
                supplier_id , 
                is_active , 
                effective_from , 
                effective_to ,
                ingested_timestamp 
              ) 
              select
              product_id ,
              product_name , 
              category , 
              sub_category , 
              brand , 
              cost_price  , 
              selling_price , 
              stock_qty , 
              supplier_id , 
              True , 
              effective_from , 
              effective_to ,
              ingested_timestamp 
              from product_view """)
    
    ##Removing record for this table and adding a new record for the table
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
    
    #Adding new table
    spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")