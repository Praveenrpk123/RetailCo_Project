# Databricks notebook source
# MAGIC %md
# MAGIC ### Importing required libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import date, timedelta, datetime
import os, json
from functools import reduce

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

# COMMAND ----------

job_id = dbutils.widgets.get('Job_id')
job_datetime = dbutils.widgets.get('Job_datetime')
job_name = dbutils.widgets.get('Job_name')
no_of_retries = dbutils.widgets.get('No_of_retries')
run_id = dbutils.widgets.get('Run_id')
trigger_datetime = dbutils.widgets.get('Trigger_datetime')
trigger_type = dbutils.widgets.get('Trigger_type')

# spark.sql(f"""insert into retailco.retailco_schema.job_logs(job_id , job_name, run_id , job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) values ('{job_id}', '{job_name}', '{run_id}' , '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Incremental Loading

# COMMAND ----------

#getting max timestamp for this table from control table.
src = 'bronze'
tgt = 'silver'
tn = 'dim_customers'
max_timestamp = ''

ts_val = spark.sql(f"""select coalesce(max_timestamp,timestamp('9999-12-31')) as max_dt from retailco.retailco_schema.control_table where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}' order by max_dt desc;""")

# print(ts_val.collect())
#ts.collect() will return a list. if len of list is not equal to zero, gets from control table else default value
if len(ts_val.collect()) != 0:
    max_timestamp = ts_val.collect()[0]['max_dt']
else:
    max_timestamp = datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
# display(max_timestamp)

# COMMAND ----------

path = '/Volumes/retailco/retailco_schema/bronze/customers/'

##Data should be greater than last batch run - Incremental loading

df = spark.read.format('delta').load(path).filter(col('ingested_timestamp') > lit(max_timestamp))

#initial records count
init_cnt = df.count()

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

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dropping unwanted columns

# COMMAND ----------

df = df.\
    drop('Job_id',
         'Run_id',
         'folder_path',
         'file_name')
# display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trim and standardize

# COMMAND ----------

df = df.\
    withColumn("customer_id", 
               trim(col('customer_id'))).\
    withColumn("customer_id", 
               upper(col('customer_id'))).\
    withColumn("customer_name", 
               initcap(col('customer_name'))).\
    withColumn("phone", 
               trim(col('phone'))).\
    withColumn("city", 
               trim(col('city'))).\
    withColumn("state", 
               trim(col('state'))).\
    withColumn("region",
               trim(col("region"))).\
    withColumn("segment",
               trim(col("segment"))).\
    withColumn("registration_date",
               trim(col("registration_date"))).\
    withColumn("is_active",
               trim(col("is_active")))

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA DUPLICATION

# COMMAND ----------

window_val = Window.partitionBy(col('primary_record_hash')).orderBy(col('customer_id'))

df = df.\
    withColumn('primary_record_hash', 
               sha2(concat_ws("-", 
                              coalesce(col("customer_id"), lit("")), 
                              coalesce(col("customer_name"), lit("")), 
                              coalesce(col("customer_email"), lit("")), 
                              coalesce(col("phone"), lit("")),
                              coalesce(col("city"), lit("")), 
                              coalesce(col("state"), lit("")), 
                              coalesce(col("region"), lit("")), 
                              coalesce(col("segment"), lit("")), 
                              coalesce(col("registration_date"), lit(""))), 256)).\
    withColumn("row_number", 
               row_number().over(window_val)).\
    filter(col('row_number') ==  1).\
        drop('primary_record_hash', 'row_number')

# COMMAND ----------

window_val = Window.partitionBy(col('customer_id')).orderBy(desc(col('registration_date')))
df = df.\
    withColumn('pk_record_cnt', row_number().over(window_val)).\
    withColumn('is_active', when(col('pk_record_cnt') != 1, False).otherwise(True))
# display(df.filter(col('pk_record_cnt') > 1 ))

# COMMAND ----------

# MAGIC %md
# MAGIC ##DATA QUALITY CHECKS

# COMMAND ----------

# import os, json
# path = os.path.abspath('../../config_files/silver_configs/silver_cust_dq.json')
# with open(path, 'r') as f:
#     configs = json.load(f)
#     config_val = configs['configs']
#     for i in config_val:
#         if i['check'] == 'pattern' and i['ends_with'] != 'none':
#             print(i)
#             val  = i['ends_with']
#             print(val,len(val), type(val))
#         else:
#             pass

# COMMAND ----------

#Updating to proper phone number format
# df.withColumn('phone', when((~(col('phone').startswith('+91'))),concat(lit('+91-'), col('phone'))).otherwise(replace(col('phone'),lit('+91'), lit('+91-')))).limit(10).display()

#getting the dq file for customers from config path for data validation check
path = os.path.abspath('../../config_files/silver_configs/silver_cust_dq.json')

with open(path, 'r') as f:
    configs = json.load(f)
    config_val = configs['configs']
    for i in config_val:
        total_checks = 0
        perc = 0
        #Not Null Checks
        if i["check"] == 'Not_Null' and i["col_name"] != None  and i["enabled"] == True:
            older_count = df.count()
            df = df.filter(col(f'{i["col_name"]}').isNotNull())
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {final_cnt}")

        #starts with check for Customer_Id etc..
        if i["check"] == 'pattern' and i["starts_with"] != None and i["col_name"] != None and i["ends_with"] == 'none' and i["enabled"] == True:
            older_count = df.count()
            if type(i["starts_with"]) == str:
                # df = df.filter(col(f'{i["col_name"]}').startswith(f'{i["starts_with"]}'))
                df = df.filter(col(f'{i["col_name"]}').rlike(f'^{i["starts_with"]}[0-9]+$'))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f"No of records dropped due to {i["check"]} starts_with check for {i["col_name"]} is {final_cnt}")
            elif type(i["starts_with"]) == list:
                df = df.filter(reduce(lambda x, y : x | y , [col(f'i["col_name"]').startswith(i) for i in i["starts_with"]]))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f"No of records dropped due to {i["check"]} starts_with check for {i["col_name"]} is {final_cnt}")

        #endswith check for email etc
        if i["check"] == 'pattern' and i["ends_with"] != None and i["starts_with"] == 'none' and i["col_name"] != None  and i["enabled"] == True:
            older_count = df.count()
            # reduce(lambda x, y : x | y , [col('customer_email').endswith(p) for p in suf])
            if type(i["ends_with"]) == list:
                df = df.filter(reduce(lambda x, y : x | y , [col(f'{i["col_name"]}').endswith(p) for p in i["ends_with"]]))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f"No of records dropped due to {i["check"]} ends_with check for {i["col_name"]} is {final_cnt}")
            elif type(i["ends_with"]) == str:
                df = df.\
                    filter(col(f'{i["col_name"]}').endswith(f'{i["ends_with"]}'))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f"No of records dropped due to {i["check"]} ends_with check for {i["col_name"]} is {final_cnt}")
            
        #phone_number_pattern_check
        if i["check"] == 'phone' and i["col_name"] != None  and i["enabled"] == True:
            older_count = df.count()
            df = df.\
                withColumn(f'{i["col_name"]}', 
                        when((col(f'{i["col_name"]}').startswith('+91')) & (length(col(f'{i["col_name"]}')) ==13), 
                            substring(col(f'{i["col_name"]}'), 4, length(col(f'{i["col_name"]}'))).cast('bigint')).\
                        when(col(f'{i["col_name"]}').startswith('91') & (length(col(f'{i["col_name"]}')) == 10) , 
                            substring(col(f'{i["col_name"]}'), 1, length(col(f'{i["col_name"]}'))).cast('bigint')).\
                        when(col(f'{i["col_name"]}').startswith('91') & (length(col(f'{i["col_name"]}')) == 12) , 
                            substring(col(f'{i["col_name"]}'), 3, length(col(f'{i["col_name"]}'))).cast('bigint')).\
                        when((col(f'{i["col_name"]}').startswith('0')) & (length(col(f'{i["col_name"]}')) == 11), 
                            substring(col(f'{i["col_name"]}'), 2, length(col(f'{i["col_name"]}'))).cast('bigint')).\
                        when((~(col(f'{i["col_name"]}').startswith('+91'))) & 
                            (~(col(f'{i["col_name"]}').startswith('91'))) & 
                            (length(col(f'{i["col_name"]}')) == 10), 
                            col(f'{i["col_name"]}').cast('bigint')).\
                        otherwise(lit('0000000000').cast('bigint'))
                        ).filter(col(f'{i["col_name"]}') != 0000000000)
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {final_cnt}")

        #Date Validity i["check"]
        if i["check"] == 'validity' and i["col_name"] != None  and i["enabled"] == True:
            end_date = date.today() + timedelta(days = 1)
            older_count = df.count()
            df = df.withColumn(f'{i["col_name"]}', 
                        to_date(col(f'{i["col_name"]}'),'yyyy-MM-dd')).\
                            filter(col(f'{i["col_name"]}') <= end_date)
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {final_cnt}")


        # datatype_validation and update
        if i["check"] == 'datatype_val' and i["check_type"] != None and i["col_name"] != None  and i["enabled"] == True:
            older_count  = df.count()
            df = df.filter(col(f'{i["col_name"]}').cast(f'{i["check_type"]}').isNotNull()).\
                withColumn(f'{i["col_name"]}', col(f'{i["col_name"]}').cast(f'{i["check_type"]}'))
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f"No of records dropped due to {i["check"]} check for {i["col_name"]} is {final_cnt}")
            
    #Data Quality percent
    print("Data Quality Perc is : " , (perc/total_checks)*100)
                            

# COMMAND ----------

#Standardize to common format
# #is_active column should hold True, False
# df = df.\
#     withColumn('is_active', 
#                when(col('is_active').isin('True', 1, 'Active', True), 
#                     True).\
#                when(col('is_active').isin('False', 0, 'Not Active', False), 
#                     False).\
#                 when(col('is_active').isNull() | col('is_active') == '', 
#                      lit('Unknown')).\
#                otherwise(lit('Unknown')))
    


# segments = ['Premium', 'Regular', 'Budget']
df = df.\
    withColumn('segment', 
               when(col('segment').
                    isin('Premium', 'Regular', 'Budget'),col('segment')).\
                when(col('segment').isNull() | col('segment') == '', lit('Unknown')))
    

# Region = ['North', 'East', 'South', 'Central', 'West']
df = df.\
    withColumn('region', 
               when(col('region').
                    isin('North', 'East', 'South', 'Central', 'West'),col('region')).\
                when(col('region').isNull() | col('region') == '', lit('Unknown')))

# State -is Null Then Unknown
df_stage = df.\
    withColumn('state', 
               when((col('state').isNull()) | (col('state') == lit('')) , lit('Unknown')).otherwise(col('state')))
# display(df_stage)

# COMMAND ----------

window_val = Window.partitionBy(col('customer_id')).orderBy(desc(col('registration_date')))
#Adding effective from, effective to and ingested timestamp 
df_stage2 = df_stage.\
    withColumn('lag_val', date_add(lag(col('registration_date')).over(window_val), -1)).\
    withColumn('effective_to', when(col('lag_val').isNull(), to_date(date_format(lit('9999-12-31'), 'yyyy-MM-dd'))).otherwise(col('lag_val'))).\
    withColumnRenamed('registration_date' , 'effective_from').\
    withColumnRenamed('Ingested_timestamp', 'ingested_timestamp').\
    drop('lag_val', '_rescued_data')
    # withColumn('effective_to', when(col('is_active')) == True,)

#Selecting the final colums to be updated in the table
df_final = df_stage2.select(
    'customer_id',
    'customer_name',
    'customer_email',
    'phone',
    'city',
    'state',
    'region',
    'segment',
    'record_hash',
    'is_active',
    'effective_from',
    'effective_to',
    'ingested_timestamp'
)

final_cnt = df_final.count()

# COMMAND ----------

#Getting maximum ingested timestamp to store in the control table - for incremental loading 
max_ts = df_final.select(max('Ingested_timestamp').alias('max_dt_val'))
ts = max_ts.collect()[0]['max_dt_val']
print(ts)

# COMMAND ----------

# MAGIC %md
# MAGIC ###SCD-2

# COMMAND ----------

from delta.tables import DeltaTable
#Source
df_final.createOrReplaceTempView('cust_temp_view')

# If target table exists
if spark.catalog.tableExists('retailco.retailco_schema.dim_customers'):
    #Getting target.
    target = DeltaTable.forName(spark, 'retailco.retailco_schema.dim_customers')
    target_df = target.toDF()
    #if target table count is greater than zero
    if target_df.count() > 0:
        # print(df_final.count())
        # Step 1: Doing left outer join with source and target table based on customer_id column
        match_df = df_final.\
            join(target_df, (
                (df_final['customer_id'] == target_df['customer_id']) & 
                (target_df['is_active'] == True)             
                ), 'left_outer').\
            select(df_final['*'] ,
                target_df['customer_id'].alias('tgt_customer_id'),
                target_df['customer_name'].alias('tgt_customer_name'),
                target_df['customer_email'].alias('tgt_customer_email'),
                target_df['phone'].alias('tgt_phone'),
                target_df['city'].alias('tgt_city'),
                target_df['state'].alias('tgt_state'),
                target_df['region'].alias('tgt_region'),
                target_df['segment'].alias('tgt_segment'),
                target_df['record_hash'].alias('tgt_record_hash'),
                target_df['is_active'].alias('tgt_is_active'),
                target_df['effective_from'].alias('tgt_effective_from'),
                target_df['effective_to'].alias('tgt_effective_to'),
                target_df['ingested_timestamp'].alias('tgt_ingested_timestamp')
                )
        # Step 2: Getting only the records whose effective from is greater than targets effective from
        match_df = match_df.filter((col('effective_from') > col('tgt_effective_from')) | (col('tgt_effective_from').isNull()))

       # for insert all rows - filter the rows whose 
       # target record hash is null
       # target record hash will be null only when source doesn't matches 
       # target and source record hash != meaning no EXACT Matching records.
        filter_hash_df = match_df.\
            filter((col('record_hash') != col('tgt_record_hash')) | (col('tgt_record_hash').isNull())).\
            withColumn('merge_key', concat_ws("||", col('customer_id'), col('customer_id')))

        #find records to be updated - if target record hash is not null it means source and target is matching by key column
        update_df = match_df.\
            filter(col('tgt_record_hash').isNotNull()).\
            withColumn('merge_key', lit(None))

        #Union both
        union_df = filter_hash_df.union(update_df)

        #using union_df as source and creating a view on top of it,
        source_view = union_df.createOrReplaceTempView('cust_temp_view')

        #SCD - 2 
        spark.sql("""
                merge into retailco.retailco_schema.dim_customers as target
                using cust_temp_view as src 
                on src.merge_key == concat_ws("||", target.customer_id, target.customer_id)
                when matched and src.record_hash <> target.record_hash and target.is_active = True then
                update set 
                target.is_active = False , 
                target.effective_to = date_sub(src.effective_from, 1)
                when not matched then
                Insert 
                (
                    customer_id,
                    customer_name,
                    customer_email,
                    phone,
                    city,
                    state,
                    region,
                    segment,
                    record_hash,
                    is_active,
                    effective_from,
                    effective_to,
                    ingested_timestamp
                )
                values 
                (
                    src.customer_id,
                    src.customer_name,
                    src.customer_email,
                    src.phone,
                    src.city,
                    src.state,
                    src.region,
                    src.segment,
                    src.record_hash,
                    True,
                    src.effective_from,
                    src.effective_to,
                    src.ingested_timestamp
                )
                """)
        
        #Removing record for this table and adding a new record for the table
        spark.sql(f"""delete from retailco.retailco_schema.control_table 
              where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")
    
        spark.sql(f"""insert into retailco.retailco_schema.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{tn}', '{ts}', true, current_timestamp());""")
        
        #for updating the job_logs
        spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
        
    else :
        #It will come inside only when we have an empty table
        #Here it will insert the records to dim_customers from source_view
        spark.sql("""insert into retailco.retailco_schema.dim_customers(
            customer_id,
            customer_name,
            customer_email,
            phone,
            city,
            state,
            region,
            segment,
            record_hash,
            is_active,
            effective_from,
            effective_to,
            ingested_timestamp
        )
              select 
              customer_id,
              customer_name,
              customer_email,
              phone,
              city,
              state,
              region,
              segment,
              record_hash,
              True,
              effective_from,
              effective_to,
              ingested_timestamp from cust_temp_view""")
        
        #Removing record for this table and adding a new record for the table - control table
        spark.sql(f"""delete from retailco.retailco_schema.control_table 
              where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")
    
        spark.sql(f"""insert into retailco.retailco_schema.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{tn}', '{ts}', true, current_timestamp());""")
        
        #for updating the job_logs
        spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
else:
    #It will come inside only when there is no such table exists
    #Creating table
    spark.sql("""
            create table retailco.retailco_schema.dim_customers(
            customer_sk bigint generated by default as identity primary key,
            customer_id string,
            customer_name string,
            customer_email string,
            phone long,
            city string,
            state string,
            region string,
            segment string,
            record_hash string,
            is_active boolean,
            effective_from date,
            effective_to date,
            ingested_timestamp timestamp
            )
            using delta
            partitioned by (ingested_timestamp);
              """)
    
    #Inserting data inside this table
    spark.sql("""insert into retailco.retailco_schema.dim_customers(
            customer_id,
            customer_name,
            customer_email,
            phone,
            city,
            state,
            region,
            segment,
            record_hash,
            is_active,
            effective_from,
            effective_to,
            ingested_timestamp
        )
              select 
              customer_id,
              customer_name,
              customer_email,
              phone,
              city,
              state,
              region,
              segment,
              record_hash,
              True,
              effective_from,
              effective_to,
              ingested_timestamp from cust_temp_view""")
    
    #Removing record for this table and adding a new record for the table
    spark.sql(f"""delete from retailco.retailco_schema.control_table 
              where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")
    
    spark.sql(f"""insert into retailco.retailco_schema.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{tn}', '{ts}', true, current_timestamp());""")
    
    #for updating the job_logs
    spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{tn}', '{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")


# COMMAND ----------

# if check == 'Not_Null' and col_name != None  and enabled == True:
        #     older_count = df.count()
        #     df = df.filter(col(f'{col_name}').isNotNull())
        #     new_count = df.count()
        #     print(f"No of records dropped due to {check} check is {older_count - new_count}")


        # #starts with check for Customer_Id etc..
        # if check == 'pattern' and starts_with != None and col_name != None  and enabled == True:
        #     older_count = df.count()
        #     if type(starts_with) == str:
        #         # df = df.filter(col(f'{col_name}').startswith(f'{starts_with}'))
        #         df = df.filter(col(f'{col_name}').rlike(f'^{starts_with}[0-9]+$'))
        #         new_count = df.count()
        #         print(f"No of records dropped due to {check} check is {older_count - new_count}")
        #     elif type(starts_with) == list:
        #         df = df.filter(reduce(lambda x, y : x | y , [col(f'col_name').startswith(i) for i in starts_with]))
        #         new_count = df.count()
        #         print(f"No of records dropped due to {check} check is {older_count - new_count}")

        # #endswith check for email etc
        # if check == 'pattern' and ends_with != None and col_name != None  and enabled == True:
        #     older_count = df.count()
        #     # reduce(lambda x, y : x | y , [col('customer_email').endswith(p) for p in suf])
        #     if type(ends_with) == list:
        #         df = df.filter(reduce(lambda x, y : x | y , [col(f'{col_name}').endswith(p) for p in ends_with]))
        #         new_count = df.count()
        #         print(f"No of records dropped due to {check} check is {older_count - new_count}")
        #     elif type(ends_with) == str:
        #         df = df.\
        #             filter(col(f'{col_name}').endswith(f'{ends_with}'))
        #         new_count = df.count()
        #         print(f"No of records dropped due to {check} check is {older_count - new_count}")
            
        # #phone_number_pattern_Check
        # if check == 'phone' and col_name != None  and enabled == True:
        #     older_count = df.count()
        #     df = df.\
        #         withColumn(f'col_name', 
        #                 when((col(f'{col_name}').startswith('+91')) & (length(col(f'{col_name}')) ==13), 
        #                     substring(col(f'{col_name}'), 4, length(col(f'{col_name}'))).cast('int')).\
        #                 when(col(f'{col_name}').startswith('91') & (length(col(f'{col_name}')) == 10) , 
        #                     substring(col(f'{col_name}'), 1, length(col(f'{col_name}'))).cast('int')).\
        #                 when(col(f'{col_name}').startswith('91') & (length(col(f'{col_name}')) == 12) , 
        #                     substring(col(f'{col_name}'), 3, length(col(f'{col_name}'))).cast('int')).\
        #                 when((col(f'{col_name}').startswith('0')) & (length(col(f'{col_name}')) == 11), 
        #                     substring(col(f'{col_name}'), 2, length(col(f'{col_name}'))).cast('int')).\
        #                 when((~(col(f'{col_name}').startswith('+91'))) & 
        #                     (~(col(f'{col_name}').startswith('91'))) & 
        #                     (length(col(f'{col_name}')) == 10), 
        #                     col(f'{col_name}').cast('int')).\
        #                 otherwise(lit('0000000000').cast('int'))
        #                 ).filter(col(f'col_name') != '0000000000')
        #     new_count = df.count()
        #     print(f"No of records dropped due to {check} check is {older_count - new_count}")

        # #Date Validity check
        # if check == 'validity' and col_name != None  and enabled == True:
        #     end_date = date.today() + timedelta(days = 1)
        #     older_count = df.count()
        #     df = df.withColumn(f'{col_name}', 
        #                 to_date(col(f'{col_name}'),'yyyy-MM-dd')).\
        #                     filter(col(f'{col_name}' <= end_date))
        #     new_count = df.count()
        #     print(f"No of records dropped due to {check} check is {older_count - new_count}")


        # # datatype_validation and update
        # if check == 'datatype_val' and check_type != None and col_name != None  and enabled == True:
        #     older_count  = df.count()
        #     df = df.filter(col(f'{col_name}').cast(f'{check_type}').isNotNull()).\
        #         withColumn(f'{col_name}', col(f'{col_name}').cast(f'{check_type}'))
        #     new_count = df.count()
        #     print(f"No of records dropped due to {check} check is {older_count - new_count}")
                            

# # print(type(['a','b']) == str)
# # ba = '+916555599708'
# # print([ba[i] for i in range(len(ba))])
# # suf = ['@example.net', '@example.org', '@example.com']
# # df = df.withColumn('fl',reduce(lambda x, y : x | y , [col('customer_email').endswith(p) for p in suf]))
# # df = df.filter(reduce(lambda x, y : x | y , [col('customer_email').endswith(p) for p in suf]))
# # df = df.\
# #         withColumn('phone', 
# #                 when((col('phone').startswith('+91')) & (length(col('phone')) ==13), 
# #                      substring(col('phone'), 4, length(col('phone')) )).\
# #                 when(col('phone').startswith('91') & (length(col('phone')) == 10) , 
# #                      substring(col('phone'), 1, length(col('phone')) )).\
# #                 when(col('phone').startswith('91') & (length(col('phone')) == 12) , 
# #                      substring(col('phone'), 3, length(col('phone')) )).\
# #                 when((col('phone').startswith('0')) & (length(col('phone')) == 11), 
# #                      substring(col('phone'), 2, length(col('phone')))).\
# #                 when((~(col('phone').startswith('+91'))) & 
# #                      (~(col('phone').startswith('91'))) & 
# #                      (length(col('phone')) == 10), 
# #                      col('phone')).\
# #                 otherwise(lit('0000000000'))
# #                 )
# col_name = 'phone'
# df = df.\
#         withColumn(f'col_name', 
#                 when((col(f'{col_name}').startswith('+91')) & (length(col(f'{col_name}')) ==13), 
#                      substring(col(f'{col_name}'), 4, length(col(f'{col_name}')) )).\
#                 when(col(f'{col_name}').startswith('91') & (length(col(f'{col_name}')) == 10) , 
#                      substring(col(f'{col_name}'), 1, length(col(f'{col_name}')) )).\
#                 when(col(f'{col_name}').startswith('91') & (length(col(f'{col_name}')) == 12) , 
#                      substring(col(f'{col_name}'), 3, length(col(f'{col_name}')) )).\
#                 when((col(f'{col_name}').startswith('0')) & (length(col(f'{col_name}')) == 11), 
#                      substring(col(f'{col_name}'), 2, length(col(f'{col_name}')))).\
#                 when((~(col(f'{col_name}').startswith('+91'))) & 
#                      (~(col(f'{col_name}').startswith('91'))) & 
#                      (length(col(f'{col_name}')) == 10), 
#                      col(f'{col_name}')).\
#                 otherwise(lit('0000000000'))
#                 )

                
#                         # slice(split(col('phone'),""), 3, size(split(col('phone'), "")-1))).otherwise(col('phone')))
# # df.filter(col('phone').startswith('91')).display()
# df = df.withColumn('registration_date',
#                            to_date(col('registration_date'), 'yyyy-MM-dd'))
# display(df)
# # display(df.filter(col('fl') == False))