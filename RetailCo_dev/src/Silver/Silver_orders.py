# Databricks notebook source
# MAGIC %md
# MAGIC ###Importing required libraries

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
from functools import reduce
from delta.tables import DeltaTable
import os, json

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
tn = 'fact_orders'
max_timestamp = ''

ts_val = spark.sql(f"""select coalesce(max_timestamp,timestamp('9999-12-31')) as max_dt from retailco.retailco_schema.control_table where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{tn}';""")

print(len(ts_val.collect()))
#ts.collect() will return a list. if len of list is not equal to zero, gets from control table else default value

if len(ts_val.collect()) != 0:
    max_timestamp = ts_val.collect()[0]['max_dt']
else:
    max_timestamp = datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
# max_timestamp = datetime.strptime('1900-12-31 00:00:00', '%Y-%m-%d %H:%M:%S')
display(max_timestamp)

# COMMAND ----------

path = '/Volumes/retailco/retailco_schema/bronze/orders/'

#Data should be greater than last batch run - Incremental loading
df_orders = spark.read.format('delta').load(path).filter(col('ingested_timestamp') > lit(max_timestamp))

#if count of our data is zero, Need to get out from this notebook(It will skip it)
# and inserting in our job logs with value as 0, else continue..
if df_orders.count() == 0:
    init_cnt = 0
    final_cnt = 0
    spark.sql(f"""insert into retailco.retailco_schema.job_logs
              (job_id , job_name, run_id, table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}', '{tn}' ,'{init_cnt}' , '{final_cnt}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")
    # logger.info("No records found. Skipping processing.")
    dbutils.notebook.exit("NO_DATA")
init_cnt = df_orders.count()
print(init_cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Trim and Standardize

# COMMAND ----------

df_trim_standardize = df_orders.\
    withColumn("order_id", upper(trim(col('order_id')))).\
    withColumn("order_date", trim(col('order_date'))).\
    withColumn("customer_id", upper(trim(col('customer_id')))).\
    withColumn("product_id", upper(trim(col("product_id")))).\
    withColumn("quantity", trim(col('quantity'))).\
    withColumn("unit_price", round(trim(col('unit_price')),2)).\
    withColumn("discount_pct", trim(col("discount_pct"))).\
    withColumn("total_amount", round(trim(col('total_amount')),2)).\
    withColumn('payment_method', initcap(trim(col('payment_method')))).\
    withColumn("status", initcap(col('status'))).\
    withColumn("delivery_days", trim(col('delivery_days'))).\
    withColumn("customer_email", trim(col("customer_email"))).\
    withColumn("source_system", trim(col('source_system'))).\
    withColumn("unit_price_formatted", trim(col('unit_price_formatted')))
print(df_trim_standardize.count())

df_unit_price = df_trim_standardize.withColumn("up_flag", 
                   when(col('unit_price_formatted').try_cast('double').isNotNull(), True).
                   otherwise(False)).\
                   drop('file_name', 'Job_Id','Run_id', 'folder_path')


df_up = df_unit_price.withColumn('unit_price_formatted', 
                   when((col('unit_price_formatted').startswith('₹')) & (col('up_flag') == False), 
                       regexp_replace(col("unit_price_formatted"), r"^\₹", "")).\
                otherwise(col('unit_price_formatted')))
                
#removing up_flag as false as it is not required

                # drop('up_flag')        
# display(df_up.filter(col('up_flag') == True)) 
# display(df.limit(1))
# display(df.filter(col('unit_price') != col('unit_price_formatted')) )
        # withColumn('upf', when(col('unit_price_formatted').startswith('$'), col('unit_price_formatted').replace('$', ''))).otherwise(col('unit_price_formatted')))


# df = df.withColumn("delivery_date", date_add(col('order_date'), col('delivery_days')))


# COMMAND ----------

# MAGIC %md
# MAGIC ###DEDUP Check

# COMMAND ----------

window_val = Window.partitionBy(col('row_hash')).orderBy(desc(col('ingested_timestamp')),(desc(col('up_flag'))))

un_col = ['up_flag','_rescued_data', 'Ingested_timestamp','file_name', 'Job_id', 'Run_id', 'folder_path', 'record_hash']
cols = []

for i in df_up.columns:
    if i not in un_col:
        cols.append(i)
print(cols)

df_hash = df_up.withColumn('row_hash', sha2(
    concat_ws(
        "-", *cols), 256))

print(df_hash.count())
df = df_hash.withColumn("rn", 
                   row_number().over(window_val)).filter(col('rn') == 1).\
                   drop(col('rn'), col('row_hash'))
# display(df.filter(col('order_id') == 'ORD0211599'))                   
                #    
# display(df.limit(10))
# print(df.count())
# display(df.filter(col('up_flag') == True).count())
# display(df.filter(col('up_flag') == False).count())
# df.createOrReplaceTempView('fact_ord')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Quality check

# COMMAND ----------

path = os.path.abspath('../../config_files/silver_configs/silver_orders_dq.json')
with open(path, 'r') as f:
    config = json.load(f)
    total_checks = 0
    perc = 0
    for i in config['configs']:
        #Not Null checks
        if i["check"] == 'Not_Null' and i['enabled'] == True:
            older_count = df.count()
            df = df.filter(col(f'{i["col_name"]}').isNotNull())
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')

        #DataType handling
        if i["check"] == 'datatype_handling' and i["check_type"] != None and i['enabled'] == True:
            old_cnt = df.count()
            df = df.\
                filter(col(f'{i["col_name"]}').try_cast(f'{i["check_type"]}').isNotNull()).\
                withColumn(f'{i["col_name"]}', col(f'{i["col_name"]}').cast(f'{i["check_type"]}'))
            new_count = df.count()
            final_cnt = older_count - new_count
            total_checks += 1
            perc += 1 - (final_cnt/older_count)
            print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {old_cnt - new_count}')

        #Starts with issue
        if i["check"] == 'pattern' and i["starts_with"] != None and i['enabled'] == True:
            older_count = df.count()
            if type(i["starts_with"]) == str:
                # df = df.filter(col(f'{i["col_name"]}').startswith(f'{i["starts_with"]}'))
                df = df.filter(col(f'{i["col_name"]}').rlike(f'^{i["starts_with"]}[0-9]+$'))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')
            elif type(i["starts_with"]) == list:
                df = df.filter(reduce(lambda x, y : x | y , [col(f'i["col_name"]').startswith(i) for i in i["starts_with"]]))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')


        #Greater than some value
        if i["check"] == 'greater_than' and i["value_to_ck"] != None and i['enabled'] == True:
            older_count = df.count()
            date_val = current_date()
            if i["value_to_ck"] == "current_date":
                df = df.filter(col(f'{i["col_name"]}') > lit(date_val))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')
            else:
                df = df.filter(col(f'{i["col_name"]}') > i["value_to_ck"])
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')

        #Less than some value
        if i["check"] == 'less_than' and i["value_to_ck"] != None and i['enabled'] == True:
            older_count = df.count()
            date_val = current_date()
            if i["value_to_ck"] == "current_date":
                df = df.filter(col(f'{i["col_name"]}') < lit(date_val))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')
            else:
                df = df.filter(col(f'{i["col_name"]}') < i["value_to_ck"])
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')

        #sequence presence
        if i["check"] == 'Sequence_ck'and i["check_list"] != None and i['enabled'] == True:
            if type(i["check_list"]) == list:
                older_count = df.count()
                df = df.filter(~(col(f'{i["col_name"]}').isin(f'{i["check_list"]}')))
                new_count = df.count()
                final_cnt = older_count - new_count
                total_checks += 1
                perc += 1 - (final_cnt/older_count)
                print(f'No of records dropped due to {i["check"]} check for {i["col_name"]} is {older_count - new_count}')
    print("Data Quality Perc is : " , (perc/total_checks)*100)
df_dq = df

# COMMAND ----------

# MAGIC %md
# MAGIC ####Basic Transformation.

# COMMAND ----------

df_extra = df_dq.\
    withColumn('gross_amount', round((col('quantity') * col('unit_price')),2)).\
    withColumnRenamed('total_amount','net_amount').\
    withColumnRenamed('Ingested_timestamp','ingested_timestamp').\
    withColumn('discount_amount', round(col('gross_amount') - col('net_amount') , 2)).\
    withColumn('delivery_date', date_add(col('order_date'), col('delivery_days').cast('int')))

#Dropping unit price formatted as it has same value of unit price
#Dropping total amount which is renamed to net amount
df_extra = df_extra.drop(col('unit_price_formatted'), col('total_amount'))
# display(df_extra.limit(10))
# df_extra.printSchema()
# print(df_extra.count(), df_extra.schema)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Join columns to filter out only macthing records of the table

# COMMAND ----------

#dim customer
df_cust = spark.read.table('retailco.retailco_schema.dim_customers').\
    filter(col('is_active') == True)

#dim products
df_prod = spark.read.table('retailco.retailco_schema.dim_products').\
    filter(col('is_active') == True)

#dim payment methods
df_pay_mts = spark.read.table('retailco.retailco_schema.dim_payment_mts')
# display(df_cust.limit(10))
# print(df_extra.count())

#Joining final results based on dim customer table and exchanging customer_id with customer_sk
df_cust_join = df_extra.join(df_cust, 'customer_id', 'inner').\
    select(df_extra['*'] , df_cust['customer_sk']).\
        drop('customer_id')
    
#Joining customer results based on dim payments methods table and exchanging payment_method with payment_sk
df_pm_join = df_cust_join.join(df_pay_mts, upper(df_cust_join['payment_method']) == upper(df_pay_mts['payment_method']) , 'inner').\
    select(df_cust_join['*'],
           df_pay_mts['payment_sk']).\
    drop('payment_method')

#Joining payment method results based on dim products table and exchanging product_id with product_sk
df_prod_join = df_pm_join.join(df_prod, 'product_id', 'inner').\
    select(df_pm_join['*'], df_prod['product_sk']).\
    drop('product_id')

#Selecting the final values to be displayed in a returns table.
df_final =  df_prod_join.select(
    'order_id',
    'order_date', 
    'delivery_date', 
    'status', 
    'quantity',
    'unit_price', 
    'gross_amount', 
    'discount_amount', 
    'net_amount',  
    'product_sk', 
    'customer_sk', 
    'payment_sk', 
    'source_system', 
    'ingested_timestamp', 
    'record_hash'
)

display(df_prod_join.count())

# COMMAND ----------

#storing final data as a view
df_final.createOrReplaceTempView('orders_view')

#storing maximum ingested timestamp in a max_dt variable
max_ts = df_final.select(max(col('ingested_timestamp')).alias('max_dt'))
max_dt =  max_ts.collect()[0]['max_dt']

#final count of records
final_cnt = df_final.count()

#If a particular table exists
if spark.catalog.tableExists('retailco.retailco_schema.fact_orders'):
    
    #Getting Target Delta table  
    target = DeltaTable.forName(spark,'retailco.retailco_schema.fact_orders')
    target_df = target.toDF()

    #if target is not an empty table
    if target_df.count() > 0:
        spark.sql("""
                  merge into retailco.retailco_schema.fact_orders as target
                  using orders_view as source
                  on target.order_id = source.order_id 
                  when matched 
                  and source.record_hash != target.record_hash
                  then update 
                  set
                    target.order_id = source.order_id,
                    target.order_date = source.order_date, 
                    target.delivery_date = source.delivery_date, 
                    target.status = source.status, 
                    target.quantity = source.quantity,
                    target.unit_price = source.unit_price, 
                    target.gross_amount = source.gross_amount, 
                    target.discount_amount = source.discount_amount,  
                    target.net_amount = source.net_amount, 
                    target.product_sk = source.product_sk,  
                    target.customer_sk = source.customer_sk, 
                    target.payment_sk = source.payment_sk, 
                    target.source_system = source.source_system,
                    target.ingested_timestamp = source.ingested_timestamp, 
                    target.record_hash = source.record_hash
                  when not matched then insert 
                  (
                    order_id,
                    order_date, 
                    delivery_date, 
                    status, 
                    quantity,
                    unit_price, 
                    gross_amount, 
                    discount_amount, 
                    net_amount,  
                    product_sk, 
                    customer_sk, 
                    payment_sk, 
                    source_system, 
                    ingested_timestamp, 
                    record_hash
                  )
                  values
                  (
                    order_id,
                    order_date, 
                    delivery_date, 
                    status, 
                    quantity,
                    unit_price, 
                    gross_amount, 
                    discount_amount, 
                    net_amount,  
                    product_sk, 
                    customer_sk, 
                    payment_sk, 
                    source_system, 
                    ingested_timestamp, 
                    record_hash)
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
        #If it is an empty table
        spark.sql("""
                  Insert into retailco.retailco_schema.fact_orders
                  (
                    order_id,
                    order_date, 
                    delivery_date, 
                    status, 
                    quantity,
                    unit_price, 
                    gross_amount, 
                    discount_amount, 
                    net_amount,  
                    product_sk, 
                    customer_sk, 
                    payment_sk, 
                    source_system, 
                    ingested_timestamp, 
                    record_hash
                  )
                  select 
                    order_id,
                    order_date, 
                    delivery_date, 
                    status, 
                    quantity,
                    unit_price, 
                    gross_amount, 
                    discount_amount, 
                    net_amount,  
                    product_sk, 
                    customer_sk, 
                    payment_sk, 
                    source_system, 
                    ingested_timestamp, 
                    record_hash 
                  from orders_view
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
              create table retailco.retailco_schema.fact_orders
              (
              order_sk bigint generated by default as identity primary key,
              order_id string,
              order_date date,
              delivery_date date,
              status string,
              quantity integer,
              unit_price double,
              gross_amount double,
              discount_amount double,
              net_amount double,
              product_sk long,
              customer_sk long,
              payment_sk long,
              source_system string,
              ingested_timestamp timestamp,
              record_hash string
              )
              using delta
              partitioned by (ingested_timestamp);
              """)

    spark.sql("""
                  Insert into retailco.retailco_schema.fact_orders
                  (
                    order_id,
                    order_date, 
                    delivery_date, 
                    status, 
                    quantity,
                    unit_price, 
                    gross_amount, 
                    discount_amount, 
                    net_amount,  
                    product_sk, 
                    customer_sk, 
                    payment_sk, 
                    source_system, 
                    ingested_timestamp, 
                    record_hash
                  )
                  select 
                    order_id,
                    order_date, 
                    delivery_date, 
                    status, 
                    quantity,
                    unit_price, 
                    gross_amount, 
                    discount_amount, 
                    net_amount,  
                    product_sk, 
                    customer_sk, 
                    payment_sk, 
                    source_system, 
                    ingested_timestamp, 
                    record_hash 
                  from orders_view;
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

# #string to date check and update

# # df = df.withColumn('date_flag', when(col('order_date').try_cast('date').isNotNull(), True).otherwise(False))
# # df = df.withColumn('order_date', when(col('date_flag') == True, to_date(col('order_date'), 'yyyy-MM-dd')).otherwise('Unknown')).drop(col('date_flag'))
# #Handling other datatypes
# # check = 'datatype_handling'
# # check_type = 'date'
# # col_name = 'order_date'

# #Not Null Checks
# if check == 'Not_Null':
#     older_count = df.count()
#     df = df.filter(col(f'{col_name}').isNotNull())
#     new_count = df.count()
#     print(f"No of records dropped due to {check} check is {older_count - new_count}")

# #DataType handling
# if check == 'datatype_handling' and check_type != None:
#     old_cnt = df.count()
#     df = df.\
#         filter(col(f'{col_name}').try_cast(f'{check_type}').isNotNull()).\
#         withColumn(f'{col_name}', col(f'{col_name}').cast(f'{check_type}'))
#     new_count = df.count()
#     print(f"No of records dropped due to {check} check is {old_cnt - new_count}")

# #Date DataType_check
# # if check == 'date_dataType_Check' and check_type != None:
# #     old_cnt = df.count()
# #     df = df.\
# #         filter(col(f'{col_name}_check').try_cast(f'{check_type}').isNotNull()).\
# #         withColumn(f'{col_name}', col(f'{col_name}').cast(f'{check_type}'))
# #     new_count = df.count()
# #     print(f"No of records dropped due to {check} check is {old_cnt - new_count}")


# #Starts with issue
# if check == 'pattern' and starts_with != None:
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


# #Greater than some value
# if check == 'greater_than' and value_to_ck != None:
#     older_count = df.count()
#     df = df.filter(col(f"col_name") > value_to_ck)
#     new_count = df.count()
#     print(f"No of records dropped due to {check} check is {older_count - new_count}")

# #Less than some value
# if check == 'less_than' and value_to_ck != None:
#     older_count = df.count()
#     df = df.filter(col(f"col_name") < value_to_ck)
#     new_count = df.count()
#     print(f"No of records dropped due to {check} check is {older_count - new_count}")

# #sequence_check
# if check == 'Sequence_ck'and check_list != None:
#     if type(check_list) == list:
#         older_count = df.count()
#         df = df.filter(~(col(f'{col_name}').isin(f'{check_list}')))
#         new_count = df.count()
#         print(f"No of records dropped due to {check} check is {older_count - new_count}")
