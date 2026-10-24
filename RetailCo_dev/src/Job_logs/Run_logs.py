# Databricks notebook source
import requests, json
from datetime import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# dbutils.widgets.text('Run_id', '','None')
# # dbutils.widgets.text('Job_id', '', 'None')

# run_id = dbutils.widgets.get('Run_id')
# # job_id = dbutils.widgets.get('Job_id')

# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","RetailCo_Dev_Catalog")
dbutils.widgets.text("schema_name", "RetailCo_Dev_schema")

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")


job_df = spark.\
    sql(f"""select job_id, run_id from {catalog_name}.{schema_name}.job_id order by updated_timestamp desc limit 1""")
job_id = str(job_df.collect()[0]['job_id'])
run_id = str(job_df.collect()[0]['run_id'])
print(job_id, run_id)

# COMMAND ----------


workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
# token = dbutils.secrets.get("my_scope", "databricks_pat")
# token = "dapi529473ccc08129973435d5fe8a62bdb8"
token = dbutils.secrets.get("Retailco_scope","job_token")
headers = {
    "Authorization": f"Bearer {token}"
}
# run_id = 562182904517179
# run_id = dbutils.widgets.get('Run_id')
# print(run_id)
url = f"https://{workspace_url}/api/2.1/jobs/runs/get?run_id={run_id}"
response = requests.get(url, headers = headers)
response_val = response.json()
# dump = json.dump(response_val)
# print(type(dump))


# run_details = json.dumps(response_val, indent = 4)
date_val = datetime.strftime(datetime.now(), '%d_%m_%Y_%H_%M')
run_log_path = '/Volumes/retailco/retailco_schema/run_logs/'
path = run_log_path + run_id + '_' + date_val + '.json'
# print(path)

with open(f"{path}","w") as f:
    json.dump(response_val, f, indent = 4)

# COMMAND ----------

# path = '/Volumes/retailco/retailco_schema/run_logs/887631085213173_27_07_2026_12_27.json'
# path = '/Volumes/retailco/retailco_schema/run_logs/959315520596903_27_07_2026_14_35.json'
# path = '/Volumes/retailco/retailco_schema/run_logs/869809466137448_27_07_2026_14_37.json'/
# path = '/Volumes/retailco/retailco_schema/run_logs/78878981153218_27_07_2026_17_15.json'
logs_df  = spark.read.\
    format('json').\
    option('multiline', True).\
    load(f'{path}')

logs_df = logs_df.\
    withColumn('task_val', explode(col('tasks')))

df_stage = logs_df.\
    select(col('job_id'), 
           col('run_id'),
           col('run_name'),
           col('run_type'),
           to_timestamp(col('start_time')/1000).alias('job_start_date'),
           to_timestamp(col('end_time')/1000).alias('job_end_time'),
           col('state')['result_state'].alias('job_state'),
           col('task_val'))
    
df_stage2 = df_stage.select('*',
    col('task_val').getItem('run_id').alias('task_id'),
    col('task_val').getItem('task_key').alias('task_name'),
    to_timestamp(col('task_val')['start_time']/1000).alias('task_start_time'),
    to_timestamp(col('task_val')['end_time']/1000).alias('task_end_time'),
    round((col('task_val')['setup_duration']/1000),2).alias('env_setup'),
    round((col('task_val')['execution_duration']/1000),2).alias('task_duration'),
    col('task_val')['state']['result_state'].alias('task_state'),
    col('task_val')['status']['termination_details']['code'].alias('error_code'),
    col('task_val')['status']['termination_details']['type'].alias('error_type'),
    col('task_val')['state']['state_message'].alias('Error_msg')
).\
    drop(col('task_val')).\
    orderBy(col('task_start_time'))
df_ctrl_table = spark.read.table(f'{catalog_name}.{schema_name}.job_logs')
df_final = df_stage2.join(df_ctrl_table, 
               (
                   df_stage2['task_name'] == df_ctrl_table['table_name']
                )
               &
               (
                   df_stage2['run_id'] == df_ctrl_table['run_id']
               ), 'left').\
                   select(df_stage2['*'], 
                          df_ctrl_table['rows_read'],
                          df_ctrl_table['rows_written']
                          )

display(df_final)
    # (col('task_end_time') - col('task_start_time')).alias('diff'),

# COMMAND ----------

df_final.createOrReplaceTempView('final_view')
if spark.catalog.tableExists(f'{catalog_name}.{schema_name}.pipeline_run_log'):
    spark.sql(f"""insert into {catalog_name}.{schema_name}.pipeline_run_log(
        job_id,
        run_id  ,
        run_name ,
        run_type ,
        job_start_date ,
        job_end_time ,
        job_state ,
        task_id  ,
        task_name ,
        task_start_time ,
        task_end_time ,
        env_setup ,
        task_duration ,
        task_state ,
        error_code ,
        rows_read ,
        rows_written ,
        error_type ,
        error_msg 
    )       select 
                    job_id ,
                    run_id  ,
                    run_name ,
                    run_type ,
                    job_start_date ,
                    job_end_time ,
                    job_state ,
                    task_id  ,
                    task_name ,
                    task_start_time ,
                    task_end_time ,
                    env_setup ,
                    task_duration ,
                    task_state ,
                    error_code ,
                    rows_read ,
                    rows_written ,
                    error_type ,
                    error_msg 
                  from final_view
              """)
else:
    spark.sql(f"""create or replace table {catalog_name}.{schema_name}.pipeline_run_log(
        pipeline_log_id bigint generated by default as identity,
        job_id long, 
        run_id long,
        run_name string,
        run_type string,
        job_start_date timestamp,
        job_end_time timestamp,
        job_state string,
        task_id long,
        task_name string,
        task_start_time timestamp,
        task_end_time timestamp,
        env_setup double,
        task_duration double,
        task_state string,
        error_code string,
        rows_read int,
        rows_written int,
        error_type string,
        error_msg string);""")
    
    spark.sql(f"""insert into {catalog_name}.{schema_name}.pipeline_run_log(
        job_id ,
        run_id  ,
        run_name ,
        run_type ,
        job_start_date ,
        job_end_time ,
        job_state ,
        task_id  ,
        task_name ,
        task_start_time ,
        task_end_time ,
        env_setup ,
        task_duration ,
        task_state ,
        error_code ,
        rows_read ,
        rows_written ,
        error_type ,
        error_msg 
    )
               select 
                    job_id ,
                    run_id  ,
                    run_name ,
                    run_type ,
                    job_start_date ,
                    job_end_time ,
                    job_state ,
                    task_id  ,
                    task_name ,
                    task_start_time ,
                    task_end_time ,
                    env_setup ,
                    task_duration ,
                    task_state ,
                    error_code ,
                    rows_read int,
                    rows_written int,
                    error_type ,
                    error_msg 
                  from final_view
              """)

# COMMAND ----------

# json_run = json.(dump)
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



# path = '/Volumes/retailco/retailco_schema/run_logs/887631085213173_27_07_2026_11_50.json'
# raw = spark.read.option("wholetext", True).text('/Volumes/retailco/retailco_schema/run_logs/887631085213173_27_07_2026_11_50.json')
# json_str = raw.collect()[0][0]
# val = (json.dumps(json_str, indent = 4))
# print(type(json_str))
# df = (spark.read
#           .option("multiline", "true")
#           .option("columnNameOfCorruptRecord", "_bad_record")
#           .json(path))

# obj = json.loads(json_str)
# obj = json.dumps(json_str)
# print(obj)
# df.printSchema()