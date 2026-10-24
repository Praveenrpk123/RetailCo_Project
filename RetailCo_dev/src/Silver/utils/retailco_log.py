class retailco_log():
    def __init__(self,spark):
        self.spark = spark
        print('welcome to retailco pipeline')
    
    def control_table_update(self, src, tgt, table_name, max_ts):
        print(src, tgt, table_name, max_ts)
        self.spark.sql(f"""delete from {catalog_name}.{schema_name}.control_table 
              where src_layer = '{src}' and tgt_layer = '{tgt}' and table_name = '{table_name}';""")
    
        self.spark.sql(f"""insert into {catalog_name}.{schema_name}.control_table
              (src_layer, tgt_layer, table_name, max_timestamp, is_active, ct_ingested_timestamp)
              values
              ('{src}', '{tgt}', '{table_name}', '{max_ts}', true, current_timestamp());""")
        return 'Updated in control table successfully'
        
    def job_logs(self, job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime):
        self.spark.sql(f"""insert into {catalog_name}.{schema_name}.job_logs
              (job_id , job_name, run_id , table_name, rows_read, rows_written, job_datetime ,no_of_retries ,trigger_type ,trigger_datetime) 
              values 
              ('{job_id}', '{job_name}', '{run_id}' , '{table_name}', '{rows_read}' , '{rows_written}', '{job_datetime}' , '{no_of_retries}' ,'{trigger_type}' ,'{trigger_datetime}');""")