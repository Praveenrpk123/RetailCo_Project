# Databricks notebook source
# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","")
dbutils.widgets.text("schema_name", "")

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Newly added customers this month

# COMMAND ----------

new_customers_this_month = spark.sql(f"""with cte as (
    select customer_id, effective_from, trunc(effective_from, 'MM')as effective_start_date,  
     trunc(current_date(), 'MM') as current_month_start_date, 
     row_number() over(partition by customer_id order by effective_from) as rn 
     from {catalog_name}.{schema_name}.dim_customers) 
    select  count(customer_id) as cnt from cte where rn = 1 and effective_from >= current_month_start_date;""")


new_customers_this_month.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.new_customers_this_month")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Newly added customers in last 90 days

# COMMAND ----------

new_customers_90_days = spark.sql(f"""with cte as (
    select customer_id, 
    effective_from,
    date_sub(current_date, 90) as effective_start_date, 
    row_number() over(partition by customer_id order by effective_from) as rn 
    from {catalog_name}.{schema_name}.dim_customers) 
    select count(*) as New_customers_1_year from cte 
    where effective_from >= effective_start_date and rn = 1;""")

new_customers_90_days.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.new_customers_90_days")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Newly added customers in last 180 days

# COMMAND ----------

#  newly added customers in last 180 days
new_customers_180_days = spark.sql(f"""with cte as (
    select customer_id, 
    effective_from,
    date_sub(current_date, 180) as effective_start_date, 
    row_number() over(partition by customer_id order by effective_from) as rn 
    from {catalog_name}.{schema_name}.dim_customers) 
    select count(*) as New_customers_1_year from cte where effective_from >= effective_start_date;""")

new_customers_180_days.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.new_customers_180_days")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Newly added customers in last 1 year

# COMMAND ----------

new_customers_1_year = spark.sql(f"""with cte as (
    select customer_id, 
    effective_from,
    date_sub(current_date, 356) as effective_start_date, 
    row_number() over(partition by customer_id order by effective_from) as rn 
    from {catalog_name}.{schema_name}.dim_customers) 
    select count(customer_id) as New_customers_1_year from cte 
    where effective_from >= effective_start_date and rn = 1;""")

new_customers_1_year.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.new_customers_1_year")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 Highest number of customers in a month

# COMMAND ----------

top_5_month_with_highest_customer = spark.sql(f"""with cte as (
    select 
    customer_id, 
    effective_from, 
    trunc(effective_from, 'MM') as effective_start_date, 
    row_number() over(partition by customer_id order by effective_from) as rn
    from {catalog_name}.{schema_name}.dim_customers), 
top_5_customer_count as (
select effective_start_date, count(*) as no_of_customers , 
dense_rank() over(order by count(*) desc) as rnk
from cte
where rn = 1
group by effective_start_date
order by no_of_customers desc)
select effective_start_date, no_of_customers from top_5_customer_count
where rnk <= 5;""")

top_5_month_with_highest_customer.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.top_5_month_with_highest_customer")


# COMMAND ----------

# MAGIC %md
# MAGIC ###  No of active customers in each segment

# COMMAND ----------

active_customers_in_each_segment = spark.sql(f"""select segment, count(*)  as no_of_customers from 
{catalog_name}.{schema_name}.dim_customers  
where is_active = true
group by segment;""")

active_customers_in_each_segment.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.active_customers_in_each_segment")

# COMMAND ----------

# MAGIC %md
# MAGIC ### No of active customers in each region

# COMMAND ----------

active_customers_in_each_region = spark.sql(f"""select region, count(*)  as no_of_customers from 
{catalog_name}.{schema_name}.dim_customers  
where is_active = true
group by region;""")

active_customers_in_each_region.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.active_customers_in_each_region")