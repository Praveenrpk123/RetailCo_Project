# Databricks notebook source
# MAGIC %sql
# MAGIC -- select return_id, count(*) as cnt from {catalog_name}.{schema_name}.fact_returns
# MAGIC -- group by return_id
# MAGIC -- order by cnt desc;-- where return_date < delivery_date;
# MAGIC
# MAGIC -- select order_sk, count(*) as cnt from {catalog_name}.{schema_name}.fact_returns
# MAGIC -- group by order_sk
# MAGIC -- having count(*) >= 2
# MAGIC -- order by cnt desc;

# COMMAND ----------

# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","")
dbutils.widgets.text("schema_name", "")

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Total Orders Returned

# COMMAND ----------

total_orders_returned = spark.sql(f"""with cte as (select * , row_number() over(partition by order_sk order by order_date desc) as rn 
from {catalog_name}.{schema_name}.fact_returns
order by order_sk, rn), 
dedup_order_sk(select * from cte where rn = 1)
select count(*) as total_orders_returned from dedup_order_sk;""")

total_orders_returned.\
    write.\
    mode('overwrite').\
    saveAsTable(f"{catalog_name}.{schema_name}.total_orders_returned")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Top 5 products returned most

# COMMAND ----------

total_prod_returned = spark.sql(f"""with cte as (select * , row_number() over(partition by order_sk order by order_date desc) as rn 
from {catalog_name}.{schema_name}.fact_returns
order by order_sk, rn), 
dedup_order_sk(select * from cte where rn = 1),
cnt_prods as (select product_sk, count(*) as cnt
from dedup_order_sk
group by product_sk
order by count(*) desc),
stage_cte(select * , dense_rank() over(order by cnt desc) as rk from cnt_prods)
select rk as S_no ,product_id, cnt from stage_cte 
join {catalog_name}.{schema_name}.dim_products dp on dp.product_sk = stage_cte.product_sk
where rk <= 5
order by S_no;""")


total_prod_returned.\
    write.\
    mode('overwrite').\
    saveAsTable(f"{catalog_name}.{schema_name}.top_5_products_returned_most")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 customers returned most

# COMMAND ----------

customers_returned_most = spark.sql(f"""with cte as 
(select * , row_number() over(partition by order_sk order by order_date desc) as rn 
from {catalog_name}.{schema_name}.fact_returns
order by order_sk, rn), 
dedup_order_sk(select * from cte where rn = 1),
cnt_cust as (select customer_sk, count(*) as cnt
from dedup_order_sk
group by customer_sk
order by count(*) desc),
stage_cte(select * , dense_rank() over(order by cnt desc) as rk from cnt_cust)
select rk as S_no ,customer_id, cnt from stage_cte 
join {catalog_name}.{schema_name}.dim_customers dc on dc.customer_sk = stage_cte.customer_sk
where rk <= 5
order by S_no;""")

customers_returned_most.\
    write.\
    mode('overwrite').\
    saveAsTable(f"{catalog_name}.{schema_name}.top_5_customers_returned_most")

# COMMAND ----------

# MAGIC %md
# MAGIC ### total refund, gross amount, net amount

# COMMAND ----------

Amount_refunded_upn = spark.sql(f"""with cte as (select * , row_number() over(partition by order_sk order by order_date desc) as rn 
from {catalog_name}.{schema_name}.fact_returns
order by order_sk, rn), 
dedup_order_sk(select * from cte where rn = 1)
select round(sum(refund_amount),2) as total_refund, 
round(sum(gross_amount)) as gross_amount,
round(sum(net_amount)) as net_amount
from dedup_order_sk;""")

Amount_refunded_upn.\
    write.\
    mode('overwrite').\
    saveAsTable(f"{catalog_name}.{schema_name}.total_Amount_refunded_vs_net")