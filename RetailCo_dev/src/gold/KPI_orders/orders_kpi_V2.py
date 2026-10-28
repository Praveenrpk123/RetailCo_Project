# Databricks notebook source
# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","")
dbutils.widgets.text("schema_name", "")

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Total Net and Gross Revenue with total number of orders in each month

# COMMAND ----------

#  Total Net and Gross Revenue with total number of orders in each month
# filter based on partition column as well
df_net_gross_rev_monthly = spark.sql(f"""select trunc(order_date, 'MM') as order_month, 
    count(order_id) as total_orders,
    floor(sum(unit_price)) as total_gross_revenue,
    floor(sum(net_amount)) as total_net_revenue
    from  {catalog_name}.{schema_name}.fact_orders
    where status in ('Delivered', 'Shipped') and year(ingested_timestamp) = year(current_date)
    group by trunc(order_date, 'MM')
    order by trunc(order_date, 'MM');""")

df_net_gross_rev_monthly.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.Monthly_Net_and_gross_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC # highest no of orders shipped in a last 10 days.

# COMMAND ----------

# highest no of orders shipped in a last 10 days.
shipped_orders_in_last_10_days = spark.sql(f"""
    select order_date, count(*) as total_orders
    from {catalog_name}.{schema_name}.fact_orders
    where status in ('Shipped') and 
    order_date > date_sub(current_date, 10)
    group by order_date
    order by order_date desc;""")

shipped_orders_in_last_10_days.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.orders_shipped_10_days_dayWise")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Highest no of orders delivered in last 10 days.

# COMMAND ----------

# highest no of orders delivered in last 10 days.
orders_delivered_10_days_dayWise = spark.sql(f"""
    select order_date, count(*) as total_orders 
    from {catalog_name}.{schema_name}.fact_orders
    where status in ('Delivered') and 
    order_date > date_sub(current_date, 10)
    group by order_date
    order by order_date desc;""")

orders_delivered_10_days_dayWise.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.orders_delivered_10_days_dayWise")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 customers with more orders

# COMMAND ----------

# Top 5 customers with more orders
top_5_customers_with_orders = spark.sql(f"""with total_orders_per_customer_cte as 
        (
        select customer_sk, count(*) as total_orders,
        dense_rank() over(order by count(*) desc)  as rnk
        from {catalog_name}.{schema_name}.fact_orders
        group by customer_sk
        order by total_orders desc)
        select rnk as s_no, customer_id, customer_name, total_orders 
        from total_orders_per_customer_cte as tc join {catalog_name}.{schema_name}.dim_customers as dc
        on tc.customer_sk = dc.customer_sk
        where rnk <= 5
        order by total_orders desc;""")


top_5_customers_with_orders.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.top_5_customers_with_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###   Top 5 products with more orders

# COMMAND ----------

#   Top 5 products with more orders
top_5_products_with_high_orders = spark.sql(f"""with total_orders_per_prod_cte as (
        select product_sk, count(*) as no_of_products,
        dense_rank() over(order by count(*) desc)  as rnk
        from {catalog_name}.{schema_name}.fact_orders
        group by product_sk
        order by no_of_products desc)
        select rnk as s_no, product_id, product_name, no_of_products 
        from total_orders_per_prod_cte as pc join {catalog_name}.{schema_name}.dim_products as dp
        on dp.product_sk = pc.product_sk
        where rnk <= 5;""")


top_5_products_with_high_orders.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.top_5_products_with_high_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Top 5 products with quantity sold

# COMMAND ----------

#   Top 5 products with quantity sold
top_5_products_with_high_quantity_sold = spark.sql(f"""with total_orders_per_prod_cte as (
        select product_sk, sum(quantity) as total_quantity,
        dense_rank() over(order by sum(quantity) desc)  as rnk
        from {catalog_name}.{schema_name}.fact_orders
        where status in ('Shipped', 'Delivered')
        group by product_sk
        order by total_quantity desc)
        select rnk as s_no, product_id, product_name, total_quantity 
        from total_orders_per_prod_cte as pc join {catalog_name}.{schema_name}.dim_products as dp
        on dp.product_sk = pc.product_sk
        where rnk <= 5;""")

# display(top_5_products_with_high_quantity_sold)
top_5_products_with_high_quantity_sold.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.top_5_products_with_high_quantity_sold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 products with highest revenue

# COMMAND ----------

top_5_products_with_high_revenue = spark.sql(f"""with highest_revenue_prods as (
        select product_sk, 
        floor(sum(net_amount)) as product_revenue,
        rank() over(order by sum(net_amount) desc) as rnk
        from {catalog_name}.{schema_name}.fact_orders
        where status in ('Shipped', 'Delivered')
        group by product_sk
        order by product_revenue desc)
        select rnk as s_no, product_id, product_name, product_revenue 
        from highest_revenue_prods hp join {catalog_name}.{schema_name}.dim_products dp
        on hp.product_sk = dp.product_sk
        where rnk <= 5;""")

top_5_products_with_high_revenue.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.top_5_products_with_high_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Orders KPI

# COMMAND ----------

order_kpi_rate =  spark.sql(f"""with val_cte as (select 
        sum(quantity) as total_qty_sold,
        count(*) as total_orders, 
        sum(case when status in ('Shipped', 'Delivered') then 1 else 0 end) as successful_orders,
        sum(case when status in ('Cancelled') then 1 else 0 end) as cancelled_orders,
        sum(case when status in ('Returned') then 1 else 0 end) as returned_orders,
        floor(avg(datediff(delivery_date, order_date))) as avg_delivery_days
        from {catalog_name}.{schema_name}.fact_orders)
        select total_qty_sold, 
        concat(round((1 - ((total_orders - successful_orders) / total_orders))*100,2)  , '%') as order_fulfillment_rate ,
        concat(round((1 - ((total_orders - cancelled_orders) / total_orders))*100,2)  , '%') as order_cancellation_rate,
        concat(round((1 - ((total_orders - returned_orders) / total_orders))*100,2)  , '%') as order_return_rate,
        avg_delivery_days
        from val_cte;""")

order_kpi_rate.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.orders_kpi")