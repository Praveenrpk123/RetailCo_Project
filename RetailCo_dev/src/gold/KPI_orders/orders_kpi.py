# Databricks notebook source
# MAGIC %sql
# MAGIC -- select order_id, order_date, delivery_date, status, quantity, unit_price, gross_amount, discount_amount, net_amount, product_sk, customer_sk, payment_sk, source_system, ingested_timestamp from retailco.retailco_schema.fact_orders where order_id = 'ORD0211599'
# MAGIC
# MAGIC -- select 
# MAGIC -- trunc(order_date, 'MM') as order_month,
# MAGIC -- count(*) as cnt, 
# MAGIC -- round(sum(unit_price),2) as total__gross_revenue, 
# MAGIC -- round(sum(net_amount),2)  as total_net_revenue 
# MAGIC -- from retailco.retailco_schema.fact_orders 
# MAGIC -- where status in ('Delivered', 'Shipped')
# MAGIC -- group by trunc(order_date, 'MM')
# MAGIC -- order by trunc(order_date, 'MM');

# COMMAND ----------

# MAGIC %sql
# MAGIC explain select * from retailco.retailco_schema.fact_orders
# MAGIC order by order_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC explain with cte as (
# MAGIC     select * , 
# MAGIC     row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
# MAGIC     from retailco.retailco_schema.fact_orders), 
# MAGIC     non_dup_cte(
# MAGIC         select * from cte where rn =  1
# MAGIC         ) select 
# MAGIC         trunc(order_date, 'MM') as order_month, 
# MAGIC         count(order_id) as total_orders,
# MAGIC         floor(sum(unit_price)) as total_gross_revenue,
# MAGIC         floor(sum(net_amount)) as total_net_revenue
# MAGIC         from non_dup_cte 
# MAGIC         where status in ('Delivered', 'Shipped')
# MAGIC         group by trunc(order_date, 'MM')
# MAGIC         order by trunc(order_date, 'MM');

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Total Net and Gross Revenue with total number of orders in each month

# COMMAND ----------

#  Total Net and Gross Revenue with total number of orders in each month
df_net_gross_rev_monthly = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ) select 
        trunc(order_date, 'MM') as order_month, 
        count(order_id) as total_orders,
        floor(sum(unit_price)) as total_gross_revenue,
        floor(sum(net_amount)) as total_net_revenue
        from non_dup_cte 
        where status in ('Delivered', 'Shipped')
        group by trunc(order_date, 'MM')
        order by trunc(order_date, 'MM');""")


df_net_gross_rev_monthly.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.Monthly_Net_and_gross_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC # highest no of orders shipped in a last 10 days.

# COMMAND ----------

# highest no of orders shipped in a last 10 days.
shipped_orders_in_last_10_days = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ) select order_date, count(*) as total_orders
        from non_dup_cte
        where status in ('Shipped') and 
        order_date > date_sub(current_date, 10)
        group by order_date
        order by order_date desc;""")

shipped_orders_in_last_10_days.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.orders_shipped_10_days_dayWise")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Highest no of orders delivered in last 10 days.

# COMMAND ----------

# highest no of orders delivered in last 10 days.
orders_delivered_10_days_dayWise = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ) select order_date, count(*) as total_orders 
        from non_dup_cte
        where status in ('Delivered') and 
        order_date > date_sub(current_date, 10)
        group by order_date
        order by order_date desc;""")

orders_delivered_10_days_dayWise.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.orders_delivered_10_days_dayWise")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 customers with more orders

# COMMAND ----------

# Top 5 customers with more orders
top_5_customers_with_orders = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ),
        total_orders_per_customer_cte as 
        (
        select customer_sk, count(*) as total_orders,
        dense_rank() over(order by count(*) desc)  as rnk
        from non_dup_cte
        group by customer_sk
        order by total_orders desc)
        select rnk as s_no, customer_id, customer_name, total_orders 
        from total_orders_per_customer_cte as tc join retailco.retailco_schema.dim_customers as dc
        on tc.customer_sk = dc.customer_sk
        where rnk <= 5
        order by total_orders desc;""")


top_5_customers_with_orders.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.top_5_customers_with_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###   Top 5 products with more orders

# COMMAND ----------

#   Top 5 products with more orders
top_5_products_with_high_orders = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ),
        total_orders_per_prod_cte as 
        (
        select product_sk, count(*) as no_of_products,
        dense_rank() over(order by count(*) desc)  as rnk
        from non_dup_cte
        group by product_sk
        order by no_of_products desc)
        select rnk as s_no, product_id, product_name, no_of_products 
        from total_orders_per_prod_cte as pc join retailco.retailco_schema.dim_products as dp
        on dp.product_sk = pc.product_sk
        where rnk <= 5;""")


top_5_products_with_high_orders.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.top_5_products_with_high_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Top 5 products with quantity sold

# COMMAND ----------

#   Top 5 products with quantity sold
top_5_products_with_high_quantity_sold = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ),
        total_orders_per_prod_cte as 
        (
        select product_sk, sum(quantity) as total_quantity,
        dense_rank() over(order by sum(quantity) desc)  as rnk
        from non_dup_cte
        where status in ('Shipped', 'Delivered')
        group by product_sk
        order by total_quantity desc)
        select rnk as s_no, product_id, product_name, total_quantity 
        from total_orders_per_prod_cte as pc join retailco.retailco_schema.dim_products as dp
        on dp.product_sk = pc.product_sk
        where rnk <= 5;""")

# display(top_5_products_with_high_quantity_sold)
top_5_products_with_high_quantity_sold.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.top_5_products_with_high_quantity_sold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 products with highest revenue

# COMMAND ----------

top_5_products_with_high_revenue = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ),
    highest_revenue_prods as 
    (
        select product_sk, 
        floor(sum(net_amount)) as product_revenue,
        rank() over(order by sum(net_amount) desc) as rnk
        from non_dup_cte
        where status in ('Shipped', 'Delivered')
        group by product_sk
        order by product_revenue desc)
        select rnk as s_no, product_id, product_name, product_revenue 
        from highest_revenue_prods hp join retailco.retailco_schema.dim_products dp
        on hp.product_sk = dp.product_sk
        where rnk <= 5;""")

top_5_products_with_high_revenue.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.top_5_products_with_high_revenue")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Total Quantity Sold

# COMMAND ----------

# Total Quantity Sold
total_quantity_sold = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ) select sum(quantity) as total_qty_sold from non_dup_cte
        where status in ('Shipped', 'Delivered');""")

total_quantity_sold.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.total_quantity_sold")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order_Fulfillment_rate

# COMMAND ----------

order_Fulfillment_rate =  spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ),
        val_cte as (select 
        count(*) as total_orders, 
        sum(case when status in ('Shipped', 'Delivered') then 1 else 0 end) as successful_orders 
        from non_dup_cte)
        select 
        concat(round((1 - ((total_orders - successful_orders) / total_orders))*100,2)  , '%') as order_fulfillment_rate from val_cte;""")
        # select  status, 
        # sum(case when status in ('Shipped', 'Delivered') then 1 else 0 end) as tot, 
        # count(*) as total_orders
        # from non_dup_ct
        # group by status;
        # where status in ('Shipped', 'Delivered');

order_Fulfillment_rate.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.order_Fulfillment_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Cancellation Rate

# COMMAND ----------


cancellation_rate = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ),
        val_cte as (select 
        count(*) as total_orders, 
        sum(case when status in ('Cancelled') then 1 else 0 end) as cancelled_orders 
        from non_dup_cte)
        select 
        concat(round((1 - ((total_orders - cancelled_orders) / total_orders))*100,2)  , '%') as order_cancellation_rate from val_cte;""")

cancellation_rate.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.cancellation_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Return Rate

# COMMAND ----------

return_rate = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ),
        val_cte as (select 
        count(*) as total_orders, 
        sum(case when status in ('Returned') then 1 else 0 end) as returned_orders 
        from non_dup_cte)
        select 
        concat(round((1 - ((total_orders - returned_orders) / total_orders))*100,2)  , '%') as order_return_rate from val_cte;""")

return_rate.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.order_return_rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Average Delivery Time

# COMMAND ----------

avg_delivery_time = spark.sql("""with cte as (
    select * , 
    row_number() over(partition by order_id order by ingested_timestamp desc) as rn  
    from retailco.retailco_schema.fact_orders), 
    non_dup_cte(
        select * from cte where rn =  1
        ) select floor(avg(datediff(delivery_date, order_date))) as avg_days from non_dup_cte;""")

avg_delivery_time.\
    write.\
    mode("overwrite").\
    saveAsTable("retailco.retailco_schema.avg_delivery_time")