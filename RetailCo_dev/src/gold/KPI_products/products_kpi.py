# Databricks notebook source
# Declare the widget (required, even when running as a job) -- coming from DAB
dbutils.widgets.text("catalog_name","RetailCo_Dev_Catalog")
dbutils.widgets.text("schema_name", "RetailCo_Dev_schema")

# Retrieve the value
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Top Selling products

# COMMAND ----------

top_5_selling_prods =  spark.sql(f"""
            with prod_id_cte as (select ord.* , prod.product_id from 
            {catalog_name}.{schema_name}.fact_orders ord 
            join 
            {catalog_name}.{schema_name}.dim_products prod 
            on ord.product_sk = prod.product_sk
            and ord.status in ('Delivered', 'Shipped')), 
            total_qty_cte as (select product_id, sum(quantity) as total_products_sold , dense_rank() over(order by sum(quantity) desc) as rk
            from 
            prod_id_cte group by product_id
            order by total_products_sold desc)
            select product_id, total_products_sold from total_qty_cte 
            where rk  <= 5;
        """)

top_5_selling_prods.\
    write.\
    mode("overwrite").\
    saveAsTable(f"{catalog_name}.{schema_name}.Top_5_selling_products_overall")

# COMMAND ----------

# MAGIC %md
# MAGIC ###No of products in each Category

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select category, count(product_id) as total_products from {catalog_name}.{schema_name}.dim_products 
# MAGIC where is_active = true
# MAGIC group by category
# MAGIC order by count(product_id) desc;

# COMMAND ----------

# MAGIC %md
# MAGIC ###Suppliers with no of products

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select supplier_id, count(product_id) as total_products from {catalog_name}.{schema_name}.dim_products 
# MAGIC where is_active = true
# MAGIC group by supplier_id
# MAGIC order by count(product_id) desc;