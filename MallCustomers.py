# Databricks notebook source
# File location and type
file_location = "/FileStore/tables/Mall_Customers.csv"
file_type = "csv"

# CSV options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# Create a table
permanent_table_name = "Mall_Customers_csv"

df.write.format("parquet").saveAsTable(permanent_table_name)



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC 
# MAGIC select * from `Mall_Customers_csv`

# COMMAND ----------


