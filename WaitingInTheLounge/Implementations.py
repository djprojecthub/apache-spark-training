# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Reading data from a CSV file into a PySpark Dataframe

# COMMAND ----------

csvfilepath = '/FileStore/tables/customer_dataset-2.csv' # file stored in DBFS
customer_df = spark.read.csv(
    path=csvfilepath,
    header=True,
    inferSchema=True,
)
print(customer_df.printSchema())
customer_df.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 2. Reading data from a Complex JSON file into a PySpark Dataframe

# COMMAND ----------

jsonfilepath = '/FileStore/tables/car_owner.json'
orderDetails_df = (
    spark.read.format('json')
              .option('multiline','true')
              .option("inferSchema","true")
              .load(jsonfilepath)
)
display(orderDetails_df)
