# Databricks notebook source
# MAGIC %md
# MAGIC ## How to select a column name with spaces? 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Understand the use of ``

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Access databricks datasets

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/asa/airlines

# COMMAND ----------

# MAGIC %md
# MAGIC ##### When querying the file use `` for file path

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/databricks-datasets/asa/airlines/1987.csv`

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/amazon/data20K/

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Accessing parquet file using `` and creating a dataframe

# COMMAND ----------

df = spark.sql("Select * from parquet.`dbfs:/databricks-datasets/amazon/data20K/part-r-00000-112e73de-1ab1-447b-b167-0919dd731adf.gz.parquet`")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create and put a file with some data on databricks Filestore.

# COMMAND ----------

dbutils.fs.put(
    "/Filestore/emp_new.csv","""
    id, first name, loc
    1,Diwakar,Ghaziabad
    2,Piyush,Noida
    3,Ashu,Delhi
    4,Lalit,Ghaziabad
    """,
)

# COMMAND ----------

empDF = spark.read.csv("/Filestore/emp_new.csv", header=True)
display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Writing a dataframe which has spaces in any of the column name will result in error. Uncomment and run below cell to see the error.

# COMMAND ----------

# empDF.write.format("delta").saveAsTable("emp_delta_table")

# COMMAND ----------

empDF.createOrReplaceTempView("emp_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### FIX : Rename column

# COMMAND ----------

from pyspark.sql.functions import col
empDF1 = empDF.withColumn("first_name", col("` first name`")) \
                .drop(col("` first name`"))
display(empDF1)
