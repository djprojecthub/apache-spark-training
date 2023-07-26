# Databricks notebook source
# MAGIC %md
# MAGIC ### 1. Reading data from a CSV file into a PySpark Dataframe

# COMMAND ----------

csvfilepath = '/FileStore/customer_dataset.csv' # file stored in DBFS
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

jsonfilepath = '/FileStore/car_owner.json'
orderDetails_df = (
    spark.read.format('json')
              .option('multiline','true')
              .option("inferSchema","true")
              .load(jsonfilepath)
)
display(orderDetails_df.select("cars.car1"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Reading data from a Parquet file into a PySpark Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC #### What is Apache Parquet?
# MAGIC Apache Parquet is a binary file format that stores data in a columnar fashion. Data inside a Parquet file is similar to an RDBMS style table where you have columns and rows. But instead of accessing the data one row at a time, you typically access it one column at a time.
# MAGIC
# MAGIC Apache Parquet is one of the modern big data storage formats. It has several advantages, some of which are:
# MAGIC
# MAGIC   > Columnar storage: efficient data retrieval, efficient compression, etc...<br/>
# MAGIC   > Metadata is at the end of the file: allows Parquet files to be generated from a stream of data. (common in big data scenarios)<br/>
# MAGIC   > Supported by all Apache big data products

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create dataframe to bo written to parquet file

# COMMAND ----------

data = [
    ("Diwakar", "", "Joshi", "20-09-1994", "M", 3000),
    ("Piyush", "Kumar", "Tiwari", "26-05-1994", "M", 3500),
    ("Lalit", "Mohan", "Tiwari", "13-12-1995", "M", 4000),
    ("Harleen", "", "Sandhu", "01-01-1993", "F", 2000),
    ("Simran", "", "Kaur", "12-10-1993", "F", 2000)
]
columns = ['FirstName', 'MiddleName', 'LastName', 'DOB', 'Gender', 'Salary']

userdata_df = spark.createDataFrame(data, columns)
display(userdata_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write dataframe to parquet file

# COMMAND ----------

parquetfilepath = '/FileStore/tables/user_details.parquet'

### 3 ways to right dataframe to parquet file (run any one at a time) ###

### 1. Write dataframe to parquet file ###

# userdata_df.write.parquet(parquetfilepath)

### 2. Write dataframe to parquet file with write mode overwrite ###

userdata_df.write\
    .format('parquet')\
        .mode('overwrite')\
            .save(parquetfilepath)    

### 3. Write dataframe to parquet file with write mode append ###

# userdata_df.write\
#     .format('parquet')\
#         .mode('append')\
#             .save(parquetfilepath)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read parquet file

# COMMAND ----------

userdataparquetDF = spark.read.parquet(parquetfilepath)
userdataparquetDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create parquet partition file

# COMMAND ----------

parquetfilepath = '/FileStore/tables/user_details_with_partition.parquet'
userdata_df.write.partitionBy("gender","salary").mode("overwrite").save(parquetfilepath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read partitioned parquet file

# COMMAND ----------

spark.sql("SET spark.databricks.delta.formatCheck.enabled=false")

# COMMAND ----------

# run cmd 16 to read partitioned data using parquet format
userdatapartitionedparquetDF = spark.read.parquet(parquetfilepath+'/Gender=M')
userdatapartitionedparquetDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Reading data from a MySQL database into a PySpark Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Reading data from a web API into a PySpark Dataframe (the right way)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Below code uses json.dumps() for api response body. <br/> Refer https://www.geeksforgeeks.org/json-dumps-in-python/ link for understanding how it works.

# COMMAND ----------

import requests
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def rest_api_call(url):
    res = None
    try:
        res = requests.get(url)
    except Exception as e:
        print(e)
    if res != None and res.status_code == 200:
        return res.text

rest_api_udf = udf(rest_api_call, StringType())

df = spark.createDataFrame([(1, "https://jsonplaceholder.typicode.com/todos/1")], ["id", "url"])

result = df.withColumn("response", rest_api_udf(df.url))
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Reading data from a Kafka stream into a PySpark Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Reading data from file stream using structured streaming into a PySpark dataframe
