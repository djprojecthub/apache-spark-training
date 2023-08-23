# Databricks notebook source
# MAGIC %md
# MAGIC ## How to handle dynamic columns?

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create sample data file with columns and obeserve output

# COMMAND ----------

dbutils.fs.put("/scenarios/dynamic_columns.csv","""
               id,name,loc,emailid,phone
               1,Ravi
               2,Ram,Bangalore
               3,Prasad,Chennai,sample123@gmail.com,9129193
               4,Sam,Pune
               """)

# COMMAND ----------

df = spark.read.csv("/scenarios/dynamic_columns.csv",header=True)
df.display()

# COMMAND ----------

dbutils.fs.rm("/scenarios/dynamic_withoutCols.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create sample data file WITHOUT columns(or headers) and observe output

# COMMAND ----------

dbutils.fs.put("/scenarios/dynamic_withoutCols.csv",
            """1,Ravi
               2,Ram,Bangalore
               3,Prasad,Chennai,sample123@gmail.com,9129193
               4,Sam,Pune""")

# COMMAND ----------

df = spark.read.csv("/scenarios/dynamic_withoutCols.csv")
df.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### This scenario handles the situation where source systems does not give the column names

# COMMAND ----------

schema="id integer, name string, city string, email string, mob integer"

# COMMAND ----------

df = spark.read.csv("/scenarios/dynamic_withoutCols.csv",schema=schema)
df.display()
