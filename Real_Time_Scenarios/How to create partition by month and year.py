# Databricks notebook source
# MAGIC %md
# MAGIC ## How to create partition by month and year

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spark's default date fromat : yyyy-MM-dd

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create and apply schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

emp_schema = StructType([
    StructField("EmpNo",IntegerType(),True),
    StructField("EName",StringType(),True),
    StructField("Job",StringType(),True),
    StructField("Manager",IntegerType(),True),
    StructField("HireDate",StringType(),True),
    StructField("Salary",IntegerType(),True),
    StructField("Comm",IntegerType(),True),
    StructField("DeptNo",IntegerType(),True),
    StructField("UpdateDate",StringType(),True)
])

empDF = spark.read.csv("/FileStore/emp.csv", header=True, schema=emp_schema)
display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Format "HireDate" to have any one type of special character and add zeros to single digit date and month

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

empDF = empDF.withColumn("HireDate", regexp_replace("HireDate","/","-"))
display(empDF)

# COMMAND ----------

@udf('string')
def formatDateString(dateStr: str) -> str:
    print(dateStr)
    if dateStr != 'null':
        if len(dateStr) == 8:
            return "0"+dateStr[0:2]+"0"+dateStr[2:4]+dateStr[4:len(dateStr)]
        elif len(dateStr[0:dateStr.index("-")]) == 1:
            return "0"+dateStr[0:2]+dateStr[2:len(dateStr)] 
        elif len(dateStr[dateStr.index("-"):len(dateStr)]) == 7:
            return dateStr[0:3]+"0"+dateStr[3:len(dateStr)]  
        else:
            return dateStr  
    else:
        return dateStr        


# COMMAND ----------

empDF = empDF.withColumn("HireDate", formatDateString("HireDate"))
display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Convert "HireDate" column to Spark's default date format

# COMMAND ----------

from pyspark.sql.functions import col, to_date
empDF = empDF.withColumn("HireDate", to_date(col("HireDate"),"dd-MM-yyyy")).fillna({"HireDate":'9999-12-31'})
display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create columns MONTH and YEAR

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 1

# COMMAND ----------

# from pyspark.sql.functions import month,year
# empDF = empDF.withColumn("Month", month("HireDate")) \
#              .withColumn("Year", year("HireDate"))
# display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method 2

# COMMAND ----------

from pyspark.sql.functions import date_format
empDF = empDF.withColumn("Month", date_format("HireDate","MM")) \
             .withColumn("Year", date_format("HireDate","yyyy"))
display(empDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write to a delta table in partitioned format

# COMMAND ----------

empDF.write.format("delta").partitionBy("Year","Month").mode("overwrite").saveAsTable("emp_partition")

# COMMAND ----------

df = spark.read.format("delta").load("/user/hive/warehouse/emp_partition")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Spark automatically casts the value provided for filtering. Check the Physical Plan.

# COMMAND ----------

display(spark.sql("explain select * from emp_partition where Year=1980"))
