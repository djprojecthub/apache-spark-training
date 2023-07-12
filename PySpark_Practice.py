# Databricks notebook source
# MAGIC %md
# MAGIC ### 1.  Create dataframe from list of data and list of columns

# COMMAND ----------

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("learnspark").getOrCreate()

data = [
    ["Diwakar",29,"Noida","1994-09-20"],
    ["Piyush",29,"Delhi","1994-05-26"],
    ["Ashish",35,"Delhi","1988-01-01"],
    ["Lalit",28,"Noida","1995-12-13"],
    ["Aman",28,"Haldwani","1995-10-12"],
    ["Nimesh",27,"Haldwani","1996-01-01"],
    ["Pushpesh",None,"Rudrapur",None]
]
columns = ["Name","Age","City","Date"]
df = spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

display(df.rdd.getNumPartitions())

# COMMAND ----------

from pyspark.sql.functions import col

output1 = df.select("Name","Age").where("age < 30").orderBy(col("age").desc())
output1.show()

# COMMAND ----------

df.agg({"Age":"avg"}).show()

# COMMAND ----------

output2 = (
    df.filter(col("Age").isNotNull())
        .withColumn("Age after 5 years",col("Age")+5)
        .withColumn("Half of the age",col("Age")/2)
)
output2.show()

# COMMAND ----------

from pyspark.sql.functions import avg,min,window

df.groupBy("City").agg(
    avg("Age").alias("AvgAge")
).show()
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3rd highest Age

# COMMAND ----------

df.select("Age").distinct().orderBy(col("Age").desc()).limit(3).sort(col("Age")).limit(1).show()

# COMMAND ----------

df.select("Age").distinct().sort(col("Age").desc()).limit(3).agg({"Age":"min"}).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### joins in pyspark

# COMMAND ----------

j1 = [["1"],["2"],["a"],["a"],["1"],["1"],[None]]
dj1 = spark.createDataFrame(j1,["col1"])
j2 = [["b"],["a"],["1"],["1"],["1"],["2"],["2"],["2"],["3"],["b"],[None],[None]]
dj2 = spark.createDataFrame(j2,["col1"])

#inner join
dj1.join(dj2,"col1").count()
#left join
# dj1.join(dj2,"col1","left_outer").count()
#right join
# dj1.join(dj2,"col1","right_outer").count()
#outer join
# dj1.join(dj2,"col1","outer").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### window functions in pyspark

# COMMAND ----------

from pyspark.sql.window import Window

sampleData = (("Ram", 28, "Sales", 3000),
              ("Meena", 33, "Sales", 4600),
              ("Robin", 40, "Sales", 4100),
              ("Kunal", 25, "Finance", 3000),
              ("Ram", 28, "Sales", 3000),
              ("Srishti", 46, "Management", 3300),
              ("Jeny", 26, "Finance", 3900),
              ("Hitesh", 30, "Marketing", 3000),
              ("Kailash", 29, "Marketing", 2000),
              ("Sharad", 39, "Sales", 4100)
              )
 
# column names for dataframe
columns = ["Employee_Name", "Age",
           "Department", "Salary"]

df = spark.createDataFrame(sampleData,columns)
df.show()

# COMMAND ----------

window_partition = Window.partitionBy("Department").orderBy("Salary")

# COMMAND ----------

from pyspark.sql.functions import row_number,dense_rank
df.withColumn("rank",dense_rank().over(window_partition)).show()

# COMMAND ----------

from pyspark.sql.functions import lag

wp = Window.partitionBy("Salary").orderBy("Age")

df.withColumn("lag",lag("Salary",1).over(window_partition)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implement broadcast joins in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot in PySpark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implement Map & FlatMap
