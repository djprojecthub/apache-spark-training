# Databricks notebook source
# MAGIC %md
# MAGIC ## How to skip first few rows of data?

# COMMAND ----------

dbutils.fs.put(
"/scenarios/skip_lines.csv","""line1
line2
line3
line4
id,name,loc
1,Diwakar,GZB
2,Piyush,NOIDA
3,Ashu,DLI
"""
)

# COMMAND ----------

df = spark.read.csv("/scenarios/skip_lines.csv",header=True)
display(df)

# COMMAND ----------

rdd = spark.sparkContext.textFile("/scenarios/skip_lines.csv")
rdd_final = rdd.zipWithIndex().filter(lambda a:a[1]>3).map(lambda a:a[0].split(","))
rdd_final

# COMMAND ----------

columns = rdd_final.collect()[0]

# COMMAND ----------

skipRow = rdd_final.first()

# COMMAND ----------

final_df = rdd_final.filter(lambda a:a!=skipRow).toDF(columns).show()
display(final_df)
