# Databricks notebook source
# MAGIC %md 
# MAGIC ### Data Preparation

# COMMAND ----------

data = [
    (101,"Chocolate","KitKat",65.00,3),
    (102,"Beverages","Coconut Water",40.00,2),
    (103,"Bathing Essentials","Soap",50.00,5),
    (104,"Fragnance","Room Freshner",120.00,1),
    (101,"Snacks","Biscuits",10.00,5),
    (102,"Chocolate","Hershey",50.00,2),
    (102,"Snacks","Potato Chips",50.00,2)
]
columns = ["OrderId","Item Type","Item Name","Unit Price","Quantity"]

salesDF = spark.createDataFrame(data, columns)
display(salesDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Select specific columns from a PySpark Dataframe

# COMMAND ----------

from pyspark.sql.functions import col
# Way 1.
allItems = salesDF.select("Item Type")
allItems.show()

# Way 2.
allItems_ = salesDF.select(col("Item Type"))
allItems_.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Rename columns in a PySpark Dataframe

# COMMAND ----------

salesDF_modified = salesDF.withColumnRenamed("Item Type","Product Category")
salesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Filter rows based on a condition

# COMMAND ----------

# Way 1.
salesDF.where(col("Unit Price") < 100).show()

# Way 2.
salesDF.select("Item Type").where(col("Item Type").startswith("C")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Group and aggregate data by a specific column

# COMMAND ----------

salesDF_modified.show()

# COMMAND ----------

from pyspark.sql.functions import sum

totalSalesByProductCategory = salesDF_modified \
    .groupBy("Product Category") \
    .agg(sum(col("Unit Price") * col("Quantity")).alias("Total Sales"))

totalSalesByProductCategory.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Split a PySpark Dataframe into multiple smaller PySpark Dataframes

# COMMAND ----------

salesDF.filter(salesDF["Unit Price"] < 50).show()
salesDF.filter(salesDF["Unit Price"] > 50).show()
salesDF.filter(col("Unit Price").between(50,100)).show() # important
salesDF.filter(salesDF["Unit Price"].between(20,60)).show() # important

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Merge multiple PySpark Dataframes together

# COMMAND ----------

df1 = salesDF.filter(salesDF["Unit Price"] < 50)
df2 = salesDF.filter(salesDF["Unit Price"] > 50)
df3 = salesDF.filter(col("Unit Price").between(50,100))
df4 = salesDF.filter(salesDF["Unit Price"].between(20,60))

mergedDF1to4 = df1.union(df2).union(df3).union(df4)
mergedDF1to4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Convert a PySpark Dataframe to a list of dictionaries

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Convert a PySpark Dataframe to a list of tuples

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Convert a PySpark Dataframe to a dictionary of lists

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Convert a PySpark Dataframe to a dictionary of PySpark Dataframes
