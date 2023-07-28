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
salesDF_modified.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Filter rows based on a condition

# COMMAND ----------

# Way 1.
salesDF.filter(col("Unit Price") < 100).show()

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

salesDF.rdd.map(lambda row: row.asDict()).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Convert a PySpark Dataframe to a list of tuples

# COMMAND ----------

sales_in_lst_of_tuples = []

for row in salesDF.collect():
    sales_in_lst_of_tuples.append(tuple(row))

print(sales_in_lst_of_tuples)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Convert a PySpark Dataframe to a dictionary of lists

# COMMAND ----------

salesdict = {}
df = salesDF.toPandas()

for col in df.columns:
    salesdict[col] = df[col].values.tolist()

print(salesdict)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Convert a PySpark Dataframe to a dictionary of PySpark Dataframes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Pivot a PySpark Dataframe to create new columns

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Concept of PIVOT
# MAGIC PySpark pivot() function is used to rotate/transpose the data from one column into multiple Dataframe columns and back using unpivot(). Pivot() is an aggregation where one of the grouping columns values is transposed into individual columns with distinct data.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Prep

# COMMAND ----------

data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
forPivot_df = spark.createDataFrame(data = data, schema = columns)
forPivot_df.printSchema()
forPivot_df.show(truncate=False)

# COMMAND ----------

pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
pivotDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Unpivot a PySpark Dataframe to merge columns into rows

# COMMAND ----------

unpivotDF = pivotDF.unpivot("Product",["Canada","China","Mexico"],"Country","Total Amount")
unpivotDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Replace null values with a specific value or calculated value

# COMMAND ----------

unpivotDF.fillna(0).show()

# COMMAND ----------

unpivotDF.fillna(value=0).show()

# COMMAND ----------

unpivotDF.fillna(value=0, subset=['Total Amount']).show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 14. Replace string values with a specific value or calculated value

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Prep
# MAGIC

# COMMAND ----------

address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA"),
    (4,"201014 Indirapuram","GZB")]
addressDF =spark.createDataFrame(address,["id","address","state"])
addressDF.show()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

addressDF.withColumn("address", regexp_replace("address", "Rd", "Road")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### With a condition

# COMMAND ----------

from pyspark.sql.functions import when
addressDF.withColumn("address", 
              when(addressDF.address.endswith("Rd"), regexp_replace(addressDF.address,"Rd","Road"))
              .when(addressDF.address.endswith("St"), regexp_replace(addressDF.address,"St","Street"))
              .when(addressDF.address.endswith("Ave"), regexp_replace(addressDF.address,"Ave","Avenue"))
              .otherwise(addressDF.address)
              ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. Remove duplicate rows from a PySpark Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Prep

# COMMAND ----------

data1 = [
    (1,"ABC","1TWO3",999),
    (2,"POP","3FOUR5",888),
    (2,"POP","3FOUR5",888),
    (3,"BNB","3ONE2",666)
]
columns1 = ["Sno","Col1","Col2","Code"]
dupCheckdf1 = spark.createDataFrame(data1, columns1)
dupCheckdf1.show()

data2 = [
    (1,"ABC","1TWO3",999),
    (2,"POP","3FOUR5",888),
    (3,"POP","3FOUR5",111),
    (3,"BNB","3ONE2",666)
]
columns2 = ["Sno","Col1","Col2","Code"]
dupCheckdf2 = spark.createDataFrame(data2, columns2)
dupCheckdf2.show()

# COMMAND ----------

# complete dataframe
dupCheckdf1.distinct().show()

# COMMAND ----------

# specific columns 
dupCheckdf2.select(["Col1","Col2"]).distinct().show()

# COMMAND ----------

dupCheckdf2.dropDuplicates(["col1","col2"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 16. Remove rows with null values

# COMMAND ----------

# removes rows with any null value column
unpivotDF.na.drop().show()

# COMMAND ----------

# remove rows only when all the columns in that row has null value
unpivotDF.na.drop(how="all").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 17. Create a new column based on calculations from existing columns

# COMMAND ----------

salesDF.show()

# COMMAND ----------

from pyspark.sql.functions import lit
total_salesDF = salesDF.withColumn(
    "Total Sales", salesDF["Unit Price"] * salesDF["Quantity"]
)
total_salesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 18. Convert a PySpark Dataframe to a sparse matrix

# COMMAND ----------

# MAGIC %md
# MAGIC ### 19. Convert a PySpark Dataframe to a dense matrix

# COMMAND ----------

# MAGIC %md 
# MAGIC ### 20. Create a new PySpark Dataframe from sampling rows from an existing PySpark Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Logic
# MAGIC We are creating a new PySpark DataFrame by sampling 70% of the rows from an existing PySpark DataFrame called salesDF. <br/> We are using the withReplacement=False parameter to ensure that we do not sample the same row more than once. <br/> We are also using the seed=42 parameter to ensure that we get the same random sample every time we run this code.

# COMMAND ----------

from pyspark.sql.functions import rand

samplesalesDF = salesDF.sample(withReplacement=False, fraction=0.7, seed=42).orderBy(rand())
samplesalesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Can be done using pandas as well.

# COMMAND ----------

pdsalesDF = salesDF.toPandas()

samp_row_salesDF = pdsalesDF.sample(axis="rows", frac=0.50)

df = spark.createDataFrame(samp_row_salesDF)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 21. Create a new PySpark Dataframe from sampling columns from an existing PySpark Dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Steps:
# MAGIC 1.  Convert to pandas dataframe.
# MAGIC 2.  Apply sample(axis=) method.
# MAGIC 3.  Again, convert to spark dataframe.

# COMMAND ----------

pdsalesDF = salesDF.toPandas()

samp_col_salesDF = pdsalesDF.sample(axis="columns")

df = spark.createDataFrame(samp_col_salesDF)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 22. Create a new PySpark Dataframe by sampling both rows and columns from an existing PySpark Dataframe

# COMMAND ----------

pdsalesDF = salesDF.toPandas()

samp_row_col_salesDF = pdsalesDF.sample(axis="columns",n=3).sample(axis="rows")

df = spark.createDataFrame(samp_row_col_salesDF)
df.show()

# COMMAND ----------

salesDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 23. Save a PySpark Dataframe to a file or database.

# COMMAND ----------

salesDF.write.partitionBy("Item type").mode("overwrite").parquet("/FileStore/tables/sales.parquet")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 24. Filter a PySpark Dataframe to only include rows where a certain column value is above a certain threshold.

# COMMAND ----------

from pyspark.sql.functions import col
salesDF.filter(salesDF["Unit Price"] > 50).show()
salesDF.filter(col("Unit Price") > 50).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 25. Group a PySpark Dataframe by a certain column and calculate the mean value of another column.

# COMMAND ----------

salesDF.show()

# COMMAND ----------

from pyspark.sql.functions import avg
salesDF.groupBy("Item Type").agg(avg("Unit Price")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 26. Replace all null values in a PySpark Dataframe with a specific value.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Prep

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

data = [("Alice", 25, None), ("Bob", None, 80), (None, 30, 75)]
schema = StructType([StructField("Name", StringType(), True), StructField("Age", IntegerType(), True), StructField("Score", IntegerType(), True)])
df = spark.createDataFrame(data, schema)
df.show()

# COMMAND ----------

df.na.fill(0).na.fill("NA").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 27. Join two PySpark Dataframes on a common column.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data Prep

# COMMAND ----------

# list  of employee data
data = [["1", "sravan", "KPMG"],
        ["2", "ojaswi", "KPMG"], 
        ["3", "rohith", "company 2"],
        ["4", "sridevi", "KPMG"], 
        ["5", "bobby", "KPMG"]]
  
# specify column names
columns = ['ID', 'NAME', 'Company']

forJoindf1 = spark.createDataFrame(data, columns)
forJoindf1.show()

# list  of employee data
data1 = [["1", "45000", "IT"],
         ["2", "145000", "Manager"],
         ["6", "45000", "HR"],
         ["5", "34000", "Sales"]]
  
# specify column names
columns = ['ID', 'salary', 'department']
  
# creating a dataframe from the lists of data
forJoindf2 = spark.createDataFrame(data1, columns)
forJoindf2.show()

  

# COMMAND ----------

joinedDF = forJoindf1.join(forJoindf2,"ID")
joinedDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 28. Convert a PySpark Dataframe column from one data type to another.

# COMMAND ----------

from pyspark.sql.functions import col

salesDF = salesDF.withColumn("OrderId", col("OrderId").cast("Integer"))
salesDF.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 29. Aggregate a PySpark Dataframe by grouping on multiple columns and applying a function to each group.

# COMMAND ----------

unpivotDF.groupBy("Product","Country").agg(sum(col("Total Amount")).alias("Total by each Product and Country")).dropna().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 30. Convert a PySpark Dataframe from wide format to long format.

# COMMAND ----------

pivotDF.unpivot("Product",["Canada","China","Mexico","USA"],"Country","Total Amount").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 31. Pivot a PySpark Dataframe to create a new column for each unique value in a specified column.

# COMMAND ----------

forPivot_df.groupBy("Country").pivot("Country").sum("Amount").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 32. Create new columns in a PySpark Dataframe based on transformations of existing columns.

# COMMAND ----------

from pyspark.sql.functions import concat,substring
salesDF.withColumn("Item Code", concat(salesDF["OrderId"],substring(salesDF["Item Type"],1,3))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 33. Drop duplicates from a PySpark Dataframe based on certain columns.

# COMMAND ----------

dupCheckdf2.dropDuplicates(["col1","col2"]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 34. Remove rows with missing values in certain columns.

# COMMAND ----------

unpivotDF.dropna().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 35. Create a rolling average of a certain column over a specified window size.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import avg

salesWindow = Window.orderBy("Item Type","Item Name")

total_salesDF.withColumn("Rolling Average", sum("Total Sales").over(salesWindow)).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### 36. Create a new column based on a conditional statement applied to other columns.

# COMMAND ----------

salesDF.withColumn("Price Tier", 
              when(col("Unit Price") >= 100, "Premium")
              .when(col("Unit Price") > 60, "Mid")
              .otherwise("Low")
              ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 37. Perform a left outer join on two PySpark Dataframes using a common column.

# COMMAND ----------

forJoindf1.join(forJoindf2,"Id","leftouter").show()
