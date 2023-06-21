# Databricks notebook source
# MAGIC %python
# MAGIC # ALL_NOTEBOOKS
# MAGIC # ****************************************************************************
# MAGIC # Utility method to count & print the number of records in each partition.
# MAGIC # ****************************************************************************
# MAGIC
# MAGIC def printRecordsPerPartition(df):
# MAGIC   def countInPartition(iterator): yield __builtin__.sum(1 for _ in iterator)
# MAGIC   results = (df.rdd                   # Convert to an RDD
# MAGIC     .mapPartitions(countInPartition)  # For each partition, count
# MAGIC     .collect()                        # Return the counts to the driver
# MAGIC   )
# MAGIC   
# MAGIC   print("Per-Partition Counts")
# MAGIC   i = 0
# MAGIC   for result in results: 
# MAGIC     i = i + 1
# MAGIC     print("#{}: {:,}".format(i, result))
# MAGIC   
# MAGIC # ****************************************************************************
# MAGIC # Utility to count the number of files in and size of a directory
# MAGIC # ****************************************************************************
# MAGIC
# MAGIC def computeFileStats(path):
# MAGIC   bytes = 0
# MAGIC   count = 0
# MAGIC
# MAGIC   files = dbutils.fs.ls(path)
# MAGIC   
# MAGIC   while (len(files) > 0):
# MAGIC     fileInfo = files.pop(0)
# MAGIC     if (fileInfo.isDir() == False):               # isDir() is a method on the fileInfo object
# MAGIC       count += 1
# MAGIC       bytes += fileInfo.size                      # size is a parameter on the fileInfo object
# MAGIC     else:
# MAGIC       files.extend(dbutils.fs.ls(fileInfo.path))  # append multiple object to files
# MAGIC       
# MAGIC   return (count, bytes)
# MAGIC
# MAGIC # ****************************************************************************
# MAGIC # Utility method to cache a table with a specific name
# MAGIC # ****************************************************************************
# MAGIC
# MAGIC def cacheAs(df, name, level = "MEMORY-ONLY"):
# MAGIC   from pyspark.sql.utils import AnalysisException
# MAGIC   if level != "MEMORY-ONLY":
# MAGIC     print("WARNING: The PySpark API currently does not allow specification of the storage level - using MEMORY-ONLY")  
# MAGIC     
# MAGIC   try: spark.catalog.uncacheTable(name)
# MAGIC   except AnalysisException: None
# MAGIC   
# MAGIC   df.createOrReplaceTempView(name)
# MAGIC   spark.catalog.cacheTable(name)
# MAGIC   
# MAGIC   return df
# MAGIC
# MAGIC
# MAGIC # ****************************************************************************
# MAGIC # Simplified benchmark of count()
# MAGIC # ****************************************************************************
# MAGIC
# MAGIC def benchmarkCount(func):
# MAGIC   import time
# MAGIC   start = float(time.time() * 1000)                    # Start the clock
# MAGIC   df = func()
# MAGIC   total = df.count()                                   # Count the records
# MAGIC   duration = float(time.time() * 1000) - start         # Stop the clock
# MAGIC   return (df, total, duration)
# MAGIC
# MAGIC # ****************************************************************************
# MAGIC # Utility methods to terminate streams
# MAGIC # ****************************************************************************
# MAGIC
# MAGIC def getActiveStreams():
# MAGIC   try:
# MAGIC     return spark.streams.active
# MAGIC   except:
# MAGIC     # In extream cases, this funtion may throw an ignorable error.
# MAGIC     print("Unable to iterate over all active streams - using an empty set instead.")
# MAGIC     return []
# MAGIC
# MAGIC def stopStream(s):
# MAGIC   try:
# MAGIC     print("Stopping the stream {}.".format(s.name))
# MAGIC     s.stop()
# MAGIC     print("The stream {} was stopped.".format(s.name))
# MAGIC   except:
# MAGIC     # In extream cases, this funtion may throw an ignorable error.
# MAGIC     print("An [ignorable] error has occured while stoping the stream.")
# MAGIC
# MAGIC def stopAllStreams():
# MAGIC   streams = getActiveStreams()
# MAGIC   while len(streams) > 0:
# MAGIC     stopStream(streams[0])
# MAGIC     streams = getActiveStreams()
# MAGIC     
# MAGIC # ****************************************************************************
# MAGIC # Utility method to wait until the stream is read
# MAGIC # ****************************************************************************
# MAGIC
# MAGIC def untilStreamIsReady(name, progressions=3):
# MAGIC   import time
# MAGIC   queries = list(filter(lambda query: query.name == name or query.name == name + "_p", getActiveStreams()))
# MAGIC
# MAGIC   while (len(queries) == 0 or len(queries[0].recentProgress) < progressions):
# MAGIC     time.sleep(5) # Give it a couple of seconds
# MAGIC     queries = list(filter(lambda query: query.name == name or query.name == name + "_p", getActiveStreams()))
# MAGIC
# MAGIC   print("The stream {} is active and ready.".format(name))

# COMMAND ----------

# MAGIC %scala
# MAGIC // ALL_NOTEBOOKS
# MAGIC // ****************************************************************************
# MAGIC // Utility method to count & print the number of records in each partition.
# MAGIC // ****************************************************************************
# MAGIC
# MAGIC def printRecordsPerPartition(df:org.apache.spark.sql.Dataset[Row]):Unit = {
# MAGIC   // import org.apache.spark.sql.functions._
# MAGIC   val results = df.rdd                                   // Convert to an RDD
# MAGIC     .mapPartitions(it => Array(it.size).iterator, true)  // For each partition, count
# MAGIC     .collect()                                           // Return the counts to the driver
# MAGIC
# MAGIC   println("Per-Partition Counts")
# MAGIC   var i = 0
# MAGIC   for (r <- results) {
# MAGIC     i = i +1
# MAGIC     println("#%s: %,d".format(i,r))
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC // ****************************************************************************
# MAGIC // Utility to count the number of files in and size of a directory
# MAGIC // ****************************************************************************
# MAGIC
# MAGIC def computeFileStats(path:String):(Long,Long) = {
# MAGIC   var bytes = 0L
# MAGIC   var count = 0L
# MAGIC
# MAGIC   import scala.collection.mutable.ArrayBuffer
# MAGIC   var files=ArrayBuffer(dbutils.fs.ls(path):_ *)
# MAGIC
# MAGIC   while (files.isEmpty == false) {
# MAGIC     val fileInfo = files.remove(0)
# MAGIC     if (fileInfo.isDir == false) {
# MAGIC       count += 1
# MAGIC       bytes += fileInfo.size
# MAGIC     } else {
# MAGIC       files.append(dbutils.fs.ls(fileInfo.path):_ *)
# MAGIC     }
# MAGIC   }
# MAGIC   (count, bytes)
# MAGIC }
# MAGIC
# MAGIC // ****************************************************************************
# MAGIC // Utility method to cache a table with a specific name
# MAGIC // ****************************************************************************
# MAGIC
# MAGIC def cacheAs(df:org.apache.spark.sql.DataFrame, name:String, level:org.apache.spark.storage.StorageLevel):org.apache.spark.sql.DataFrame = {
# MAGIC   try spark.catalog.uncacheTable(name)
# MAGIC   catch { case _: org.apache.spark.sql.AnalysisException => () }
# MAGIC   
# MAGIC   df.createOrReplaceTempView(name)
# MAGIC   spark.catalog.cacheTable(name, level)
# MAGIC   return df
# MAGIC }
# MAGIC
# MAGIC // ****************************************************************************
# MAGIC // Simplified benchmark of count()
# MAGIC // ****************************************************************************
# MAGIC
# MAGIC def benchmarkCount(func:() => org.apache.spark.sql.DataFrame):(org.apache.spark.sql.DataFrame, Long, Long) = {
# MAGIC   val start = System.currentTimeMillis            // Start the clock
# MAGIC   val df = func()                                 // Get our lambda
# MAGIC   val total = df.count()                          // Count the records
# MAGIC   val duration = System.currentTimeMillis - start // Stop the clock
# MAGIC   (df, total, duration)
# MAGIC }
# MAGIC
# MAGIC // ****************************************************************************
# MAGIC // Benchmarking and cache tracking tool
# MAGIC // ****************************************************************************
# MAGIC
# MAGIC case class JobResults[T](runtime:Long, duration:Long, cacheSize:Long, maxCacheBefore:Long, remCacheBefore:Long, maxCacheAfter:Long, remCacheAfter:Long, result:T) {
# MAGIC   def printTime():Unit = {
# MAGIC     if (runtime < 1000)                 println(f"Runtime:  ${runtime}%,d ms")
# MAGIC     else if (runtime < 60 * 1000)       println(f"Runtime:  ${runtime/1000.0}%,.2f sec")
# MAGIC     else if (runtime < 60 * 60 * 1000)  println(f"Runtime:  ${runtime/1000.0/60.0}%,.2f min")
# MAGIC     else                                println(f"Runtime:  ${runtime/1000.0/60.0/60.0}%,.2f hr")
# MAGIC     
# MAGIC     if (duration < 1000)                println(f"All Jobs: ${duration}%,d ms")
# MAGIC     else if (duration < 60 * 1000)      println(f"All Jobs: ${duration/1000.0}%,.2f sec")
# MAGIC     else if (duration < 60 * 60 * 1000) println(f"All Jobs: ${duration/1000.0/60.0}%,.2f min")
# MAGIC     else                                println(f"Job Dur: ${duration/1000.0/60.0/60.0}%,.2f hr")
# MAGIC   }
# MAGIC   def printCache():Unit = {
# MAGIC     if (Math.abs(cacheSize) < 1024)                    println(f"Cached:   ${cacheSize}%,d bytes")
# MAGIC     else if (Math.abs(cacheSize) < 1024 * 1024)        println(f"Cached:   ${cacheSize/1024.0}%,.3f KB")
# MAGIC     else if (Math.abs(cacheSize) < 1024 * 1024 * 1024) println(f"Cached:   ${cacheSize/1024.0/1024.0}%,.3f MB")
# MAGIC     else                                               println(f"Cached:   ${cacheSize/1024.0/1024.0/1024.0}%,.3f GB")
# MAGIC     
# MAGIC     println(f"Before:   ${remCacheBefore / 1024.0 / 1024.0}%,.3f / ${maxCacheBefore / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheBefore/maxCacheBefore}%.2f%%")
# MAGIC     println(f"After:    ${remCacheAfter / 1024.0 / 1024.0}%,.3f / ${maxCacheAfter / 1024.0 / 1024.0}%,.3f MB / ${100.0*remCacheAfter/maxCacheAfter}%.2f%%")
# MAGIC   }
# MAGIC   def print():Unit = {
# MAGIC     printTime()
# MAGIC     printCache()
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC case class Node(driver:Boolean, executor:Boolean, address:String, maximum:Long, available:Long) {
# MAGIC   def this(address:String, maximum:Long, available:Long) = this(address.contains("-"), !address.contains("-"), address, maximum, available)
# MAGIC }
# MAGIC
# MAGIC class Tracker() extends org.apache.spark.scheduler.SparkListener() {
# MAGIC   
# MAGIC   sc.addSparkListener(this)
# MAGIC   
# MAGIC   val jobStarts = scala.collection.mutable.Map[Int,Long]()
# MAGIC   val jobEnds = scala.collection.mutable.Map[Int,Long]()
# MAGIC   
# MAGIC   def track[T](func:() => T):JobResults[T] = {
# MAGIC     jobEnds.clear()
# MAGIC     jobStarts.clear()
# MAGIC
# MAGIC     val executorsBefore = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
# MAGIC     val maxCacheBefore = executorsBefore.map(_.maximum).sum
# MAGIC     val remCacheBefore = executorsBefore.map(_.available).sum
# MAGIC     
# MAGIC     val start = System.currentTimeMillis()
# MAGIC     val result = func()
# MAGIC     val runtime = System.currentTimeMillis() - start
# MAGIC     
# MAGIC     Thread.sleep(1000) // give it a second to catch up
# MAGIC
# MAGIC     val executorsAfter = sc.getExecutorMemoryStatus.map(x => new Node(x._1, x._2._1, x._2._2)).filter(_.executor)
# MAGIC     val maxCacheAfter = executorsAfter.map(_.maximum).sum
# MAGIC     val remCacheAfter = executorsAfter.map(_.available).sum
# MAGIC
# MAGIC     var duration = 0L
# MAGIC     
# MAGIC     for ((jobId, startAt) <- jobStarts) {
# MAGIC       assert(jobEnds.keySet.exists(_ == jobId), s"A conclusion for Job ID $jobId was not found.") 
# MAGIC       duration += jobEnds(jobId) - startAt
# MAGIC     }
# MAGIC     JobResults(runtime, duration, remCacheBefore-remCacheAfter, maxCacheBefore, remCacheBefore, maxCacheAfter, remCacheAfter, result)
# MAGIC   }
# MAGIC   override def onJobStart(jobStart: org.apache.spark.scheduler.SparkListenerJobStart):Unit = jobStarts.put(jobStart.jobId, jobStart.time)
# MAGIC   override def onJobEnd(jobEnd: org.apache.spark.scheduler.SparkListenerJobEnd): Unit = jobEnds.put(jobEnd.jobId, jobEnd.time)
# MAGIC }
# MAGIC
# MAGIC val tracker = new Tracker()
# MAGIC
# MAGIC // ****************************************************************************
# MAGIC // Utility methods to terminate streams
# MAGIC // ****************************************************************************
# MAGIC
# MAGIC def getActiveStreams():Seq[org.apache.spark.sql.streaming.StreamingQuery] = {
# MAGIC   return try {
# MAGIC     spark.streams.active
# MAGIC   } catch {
# MAGIC     case e:Throwable => {
# MAGIC       // In extream cases, this funtion may throw an ignorable error.
# MAGIC       println("Unable to iterate over all active streams - using an empty set instead.")
# MAGIC       Seq[org.apache.spark.sql.streaming.StreamingQuery]()
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def stopStream(s:org.apache.spark.sql.streaming.StreamingQuery):Unit = {
# MAGIC   try {
# MAGIC     s.stop()
# MAGIC   } catch {
# MAGIC     case e:Throwable => {
# MAGIC       // In extream cases, this funtion may throw an ignorable error.
# MAGIC       println(s"An [ignorable] error has occured while stoping the stream.")
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def stopAllStreams():Unit = {
# MAGIC   var streams = getActiveStreams()
# MAGIC   while (streams.length > 0) {
# MAGIC     stopStream(streams(0))
# MAGIC     streams = getActiveStreams()
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC // ****************************************************************************
# MAGIC // Utility method to wait until the stream is read
# MAGIC // ****************************************************************************
# MAGIC
# MAGIC def untilStreamIsReady(name:String, progressions:Int = 3):Unit = {
# MAGIC   var queries = getActiveStreams().filter(s => s.name == name || s.name == name + "_s")
# MAGIC   
# MAGIC   while (queries.length == 0 || queries(0).recentProgress.length < progressions) {
# MAGIC     Thread.sleep(5*1000) // Give it a couple of seconds
# MAGIC     queries = getActiveStreams().filter(s => s.name == name || s.name == name + "_s")
# MAGIC   }
# MAGIC   println("The stream %s is active and ready.".format(name))
# MAGIC }
# MAGIC
# MAGIC displayHTML("Finished setting up utiltity methods...")

# COMMAND ----------

# MAGIC %scala
# MAGIC // ALL_NOTEBOOKS
# MAGIC //**********************************
# MAGIC // CREATE THE MOUNTS
# MAGIC //**********************************
# MAGIC
# MAGIC def cloudAndRegion(cloudAndRegionOverride:Tuple2[String,String]) = {
# MAGIC   import com.databricks.backend.common.util.Project
# MAGIC   import com.databricks.conf.trusted.ProjectConf
# MAGIC   import com.databricks.backend.daemon.driver.DriverConf
# MAGIC   
# MAGIC   if (cloudAndRegionOverride != null) {
# MAGIC     // This override mechanisim is provided for testing purposes
# MAGIC     cloudAndRegionOverride
# MAGIC   } else {
# MAGIC     val conf = new DriverConf(ProjectConf.loadLocalConfig(Project.Driver))
# MAGIC     (conf.cloudProvider.getOrElse("Unknown"), conf.region)
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC // These keys are read-only so they're okay to have here
# MAGIC val awsAccessKey = "AKIAJBRYNXGHORDHZB4A"
# MAGIC val awsSecretKey = "a0BzE1bSegfydr3%2FGE3LSPM6uIV5A4hOUfpH8aFF"
# MAGIC val awsAuth = s"${awsAccessKey}:${awsSecretKey}"
# MAGIC
# MAGIC def AWS_REGION_MAP() = {
# MAGIC   Map(
# MAGIC     "ap-northeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-1/common", Map[String,String]()),
# MAGIC     "ap-northeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-northeast-2/common", Map[String,String]()),
# MAGIC     "ap-south-1"     -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-south-1/common", Map[String,String]()),
# MAGIC     "ap-southeast-1" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-1/common", Map[String,String]()),
# MAGIC     "ap-southeast-2" -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ap-southeast-2/common", Map[String,String]()),
# MAGIC     "ca-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-ca-central-1/common", Map[String,String]()),
# MAGIC     "eu-central-1"   -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
# MAGIC     "eu-west-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-1/common", Map[String,String]()),
# MAGIC     "eu-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-west-2/common", Map[String,String]()),
# MAGIC     "eu-west-3"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-eu-central-1/common", Map[String,String]()),
# MAGIC     "sa-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-sa-east-1/common", Map[String,String]()),
# MAGIC     "us-east-1"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-1/common", Map[String,String]()),
# MAGIC     "us-east-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training-us-east-2/common", Map[String,String]()),
# MAGIC     "us-west-2"      -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]()),
# MAGIC     "_default"       -> (s"s3a://${awsAccessKey}:${awsSecretKey}@databricks-corp-training/common", Map[String,String]())
# MAGIC   )
# MAGIC }
# MAGIC
# MAGIC def getAwsMapping(region:String):(String,Map[String,String]) = {
# MAGIC   AWS_REGION_MAP().getOrElse(region, AWS_REGION_MAP()("_default"))
# MAGIC }
# MAGIC
# MAGIC def MSA_REGION_MAP() = {
# MAGIC   Map(
# MAGIC     "australiacentral"    -> ("dbtrainaustraliasoutheas", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=fPyXr23bQzVjXSn%2BLw1ZeY0dL7cb646I5VlRpCthqvU%3D"),
# MAGIC     "australiacentral2"   -> ("dbtrainaustraliasoutheas", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=fPyXr23bQzVjXSn%2BLw1ZeY0dL7cb646I5VlRpCthqvU%3D"),
# MAGIC     "australiaeast"       -> ("dbtrainaustraliaeast", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=0xBiPX4l/qUj2My2ODgljhsM68kArujXf/seD05lg%2Bo%3D"),
# MAGIC     "australiasoutheast"  -> ("dbtrainaustraliasoutheas", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=fPyXr23bQzVjXSn%2BLw1ZeY0dL7cb646I5VlRpCthqvU%3D"),
# MAGIC     "canadacentral"       -> ("dbtraincanadacentral", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=CVZs6YRXcr%2BZBMVF5tv%2Bj/asWa1irGiejyoOzP4HRXI%3D"),
# MAGIC     "canadaeast"          -> ("dbtraincanadaeast", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=pT8Qv3n2UKVR3%2B9i8bBDg99RgjCjoJZ7SmuMK5XgB9A%3D"),
# MAGIC     "centralindia"        -> ("dbtraincentralindia", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=YMqGZo/LzF0fJ5Bm3TCbMKWct3BJ4Js%2Bf4Mol20v9mc%3D"),
# MAGIC     "centralus"           -> ("dbtraincentralus", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=/5PhUn3WYrRKCmaPG2fwLHhcvmIqI7qwziE9OAFOSDk%3D"),
# MAGIC     "eastasia"            -> ("dbtraineastasia", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=8/pExGnKcY643rtx38hu6sZh1FF6FdgT6YTZ2Kx/1VE%3D"),
# MAGIC     "eastus"              -> ("dbtraineastus", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=QTrPlRuZ3wTkvp7HV%2Bh25FFcCvW8cMO5Sb1nywRUc48%3D"),
# MAGIC     "eastus2"             -> ("dbtraineastus2", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=sQwM/HT8HyYeZZcZ5cKyvGnXtmsGQSSZ4Yjt20gDiSA%3D"),
# MAGIC     "japaneast"           -> ("dbtrainjapaneast", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=oU%2BlhuVCRpO85hzojDgjD3TkthivTnwc91y1%2BQhprmk%3D"),
# MAGIC     "japanwest"           -> ("dbtrainjapanwest", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=qBKKJrnjpLis3l1lvSZWq5VtiVoIYyzjWmuRl%2BkIHJI%3D"),
# MAGIC     "northcentralus"      -> ("dbtrainnorthcentralus", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=94JZeI3SMo31xKhaNeDtyQvglot68466pjvP%2BRZxjas%3D"),
# MAGIC     "northeurope"         -> ("dbtrainnortheurope", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=Q35Efn0sy3J%2BX/%2BRuOqeRpK4othT0cE6ULAOJjOsKFw%3D"),
# MAGIC     "southcentralus"      -> ("dbtrainsouthcentralus", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=plTYPAM8ccCeis/Imt/pgOaQQFVXQN9CVYL%2BLDII1l4%3D"),
# MAGIC     "southeastasia"       -> ("dbtrainstreams", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=zX9DlGYi8/4uF/P7psfrhZEgZ3o5xOmZ0h4rpGGstng%3D"),
# MAGIC     "southindia"          -> ("dbtrainsouthindia", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=AReesLBwoFxQTXuvWbyYbY7TWRxd9suVmfgsz5oHiGs%3D"),
# MAGIC     "uksouth"             -> ("dbtrainuksouth", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=u5xYin1SY0lWsetSLKkx9pBqc1Mj276r7Mw64kM2bgY%3D"),
# MAGIC     "ukwest"              -> ("dbtrainukwest", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=/6rO0tFfvy7hN6kFySw%2BIC3E9ZshrQNXtVENc2Ejtks%3D"),
# MAGIC     "westcentralus"       -> ("dbtrainwestcentralus", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=N02cFru0iC0IXBjVN8Q80W78cN6Ls2oL01nncNNqCQE%3D"),
# MAGIC     "westeurope"          -> ("dbtrainwesteurope", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=PFvsAOG2/q9/L6De1DtGfyxQq%2Bcc5fO0cGRlReNCCNc%3D"),
# MAGIC     "westindia"           -> ("dbtrainwestindia", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=9tpcc//xi09EQ3oWDXHIV0fZcOU80VndBe3bG572bnY%3D"),
# MAGIC     "westus"              -> ("dbtrainwestus", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=%2Bu%2BeHazZq9jWxihHEjcuOYtUsk2KAcLXlGJszELjYXw%3D"),
# MAGIC     "westus2"             -> ("dbtrainwestus2", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=4HCcsB0fgupf0cfO6057V8pmSTp6CzKMsnIaeBHleuY%3D"),
# MAGIC     "_default"            -> ("dbtrainwestus2", "?se=2030-02-01T00%3A00%3A00Z&sp=rl&sv=2022-11-02&ss=b&srt=sco&sig=4HCcsB0fgupf0cfO6057V8pmSTp6CzKMsnIaeBHleuY%3D")
# MAGIC   )
# MAGIC }
# MAGIC
# MAGIC def getAzureMapping(region:String):(String,Map[String,String]) = {
# MAGIC   val (account: String, sasKey: String) = MSA_REGION_MAP().getOrElse(region, MSA_REGION_MAP()("_default"))
# MAGIC   val blob = "training"
# MAGIC   val source = s"wasbs://$blob@$account.blob.core.windows.net/"
# MAGIC   val configMap = Map(
# MAGIC     s"fs.azure.sas.$blob.$account.blob.core.windows.net" -> sasKey
# MAGIC   )
# MAGIC   (source, configMap)
# MAGIC }
# MAGIC
# MAGIC def retryMount(source: String, mountPoint: String): Unit = {
# MAGIC   try { 
# MAGIC     // Mount with IAM roles instead of keys for PVC
# MAGIC     dbutils.fs.mount(source, mountPoint)
# MAGIC     dbutils.fs.ls(mountPoint) // Test read to confirm successful mount.
# MAGIC   } catch {
# MAGIC     case e: Exception => throw new RuntimeException(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}", e)
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def mount(source: String, extraConfigs:Map[String,String], mountPoint: String): Unit = {
# MAGIC   try {
# MAGIC     dbutils.fs.mount(source, mountPoint, extraConfigs=extraConfigs)
# MAGIC     dbutils.fs.ls(mountPoint) // Test read to confirm successful mount.
# MAGIC   } catch {
# MAGIC     case ioe: java.lang.IllegalArgumentException => retryMount(source, mountPoint)
# MAGIC     case e: Exception => throw new RuntimeException(s"*** ERROR: Unable to mount $mountPoint: ${e.getMessage}", e)
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def autoMount(fix:Boolean = false, failFast:Boolean = false, mountPoint:String = "/mnt/training", cloudAndRegionOverride:Tuple2[String,String] = null, outputFormat:String = "HTML"): Unit = {
# MAGIC   val (cloud, region) = cloudAndRegion(cloudAndRegionOverride)
# MAGIC   spark.conf.set("com.databricks.training.cloud.name", cloud)
# MAGIC   spark.conf.set("com.databricks.training.region.name", region)
# MAGIC   
# MAGIC   if (cloud=="AWS")  {
# MAGIC     val (source, extraConfigs) = getAwsMapping(region)
# MAGIC     val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC     renderOutput(outputFormat, s"Mounting course-specific datasets to <b>$mountPoint</b>...<br/>"+resultMsg)
# MAGIC     
# MAGIC   } else if (cloud=="Azure") {
# MAGIC     val (source, extraConfigs) = initAzureDataSource(region)
# MAGIC     val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC     renderOutput(outputFormat, s"Mounting course-specific datasets to <b>$mountPoint</b>...<br/>"+resultMsg)
# MAGIC     
# MAGIC   } else {
# MAGIC     val (source, extraConfigs) = ("s3a://databricks-corp-training/common", Map[String,String]())
# MAGIC     val resultMsg = mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC     renderOutput(outputFormat, s"Mounted course-specific datasets to <b>$mountPoint</b>.")
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC // Utility method used to control output during testing
# MAGIC def renderOutput(outputFormat:String, html:String) = {
# MAGIC   if (outputFormat == "HTML") {
# MAGIC     displayHTML(html)
# MAGIC   } else {
# MAGIC     val text = "| " + html.replaceAll("""<b>""", "")
# MAGIC                           .replaceAll("""<\/b>""", "")
# MAGIC                           .replaceAll("""<br>""", "\n")
# MAGIC                           .replaceAll("""<\/br>""", "\n")
# MAGIC                           .replaceAll("""<br\/>""", "\n")
# MAGIC                           .trim()
# MAGIC                           .replaceAll("\n", "\n| ")
# MAGIC     println(text)
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def initAzureDataSource(azureRegion:String):(String,Map[String,String]) = {
# MAGIC   val mapping = getAzureMapping(azureRegion)
# MAGIC   val (source, config) = mapping
# MAGIC   val (sasEntity, sasToken) = config.head
# MAGIC
# MAGIC   val datasource = "%s\t%s\t%s".format(source, sasEntity, sasToken)
# MAGIC   spark.conf.set("com.databricks.training.azure.datasource", datasource)
# MAGIC
# MAGIC   return mapping
# MAGIC }
# MAGIC
# MAGIC def mountSource(fix:Boolean, failFast:Boolean, mountPoint:String, source:String, extraConfigs:Map[String,String]): String = {
# MAGIC   val mntSource = source.replace(awsAuth+"@", "")
# MAGIC
# MAGIC   if (dbutils.fs.mounts().map(_.mountPoint).contains(mountPoint)) {
# MAGIC     val mount = dbutils.fs.mounts().filter(_.mountPoint == mountPoint).head
# MAGIC     if (mount.source == mntSource) {
# MAGIC       return s"""Datasets are already mounted to <b>$mountPoint</b>."""
# MAGIC       
# MAGIC     } else if (failFast) {
# MAGIC       throw new IllegalStateException(s"Expected $mntSource but found ${mount.source}")
# MAGIC       
# MAGIC     } else if (fix) {
# MAGIC       println(s"Unmounting existing datasets ($mountPoint from ${mount.source}).")
# MAGIC       dbutils.fs.unmount(mountPoint)
# MAGIC       mountSource(fix, failFast, mountPoint, source, extraConfigs)
# MAGIC       
# MAGIC     } else {
# MAGIC       return s"""<b style="color:red">Invalid Mounts!</b></br>
# MAGIC                       <ul>
# MAGIC                       <li>The training datasets you are using are from an unexpected source</li>
# MAGIC                       <li>Expected <b>$mntSource</b> but found <b>${mount.source}</b></li>
# MAGIC                       <li>Failure to address this issue may result in significant performance degradation. To address this issue:</li>
# MAGIC                       <ol>
# MAGIC                         <li>Insert a new cell after this one</li>
# MAGIC                         <li>In that new cell, run the command <code style="color:blue; font-weight:bold">%scala fixMounts()</code></li>
# MAGIC                         <li>Verify that the problem has been resolved.</li>
# MAGIC                       </ol>"""
# MAGIC     }
# MAGIC   } else {
# MAGIC     println(s"""Mounting course-specific datasets to $mountPoint...""")
# MAGIC     mount(source, extraConfigs, mountPoint)
# MAGIC     return s"""Mounted datasets to <b>$mountPoint</b> from <b>$mntSource<b>."""
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC def fixMounts(): Unit = {
# MAGIC   autoMount(true)
# MAGIC }
# MAGIC
# MAGIC autoMount(true)
# MAGIC
# MAGIC displayHTML("Datasets mounted and student environment set up")
