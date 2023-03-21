# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Q1. Question: 
# MAGIC 
# MAGIC # - Input: 
# MAGIC     - bookId(Int)                     SalesOfBookByStore (Array(Int))
# MAGIC         1                               Array(30, 5, 20, 10) 
# MAGIC         2                               Array(20, 10) 
# MAGIC         3                               Array(60) 
# MAGIC         . 
# MAGIC         . 
# MAGIC         . 
# MAGIC         n 
# MAGIC # - output 
# MAGIC         1 65 
# MAGIC 
# MAGIC         2 30 
# MAGIC         3 60 
# MAGIC         . 
# MAGIC         . 
# MAGIC         . 
# MAGIC         n 

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql._
# MAGIC import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}
# MAGIC 
# MAGIC val  book_schema =StructType(
# MAGIC     List(
# MAGIC       StructField("bookId",IntegerType,true),
# MAGIC       StructField("SalesOfBookByStore",ArrayType(IntegerType),true)
# MAGIC     ))
# MAGIC 
# MAGIC import org.apache.spark.sql.Row
# MAGIC import scala.collection.JavaConversions._//JavaConverters._
# MAGIC import org.apache.spark.sql.functions.explode
# MAGIC                 
# MAGIC val rowData= Seq(Row(1,Seq(30,5,20,10)), 
# MAGIC                Row(2,Seq(20,10)), 
# MAGIC                Row(3,Seq(60))
# MAGIC                 )
# MAGIC 
# MAGIC import spark.implicits._
# MAGIC 
# MAGIC val df = spark.createDataFrame(rowData,book_schema)
# MAGIC 
# MAGIC val result = df.select($"bookId",explode($"SalesOfBookByStore") as "SalesOfBookByStore")
# MAGIC val final_result = result.groupBy("bookId").sum("SalesOfBookByStore")
# MAGIC 
# MAGIC final_result.show()

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC from pyspark.sql.types import StringType, StructField, StructType, ArrayType, IntegerType
# MAGIC from pyspark.sql.functions import explode
# MAGIC from pyspark.sql import Row
# MAGIC 
# MAGIC book_schema =StructType(
# MAGIC     [
# MAGIC       StructField("bookId",IntegerType(),True),
# MAGIC       StructField("SalesOfBookByStore",ArrayType(IntegerType()),True)
# MAGIC     ])
# MAGIC 
# MAGIC #import org.apache.spark.sql.Row
# MAGIC #import scala.collection.JavaConversions._//JavaConverters._
# MAGIC #import org.apache.spark.sql.functions.explode
# MAGIC        
# MAGIC 
# MAGIC rowData= [Row(1,[30,5,20,10]), 
# MAGIC                Row(2,[20,10]), 
# MAGIC                Row(3,[60])
# MAGIC             ]
# MAGIC 
# MAGIC #import spark.implicits._
# MAGIC 
# MAGIC df = spark.createDataFrame(rowData,book_schema)
# MAGIC 
# MAGIC df1 = df.select("bookId",explode('SalesOfBookByStore') )
# MAGIC 
# MAGIC result =  df1.selectExpr("bookId","col as SalesOfBookByStore")
# MAGIC final_result = result.groupBy("bookId").sum("SalesOfBookByStore")
# MAGIC 
# MAGIC final_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Q4. Given 
# MAGIC 
# MAGIC - s = "the sky is blue" 
# MAGIC 
# MAGIC - return "blue is sky the" 

# COMMAND ----------

# MAGIC %scala
# MAGIC val s = "the sky is blue" 
# MAGIC val split = s.split(" ")
# MAGIC var sum = ""
# MAGIC val result = split.reverse.fold("")((x,y) => x + y +' ')
# MAGIC 
# MAGIC print(result) 
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Q5.  
# MAGIC # - Given: 
# MAGIC     date  store product     sales_val   sales_vol sales_discount_val 
# MAGIC     date  store product     stock_val   stock_vol 
# MAGIC     date  store product     order_val   order_vol 
# MAGIC  
# MAGIC # - Get below, without using Joins 
# MAGIC 
# MAGIC     date store product sales_val sales_vol sales_discount_val date store product stock_val stock_vol date store product order_val order_vol 

# COMMAND ----------

# MAGIC %md
# MAGIC # WAP to find out top movies with following conditions
# MAGIC   1) at least 100 people should have rated for that movie.
# MAGIC   2)  average rating > 4.5
# MAGIC # DataSet1
# MAGIC     user_id, movie_id, ratings, timestamp(hipoc time, no of seconds after 1st jan 1970)
# MAGIC     1::1193::5::978300760
# MAGIC     1::661::3::978302109
# MAGIC     1::914::3::978301968
# MAGIC     1::3408::4::978300275
# MAGIC     1::2355::5::978824291
# MAGIC     1::1197::3::978302268
# MAGIC     1::1287::5::978302039
# MAGIC     1::2804::5::978300719
# MAGIC     1::594::4::978302268
# MAGIC # DataSet 2
# MAGIC      user_id, movie_name, movie_type
# MAGIC     1::Toy Story (1995):: Animation | Children's | Comedy
# MAGIC     2::Jumanji (1995):: Adventure | Children's | Fantasy
# MAGIC     3::Grumpier Old Men (1995):: Comedy | Romance 4 4::Waiting to Exhale (1995):: Comedy | Drama
# MAGIC     5::Father of the Bride Part II (1995):: Comedy
# MAGIC     6::Heat (1995):: Action | Crime | Thriller
# MAGIC     7::Sabrina (1995):: Comedy | Romance
# MAGIC     8::Tom and Huck (1995):: Adventure | Children's
# MAGIC     9::Sudden Death (1995)::Action
# MAGIC     10::GoldenEye (1995):: Action | Adventure | Thriller
# MAGIC     11::American President, The (1995):: Comedy | Drama | Romance
# MAGIC     12::Dracula: Dead and Loving It (1995):: Comedy
# MAGIC     13::Balto (1995):: Animation | Children's
# MAGIC     14::Nixon (1995):: Drama
# MAGIC     15::Cutthroat Island (1995):: Action | Adventure | Romance

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Question:

# COMMAND ----------


