import pymongo
import json
import os
import numpy as np
import pandas as pd
import sys
from linkedInutility import get_db_connection


import pymongo
import logging
import pyspark
import pyspark.sql
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import SQLContext
def company_master_data(cfg):
  print("Creating Database connection for master")
 #db_connection = get_db_connection(cfg)
 #dblist=db_connection.list_database_names()
  conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster("local").setAppName("My First Spark Job").setAll([("spark.driver.memory", "40g"),("spark.executor.memory", "50g")])
  print("hi")
  #spark = SparkSession.builder.getOrCreate();
 

  #spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/LinkedInJob.company_master").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/LinkedInJob.company_master").getOrCreate()
  print("h0")
  sc = SparkContext(conf=conf)
  sqlC = SQLContext(sc)
  print("HELLO")
  mongo_ip = "mongodb://localhost:27017/LinkedInJob."
  #df = sqlC.read.format("mongo").option("uri","mongodb://localhost:27017/LinkedInJob.Staging_raw_data").load()
  master = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_ip+"Staging_raw_data").load()
  master.createOrReplaceTempView("data")
  df =sqlC.sql("SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS cmp_id,company as cmp_name FROM data")
  
  #df = spark.read.format("csv").option("header","true").load("data.csv")
  df = df.withColumn('cmp_ceo_name', F.lit(None).cast('string'))
  df = df.withColumn('cmp_head_office', F.lit(None).cast('string'))
  df = df.withColumn('cmp_current_openings', F.lit(None).cast('string'))
  df = df.withColumn('cmp_date_created', F.lit(None).cast('string'))
  df = df.withColumn('cmp_user_created', F.lit(None).cast('string'))
  print(df.head(1))
  db_connection = get_db_connection(cfg)
  dblist = db_connection.list_database_names()
  if "LinkedInJob" in dblist:
    mydb = db_connection["LinkedInJob"]
    db_cm = mydb["company_master"]
    #data_json = json.loads(df.toJSON().collect())

  #results = df.toJSON().map(lambda j: json.loads(j))
  #results = df.toJSON().collect()
  # print(results.collect())\
  #db_cm.insert_many(data_json)
  print(type(df))
  #print(json.dumps(results))
  #data_json = json.loads(json.dumps(results).to_json(orient='records'))
  # 
  #df.write.format('com.mongodb.spark.sql.DefaultSource').option( "uri", "mongodb://MinerLeague:MinerLeague@127.0.0.1:27017/LinkedInJob.company_master").save()
  data = df.toPandas()
  data_json = json.loads(data.to_json(orient='records'))
  db_cm.insert_many(data_json)
  #results = df.toJSON().map(lambda j: json.loads(j)).collect()
  #print(results)
  #db_cm.insert_many(results)
  #db_cm.insert_one(results[0])
  sc.stop();
  #mycol = mydb["Staging"]
  print("The data is inserted into the database")


