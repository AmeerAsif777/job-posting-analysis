import pymongo
import json
import os
import numpy as np
import pandas as pd
from linkedInutility import get_db_connection


import pymongo
import logging
import pyspark
import pyspark.sql
from pyspark import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import SQLContext
def company_master_data(cfg):
  print("Creating Database connection for master")
 #db_connection = get_db_connection(cfg)
 #dblist=db_connection.list_database_names()
  conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster(
   "local").setAppName("My First Spark Job").setAll([('spark.driver.memory', '40g'), ('spark.executor.memory', '50g')])
  sc = SparkContext(conf=conf)
  sqlC = SQLContext(sc)
  mongo_ip = "mongodb://localhost:27017/LinkedInJob."
  print(mongo_ip)
  master=sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://MinerLeague:MinerLeague@127.0.0.1:27017/LinkedInJob.Staging_raw_data").load()
  master.createOrReplaceTempView("data")
  df =sqlC.sql("SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS cmp_id,company as cmp_name FROM data")
  #df = df.withColumn('cmp_ceo_name', lit(None).cast(StringType()))
  df = df.withColumn('cmp_head_office', F.lit(None).cast('string'))
  df = df.withColumn('cmp_current_openings', F.lit(None).cast('string'))
  df = df.withColumn('cmp_date_created', F.lit(None).cast('string'))
  df = df.withColumn('cmp_user_created', F.lit(None).cast('string'))
  #print(df.head(100))
  db_connection = get_db_connection(cfg)
  dblist = db_connection.list_database_names()
  if "LinkedInJob" in dblist:
    mydb = db_connection["LinkedInJob"]
    db_cm = mydb["company_master"]
  # data_json = json.loads(df.toJSON().collect())
  print(type(df))
  #results = df.toJSON().map(lambda j: json.loads(j)).collect()
  data = df.toPandas()
  data_json = json.loads(data.to_json(orient='records'))
  db_cm.insert_many(data_json)

  # print(type(results))
  # db_cm.insert_many(results)
  sc.stop()
  #mycol = mydb["Staging"]
  print("The data is inserted into the database")

