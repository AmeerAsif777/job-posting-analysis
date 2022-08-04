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
from pyspark.sql.functions import when,col
from pyspark.sql import SQLContext

from pyspark.sql.functions import *
def job_type_and_seniority_data(cfg):
  print("Creating Database connection for seniority level summarization")
 #db_connection = get_db_connection(cfg)
 #dblist=db_connection.list_database_names()
  conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster(
   "local").setAppName("My First Spark Job").setAll([('spark.driver.memory', '40g'), ('spark.executor.memory', '50g')])
  sc = SparkContext(conf=conf)
  sqlC = SQLContext(sc)
  mongo_ip="mongodb://localhost:27017/LinkedInJob."
  print(mongo_ip)
  master=sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_ip+"job_master").load()
  master.createOrReplaceTempView("data")
  # job_id(pk)
  # cmp_id
  # job_post_id
  # job_title
  # job_type
  # job_function
  # job_level
  # job_country
  # job_location
  # job_apply_link
  # job_date_posted
  # job_date_created
  # job_user_created
  from pyspark.sql.functions import lit, StringType

  df =sqlC.sql("SELECT count(*) as job_count,job_type,job_seniority_level from data group by job_type,job_seniority_level")
  #df = df.withColumn('job_seniority_level', lit("Others").cast(StringType()))
  #df = df.withColumn('cmp_ceo_name', F.lit(None).cast(StringType()))
  # df = df.withColumn('cmp_head_office', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_current_openings', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_date_created', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_user_created', F.lit(None).cast('string'))
  #df=df.withColumn('job_seniority_level', when(job_seniority_level ==" ","Others"))
  #df = df.withColumn("job_seniority_level",when(col("job_seniority_level").isin('not_set', 'n/a', 'N/A', 'userid_not_set'),"Others").otherwise(col("job_seniority_level")))

  print(df.head(100))
  db_connection = get_db_connection(cfg)
  dblist = db_connection.list_database_names()
  if "LinkedInJob" in dblist:
    mydb = db_connection["LinkedInJob"]
    db_cm = mydb["job_type_and_seniority"]
  # data_json = json.loads(df.toJSON().collect())
  
  data = df.toPandas()
  results = json.loads(data.to_json(orient='records'))
  print(results)
  db_cm.delete_many({})
  db_cm.insert_many(results)
  sc.stop()
  #mycol = mydb["Staging"]
  print("The data is inserted into the database")

