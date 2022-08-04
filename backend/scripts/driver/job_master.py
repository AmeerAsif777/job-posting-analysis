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
def job_master_data(cfg):
  print("Creating Database connection for master")
  db_connection = get_db_connection(cfg)
  dblist=db_connection.list_database_names()
  conf = pyspark.SparkConf().set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1").setMaster("local").setAppName("My First Spark Job").setAll([('spark.driver.memory', '40g'), ('spark.executor.memory', '50g')])
  sc = SparkContext(conf=conf)
  sqlC = SQLContext(sc)
  print("hello")
  mongo_ip="mongodb://localhost:27017/LinkedInJob."
  print(mongo_ip)
  # spark = SparkSession.builder.appName("Spark SQL Query Dataframes").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/LinkedInJob.Staging_raw_data").getOrCreate ()
  # master=spark.read.format("mongo").option("uri","mongodb://127.0.0.1/LinkedInJob.Staging_raw_data").load()
  # master.createOrReplaceTempView("data")
  master = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_ip+"Staging_raw_data").load()
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

  df = sqlC.sql("SELECT ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) AS job_id,job_id as job_post_id,company as job_cmp_name,title as job_title,location as job_country,place as job_place, seniority_level as job_seniority_level,job_function,link as job_apply_link,date as job_date_posted,employment_type as job_type FROM data")
  #df = df.withColumn('job_seniority_level', lit("Others").cast(StringType()))
  #df = df.withColumn('cmp_ceo_name', F.lit(None).cast(StringType()))
  # df = df.withColumn('cmp_head_office', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_current_openings', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_date_created', F.lit(None).cast('string'))
  # df = df.withColumn('cmp_user_created', F.lit(None).cast('string'))
  #df=df.withColumn('job_seniority_level', when(job_seniority_level ==" ","Others"))
  #df = df.withColumn("job_seniority_level",when(col("job_seniority_level").isin('not_set', 'n/a', 'N/A', 'userid_not_set'),"Others").otherwise(col("job_seniority_level")))
  from pyspark.sql import functions as f
  df = df.withColumn('job_seniority_level', f.expr("coalesce(job_seniority_level, 'Others')"))
  df = df.withColumn('job_type', f.expr("coalesce(job_type, 'Others')"))
  df = df.withColumn('job_function', f.expr("coalesce(job_seniority_level, 'Others')"))
  from pyspark.sql.functions import datediff, col
  df=df.withColumn("current_date", f.current_date())
  #df=df.withColumn('current_date',lit("12-05-1995").cast(StringType()))
  df=df.withColumn("job_total_days", datediff(f.current_date(), col("job_date_posted")))
  df = df.withColumn('job_active_flag', when(col('job_total_days') >= 90, 'N').otherwise('Y'))
  df_count = df.groupBy('job_post_id').count().select('job_post_id', f.col('count').alias('count_records'))
  print(df_count.head())
  df = df.alias('df')
  df_count = df_count.alias('df_count')
  df = df.join(df_count, df.job_post_id == df_count.job_post_id, 'inner').select('df.*','df_count.count_records')
  df = df.withColumn('job_transaction_type', when(col('count_records') == 1, 'I').otherwise('U'))
  df_insert = df.filter(df.job_transaction_type == "I")
  df_update = df.filter(df.job_transaction_type == "U")
  df_insert=df_insert.drop(df_insert.job_transaction_type)
  df_insert=df_insert.drop(df_insert.count_records)
  df_insert_count=df_insert.count()
  df_update_count=df_update.count()
  print("Count Update", df_update_count)
  df_update=df_update.drop(df_update.job_transaction_type)
  df_update=df_update.drop(df_update.count_records)
  df_update=df_update.dropDuplicates(["job_post_id"])
  print("Hello",df_insert.head(100))
  print("Bhanu",df_update.head(100))
  db_connection = get_db_connection(cfg)
  dblist = db_connection.list_database_names()
  print("Count Update", df_update_count)
  if "LinkedInJob" in dblist:
    mydb = db_connection["LinkedInJob"]
    db_cm = mydb["job_master"]
    db_cm.delete_many({})
  # data_json = json.loads(df.toJSON().collect())
  if(df_insert_count>0):
    data = df_insert.toPandas()
    results = json.loads(data.to_json(orient='records'))
    print(results)
    db_cm.insert_many(results)
  if(df_update_count>0):
    data = df_update.toPandas()
    results_update = json.loads(data.to_json(orient='records'))
    print("New Records",results_update)
    db_cm.insert_many(results_update)

  #results=df_update.toJSON().map(lambda j: json.loads(j)).collect()
  #db_cm.update_many(results,)


  sc.stop()
  #mycol = mydb["Staging"]
  print("The data is inserted into the database")

