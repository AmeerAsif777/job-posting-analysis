
import pymongo
import json
import os
import pandas as pd
from linkedInutility import get_db_connection
import pymongo
import logging
def livedata(cfg):
 print("Creating Database connection")
 db_connection = get_db_connection(cfg)
 dblist=db_connection.list_database_names()
 if "LinkedInJob" in dblist:
  mydb = db_connection["LinkedInJob"]
  db_cm = mydb["Staging_raw_data"]
  filename = os.path.abspath("data.csv")
  #file_res = os.path.join(cdir, filepath)

  data = pd.read_csv(filename)
  # print(data)
  data_json = json.loads(data.to_json(orient='records'))
  db_cm.insert_many(data_json)

  #mycol = mydb["Staging"]
  print("The data is inserted into the database")


