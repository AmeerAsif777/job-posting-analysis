import datetime as dt
import json
import os
import logging
from pymongo import MongoClient
def open_config(argv):
   path = argv[1].split("=")[1]
   print(path)
   with open(path) as config_file:
    cfg = json.load(config_file)
    process_timestamp = dt.datetime.now().strftime("%Y%m%d%H%M%S")
    cfg["process_timestamp"] = process_timestamp
    for arg in argv[1:]:
        left = arg.split("=")[0]
        right = arg.split("=")[1]
        cfg[left] = right
    today_date = dt.datetime.now().strftime('%Y%m%d')
    log_file = cfg["log_file"].format(datetime=today_date)
    cfg["log_file"] = log_file
    print("*************************************8", cfg)
    return cfg

def open_logger(log_file):
    os.makedirs(os.path.dirname(log_file), exist_ok=True) # ,exist_ok=True
    logging.basicConfig(filename=log_file,
                        filemode='a', format='%(asctime)s %(levelname)s :%(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p',
                        level=print)
    print("hello")
    os.chmod(log_file, 0o777)

def get_db_connection(cfg):
    # database = cfg["database"]["db"]
    # host=cfg["database"]["host"]
    # user = cfg["database"]["user"]
    # password=cfg["database"]["password"]
    # port=cfg["database"]["port"]
    connectionString = "mongodb://MinerLeague:MinerLeague@localhost:27017";
    try:
        client = MongoClient(connectionString)
        print("Connection Successful")
    except:
        print("Connection Error")

    return client

