from linkedInutility import open_config
from linkedInutility import open_logger
from liveData import livedata
import sys
import logging

# import datetime as dt
# import json
# import os
import logging

from linkedInutility import get_db_connection
#from company_master import company_master_data
from company_master4 import company_master_data
from job_master import job_master_data
from job_seniority_level import job_seniority_data
from job_location_summary import job_location_summary_data
from job_title_summary import job_title_data
from job_type_summary import job_type_data
from job_type_and_seniority import job_type_and_seniority_data
from job_type_and_location import job_type_and_location_data
from company_job_count_summary import company_job_count_data
from job_posting_in_month import job_posting_in_month_data
cfg = open_config(sys.argv)
print("hello")
#open_logger(cfg["log_file"])
#print(cfg)
#livedata(cfg)
#company_master_data(cfg)

#job_master_data(cfg)
#job_seniority_data(cfg)
#job_location_summary_data(cfg)
#job_title_data(cfg)
#job_type_data(cfg)
#job_type_and_seniority_data(cfg)
#job_type_and_location_data(cfg)
#company_job_count_data(cfg)
job_posting_in_month_data(cfg)


