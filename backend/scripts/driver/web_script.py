
# Import buildins
import logging
import os
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, RemoteFilters
import datetime
import csv

#import config
from config2 import jobList

# Change root logger level (default is WARN)
#logging.basicConfig(level = print)

#Scraper properties
scraper = LinkedinScraper(
    chrome_executable_path="chromedriver1.exe",
    chrome_options=None,  
    headless=True, 
    max_workers=1, 
    slow_mo=1,  
)
# scraper = LinkedinScraper(
#     chrome_executable_path="new_chrome_proxy.exe",
#     chrome_options=None,
#     headless=True,
#     max_workers=1,
#     slow_mo=0.4,
#     # proxies=[
#     #     'http://localhost:6666',
#     #     'http://localhost:7777',        
#     # ]
# )

# Creating base text file
x = datetime.datetime.now()
x = str(x).replace(" ", "_")
x = str(x).replace("-", "")
x = str(x).replace(":", "")
x = str(x).replace(".", "")
f = open("data_"+x+".csv", "w", newline='', encoding="utf-8")
writer = csv.writer(f)
row = ["job_id","title","company","location","date","link","description","employment_type","seniority_level","place","job_function"]
writer.writerow(row)

# Data extraction successful
def on_data(data: EventData):

    row = [str(data.job_id), str(data.title), str(data.company), str(data.location), str(data.date), str(data.link), str(data.description), str(data.employment_type), str(data.seniority_level), str(data.place), str(data.job_function)]
    writer.writerow(row)
    # print('[ON_DATA]', data.title, data.company, data.date, data.link, len(data.description))

# Data error
def on_error(error):
    print('[ON_ERROR]', error)

# Done 
def on_end():
    print('[ON_END]')

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

c = 0
while ( c < len(jobList)):
    queries = [
        Query(
            query=jobList[c],
            options=QueryOptions(
                locations=['Canada'],
                optimize=True,
                limit=200,
                filters=QueryFilters(  # Filter by companies
                    relevance=RelevanceFilters.RECENT,
                    time=TimeFilters.MONTH,
                    type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                    experience=None,                
                )
            )
        ),
    ]
    scraper.run(queries)
    c += 1
x = datetime.datetime.now()
x = str(x).replace(" ", "_")
x = str(x).replace("-", "")
x = str(x).replace(":", "")
x = str(x).replace(".", "")
f = open("data.csv", "w", newline='', encoding="utf-8")
writer = csv.writer(f)
row = ["job_id","title","company","location","date","link","description","employment_type","seniority_level","place","job_function"]
writer.writerow(row)

# Data extraction successful
def on_data(data: EventData):

    row = [str(data.job_id), str(data.title), str(data.company), str(data.location), str(data.date), str(data.link), str(data.description), str(data.employment_type), str(data.seniority_level), str(data.place), str(data.job_function)]
    writer.writerow(row)
    # print('[ON_DATA]', data.title, data.company, data.date, data.link, len(data.description))

# Data error
def on_error(error):
    print('[ON_ERROR]', error)

# Done 
def on_end():
    print('[ON_END]')

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)

c = 0
while ( c < len(jobList)):
    queries = [
        Query(
            query=jobList[c],
            options=QueryOptions(
                locations=['Canada'],
                optimize=True,
                limit=200,
                filters=QueryFilters(  # Filter by companies
                    relevance=RelevanceFilters.RECENT,
                    time=TimeFilters.MONTH,
                    type=[TypeFilters.FULL_TIME, TypeFilters.INTERNSHIP],
                    experience=None,                
                )
            )
        ),
    ]
    scraper.run(queries)
    c += 1