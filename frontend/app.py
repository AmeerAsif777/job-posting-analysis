import flask
from flask_pymongo import PyMongo
import pygal
from pygal.style import BlueStyle

from config import DB_PATH

app = flask.Flask(__name__)

mongodb_client = PyMongo(app, uri=DB_PATH)
db = mongodb_client.db


@app.route("/")
def home():
    
    # total companies
    company_count = get_total_companies()
    
    # total companies
    jobs_count = get_total_jobs()

    # monthly counts
    jobs_in_month = get_total_jobs_month()

    # Line
    # company_job_count_summary
    graph_company_jobs = get_company_jobs()

    # Pie
    # job_type
    graph_job_type = get_job_type()
    
    # job_seniority_level
    graph_job_seniority_level = get_job_seniority_level()

    #
    # job_title
    graph_job_title = get_job_title()

    # job_type_and_seniority
    jobtypexsen = get_job_seniority_level()

    # job_location
    job_location = get_job_location()


    return flask.render_template(   
                                    "dashboard.html",
                                    company_count = company_count,
                                    jobs_count = jobs_count,
                                    company_jobs = graph_company_jobs,
                                    job_type = graph_job_type,
                                    job_seniority_level = graph_job_seniority_level,
                                    job_title = graph_job_title,
                                    jobs_in_month = jobs_in_month,
                                    # jobtypexsen = jobtypexsen,
                                    job_location = job_location

                                )
# Total count

def get_total_companies():
    data = db.company_master.count_documents({})
    print(data)
    return data

def get_total_jobs():
    data = db.job_master.count_documents({})
    return int(data)

def get_total_jobs_month():
    data = db.job_posting_in_month.count_documents({ "month" : "4" })
    print(data)
    return int(data)

# Company jobs
def get_company_jobs():
    data = db.company_job_count_summary.find({},{'_id': 0}).sort('job_count',-1).limit(20)
    return create_graph_bar(data, para1="job_cmp_name", para2="job_count")

# job_type
def get_job_type():
    data = db.job_type.find({},{'_id': 0}).sort('job_count',-1)
    return create_graph_pie(data, para1="job_type", para2="job_count")

# job_seniority_level
def get_job_seniority_level():
    data = db.job_seniority_level.find({},{'_id': 0}).sort('job_count',-1)
    return create_graph_half_pie(data, para1="job_seniority_level", para2="job_count")

# job_title
def get_job_title():
    data = db.job_title.find({},{'_id': 0}).sort('job_count',-1).limit(20)
    return create_graph_Vbar(data, para1="job_title", para2="job_count")

# job_location
def get_job_location():
    data = db.job_location.find({},{'_id': 0}).sort('job_count',-1).limit(20)
    return create_graph_donut(data, para1="job_place", para2="job_count")

# # job_type_and_seniority
# def get_job_title():
#     data = db.job_title.find({},{'_id': 0})
#     return create_graph_radar(data, para0="job_type",para1="job_seniority_level", para2="job_count")

# 2D line
def create_graph_line(data, para1=None, para2=None):
    line_chart = pygal.Line()
    for doc in data:
        line_chart.add(doc.get(para1),int(doc.get(para2)))
    line_chart.render()
    return line_chart.render_data_uri()

# 2D Donut
def create_graph_donut(data, para1=None, para2=None):
    pie_chart = pygal.Pie(inner_radius=.75)
    for doc in data:
        pie_chart.add(doc.get(para1),int(doc.get(para2)))
    pie_chart.render()
    return pie_chart.render_data_uri()

# 2D half Pie
def create_graph_half_pie(data, para1=None, para2=None):
    pie_chart = pygal.Pie(half_pie=True)
    for doc in data:
        pie_chart.add(doc.get(para1),int(doc.get(para2)))
    pie_chart.render()
    return pie_chart.render_data_uri()

# 2D Pie
def create_graph_pie(data, para1=None, para2=None):
    pie_chart = pygal.Pie()
    for doc in data:
        pie_chart.add(doc.get(para1),int(doc.get(para2)))
    pie_chart.render()
    return pie_chart.render_data_uri()

# Bar
def create_graph_bar(data, para1=None, para2=None):
    bar_chart = pygal.HorizontalBar()
    for doc in data:
        bar_chart.add(doc.get(para1), int(doc.get(para2)))
    bar_chart.render()
    return bar_chart.render_data_uri()

# Vertical Bar
def create_graph_Vbar(data, para1=None, para2=None):
    bar_chart = pygal.Bar()
    for doc in data:
        bar_chart.add(doc.get(para1), int(doc.get(para2)))
    bar_chart.render()
    return bar_chart.render_data_uri()

# 3D Radar
def create_graph_radar(data, para0=None,para1=None, para2=None):
    radar_chart = pygal.Radar()
    xlabel = []
    for doc in data:
        xlabel.append(str(doc.get(para0)))
    radar_chart.x_labels = xlabel
    for doc in data:
        radar_chart.add(doc.get(para1),int(doc.get(para2)))
    radar_chart.render()
    return radar_chart.render_data_uri()


if __name__ == "__main__":
    app.run()