import pandas as pd
import configparser 
from sodapy import Socrata
import seaborn as sns
import plotly.express as px
import matplotlib.pyplot as plt
import numpy as np
import argparse

parser = argparse.ArgumentParser(description = "This program gives some basic statistics on New York Subway Stations and displays the stops in a chosen line")
parser.add_argument("-l", help = "This is the name of the line you want to see.", dest = "line", default = "1")
args = parser.parse_args()

line = args.line

#getting api login details
config = configparser.ConfigParser()
config.read('ny_api.conf.txt')
api_config = config['ny_api']

#connecting to api with info from config file
client = Socrata(api_config["host"], api_config["token"], username = api_config["username"], password = api_config["password"])

#retrieving results from api
results = client.get("39hk-dx4f")
#putting results in a dataframe
results_df = pd.DataFrame.from_records(results)

#taking only the relevant columns for ne04j
ny = results_df[["gtfs_stop_id", "stop_name", "line", "ada", "gtfs_latitude", "gtfs_longitude"]]

#the gtfs_stop_id gives a single letter/number (which indicates the line),
#followed by two numbers (indicating the stop number on that line)
#using the two lines below to separate these out into separate columns for creating relationships later.
ny["line_id"] = ny["gtfs_stop_id"].str[0]
ny["station_id"] = ny["gtfs_stop_id"].str[1:]

print("Number of stations on each line:")
print(ny["line_id"].value_counts())
print("Number of Stations for each area:")
print(ny["line"].value_counts())
print("Number of fully accessible stations (1 meaning fully accessible and 2 partially accessible):")
print(ny["ada"].value_counts())

#creating df of only stations in selected line
ny_filtered = ny[ny["line_id"] == line].sort_values(by = "station_id")

#creating a graph of the subway line
fig = px.scatter(ny_filtered, x = "stop_name", y = "line_id", hover_data=["stop_name"], color= "line", title= "Stops in Chosen Line")
fig.show()