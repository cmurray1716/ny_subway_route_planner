import pandas as pd
from neo4j import GraphDatabase
import configparser 
from sodapy import Socrata
import argparse

parser = argparse.ArgumentParser(description = "This is a program that allows you to plan a journey across Nwew York from one subway station to another. Subway station names can be found on the map at: https://www.mta.info/map/5256")
parser.add_argument("-s", help = "This is the name of the station you want to start your journey from.", dest = "source_station")
parser.add_argument("-t", help = "This is the name of the station you want to reach.",
                        dest = "target_station")
args = parser.parse_args()

source_station = args.source_station
target_station = args.target_station

#connecting to local neo4j db
URI = "neo4j://localhost"
AUTH = ("neo4j", "1Password2025.")
driver = GraphDatabase.driver(URI, auth=AUTH)
driver.verify_connectivity()

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
ny = results_df[["gtfs_stop_id", "stop_name", "line"]]
ny["line_id"] = ny["gtfs_stop_id"].str[0]

query = """MATCH (s1:Station {id: $s1}),(s2:Station {id: $s2}), p = shortestPath ((s1)-[*]-(s2)) 
UNWIND nodes(p) as node 
WITH p, collect(node.station_name) as names, collect (node.id) as lines
RETURN names, lines"""


results = driver.execute_query(query, s1=ny[ny["stop_name"] == source_station]["gtfs_stop_id"].values[0], s2=ny[ny["stop_name"] == target_station]["gtfs_stop_id"].values[0])

full = results[0][0][0]
full_line = results[0][0][1]

print("Full list of stops:")
print(full)
print("Steps involved:")
print("Enter station" + " " + full[0] + " " + "on line" + " " + full_line[0][0])
for i in range(len(full) - 1):
    if full[i] == full[i+1]:
        print("Change lines at" + " " + full[i] + " to line" + " " + full_line[i+1][0]) 
        



