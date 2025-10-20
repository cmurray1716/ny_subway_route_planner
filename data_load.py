import pandas as pd
import configparser 
from sodapy import Socrata
import itertools
from neo4j import GraphDatabase

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

#taking only the relevant columns for ne04j
ny = results_df[["gtfs_stop_id", "stop_name", "line"]]

#the gtfs_stop_id gives a single letter/number (which indicates the line),
#followed by two numbers (indicating the stop number on that line)
#using the two lines below to separate these out into separate columns for creating relationships later.
ny["line_id"] = ny["gtfs_stop_id"].str[0]
ny["station_id"] = ny["gtfs_stop_id"].str[1:]

#getting the list of line identifiers
lines = ny["line_id"].unique()

#query for creating station nodes
query = "CREATE (n:Station {id: $id, station_name: $station, line_name: $line}) RETURN (n)"

#creating stop nodes
for idx in range(len(ny)):
    driver.execute_query(query, id=ny["gtfs_stop_id"].iloc[idx], station=ny["stop_name"].iloc[idx], line=ny["line"].iloc[idx])

#query for creating relationships between stations
query = "MATCH (s1:Station {id:$s1}), (s2:Station {id:$s2}) CREATE (s1)-[r:TRAIN {line:$line}]->(s2)"

#for loop create relationships between each stop in each line
for x in lines:
    line_df = ny[ny["line_id"] == x].sort_values(by=["station_id"])
    #creating both forward and reverse relationships.
    for idx in range(len(line_df)-1):
        driver.execute_query(query, s1=line_df["gtfs_stop_id"].iloc[idx], s2=line_df["gtfs_stop_id"].iloc[idx + 1], line=line_df["line_id"].iloc[idx])
        driver.execute_query(query, s2=line_df["gtfs_stop_id"].iloc[idx], s1=line_df["gtfs_stop_id"].iloc[idx + 1], line=line_df["line_id"].iloc[idx])

#some stations have multiple stops on different lines.
#creating df of these stops
transfers = ny[ny.duplicated(subset = ["stop_name"], keep = False)].sort_values(by=["stop_name"])
#creating list of stops you can transfer between
transfer_list = transfers["stop_name"].unique()

#query for creating transfer relationships
query = "MATCH (s1:Station {id:$s1}), (s2:Station {id:$s2}) CREATE (s1)-[r:TRANSFER]->(s2)"

#for loop to create relationships between stops that are at the same station ie transfering between lines
for t in transfer_list:
    t_df = transfers[transfers["stop_name"] == t]
    #Using itertools to get all permutations of pairs of stops that are at the same station
    #as there may be 3 or more different lines going in/out of the same station
    for p in list(itertools.permutations(t_df["gtfs_stop_id"], 2)):
        driver.execute_query(query, s1=t_df[t_df["gtfs_stop_id"] == p[0]]["gtfs_stop_id"].values[0], s2=t_df[t_df["gtfs_stop_id"] == p[1]]["gtfs_stop_id"].values[0])
        driver.execute_query(query, s2=t_df[t_df["gtfs_stop_id"] == p[1]]["gtfs_stop_id"].values[0], s1=t_df[t_df["gtfs_stop_id"] == p[0]]["gtfs_stop_id"].values[0])

#Manually adding one last relationship between south ferry station and staten island
query_1 = "MATCH (s1:Station {id:'142'}), (s2:Station {id:'S31'}) CREATE (s1)-[r:FERRY]->(s2)"
query_2 = "MATCH (s1:Station {id:'S31'}), (s2:Station {id:'142'}) CREATE (s1)-[r:FERRY]->(s2)"
driver.execute_query(query_1)
driver.execute_query(query_2)






