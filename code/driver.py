import csv
#import utm
from scipy.spatial.distance import cityblock 
from datetime import datetime as dt

from junctions import add_junction_node
from segments import add_segment_node
from edges import create_edges

from crimenodes import add_crime_node
from nearestjnedges import add_nearjn_edge

from transitnodes import add_transit_node
from nearestssedges import add_nearss_edge

#Connect to the database
from neo4j import GraphDatabase

#Creating a basic session with the neo4j database
DATABASE_INFO_FILEPATH = "E:\Environmental Backcloth\dbinfo.txt"

def load_db_info(filepath):
    """ Load database access information from a file
    
    The file should be one downloaded from Neo4j when creating a database
    
    Parameters:
        filepath - String: The path to the file

    Returns:
        (uri, username, password): The access information if the file is valid
        None: When the information cannot be loaded
    """
    
    uri, user, password = None, None, None
    with open(filepath) as dbinfo:
        for line in dbinfo:
            line = line.strip()
            
            # Skip lines that don't have information or where it would be invalid
            if not "=" in line: continue
            label, value = line.split("=")
            if len(label) == 0 or len(value) == 0: continue
            
            # Set the variable that corresponds with the label
            match label:
                case "NEO4J_URI":
                    uri = value
                case "NEO4J_USERNAME":
                    user = value
                case "NEO4J_PASSWORD":
                    password = value
                    
    if not (uri and user and password): return None
    return (uri, user, password)

def create_session():
    """ Create a database session 

    Returns:
        Session: The connected session
        None: If the session could not be created
    """
    
    # Get the database information
    db_info = load_db_info(DATABASE_INFO_FILEPATH)
    if not db_info: return None
    uri, user, password = db_info

    # Connect to the database
    driver = GraphDatabase.driver(uri, auth=(user, password))
    session = driver.session()
    return session

session = create_session()
if not session: exit()

##INTEGRATE THE CREATION OF THE DATABASE FROM THE OTHER LOCATIONS TOO AT THE END

# #Loop to create junction nodes
# with open('../data/junctions.csv','r') as infile, session.begin_transaction() as tx:
#     print("-------------------Junctions---------------------")
#     dr = csv.DictReader(infile, quoting=csv.QUOTE_MINIMAL)
#     for dict_row in dr:
#         add_junction_node(tx, dict_row)

# tx.close()

# # #Loop to create segment nodes and relationship
# with open('../data/streetsegments.csv','r') as infile, session.begin_transaction() as tx:
#     print("-------------------Segments---------------------")
#     dr = csv.DictReader(infile, quoting=csv.QUOTE_MINIMAL)
#     for dict_row in dr:
#         add_segment_node(tx, dict_row)

# tx.close()

# # # # #Loop to create relationships
# with open('../data/streetsegments.csv','r') as infile, session.begin_transaction() as tx:
#     print("-------------------Edges---------------------")
#     dr = csv.DictReader(infile, quoting=csv.QUOTE_MINIMAL)
#     for dict_row in dr:
#         create_edges(tx, dict_row)

# tx.close()


# # #Matching algorithm to create crime nodes - comparing the crime event's location to each junction of the street network.
# crimell=[]
# junctionll=[]

# with open('../data/vanc_crime_2022.csv','r') as ccsvinput, session.begin_transaction() as tx:
#     print("---------------- Looping through crime data -----------------")
#     crime_id=0
#     mindist=0
#     jid=0
#     cdr = csv.DictReader(ccsvinput,quoting=csv.QUOTE_MINIMAL)
#     for cdict_row in cdr:
#         crime_id+=1
#         mindist=999.99
#         crimedict={}
#         nearjndict={}
#         res = utm.to_latlon(float(cdict_row["X"]), float(cdict_row["Y"]), 10, 'U')
#         crimell = [res[0], res[1]]
#         with open('../data/junctions.csv', 'r') as jcsvinput:
#             jdr = csv.DictReader(jcsvinput,quoting=csv.QUOTE_MINIMAL)
#             for jdict_row in jdr:
#                 junctionll=[float(jdict_row["latitude"]),float(jdict_row["longitude"])]
#                 distval = cityblock(crimell,junctionll) #using Manhattan distance to calculate the distance b/w 2 locations in vancouver
#                 if(distval<mindist):
#                     mindist=distval
#                     jid = jdict_row["JunctionID"]
#             #Assigning the properties of the Nearest_Junction_To relationship
#             nearjndict["distance"]=mindist
#             nearjndict["crime_id"]=str(crime_id)
#             nearjndict["junction_id"]=jid

#         #Assigning the properties of the Crime node
#         crimedict["crime_id"]=str(crime_id)
#         crimedict["type_of_crime"]=cdict_row["TYPE"]

#         tempmonth = cdict_row["MONTH"] if int(cdict_row["MONTH"])>9 else "0"+str(cdict_row["MONTH"])
#         tempday = cdict_row["DAY"] if int(cdict_row["DAY"])>9 else "0"+str(cdict_row["DAY"])
#         crimedict["date_of_crime"]=cdict_row["YEAR"]+"-"+tempmonth+"-"+tempday

#         temphour = cdict_row["HOUR"] if int(cdict_row["HOUR"])>9 else "0"+cdict_row["HOUR"]
#         tempmin = cdict_row["MINUTE"] if int(cdict_row["MINUTE"])>9 else "0"+cdict_row["MINUTE"]
#         crimedict["time_of_crime"]=temphour+":"+tempmin

#         crimedict["hundred_block"]=cdict_row["HUNDRED_BLOCK"]
#         crimedict["latitude"]=crimell[0]
#         crimedict["longitude"]=crimell[1]
#         crimedict["recency"]=cdict_row["RECENCY"]
        
#         #add_crime_node(tx,crimedict) #Adding the crime node
#         add_nearjn_edge(tx,nearjndict) #Adding the corresponding relationship b/w crime & junction
# tx.close()


# # SOURCE NODE: Transit
# #RELATIONSHIP: Present_In
# #TARGET NODE : Segment

# #Matching algorithm to create public transit nodes - comparing the bus station's location to each street segment of the network
translinkll=[]
streetll=[]

#For every (lat,long) of public transit, iterate through the (lat,long) of every street to find the closest street segment in which the translink station could be present.
with open('../data/transitstops.csv','r') as stopscsvinput, session.begin_transaction() as tx:
    print("---------------- Looping through Translink Public Transit data -----------------")
    minsdist=0
    ssid=0
    ptdr = csv.DictReader(stopscsvinput,quoting=csv.QUOTE_MINIMAL)
    for bus_row in ptdr:
        minsdist=999.99
        transitdict={}
        nearstrsegdict={}
        translinkll=[float(bus_row["stop_lat"]),float(bus_row["stop_lon"])]
        with open('../data/streetsegments.csv', 'r') as sscsvinput:
            ssdr = csv.DictReader(sscsvinput,quoting=csv.QUOTE_MINIMAL)
            for ss_row in ssdr:
                streetll=[float(ss_row["latitude"]), float(ss_row["longitude"])]
                distval = cityblock(translinkll,streetll) #using Manhattan distance to calculate the distance b/w 2 locations in vancouver
                if(distval<minsdist):
                    minsdist=distval
                    ssid = ss_row["StreetID"]
            #Assigning the properties of the Present_In relationship
            nearstrsegdict["distance"]=minsdist
            nearstrsegdict["stop_id"]=bus_row["stop_id"]
            nearstrsegdict["street_id"]=ssid
        
        #Assigning the properties of Public Transit Node
        transitdict["stop_code"] = bus_row["stop_code"]
        transitdict["stop_id"] = bus_row["stop_id"]
        transitdict["stop_name"] = bus_row["stop_name"]
        transitdict["latitude"] = bus_row["stop_lat"]
        transitdict["longitude"] = bus_row["stop_lon"]
        transitdict["zone_id"] = bus_row["zone_id"]

        #add_transit_node(tx,transitdict)
        add_nearss_edge(tx,nearstrsegdict)
tx.close()