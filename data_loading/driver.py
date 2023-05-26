import csv
import utm
from scipy.spatial.distance import cityblock 

from junctions import add_junction_nodes
from segments import add_segment_nodes
from edges import process_edge_connections

from crimenodes import add_crime_with_junction

from transitnodes import add_transit_node_with_segment

from neo4j import GraphDatabase

# Change this to point to the directory of your database information
DATABASE_INFO_FILEPATH = r"E:\Environmental Backcloth\dbinfo.txt"

ZONE_NUMBER = 10
ZONE_LETTER = 'U'

JUNCTION_FILE = '../data/junctions.csv'
SEGMENT_FILE = '../data/streetsegments_new.csv'
CRIME_FILE = '../data/vanc_crime_2022.csv'
TRANSIT_FILE = '../data/transitstops.csv'


# Helper Functions
def create_regular_str(old_str):
    if not old_str.isnumeric(): return old_str
    return old_str if int(old_str)>9 else "0"+old_str

# Database Setup

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

def create_driver():
    # Get the database information
    db_info = load_db_info(DATABASE_INFO_FILEPATH)
    if not db_info: return None
    uri, user, password = db_info

    # Connect to the database
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

## Data Loading

def load_junctions(session, delete_old=True):
    # Loop to create junction nodes
    with open(JUNCTION_FILE,'r') as infile:
        # Remove any previous junctions
        if delete_old:
            session.execute_write(lambda tx: tx.run("MATCH (j:Junction) DETACH DELETE j"))
        
        print("Loading Junctions")
        dr = csv.DictReader(infile, quoting=csv.QUOTE_MINIMAL)
        session.execute_write(add_junction_nodes, dr)
        print("Finished Loading Junctions")
        print()

def load_segments(session, delete_old=True):
    # Loop to create segment nodes and relationship
    with open(SEGMENT_FILE,'r') as infile:
        # Remove any previous segments
        if delete_old:
            session.execute_write(lambda tx: tx.run("MATCH (s:Segment) DETACH DELETE s"))
            
        print("Loading Segments")
        dr = csv.DictReader(infile, quoting=csv.QUOTE_MINIMAL)
        session.execute_write(add_segment_nodes, dr)
        print("Finished Loading Segments")
        print()
            
def connect_segment_junctions(session, delete_old=True):
    # Remove any previous connections
    if delete_old:
        session.execute_write(lambda tx: tx.run("MATCH ()-[c:CONTINUES_TO]->() DELETE c"))
    
    with open(SEGMENT_FILE,'r') as infile:
        print("Connecting Segments To Junctions")
        dr = csv.DictReader(infile, quoting=csv.QUOTE_MINIMAL)
        session.execute_write(process_edge_connections, dr)
        print("Finished Connecting Segments")
        print()

def load_crimes(session, delete_old=True):
    print("Loading Crimes")
    
    # Delete old crime nodes
    if delete_old:
        session.execute_write(lambda tx: tx.run("MATCH (c: Crime) DETACH DELETE c"))
    
    # Load the junctions
    junctions = []
    with open(JUNCTION_FILE, 'r') as jcsvinput:
        jdr = csv.DictReader(jcsvinput,quoting=csv.QUOTE_MINIMAL)
        for jdict_row in jdr:
            junctionll=[float(jdict_row["latitude"]),float(jdict_row["longitude"])]
            junctions.append(
                {
                    "id": jdict_row["JunctionID"],
                    "latlon": junctionll
                }
            )
    
    # Matching algorithm to create crime nodes - comparing the crime event's location to each junction of the street network.
    crimell=[]
    junctionll=[]

    # Open the crime data
    with open(CRIME_FILE,'r') as ccsvinput, session.begin_transaction() as tx:
        crime_id=0
        mindist=0
        jid=0
        cdr = csv.DictReader(ccsvinput,quoting=csv.QUOTE_MINIMAL)
        
        # Loop over each crime
        for cdict_row in cdr:
            crime_id+=1 # Create an id for the crime
            mindist=float('inf')
            
            crimedict={}
            nearjndict={}
            
            # Determine the location of the crime
            res = utm.to_latlon(float(cdict_row["X"]), float(cdict_row["Y"]), ZONE_NUMBER, ZONE_LETTER)
            crimell = [res[0], res[1]]
            
            # Find the closest junction
            for junction in junctions:
                distval = cityblock(crimell, junction["latlon"]) #using Manhattan distance to calculate the distance b/w 2 locations in vancouver
                
                # Update the closest junction
                if(distval<mindist):
                    mindist=distval
                    jid = junction["id"]
                        
            # Assigning the properties of the Nearest_Junction_To relationship
            nearjndict["distance"]=mindist
            nearjndict["crime_id"]=str(crime_id)
            nearjndict["junction_id"]=jid

            # Assigning the properties of the Crime node
            crimedict["crime_id"]=str(crime_id)
            crimedict["type_of_crime"]=cdict_row["TYPE"]
            
            crimedict["date_of_crime"]=f"{cdict_row['YEAR']}-{create_regular_str(cdict_row['MONTH'])}-{create_regular_str(cdict_row['DAY'])}"
            crimedict["time_of_crime"]=f"{create_regular_str(cdict_row['HOUR'])}:{create_regular_str(cdict_row['MINUTE'])}"

            crimedict["hundred_block"]=cdict_row["HUNDRED_BLOCK"]
            crimedict["latitude"]=crimell[0]
            crimedict["longitude"]=crimell[1]
            crimedict["recency"]=cdict_row["RECENCY"]
            
            add_crime_with_junction(tx, crimedict, nearjndict)
            if crime_id % 200 == 0:
                print(f"Processed {crime_id} crimes")
    tx.close()
    print("Finished Loading Crimes")
    print()

def load_transit(session, delete_old=True):
    print("Loading Transit Stations")
    
    # Delete old transit nodes
    if delete_old:
        session.execute_write(lambda tx: tx.run("MATCH (t: Transit) DETACH DELETE t"))
    
    # Load the steet segments
    segments = []
    with open(SEGMENT_FILE, 'r') as sscsvinput:
        ssdr = csv.DictReader(sscsvinput,quoting=csv.QUOTE_MINIMAL)
        for ss_row in ssdr:
            streetll=[float(ss_row["latitude"]),float(ss_row["longitude"])]
            segments.append(
                {
                    "id": ss_row["StreetID"],
                    "latlon": streetll
                }
            )
            
    # Matching algorithm to create public transit nodes - comparing the bus station's location to each street segment of the network
    translinkll=[]
    streetll=[]

    #For every (lat,long) of public transit, iterate through the (lat,long) of every street to find the closest street segment in which the translink station could be present.
    with open(TRANSIT_FILE,'r') as stopscsvinput, session.begin_transaction() as tx:
        print("---------------- Looping through Translink Public Transit data -----------------")
        minsdist=0
        ssid=0
        counter=0
        ptdr = csv.DictReader(stopscsvinput,quoting=csv.QUOTE_MINIMAL)
        for bus_row in ptdr:
            counter += 1
            minsdist=float('inf')
            transitdict={}
            nearstrsegdict={}
            translinkll=[float(bus_row["stop_lat"]),float(bus_row["stop_lon"])]

            for segment in segments:
                distval = cityblock(translinkll,segment["latlon"]) #using Manhattan distance to calculate the distance b/w 2 locations in vancouver
                if(distval<minsdist):
                    minsdist=distval
                    ssid = segment["id"]
                    
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

            add_transit_node_with_segment(tx, transitdict, nearstrsegdict)
            if counter % 200 == 0:
                print(f"Processed {counter} transit stations")
    tx.close()
    print("Finished Loading Transit Stations")
    print()
  
def main():
    driver = create_driver()
    if not driver: return
    
    with driver.session() as session:
        if not session: return
        
        print("Beginning Loading Data")
        print()
        #load_junctions(session)
        #load_segments(session)
        #connect_segment_junctions(session)
        #load_crimes(session)
        load_transit(session)
        print("Finished Loading Data")
        
    driver.close()
      
if __name__ == "__main__":
    main()