# Filename:     nearestjnedges.py
# Description:  contains helper functions to add edges between Crime nodes and Junctions in the Neo4j database

def create_edge(tx, crimeID, junctID, dist):
    query = 'MATCH (c:Crime), (j:Junction) WHERE c.crime_id = toInteger($crime_id) AND j.id = toInteger($junction_id) CREATE (c)-[r:NEAREST_CRIME_JN {distance: $distance, crime_id: $crime_id, junction_id: $junction_id}]->(j) return type(r)'
    result = tx.run(query,crime_id=crimeID, junction_id=junctID, distance=dist)
    return result


#takes in a transaction and the custom dictionary - containing the distance, crime_id and junction_id - connection to be made
# edges are directional, point to the junction (from the crime node)
def add_nearjn_edge(tx,nearjndict):
    create_edge(tx,nearjndict["crime_id"],nearjndict["junction_id"],nearjndict["distance"])
    print("Edge created")