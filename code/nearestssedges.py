# Filename:     nearestssedges.py
# Description:  contains helper functions to add edges between Public Transit nodes and Street Segments in the Neo4j database

def create_edge(tx, stopID, streetID, dist):
    query = 'MATCH (t:Transit), (s:Segment) WHERE t.stop_id = toInteger($stop_id) AND s.id = toInteger($street_id) CREATE (t)-[r:PRESENT_IN {distance: $distance, stop_id: $stop_id, street_id: $street_id}]->(s) return type(r)'
    result = tx.run(query,stop_id=stopID, street_id=streetID, distance=dist)
    return result


#takes in a transaction and the custom dictionary - containing the distance, stop_id and street_id - connection to be made
# edges are directional, point to the junction (from the transit node)
def add_nearss_edge(tx,nearstrsegdict):
    create_edge(tx,nearstrsegdict["stop_id"],nearstrsegdict["street_id"],nearstrsegdict["distance"])
    print("Edge created")