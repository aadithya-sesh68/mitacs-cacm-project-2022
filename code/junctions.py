# Filename: junctions.py
# Description: contains helper functions to add junction nodes to a neo4j database

#takes in props dict from csv.DictReader and corrects data types of nonstring properties
def get_typed_dict(props):
    props['JunctionID'] = int(props['JunctionID'])
    props['StreetIntersectCount'] = int(props['StreetIntersectCount'])
    props['longitude'] = float(props['longitude'])
    props['latitude'] = float(props['latitude'])
    props['Vulnerability'] = float(props['Vulnerability'])
    return props

#takes in transaction and dict objects from the driver and executes the query to add data to our database
def add_junction_node(tx,dict):
    typed_dict = get_typed_dict(dict)
    print('adding Junction node')
    print(typed_dict)
    print('--------\n\n')
    query = 'CREATE(node: Junction {id: $JunctionID, type: $JunctionType, street_count: $StreetIntersectCount, latitude: $latitude, longitude: $longitude, vulnerability_score: $Vulnerability}) RETURN node'
    result = tx.run(query, typed_dict)
    return result
