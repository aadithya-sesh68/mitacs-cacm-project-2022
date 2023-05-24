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
    query = 'CREATE (node: Junction {id: $JunctionID, type: $JunctionType, street_count: $StreetIntersectCount, latitude: $latitude, longitude: $longitude, vulnerability_score: $Vulnerability}) RETURN node'
    result = tx.run(query, typed_dict)
    return result

def add_node_data(data, i, node):
    data[f'JunctionID_{i}'] = int(node['JunctionID'])
    data[f'JunctionType_{i}'] = node['JunctionType']
    data[f'StreetIntersectCount_{i}'] = int(node['StreetIntersectCount'])
    data[f'longitude_{i}'] = float(node['longitude'])
    data[f'latitude_{i}'] = float(node['latitude'])
    data[f'Vulnerability_{i}'] = float(node['Vulnerability'])

def add_junction_nodes(tx, nodes):
    data = {}
    query = 'CREATE'
    for i, node in enumerate(nodes):
        add_node_data(data, i, node)
        query += f" (:Junction {{id: $JunctionID_{i}, type: $JunctionType_{i}, street_count: $StreetIntersectCount_{i}, latitude: $latitude_{i}, longitude: $longitude_{i}, vulnerability_score: $Vulnerability_{i}}})," 
    query = query.strip(',')
    tx.run(query, data)