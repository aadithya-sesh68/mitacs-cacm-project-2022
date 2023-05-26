# Filename: junctions.py
# Description: contains helper functions to add junction nodes to a neo4j database

def add_node_data(data, node, i):
    """Add the information from [node] to [data]
    
    The information about [node] will be added to [data] with keys ending in [i]

    Args:
        data (Dict): The data to be added to
        i (Any): The index of the node
        node (Dict): The information about the node
    """
    data[f'JunctionID{i}'] = int(node['JunctionID'])
    data[f'JunctionType{i}'] = node['JunctionType']
    data[f'StreetIntersectCount{i}'] = int(node['StreetIntersectCount'])
    data[f'longitude{i}'] = float(node['longitude'])
    data[f'latitude{i}'] = float(node['latitude'])
    data[f'Vulnerability{i}'] = float(node['Vulnerability'])

# takes in transaction and dict objects from the driver and executes the query to add data to our database
def add_junction_node(tx, dict):
    typed_dict = add_node_data({}, dict, '')
    print('adding Junction node')
    print(typed_dict)
    print('--------\n\n')
    query = 'CREATE (node: Junction {id: $JunctionID, type: $JunctionType, street_count: $StreetIntersectCount, latitude: $latitude, longitude: $longitude, vulnerability_score: $Vulnerability}) RETURN node'
    result = tx.run(query, typed_dict)
    return result

def add_junction_nodes(tx, nodes):
    """Add the junction nodes to the database

    Args:
        tx (Transaction): The transaction to use
        nodes (List[Dict]): The list of nodes to add to the database
    """
    data = {}
    query = 'CREATE'
    i = 0
    
    for i, node in enumerate(nodes):
        # Execute the query every 1000 nodes
        if i > 0 and i % 1000 == 0:
            tx.run(query.strip(','), data)
            print(f"Processed {i} junctions")
            data = {}
            query = 'CREATE'
            
        # Add node information to the query and data
        add_node_data(data, node, i)
        query += f" (:Junction {{id: $JunctionID{i}, type: $JunctionType{i}, street_count: $StreetIntersectCount{i}, latitude: $latitude{i}, longitude: $longitude{i}, vulnerability_score: $Vulnerability{i}}})," 
    
    # Execute the query for any remaining nodes
    tx.run(query.strip(','), data)
    print(f"Processed {i + 1} junctions")