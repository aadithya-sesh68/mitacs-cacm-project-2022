# Filename:     transitnodes.py
# Description:  contains helper functions to add Public Transit Nodes in the Neo4j database

#takes in props dict from csv.DictReader and corrects data types of nonstring properties
def get_typed_dict(props):
    props['stop_id'] = int(props['stop_id'])
    props['latitude'] = float(props['latitude'])
    props['longitude'] = float(props['longitude'])
    props['stop_code'] = int(props['stop_code'])
    return props

#takes in transaction and dict objects from the driver and executes the query to add data to our database
def add_transit_node(tx,transitdict):
    typed_dict = get_typed_dict(transitdict)
    print('Adding Public Transit Node')
    print(typed_dict)
    print('--------\n\n')
    query = 'CREATE(node: Transit {stop_id: $stop_id, latitude: $latitude, longitude: $longitude, stop_code: $stop_code, stop_name: $stop_name, zone_id: $zone_id}) RETURN node'
    result = tx.run(query, typed_dict)
    return result