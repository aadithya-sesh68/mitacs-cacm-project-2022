# Filename:     crimenodes.py
# Description:  contains helper functions to add Crime Nodes in the Neo4j database

#takes in props dict from csv.DictReader and corrects data types of nonstring properties
def get_typed_dict(props):
    props['crime_id'] = int(props['crime_id'])
    props['latitude'] = float(props['latitude'])
    props['longitude'] = float(props['longitude'])
    props['recency'] = int(props['recency'])
    return props

#takes in transaction and dict objects from the driver and executes the query to add data to our database
def add_crime_node(tx,crimedict):
    typed_dict = get_typed_dict(crimedict)
    print('Adding Crime Node')
    print(typed_dict)
    print('--------\n\n')
    query = 'CREATE(node: Crime {crime_id: $crime_id, type_of_crime: $type_of_crime, date_of_crime: $date_of_crime, time_of_crime: $time_of_crime, hundred_block: $hundred_block, latitude: $latitude, longitude: $longitude, recency: $recency}) RETURN node'
    result = tx.run(query, typed_dict)
    return result

def setup_data_dict(crimenode, nearest_junction):
    data = {}
    data['crime_id'] = int(crimenode['crime_id'])
    data['type_of_crime'] = crimenode['type_of_crime']
    data['date_of_crime'] = crimenode['date_of_crime']
    data['time_of_crime'] = crimenode['time_of_crime']
    data['hundred_block'] = crimenode['hundred_block']
    data['latitude'] = float(crimenode['latitude'])
    data['longitude'] = float(crimenode['longitude'])
    data['recency'] = int(crimenode['recency'])
    data['distance'] = nearest_junction['distance']
    data['junct_id'] = int(nearest_junction['junction_id'])
    return data

def add_crime_with_junction(tx, crimenode, nearest_junction):
    query = (
        "MATCH (j:Junction {id: $junct_id})"
        "CREATE (c:Crime {crime_id: $crime_id, type_of_crime: $type_of_crime, date_of_crime: $date_of_crime, time_of_crime: "
        "$time_of_crime, hundred_block: $hundred_block, latitude: $latitude, longitude: $longitude, recency: $recency})"
        "-[r:NEAREST_CRIME_JN {distance: $distance, crime_id: $crime_id, junction_id: $junct_id}]->(j)"
    )
    tx.run(query, setup_data_dict(crimenode, nearest_junction))