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