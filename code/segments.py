# Filename:     segments.py
# Description:  contains helper functions to add segment nodes to a neo4j database

#returns true if a given property is empty or null
def propisnotnull(val):
    return (val or str.isspace(val) or val == 0)

#takes in props dict from csv.DictReader and corrects data types of nonstring properties
def add_node_data(data, node, i):
    #manually going through properties and setting correct types
    data[f'StreetID{i}'] = int(node['StreetID'])
    data[f'hblock{i}'] = node['hblock']
    data[f'streetType{i}'] = node['streetType']
    data[f'PseudoJunctionCount{i}'] = int(node['PseudoJunctionCount'])
    data[f'pseudoJunctionID1{i}'] = int(node['pseudoJunctionID1'])
    data[f'pseudoJunctionID2{i}'] = int(node['pseudoJunctionID2'])
    data[f'adjustJunctionID1{i}'] = int(node['adjustJunctionID1'])
    data[f'adjustJunctionID2{i}'] = int(node['adjustJunctionID2'])
    data[f'adjustStreetID1{i}'] = float(node['adjustStreetID1']) if propisnotnull(node['adjustStreetID1']) else None
    data[f'adjustStreetID2{i}'] = float(node['adjustStreetID2']) if propisnotnull(node['adjustStreetID2']) else None
    data[f'PropertyCount{i}'] = float(node['PropertyCount']) if propisnotnull(node['PropertyCount']) else 0
    data[f'Avg_CURRENT_LAND_VALUE{i}'] = float(node['Avg_CURRENT_LAND_VALUE']) if propisnotnull(node['Avg_CURRENT_LAND_VALUE']) else 0
    data[f'SD_CURRENT_LAND_VALUE{i}'] = float(node['SD_CURRENT_LAND_VALUE']) if propisnotnull(node['SD_CURRENT_LAND_VALUE']) else 0
    data[f'Avg_CURRENT_IMPROVEMENT_VALUE{i}'] = float(node['Avg_CURRENT_IMPROVEMENT_VALUE']) if propisnotnull(node['Avg_CURRENT_IMPROVEMENT_VALUE']) else 0
    data[f'SD_CURRENT_IMPROVEMENT_VALUE{i}'] = float(node['SD_CURRENT_IMPROVEMENT_VALUE']) if propisnotnull(node['SD_CURRENT_IMPROVEMENT_VALUE']) else 0
    data[f'Avg_ASSESSMENT_YEAR{i}'] = float(node['Avg_ASSESSMENT_YEAR']) if propisnotnull(node['Avg_ASSESSMENT_YEAR']) else None
    data[f'Avg_PREVIOUS_LAND_VALUE{i}'] = float(node['Avg_PREVIOUS_LAND_VALUE']) if propisnotnull(node['Avg_PREVIOUS_LAND_VALUE']) else 0
    data[f'SD_PREVIOUS_LAND_VALUE{i}'] = float(node['SD_PREVIOUS_LAND_VALUE']) if propisnotnull(node['SD_PREVIOUS_LAND_VALUE']) else 0
    data[f'Avg_PREVIOUS_IMPROVEMENT_VALUE{i}'] = float(node['Avg_PREVIOUS_IMPROVEMENT_VALUE']) if propisnotnull(node['Avg_PREVIOUS_IMPROVEMENT_VALUE']) else 0
    data[f'SD_PREVIOUS_IMPROVEMENT_VALUE{i}'] = float(node['SD_PREVIOUS_IMPROVEMENT_VALUE']) if propisnotnull(node['SD_PREVIOUS_IMPROVEMENT_VALUE']) else 0
    data[f'Avg_YEAR_BUILT{i}'] = float(node['Avg_YEAR_BUILT']) if propisnotnull(node['Avg_YEAR_BUILT']) else 0
    data[f'SD_YEAR_BUILT{i}'] = float(node['SD_YEAR_BUILT']) if propisnotnull(node['SD_YEAR_BUILT']) else 0
    data[f'Avg_BIG_IMPROVEMENT_YEAR{i}'] = float(node['Avg_BIG_IMPROVEMENT_YEAR']) if propisnotnull(node['Avg_BIG_IMPROVEMENT_YEAR']) else 0
    data[f'SD_BIG_IMPROVEMENT_YEAR{i}'] = float(node['SD_BIG_IMPROVEMENT_YEAR']) if propisnotnull(node['SD_BIG_IMPROVEMENT_YEAR']) else 0
    data[f'Avg_ALL24{i}'] = float(node['Avg_ALL24']) if propisnotnull(node['Avg_ALL24']) else None
    data[f'Avg_ALL8_9{i}'] = float(node['Avg_ALL8_9']) if propisnotnull(node['Avg_ALL8_9']) else None
    data[f'Avg_ALL10_16{i}'] = float(node['Avg_ALL10_16']) if propisnotnull(node['Avg_ALL10_16']) else None
    data[f'Avg_ALL17_18{i}'] = float(node['Avg_ALL17_18']) if propisnotnull(node['Avg_ALL17_18']) else None
    data[f'Shape_Length{i}'] = float(node['Shape_Length'])
    data[f'Landuse{i}'] = list(str.split(node['Landuse'], ', '))
    data[f'latitude{i}'] = float(node['latitude'])
    data[f'longitude{i}'] = float(node['longitude'])

    return node

#takes in transaction and dict objects from the driver and executes the query to add data to our database
def add_segment_node(tx,dict):
    typed_dict = add_node_data({}, dict, '')
    print('adding Segment node')
    print(typed_dict)
    print('--------\n\n')
    query = 'CREATE(node: Segment {id: $StreetID, hblock: $hblock, type: $streetType, property_count: $PropertyCount, current_land_val_avg: $Avg_CURRENT_LAND_VALUE, current_land_val_sd: $SD_CURRENT_LAND_VALUE, current_improvement_avg: $Avg_CURRENT_IMPROVEMENT_VALUE, current_improvement_sd: $SD_CURRENT_IMPROVEMENT_VALUE, prev_land_val_avg: $Avg_PREVIOUS_LAND_VALUE, prev_land_val_sd: $SD_PREVIOUS_LAND_VALUE, prev_improv_val_avg: $Avg_PREVIOUS_IMPROVEMENT_VALUE, prev_improv_val_sd: $SD_PREVIOUS_IMPROVEMENT_VALUE, year_built_avg: $Avg_YEAR_BUILT, year_built_sd: $SD_YEAR_BUILT, big_improvement_yr_avg: $Avg_BIG_IMPROVEMENT_YEAR, big_improvement_yr_sd: $SD_BIG_IMPROVEMENT_YEAR, traffic_24_avg: $Avg_ALL24, traffic_8_9_avg: $Avg_ALL8_9, traffic_10_16_avg: $Avg_ALL10_16, traffic_17_18_avg: $Avg_ALL17_18, length_metres: $Shape_Length, land_uses: $Landuse, latitude: $latitude, longitude: $longitude}) RETURN node'
    result = tx.run(query, typed_dict)
    return result

def add_segment_nodes(session, nodes):
    """Add the segment nodes to the database

    Args:
        tx (Transaction): The transaction to use
        nodes (List[Dict]): The list of nodes to add to the database
    """
    tx = session.begin_transaction()
    data = {}
    query = 'CREATE'
    i = 0
    
    for i, node in enumerate(nodes):
        # Execute the query every 500 nodes
        if i > 0 and i % 500 == 0:
            tx.run(query.strip(','), data)
            tx.commit()
            tx = session.begin_transaction()
            print(f"Processed {i} nodes")
            data = {}
            query = 'CREATE'
            
        # Add node information to the query and data
        add_node_data(data, node, i)
        query += (
            f" (:Segment {{id: $StreetID{i}, hblock: $hblock{i}, type: $streetType{i}, property_count: $PropertyCount{i},"
            f" current_land_val_avg: $Avg_CURRENT_LAND_VALUE{i}, current_land_val_sd: $SD_CURRENT_LAND_VALUE{i},"
            f" current_improvement_avg: $Avg_CURRENT_IMPROVEMENT_VALUE{i}, current_improvement_sd: $SD_CURRENT_IMPROVEMENT_VALUE{i},"
            f" prev_land_val_avg: $Avg_PREVIOUS_LAND_VALUE{i}, prev_land_val_sd: $SD_PREVIOUS_LAND_VALUE{i},"
            f" prev_improv_val_avg: $Avg_PREVIOUS_IMPROVEMENT_VALUE{i}, prev_improv_val_sd: $SD_PREVIOUS_IMPROVEMENT_VALUE{i},"
            f" year_built_avg: $Avg_YEAR_BUILT{i}, year_built_sd: $SD_YEAR_BUILT{i}, big_improvement_yr_avg: $Avg_BIG_IMPROVEMENT_YEAR{i},"
            f" big_improvement_yr_sd: $SD_BIG_IMPROVEMENT_YEAR{i}, traffic_24_avg: $Avg_ALL24{i}, traffic_8_9_avg: $Avg_ALL8_9{i},"
            f" traffic_10_16_avg: $Avg_ALL10_16{i}, traffic_17_18_avg: $Avg_ALL17_18{i}, length_metres: $Shape_Length{i}, land_uses: $Landuse{i},"
            f" latitude: $latitude{i}, longitude: $longitude{i}}})," 
        )
    # Execute the query for any remaining nodes
    tx.run(query.strip(','), data)
    tx.commit()
    print(f"Processed {i} nodes")