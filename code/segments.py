# Filename:     segments.py
# Description:  contains helper functions to add segment nodes to a neo4j database

#returns true if a given property is empty or null
def propisnotnull(val):
    return (val or str.isspace(val) or val == 0)

#takes in props dict from csv.DictReader and corrects data types of nonstring properties
def get_typed_dict(props):
 
    #manually going through properties and setting correct types
    props['StreetID'] = int(props['StreetID'])
    props['PseudoJunctionCount'] = int(props['PseudoJunctionCount'])
    props['pseudoJunctionID1'] = int(props['pseudoJunctionID1'])
    props['pseudoJunctionID2'] = int(props['pseudoJunctionID2'])
    props['adjustJunctionID1'] = int(props['adjustJunctionID1'])
    props['adjustJunctionID2'] = int(props['adjustJunctionID2'])
    props['adjustStreetID1'] = float(props['adjustStreetID1']) if propisnotnull(props['adjustStreetID1']) else None
    props['adjustStreetID2'] = float(props['adjustStreetID2']) if propisnotnull(props['adjustStreetID2']) else None
    props['PropertyCount'] = float(props['PropertyCount']) if propisnotnull(props['PropertyCount']) else 0
    props['Avg_CURRENT_LAND_VALUE'] = float(props['Avg_CURRENT_LAND_VALUE']) if propisnotnull(props['Avg_CURRENT_LAND_VALUE']) else 0
    props['SD_CURRENT_LAND_VALUE'] = float(props['SD_CURRENT_LAND_VALUE']) if propisnotnull(props['SD_CURRENT_LAND_VALUE']) else 0
    props['Avg_CURRENT_IMPROVEMENT_VALUE'] = float(props['Avg_CURRENT_IMPROVEMENT_VALUE']) if propisnotnull(props['Avg_CURRENT_IMPROVEMENT_VALUE']) else 0
    props['SD_CURRENT_IMPROVEMENT_VALUE'] = float(props['SD_CURRENT_IMPROVEMENT_VALUE']) if propisnotnull(props['SD_CURRENT_IMPROVEMENT_VALUE']) else 0
    props['Avg_ASSESSMENT_YEAR'] = float(props['Avg_ASSESSMENT_YEAR']) if propisnotnull(props['Avg_ASSESSMENT_YEAR']) else None
    props['Avg_PREVIOUS_LAND_VALUE'] = float(props['Avg_PREVIOUS_LAND_VALUE']) if propisnotnull(props['Avg_PREVIOUS_LAND_VALUE']) else 0
    props['SD_PREVIOUS_LAND_VALUE'] = float(props['SD_PREVIOUS_LAND_VALUE']) if propisnotnull(props['SD_PREVIOUS_LAND_VALUE']) else 0
    props['Avg_PREVIOUS_IMPROVEMENT_VALUE'] = float(props['Avg_PREVIOUS_IMPROVEMENT_VALUE']) if propisnotnull(props['Avg_PREVIOUS_IMPROVEMENT_VALUE']) else 0
    props['SD_PREVIOUS_IMPROVEMENT_VALUE'] = float(props['SD_PREVIOUS_IMPROVEMENT_VALUE']) if propisnotnull(props['SD_PREVIOUS_IMPROVEMENT_VALUE']) else 0
    props['Avg_YEAR_BUILT'] = float(props['Avg_YEAR_BUILT']) if propisnotnull(props['Avg_YEAR_BUILT']) else 0
    props['SD_YEAR_BUILT'] = float(props['SD_YEAR_BUILT']) if propisnotnull(props['SD_YEAR_BUILT']) else 0
    props['Avg_BIG_IMPROVEMENT_YEAR'] = float(props['Avg_BIG_IMPROVEMENT_YEAR']) if propisnotnull(props['Avg_BIG_IMPROVEMENT_YEAR']) else 0
    props['SD_BIG_IMPROVEMENT_YEAR'] = float(props['SD_BIG_IMPROVEMENT_YEAR']) if propisnotnull(props['SD_BIG_IMPROVEMENT_YEAR']) else 0
    props['Avg_ALL24'] = float(props['Avg_ALL24']) if propisnotnull(props['Avg_ALL24']) else None
    props['Avg_ALL8_9'] = float(props['Avg_ALL8_9']) if propisnotnull(props['Avg_ALL8_9']) else None
    props['Avg_ALL10_16'] = float(props['Avg_ALL10_16']) if propisnotnull(props['Avg_ALL10_16']) else None
    props['Avg_ALL17_18'] = float(props['Avg_ALL17_18']) if propisnotnull(props['Avg_ALL17_18']) else None
    props['Shape_Length'] = float(props['Shape_Length'])
    props['Landuse'] = list(str.split(props['Landuse'], ', '))
    props['latitude'] = float(props['latitude'])
    props['longitude'] = float(props['longitude'])

    return props

#takes in transaction and dict objects from the driver and executes the query to add data to our database
def add_segment_node(tx,dict):
    typed_dict = get_typed_dict(dict)
    print('adding Segment node')
    print(typed_dict)
    print('--------\n\n')
    query = 'CREATE(node: Segment {id: $StreetID, hblock: $hblock, type: $streetType, property_count: $PropertyCount, current_land_val_avg: $Avg_CURRENT_LAND_VALUE, current_land_val_sd: $SD_CURRENT_LAND_VALUE, current_improvement_avg: $Avg_CURRENT_IMPROVEMENT_VALUE, current_improvement_sd: $SD_CURRENT_IMPROVEMENT_VALUE, prev_land_val_avg: $Avg_PREVIOUS_LAND_VALUE, prev_land_val_sd: $SD_PREVIOUS_LAND_VALUE, prev_improv_val_avg: $Avg_PREVIOUS_IMPROVEMENT_VALUE, prev_improv_val_sd: $SD_PREVIOUS_IMPROVEMENT_VALUE, year_built_avg: $Avg_YEAR_BUILT, year_built_sd: $SD_YEAR_BUILT, big_improvement_yr_avg: $Avg_BIG_IMPROVEMENT_YEAR, big_improvement_yr_sd: $SD_BIG_IMPROVEMENT_YEAR, traffic_24_avg: $Avg_ALL24, traffic_8_9_avg: $Avg_ALL8_9, traffic_10_16_avg: $Avg_ALL10_16, traffic_17_18_avg: $Avg_ALL17_18, length_metres: $Shape_Length, land_uses: $Landuse, latitude: $latitude, longitude: $longitude}) RETURN node'
    result = tx.run(query, typed_dict)
    return result