import csv

def load_nodes(filename, node_name, session, process_directions, delete_old=True):
    with open(filename,'r') as infile:
        # Remove any old nodes
        if delete_old:
            session.execute_write(lambda tx: tx.run(f"MATCH (n:{node_name}) DETACH DELETE n"))
        
        print("Loading {node_name}s")
        dr = csv.DictReader(infile, quoting=csv.QUOTE_MINIMAL)
        for node in dr:
            node = process_node_data(node, process_directions)
            session.execute_write(add_node, node_name, node)
        print("Finished Loading Junctions")
        print()
        
def process_node_data(node_data, process_directions):
    output = {}
    for output_label in process_directions:
        input_label, conversion = process_directions[output_label]
        output_label[output_label] = conversion(node_data[input_label])
    return output
        
def add_node(tx, node_name, nodes):
    """Add the junction nodes to the database
`
    Args:
        tx (Transaction): The transaction to use
        nodes (List[Dict]): The list of nodes to add to the database
    """
    query = (
        f"CREATE (n:{node_name})"
        f"WITH n"
        f"MATCH (n:{node_name} {{id: $junct_id}})"
        "CREATE (c:Crime {crime_id: $crime_id, type_of_crime: $type_of_crime, date_of_crime: $date_of_crime, time_of_crime: "
        "$time_of_crime, hundred_block: $hundred_block, latitude: $latitude, longitude: $longitude, recency: $recency})"
        "-[r:NEAREST_CRIME_JN {distance: $distance, crime_id: $crime_id, junction_id: $junct_id}]->(j)"
    )
    tx.run(query, setup_data_dict(crimenode, nearest_junction))