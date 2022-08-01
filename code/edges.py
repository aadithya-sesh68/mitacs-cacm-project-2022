# Filename:     edges.py
# Description:  contains helper functions to add edges between Segments and Junctions in the Neo4j database

#takes in a transaction, the segment id, and the junction id, adds edge between them
# edges are directional, always point to the junction
def add_edge(tx, segID, junctID):
    query = 'MATCH (s:Segment), (j:Junction) WHERE s.id = $segid AND j.id = $junctid CREATE (s)-[r:CONTINUES_TO]->(j) return type(r)'
    result = tx.run(query, segid=segID, junctid=junctID)
    return result

#takes in a transaction and a property dict, creates edge on the neo4j database
def create_edges(tx, dict):
    if(int(dict['PseudoJunctionCount']) > 0):
        if(int(dict['pseudoJunctionID1']) > 0):
            add_edge(tx, int(dict['StreetID']),int(dict['pseudoJunctionID1']))
        if(int(dict['pseudoJunctionID2']) > 0):
            add_edge(tx, int(dict['StreetID']),int(dict['pseudoJunctionID2']))
    
    if(int(dict['adjustJunctionID1']) > 0):
        add_edge(tx, int(dict['StreetID']), int(dict['adjustJunctionID1']))

    if(int(dict['adjustJunctionID2']) > 0):
        add_edge(tx, int(dict['StreetID']), int(dict['adjustJunctionID2']))

    print('edge created')

