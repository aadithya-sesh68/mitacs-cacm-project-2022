import csv

from neo4j import GraphDatabase

# Change this to point to the directory of your database information
DATABASE_INFO_FILEPATH = r"E:\Environmental Backcloth\dbinfo.txt"

# Database Setup

def load_db_info(filepath):
    """ Load database access information from a file
    
    The file should be one downloaded from Neo4j when creating a database
    
    Parameters:
        filepath - String: The path to the file

    Returns:
        (uri, username, password): The access information if the file is valid
        None: When the information cannot be loaded
    """
    
    uri, user, password = None, None, None
    with open(filepath) as dbinfo:
        for line in dbinfo:
            line = line.strip()
            
            # Skip lines that don't have information or where it would be invalid
            if not "=" in line: continue
            label, value = line.split("=")
            if len(label) == 0 or len(value) == 0: continue
            
            # Set the variable that corresponds with the label
            match label:
                case "NEO4J_URI":
                    uri = value
                case "NEO4J_USERNAME":
                    user = value
                case "NEO4J_PASSWORD":
                    password = value
                    
    if not (uri and user and password): return None
    return (uri, user, password)

def create_driver():
    # Get the database information
    db_info = load_db_info(DATABASE_INFO_FILEPATH)
    if not db_info: return None
    uri, user, password = db_info

    # Connect to the database
    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver

def main():
    driver = create_driver()
    if not driver: return
    
    with driver.session() as session:
        if not session: return
    driver.close()
      
if __name__ == "__main__":
    main()