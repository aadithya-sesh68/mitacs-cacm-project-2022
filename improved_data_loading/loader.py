import csv

from neo4j import Session

from typing import Callable
from typing import Any
from typing import TypeAlias
from collections.abc import Sequence

# Type Declarations
DataHandle: TypeAlias = int
CategoryHandle: TypeAlias = int
RelationshipHandle: TypeAlias = int
Row: TypeAlias = dict[str, Any]
ConversionFunction: TypeAlias = Callable[[Any], Any]

class RowFunction:
    def __init__(self, f: Callable[[Row, int], Any]):
        self.f = f

ConversionMap: TypeAlias = dict[str, ConversionFunction | RowFunction | tuple[ConversionFunction, str]]

def is_not_null(val):
    return (val or str.isspace(val) or val == 0)

def convert_if_not_null(val, convert=float, on_null: Any=0):
    return convert(val) if is_not_null(val) else on_null

class GraphLoader:

    def __init__(self, session: Session):
        self._session = session
        self._loaded_data: list[list[dict[str, Any]]] = []
        self._categories: list[dict[str, Any]] = []
        self._relations: list[dict[str, Any]] = []

    def load_file(self, filename: str, conversion_map: ConversionMap, delimiter: str =',', fieldnames: Sequence[str] | None=None, has_header=True) -> DataHandle:
        """Load data from a csv file
        
        [conversion_map] is used to manipulate the read data in order to turn it into more useful formats and types. A ConversionMap is a dictionary
        mapping fieldnames to either a function on one parameter, a tuple containing a function on one parameter and a fieldname, or a RowFunction.
        The result of the function is stored in the data using the first fieldname.
        
        Example:
        ```
        {
            "id": int,
            "name": (str, "Name"),
            "indexName": RowFunction(lambda row, i: str(i) + str(row["Name"]))
        }
        ```
        
        The value matched to "id" in the input is passed to int and stored in "id" in the output.
        The value matched to "Name" in the input is passed to str and stored in "name" in the output.
        The row and an index value is passed to the lambda function which joins the index with the "Name" attribute of the input, and the result is
        stored in "indexName" of the output.

        Args:
            filename (str): The name of the file to load. Should be a csv file.
            conversion_map (ConversionMap): A mapping between fieldnames in the output data and a function on the input data.
            delimiter (str, optional): The delimiter used by the csv file. Defaults to ','.
            fieldnames (Sequence[str] | None): The names to use for the fields. Defaults to None.
            has_header (boolean): Whether or not there is a header row in the file. Must be true if fieldnames is None.

        Returns:
            DataHandle: A handle to the loaded data
        """        
        
        # Make sure there is fieldnames information somewhere
        if (fieldnames == None and not has_header):
            raise Exception("If fieldnames is None then has_header must be True")
        
        # Open the file
        with open(filename, 'r') as input_file:
            handle = len(self._loaded_data)
            data = []
            
            # Make all conversions using IndexedConversionMap
            conversions = self._fix_conversion(conversion_map)
            
            # Read each row of the file
            rows = csv.DictReader(input_file, quoting=csv.QUOTE_MINIMAL, delimiter=delimiter, fieldnames=fieldnames)
            
            # Read the header if necessary
            if(fieldnames != None and has_header):
                next(rows)
            
            # Read each row
            for i, row in enumerate(rows):
                if i == 2: break
                # Apply the conversion to the row to get a result
                result = { 
                    key: conversions[key](row, i) for key in conversions
                }
                
                data.append(result)
                
            # Store all the loaded data
            self._loaded_data.append(data)
            return handle
        
    def process_data(self, data_handle, func: Callable[[Row], None]):
        data = self._loaded_data[data_handle]
        for row in data:
            func(row)
        
    def cross_data(self, data_handle_1: DataHandle, data_handle_2: DataHandle, func: Callable[[Row, Row], None]):
        
        data_1 = self._loaded_data[data_handle_1]
        data_2 = self._loaded_data[data_handle_2]
        for row_1 in data_1:
            for row_2 in data_2:
                func(row_1, row_2)
                
    def match_closest(self, data_handle_1: DataHandle, data_handle_2: DataHandle, match_keys, result_fieldnames):
        """ Pair all the nodes in one data set to the closest node in another dataset
        
        WARNING: Creates a cross product between the two data sets. May run slowly for large datasets.
        
        Distance is the manhattan distance

        Args:
            data_handle_1 (DataHandle): The set of nodes to pair
            data_handle_2 (DataHandle): The set of nodes used for finding the closest node
            match_keys (list[str | tuple(str, str)]): The fieldnames to use for distance. eg: ["latitude", "longitude"] or [("latitude", "LATITUDE"), ("longitude", "LONGITUDE")]
            result_fieldnames (dict[str, tuple | str]): The fieldnames to store the results in. Possible results include the closest node and the distance. eg: {"closest": ("near_node", "id"), "distance": "node_dist"}
        """
        
        # TODO: Add multiple distance functions
        # TODO: Add a distance limiter
        
        # Get the data that will be used
        data_1 = self._loaded_data[data_handle_1]
        data_2 = self._loaded_data[data_handle_2]
        
        # Match for each node in data set 1
        for row_1 in data_1:
            closest = None
            b_dist = float('inf')
            
            # Find the closest node in data set 2
            for row_2 in data_2:
                # Calculate the distance between [row_1] and [row_2]
                # Manhattan distance
                distance = 0
                for key in match_keys:
                    if type(key) == tuple:
                        diff = row_1[key[0]] - row_2[key[1]]
                    else:
                        diff = row_1[key] - row_2[key[2]]
                    distance += abs(diff)
                    
                # Update the closest node
                if distance < b_dist:
                    closest = row_2
                    b_dist = distance
                    
            # Write the information about the closest node to [row_1]
            if closest != None:
                if "closest" in result_fieldnames:
                   match_name, match_field = result_fieldnames["closest"]
                   row_1[match_name] = closest[match_field]
                if "distance" in result_fieldnames:
                    row_1[result_fieldnames["distance"]] = b_dist
        
    def define_category(self, category_name, data_handle, value_matcher):
            handle = len(self._categories)
            self._categories.append({
                 "name": category_name,
                 "data_handle": data_handle,
                 "value_matcher": value_matcher.copy()
            })
            return handle
    
    def define_relation(self, relation_name, category_1, category_2, links, props=None):
         handle = len(self._relations)
         self._relations.append({
            "name": relation_name,
            "category_1": category_1,
            "category_2": category_2,
            "links": links,
            "props": props if props else []
         })
         return handle
    
    def write_category(self, category_handle):
        category = self._categories[category_handle]
        data = self._loaded_data[category["data_handle"]]
        value_matcher = category["value_matcher"]
        properties = self._match_values(data, value_matcher)

        query = """
        UNWIND $properties AS props
        CALL {
            WITH props
            CREATE (n:$category_name)
            SET n = props
        } IN TRANSACTIONS
        """

        self._session.run(
            query, 
            {
                "category_name": category["name"],
                "properties": properties
            }
        )

    def write_relation(self, relation_handle):
        relation = self._relations[relation_handle]
        category1 = self._categories[relation["category_1"]]
        category2 = self._categories[relation["category_2"]]
        data_1 = self._loaded_data[category1["data_handle"]]
        data_2 = self._loaded_data[category2["data_handle"]]
        links = relation["links"]
        props = relation["props"]
        
        if len(data_1) == 0 or len(data_2) == 0: return
        
        data_1_key, data_match, data_2_key = links

        linkValues = [ 
             [
                row[data_1_key],
                row[data_match],
                [row[prop] for prop in props]
             ]
             for row in data_1
        ]

        where_1 = f"WHERE n1.{data_1_key} = row[0]"
        
        compare = "IN" if type(data_1[0][data_match]) == list else "="
        where_2 = f"WHERE n2.{data_2_key} {compare} row[1]"

        query = """
        UNWIND $data AS row
        CALL {
            WITH row
            MATCH (n1:$category_1) $where1 
            WITH n1
            MATCH (n2:$category_2) $where2
            CREATE (n1)-[r:$relation data[2]]->(n2)
        } IN TRANSACTIONS
        """

        self._session.run(
            query,
            {
                'data': linkValues,
                'where1': where_1,
                'where2': where_2
            }
        )

    def _match_values(self, data, value_matcher):
        return [
            { key: row[key] for key in value_matcher }
            for row in data
        ]
        
    def _fix_conversion(self, conversions: ConversionMap) -> dict[str, Callable[[Row, int], Any]]:
        """Modify a ConversionMap to only used IndexedConversionFunctions   

        Args:
            conversions (ConversionMap): The conversion map to fix

        Returns:
            dict[str, IndexedConversionFunction]: The fixed conversion map
        """
        result = {}
        for key in conversions:
            # The conversion is (function, key_in)
            if type(conversions[key]) == tuple:
                func = conversions[key][0]                                                  # type: ignore
                fieldname = conversions[key][1]                                             # type: ignore
                
                # Have to use instantly called lambda expression wrapper to avoid late binding
                result[key] = (lambda func, fn: 
                    lambda row, index: func(row[fn]))(func, fieldname)
            # The conversion is RowFunction(function)
            elif type(conversions[key])  == RowFunction:
                # Have to use instantly called lambda expression wrapper to avoid late binding
                result[key] = (lambda rowf: rowf.f)(conversions[key])                       # type: ignore
            # The conversion is function
            else:
                # Have to use instantly called lambda expression wrapper to avoid late binding
                result[key] = (lambda key: 
                    lambda row, index: conversions[key](row[key])                           # type: ignore
                )(key)    
        return result