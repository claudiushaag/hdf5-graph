from neo4j import GraphDatabase
import neo4j
import h5py
from pathlib import Path

def convert_value_to_cypher(dataset):
    try:
        if dataset.dtype.str == '|O':  # first strings, because also true in 2nd if
            return dataset.asstr()[()]
        elif not dataset.shape and not dataset._is_empty:
            return dataset[()]
        else:
            return None
    except:
        return None

def put_hdf5_in_neo4j(hdf5_filepath: Path,
                      session: neo4j.Session,
                      exclude_datasets: list =[],
                      exclude_paths: list =[]
                      ):

    # DELETE everything
    session.run("""
                    MATCH (n)
                    DETACH DELETE n
                    """)

    dataset_registry = []
    group_registry = []

    # Create visiting method to put datasets into the registry
    def visit(name, object):
        """
        Visit all items, check if it is dataset, if yes, put it into neo4j.

        Args:
            name (_type_): _description_
            object (_type_): _description_

        Returns:
            _type_: _description_
        """
        # decide if it is group or dataset
        if isinstance(object, h5py.Group):
            return None
        elif isinstance(object, h5py.Dataset):
            # Do sth!
            # add_dataset_to_neo4j(session, object)
            # if name.split("/")[-1] in ["macrofail", "Spline_Interp", "datapoints", "dt", "ca", "ma", "jb"]:
            if "/Spline/" not in object.name:
                temp = {
                    "parent" :object.parent.name.split("/")[1],
                    "obj_name":object.name.split("/")[-1],
                    "hdf5_path":object.name,
                    "value":convert_value_to_cypher(object)
                }
                dataset_registry.append(temp)
            else:
                pass
            return None
        else:
            return 0

    with h5py.File(hdf5_filepath, mode="r") as hdf:
        for group in hdf.values():
            group_registry.append({"obj_name": group.name.split("/")[1], "hdf5_path": group.name, "filename": hdf5_filepath.name })
            group.visititems(visit)

    # Create the file node
    session.run("""
            CREATE (f:File {name: $obj_name, filepath:$path})
            """,
            obj_name=hdf5_filepath.name,
            path=str(hdf5_filepath))

    # Group query -> Experiment Nodes
    group_query="""
            WITH $group_list AS data
            UNWIND data AS entry
            MATCH (f:File {name: entry.filename})
            CREATE (f)-[:holds]->(e:Experiment {name: entry.obj_name, hdf5_path:entry.hdf5_path})
            """
    # Database query -> Database nodes
    query = """
    WITH $registry_list AS data
    UNWIND data AS entry
    MATCH (e:Experiment)
    WHERE e.name = entry.parent
    FOREACH (ignoreMe IN CASE WHEN entry.value IS NULL THEN [1] ELSE [] END |
        CREATE (e)-[:holds]->(dset:Dataset {name: entry.obj_name, hdf5_path: entry.hdf5_path})
    )
    FOREACH (ignoreMe IN CASE WHEN entry.value IS NOT NULL THEN [1] ELSE [] END |
        MERGE (dset:Dataset {name: entry.obj_name, value: entry.value})
            ON CREATE SET dset.hdf5_path = entry.hdf5_path
        MERGE (e)-[:holds]->(dset)
    )
    """

    session.run(group_query, group_list=group_registry)
    result = session.run(query, registry_list=dataset_registry)
    summary = result.consume()
    # Extracting summary data
    nodes_created = summary.counters.nodes_created
    relationships_created = summary.counters.relationships_created
    time_taken = summary.result_available_after

    # Print the summary using f-strings
    print(f"Query executed successfully.\n"
        f"Nodes created: {nodes_created}\n"
        f"Relationships created: {relationships_created}\n"
        f"Time taken (ms): {time_taken}")

if __name__ == "__main__":
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "neo4jadmin")
    DATABASE = "neo4j"

    filepath = Path("data/CompleteData.h5")

    with GraphDatabase.driver(URI, auth=AUTH, database=DATABASE) as driver:
        with driver.session() as session:
            put_hdf5_in_neo4j(filepath, session)