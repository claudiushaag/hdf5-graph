from neo4j import GraphDatabase
import h5py
from pathlib import Path

# def add_dataset_to_neo4j(session, dataset):
#     """
#     Add a dataset to neo4j

#     Args:
#         session (_type_): _description_
#         object (_type_): _description_
#     """
#     def _write(tx, dataset):
#         result = tx.run("""
#                 CREATE ()
#                 """,
#                 filter=name_filter)
#         return list(result)
#     session.execute_write(_write, dataset)

def convert_value_to_cypher(dataset):
    try:
        if not dataset.shape:
            return dataset[()]
        else:
            return None
    except:
        return None

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
        if name.split("/")[-1] in ["macrofail", "Spline_Interp", "datapoints", "dt", "ca", "ma", "jb"]:
            result=driver.execute_query("""
            MATCH (e:Experiment)
            WHERE $parent = e.name
            FOREACH (ignoreMe IN CASE WHEN $value IS NULL THEN [1] ELSE [] END |
                CREATE (e)-[:holds]->(dset:Dataset {name: $obj_name, hdf5_path: $hdf5_path})
                // CREATE (dset:Dataset {name: $obj_name, hdf5_path: $hdf5_path})
            )
            FOREACH (ignoreMe IN CASE WHEN $value IS NOT NULL THEN [1] ELSE [] END |
                MERGE (dset:Dataset {name: $obj_name, value: $value})
                    ON CREATE
                        SET dset.hdf5_path = $hdf5_path
                // MATCH (dset:Dataset {name: $obj_name, value: $value})
                CREATE (e)-[:holds]->(dset)
            )
            """,parent=object.parent.name.split("/")[1],
            obj_name=object.name.split("/")[-1],
            hdf5_path=object.name,
            value=convert_value_to_cypher(object))
            summary = result.summary
            print("Created {nodes_created} nodes in {time} ms.".format(
                        nodes_created=summary.counters.nodes_created,
                        time=summary.result_available_after
                    ))
        else:
            pass
        return None
    else:
        return 0

if __name__ == "__main__":
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "neo4jadmin")

    filepath = Path("data/CompleteData.h5")

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()

        # DELETE everything
        driver.execute_query("""
                        MATCH (n)
                        DETACH DELETE n
                        """)
        data = []
        with h5py.File(filepath, mode="r") as hdf:
            # Create the file node
            summary=driver.execute_query("""
                        CREATE (f:File {name: $obj_name, filepath:$path})
                        """,
                        obj_name=filepath.name,
                        path=str(filepath))
            for group in hdf.values():
                # Create the experiment node
                summary=driver.execute_query("""
                        MATCH (f:File {name: $filename})
                        CREATE (f)-[:holds]->(e:Experiment {name: $obj_name, hdf5_path:$hdf5_path})
                        """,
                        obj_name=group.name.split("/")[1],
                        hdf5_path=group.name,
                        filename = filepath.name
                        ).summary
                print("Created {nodes_created} nodes in {time} ms.".format(
                            nodes_created=summary.counters.nodes_created,
                            time=summary.result_available_after
                        ))
                group.visititems(visit)