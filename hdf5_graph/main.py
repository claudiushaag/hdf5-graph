from neo4j import GraphDatabase
import neo4j
import h5py
from pathlib import Path


def convert_value_to_cypher(dataset):
    try:
        if dataset.dtype.str == "|O":  # first strings, because also true in 2nd if
            return dataset.asstr()[()]
        elif not dataset.shape and not dataset._is_empty:
            return dataset[()]
        else:
            return None
    except:
        return None


def put_hdf5_in_neo4j(
    hdf5_filepath: Path,
    session: neo4j.Session,
    exclude_datasets: list = [],
    exclude_paths: list = [],
    use_experiment: bool =False,
    experiment_path: str ="/",
    connect_to_filepath: list[Path] =None
):
    """
    Put all of the contents of the hdf5 file into a neo4j graph database, supplied by session.

    Args:
        hdf5_filepath (Path): _description_
        session (neo4j.Session): _description_
        exclude_datasets (list, optional): _description_. Defaults to [].
        exclude_paths (list, optional): _description_. Defaults to [].
        use_experiment (bool, optional): Flag for introducing experiment nodes, derived from groups. Defaults to False.
        experiment_path (str, optional): HDF5-Path in file to the folder of the experiment nodes. Defaults to "/".
        connect_to_filepath (list[Path], optional): List of Filepaths, which the File node should depend on. Defaults to None.

    Returns:
        _type_: _description_
    """

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
                if use_experiment:
                    parent = object.parent.name.split("/")[1]
                else:
                    parent = hdf5_filepath.name
                temp = {
                    "parent": parent,
                    "obj_name": object.name.split("/")[-1],
                    "hdf5_path": object.name,
                    "value": convert_value_to_cypher(object),
                }
                dataset_registry.append(temp)
            else:
                pass
            return None
        else:
            return 0

    with h5py.File(hdf5_filepath, mode="r") as hdf:
        if use_experiment:
            for group in hdf[experiment_path].values():
                group_registry.append(
                    {
                        "obj_name": group.name.split("/")[1],
                        "hdf5_path": group.name,
                        "filename": hdf5_filepath.name,
                    }
                )
        # Go through datasets
        hdf.visititems(visit)

    # Create the file node
    session.run(
        """
            CREATE (f:File {name: $obj_name, filepath:$path})
            """,
        obj_name=hdf5_filepath.name,
        path=str(hdf5_filepath),
    )

    # Group query -> Experiment Nodes
    group_query = """
            WITH $group_list AS data
            UNWIND data AS entry
            MATCH (f:File {name: entry.filename})
            CREATE (f)-[:holds]->(e:Experiment {name: entry.obj_name, hdf5_path:entry.hdf5_path})
            """
    # Database query -> Database nodes
    query = """
    WITH $registry_list AS data
    UNWIND data AS entry
    MATCH (e)
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

    if use_experiment:
        session.run(group_query, group_list=group_registry)
    result = session.run(query, registry_list=dataset_registry)
    summary = result.consume()
    # Extracting summary data
    nodes_created = summary.counters.nodes_created
    relationships_created = summary.counters.relationships_created
    time_taken = summary.result_available_after

    # Print the summary using f-strings
    print(
        f"Query executed successfully.\n"
        f"Nodes created: {nodes_created}\n"
        f"Relationships created: {relationships_created}\n"
        f"Time taken (ms): {time_taken}"
    )
    if connect_to_filepath:
        result = session.run("""
                             UNWIND $connect_path AS path
                             MATCH (f:File {filepath: $filepath}), (c{filepath: path})
                             CREATE (f)-[:depends_on]->(c)
                            """, filepath=str(hdf5_filepath), connect_path=[str(i) for i in connect_to_filepath])


if __name__ == "__main__":
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "neo4jadmin")
    DATABASE = "neo4j"

    filepath = Path("data/CompleteData.h5")

    with GraphDatabase.driver(URI, auth=AUTH, database=DATABASE) as driver:
        with driver.session() as session:
            session.run("""
                        MATCH (n)
                        DETACH DELETE n
                        """)
            put_hdf5_in_neo4j(filepath, session, use_experiment=True)
