from pathlib import Path

import h5py
import neo4j


def convert_value_to_cypher(dataset):
    try:
        if dataset.dtype.str == "|O":  # first strings, because also true in 2nd if
            return dataset.asstr()[()]
        elif not dataset.shape and not dataset._is_empty:
            return dataset[()]
        else:
            return None
    except Exception:
        # print(f"For dataset with name {dataset.name}, the conversion did not work!")
        return None


def put_hdf5_in_neo4j(
    hdf5_filepath: Path,
    session: neo4j.Session,
    exclude_datasets: list[str] = [],
    exclude_groups: list[str] = [],
    exclude_paths: list = [],
    connect_to_filepath: list[Path] = None,
    batch_size: int = 1000,
    transfer_attrs: bool =True,
) -> None:
    """Put all of the contents of the hdf5 file into a neo4j graph database, supplied by session.

    This method traverses the complete hdf5-file, putting the datasets and groups between them in a neo4j graph.
    Datasets will be transformed into valid Cypher datatypes if possible, also checking if there is already a node with the same name and value, and then not duplicating it, but instead creating a relationship.
    The File node can be connected to other nodes via the connect_to_filepath list, where one supplies a list of filenames to connect to. The graph is then traversed and looks for nodes with a fitting filepath-property.

    Args:
        hdf5_filepath (Path): Path to hdf5 which should be transformed.
        session (neo4j.Session): Neo4j session instance with connected DBMS.
        exclude_datasets (list[str], optional): List of strings with names of datasets which should not be read in. Defaults to [].
        exclude_groups (list[str], optional): List of strings with names of groups which should not be read in. Defaults to [].
        exclude_paths (list[str], optional): List of strings with pathparts of datasets which should not be read in. Defaults to [].
        connect_to_filepath (list[Path], optional): List of Filepaths, which the File node should depend on. Defaults to None.
        batch_size (int, optional): Number of transactions stored in heap before commiting. Defaults to 1000.
        transfer_attrs (bool, optional): Wether the attrs of the HDF5-objects should be put into the database. Defaults to True.

    Returns:
        None
    """
    dataset_registry = []
    group_registry = []

    # Create visiting method to put datasets into the registry
    def visit(name, object):
        """Visit all items, check if it is dataset, if yes, put it into neo4j."""
        # decide if it is group or dataset
        if isinstance(object, h5py.Group):
            if object.parent.name == "/":
                parent = hdf5_filepath.name
            elif name.split("/")[-1] not in exclude_groups and not any(
                x in object.name for x in exclude_paths
            ):
                parent = object.parent.name
            else:
                return None  # Break early and not add the group to the registriy
            group_registry.append(
                {
                    "obj_name": object.name.split("/")[-1],
                    "hdf5_path": object.name,
                    # "filename": hdf5_filepath.name,
                    "parent": parent,
                    "attrs": dict(object.attrs)
                }
            )
            return None
        elif isinstance(object, h5py.Dataset):
            if name.split("/")[-1] not in exclude_datasets and not any(
                x in object.name for x in exclude_paths
            ):
                if object.parent.name == "/":
                    parent = hdf5_filepath.name
                else:
                    parent = object.parent.name  # save path, to check for that criteria when looking for parent node
                temp = {
                    "parent": parent,
                    "obj_name": object.name.split("/")[-1],
                    "hdf5_path": object.name,
                    "value": convert_value_to_cypher(object),
                    "attrs": dict(object.attrs)
                }
                dataset_registry.append(temp)
            else:
                pass
            return None
        else:
            return 0

    # Traverse the file and gather node information
    with h5py.File(hdf5_filepath, mode="r") as hdf:
        hdf.visititems(visit)

    # Create the file node
    session.run(
        """
            CREATE (f:File {name: $obj_name, filepath:$path})
            """,
        obj_name=hdf5_filepath.name,
        path=str(hdf5_filepath),
    )

    # Group query -> Group Nodes
    group_query = f"""
    CALL apoc.periodic.iterate(
        'UNWIND $group_list AS entry RETURN entry',
        'MATCH (f)
        WHERE (f:File AND f.name = entry.parent) OR (f:Group AND f.hdf5_path = entry.parent)
        CREATE (f)-[:holds]->(e:Group {{name: entry.obj_name, hdf5_path: entry.hdf5_path}})
        FOREACH (ignoreMe IN CASE WHEN $transfer_attrs THEN [1] ELSE [] END |
            SET dset += entry.attrs
        )
        {{batchSize: $batch_size, params: {{group_list: $group_list}}}}
    )
    YIELD batches, total RETURN batches, total
    """
    # Database query -> Database nodes
    dataset_query = f"""
    CALL apoc.periodic.iterate(
        'UNWIND $registry_list AS entry RETURN entry',
        'MATCH (e)
        WHERE (e:Group AND e.hdf5_path = entry.parent) OR (e:File AND e.name = entry.parent)
        FOREACH (ignoreMe IN CASE WHEN entry.value IS NULL THEN [1] ELSE [] END |
            CREATE (e)-[:holds]->(dset:Dataset {{name: entry.obj_name, hdf5_path: entry.hdf5_path}})
            FOREACH (ignoreMe IN CASE WHEN $transfer_attrs THEN [1] ELSE [] END |
                SET dset += entry.attrs
            )
        )
        FOREACH (ignoreMe IN CASE WHEN entry.value IS NOT NULL THEN [1] ELSE [] END |
            MERGE (dset:Dataset {{name: entry.obj_name, value: entry.value}})
                ON CREATE SET dset.hdf5_path = entry.hdf5_path
            MERGE (e)-[:holds]->(dset)
        )',
        {{batchSize: $batch_size, params: {{registry_list: $registry_list}}}}
    )
    YIELD batches, total RETURN batches, total
    """
    # To create tree structure, sort the groups depending on their depth, to create them in order
    max_nested = max([i["hdf5_path"].count("/") for i in group_registry])
    for t in range(1, max_nested + 1):
        groups_branch_t = [i for i in group_registry if t == i["hdf5_path"].count("/")]
        result = session.run(
            group_query, group_list=groups_branch_t, batch_size=batch_size, transfer_attrs=transfer_attrs
        )
        for record in result:
            print(
                f"Tree-Group Query Summary: Branch: {t}, Batches processed: {record['batches']}, Total entries processed: {record['total']}"
            )
    # Add Datasets to Tree
    result = session.run(
        dataset_query, registry_list=dataset_registry, batch_size=batch_size, transfer_attrs=transfer_attrs
    )
    for record in result:
        print(
            f"Dataset Query Summary: Batches processed: {record['batches']}, Total entries processed: {record['total']}"
        )
    if connect_to_filepath:
        result = session.run(
            """
                UNWIND $connect_path AS path
                MATCH (f:File {filepath: $filepath}), (c{filepath: path})
                CREATE (f)-[:depends_on]->(c)
            """,
            filepath=str(hdf5_filepath),
            connect_path=[str(i) for i in connect_to_filepath],
        )
