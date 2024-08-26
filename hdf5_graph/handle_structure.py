from hdf5_graph.main import put_hdf5_in_neo4j, convert_value_to_cypher
from neo4j import GraphDatabase
import neo4j
from pathlib import Path
import h5py

def put_dir_in_neo4j(dir_path: Path, session: neo4j.Session, **kwargs) -> None :
    """
    Traverse a directory, and put all found h5-files into neo4j, making them dependent on each other, based on the nesting.

    Keyword arguments are supplied to the ``put_hdf5_in_neo4j`` function

    Args:
        dir_path (Path): Path to directory, which should be traversed.
        session (neo4j.Session): Open neo4j-Session.
    """
    # handle kwargs, as connected_to_filepath is set by function itself:
    kwargs = {k:v for k,v in kwargs.items() if k != 'connected_to_filepath'}
    def _find_h5_files(path, level=1, accumulated_files=None):
        # Initialize a list for the current level if it doesn't exist
        # if level not in h5_files_dict:
        #     h5_files_dict[level] = []

        # If accumulated_files is None, create an empty list
        if accumulated_files is None:
            accumulated_files = []

        # Find all .h5 files in the current directory
        current_files = list(path.glob('*.h5'))
        # accumulated_files.extend(current_files)

        # Add all accumulated .h5 files to the current level
        for i in current_files:
            put_hdf5_in_neo4j(i, session, connect_to_filepath=accumulated_files, **kwargs)

        # Recursively traverse subdirectories
        for subdir in path.iterdir():
            if subdir.is_dir():
                # Pass down the accumulated files to the subdirectory
                _find_h5_files(subdir, level + 1, current_files.copy())

    _find_h5_files(dir_path)


if __name__ == "__main__":

    URI = "neo4j://localhost"
    AUTH = ("neo4j", "neo4jadmin")
    DATABASE = "neo4j"

    filepath = Path("data/ng5/id0/pre1/ng5_id0_mass_matrix.h5")

    # dir_path = Path("data/ng5/id0/pre1/sim1")
    dir_path = Path("data/ng5")

    with GraphDatabase.driver(URI, auth=AUTH, database=DATABASE) as driver:
        with driver.session() as session:
            session.run("""
                        MATCH (n)
                        DETACH DELETE n
                        """)

            put_dir_in_neo4j(dir_path, session)