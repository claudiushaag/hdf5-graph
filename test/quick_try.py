from pathlib import Path
from hdf5_graph.single_hdf5 import put_hdf5_in_neo4j
from hdf5_graph.handle_structure import put_dir_in_neo4j
from neo4j import GraphDatabase

# Define the URI and authentication for the Neo4j test database
NEO4J_URI = "neo4j://localhost"
AUTH = ("neo4j", "neo4jadmin")
DATABASE = "testing"

def test_create_database(session):
    """
    Example test case using the driver directly.
    """
    filepath = Path("data/CompleteData.h5")
    put_hdf5_in_neo4j(
        filepath, session,
        # exclude_paths=["/Spline/"],
        # exclude_groups=["Spline"],
        # exclude_datasets=["kb"]
    )

if __name__ == "__main__":

    driver = GraphDatabase.driver(NEO4J_URI, auth=AUTH, database=DATABASE)
    with driver.session() as session:
        # Clean the database before running each test
        session.run("MATCH (n) DETACH DELETE n")
        test_create_database(session=session)
        # Optional: Clean the database after each test
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()