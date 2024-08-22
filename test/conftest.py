import pytest
from neo4j import GraphDatabase

# Define the URI and authentication for the Neo4j test database
NEO4J_URI = "neo4j://localhost"
AUTH = ("neo4j", "neo4jadmin")
DATABASE = "testing"


@pytest.fixture(scope="session")
def driver():
    """
    Pytest fixture to create and close the Neo4j driver for the session.
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=AUTH, database=DATABASE)
    yield driver
    driver.close()


@pytest.fixture(scope="function")
def session(driver):
    """
    Pytest fixture to create a new session for each test function and clean up the database.
    """
    with driver.session() as session:
        # Clean the database before running each test
        session.run("MATCH (n) DETACH DELETE n")
        yield session
        # Optional: Clean the database after each test
        session.run("MATCH (n) DETACH DELETE n")
