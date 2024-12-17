import pytest

def test_create_node(session):
    """Test case for creating a node in the Neo4j database.
    """
    session.run("CREATE (n:Person {name: 'Alice'})")
    result = session.run("MATCH (n:Person {name: 'Alice'}) RETURN n.name AS name")
    record = result.single()
    assert record["name"] == "Alice"


def test_create_relationship(session):
    """Test case for creating a relationship between nodes in the Neo4j database.
    """
    session.run("CREATE (a:Person {name: 'Alice'})")
    session.run("CREATE (b:Person {name: 'Bob'})")
    session.run(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)"
    )
    result = session.run(
        "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name AS name1, b.name AS name2"
    )
    record = result.single()
    assert record["name1"] == "Alice"
    assert record["name2"] == "Bob"


def test_query_with_driver(driver):
    """Example test case using the driver directly.
    """
    with driver.session() as session:
        session.run("CREATE (n:Person {name: 'Charlie'})")
        result = session.run("MATCH (n:Person {name: 'Charlie'}) RETURN n.name AS name")
        record = result.single()
        assert record["name"] == "Charlie"
