import pytest
from pathlib import Path
from hdf5_graph.main import put_hdf5_in_neo4j

def test_create_database(driver):
    """
    Example test case using the driver directly.
    """
    filepath = Path("data/CompleteData.h5")
    put_hdf5_in_neo4j(filepath, driver)

    with driver.session() as session:
        result = session.run("""
                    MATCH (d:Dataset{name:'datapoints'})<-[:holds]-(e:Experiment)-[:holds]->(c:Dataset{name:'dt',value:0.00025})
                    WHERE d.hdf5_path CONTAINS 'Curve_1'
                    RETURN d.hdf5_path AS hdf5_path
                    """)