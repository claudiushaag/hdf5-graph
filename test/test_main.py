import pytest
from pathlib import Path
from hdf5_graph.single_hdf5 import put_hdf5_in_neo4j
from hdf5_graph.handle_structure import put_dir_in_neo4j


def test_create_database(session):
    """
    Example test case using the driver directly.
    """
    filepath = Path("data/CompleteData.h5")
    put_hdf5_in_neo4j(
        filepath, session, use_experiment=True, exclude_paths=["/Spline/"]
    )

    result = session.run("""
                MATCH (d:Dataset{name:'datapoints'})<-[:holds]-(e:Experiment)-[:holds]->(c:Dataset{name:'dt',value:0.00025})
                WHERE d.hdf5_path CONTAINS 'Curve_1'
                RETURN d.hdf5_path AS hdf5_path
                """)

    result = session.run("""
                MATCH (n)
                RETURN count(n)
                """)

    assert result.single()[0] == 499, "The number of nodes is not as expected!"


def test_handle_structure(session):
    dir_path = Path("data/ng5")
    put_dir_in_neo4j(dir_path, session)

    result = session.run("""
                MATCH (n)
                RETURN count(n)
                """)

    assert result.single()[0] == 296, "The number of nodes is not as expected!"
