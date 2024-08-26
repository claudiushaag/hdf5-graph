import subprocess
import pytest
from conftest import NEO4J_URI, AUTH, DATABASE

command = "hdf5_graph/hdf5-graph.py"


def test_cli_create_database(session):
    filepath = "data/CompleteData.h5"

    # Run the CLI command for the 'file' subcommand
    result = subprocess.run(
        [
            "python",
            command,
            "file",
            filepath,
            "--use-experiment",
            "--exclude-paths",
            "/Spline/",
            "--uri",
            NEO4J_URI,
            "--username",
            AUTH[0],
            "--password",
            AUTH[1],
            "--database",
            DATABASE,
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"CLI command failed with stderr: {result.stderr}"

    # Verify the database content
    query_result = session.run("""
                MATCH (d:Dataset{name:'datapoints'})<-[:holds]-(e:Experiment)-[:holds]->(c:Dataset{name:'dt',value:0.00025})
                WHERE d.hdf5_path CONTAINS 'Curve_1'
                RETURN d.hdf5_path AS hdf5_path
                """)

    query_result = session.run("""
                MATCH (n)
                RETURN count(n)
                """)

    assert query_result.single()[0] == 499, "The number of nodes is not as expected!"


def test_cli_handle_structure(session):
    dir_path = "data/ng5"

    # Run the CLI command for the 'directory' subcommand
    result = subprocess.run(
        [
            "python",
            command,
            "directory",
            dir_path,
            "--uri",
            NEO4J_URI,
            "--username",
            AUTH[0],
            "--password",
            AUTH[1],
            "--database",
            DATABASE,
        ],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"CLI command failed with stderr: {result.stderr}"

    # Verify the database content
    query_result = session.run("""
                MATCH (n)
                RETURN count(n)
                """)

    assert query_result.single()[0] == 296, "The number of nodes is not as expected!"
