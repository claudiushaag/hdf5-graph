import argparse
from pathlib import Path
from neo4j import GraphDatabase
from hdf5_graph.handle_structure import put_dir_in_neo4j
from hdf5_graph.single_hdf5 import put_hdf5_in_neo4j


def gen_parser():
    parser = argparse.ArgumentParser(
        description="Command line interface for putting HDF5 data into a Neo4j graph database."
    )

    subparsers = parser.add_subparsers(dest="command")

    # Common arguments for both commands
    common_parser = argparse.ArgumentParser(add_help=False)
    # Global arguments for Neo4j connection
    common_parser.add_argument(
        "--uri",
        type=str,
        default="neo4j://localhost",
        help="URI for the Neo4j database (default: neo4j://localhost).",
    )
    common_parser.add_argument(
        "--username",
        type=str,
        default="neo4j",
        help="Username for the Neo4j database (default: neo4j).",
    )
    common_parser.add_argument(
        "--password",
        type=str,
        default="neo4jadmin",
        help="Password for the Neo4j database (default: neo4jadmin).",
    )
    common_parser.add_argument(
        "--database",
        type=str,
        default="neo4j",
        help="Database name for the Neo4j database (default: neo4j).",
    )
    common_parser.add_argument(
        "--exclude-datasets",
        nargs="*",
        default=[],
        help="List of dataset names to exclude.",
    )
    common_parser.add_argument(
        "--exclude-paths",
        nargs="*",
        default=[],
        help="List of path parts of datasets to exclude.",
    )
    common_parser.add_argument(
        "--use-experiment",
        action="store_true",
        help="Flag to introduce experiment nodes derived from groups.",
    )
    common_parser.add_argument(
        "--experiment-path",
        type=str,
        default="/",
        help="HDF5 path to the folder of experiment nodes.",
    )
    common_parser.add_argument(
        "--connect-to-filepath",
        nargs="*",
        type=Path,
        default=None,
        help="List of file paths to which the File node should be connected.",
    )
    common_parser.add_argument(
        "--batchsize",
        type=int,
        default=1000,
        help="Number of transactions stored in heap before commiting.",
    )

    # Parser for the 'file' command
    parser_file = subparsers.add_parser(
        "file",
        help="""Put all of the contents of the hdf5 file into a neo4j graph database, supplied by session.
            This method traverses the complete hdf5-file, putting the datasets and possibly groups as 'experiments' between them in a neo4j graph.
            Datasets will be transformed into valid Cypher datatypes if possible, also checking if there is already a node with the same name and value, and then not duplicating it, but instead creating a relationship.
            The File node can be connected to other nodes via the connect_to_filepath list, where one supplies a list of filenames to connect to. The graph is then traversed and looks for nodes with a fitting filepath-property.""",
        parents=[common_parser],
    )
    parser_file.add_argument(
        "hdf5_filepath", type=Path, help="Path to the HDF5 file to be transformed."
    )

    # Parser for the 'directory' command
    parser_dir = subparsers.add_parser(
        "directory",
        help="""
            Traverse a directory, and put all found h5-files into neo4j, making them dependent on each other, based on the nesting.

            Keyword arguments are supplied to the ``put_hdf5_in_neo4j`` function
            """,
        parents=[common_parser],
    )
    parser_dir.add_argument(
        "dir_path", type=Path, help="Path to the directory to be traversed."
    )

    return parser


def main():
    parser = gen_parser()

    args = parser.parse_args()

    with GraphDatabase.driver(
        args.uri, auth=(args.username, args.password), database=args.database
    ) as driver:
        with driver.session() as session:
            if args.command == "file":
                put_hdf5_in_neo4j(
                    hdf5_filepath=args.hdf5_filepath,
                    session=session,
                    exclude_datasets=args.exclude_datasets,
                    exclude_paths=args.exclude_paths,
                    use_experiment=args.use_experiment,
                    experiment_path=args.experiment_path,
                    connect_to_filepath=args.connect_to_filepath,
                )
            elif args.command == "directory":
                put_dir_in_neo4j(
                    dir_path=args.dir_path,
                    session=session,
                    exclude_datasets=args.exclude_datasets,
                    exclude_paths=args.exclude_paths,
                    use_experiment=args.use_experiment,
                    experiment_path=args.experiment_path,
                    connect_to_filepath=args.connect_to_filepath,
                )


if __name__ == "__main__":
    main()
