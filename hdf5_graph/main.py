from neo4j import GraphDatabase

if __name__ == "__main__":
    # URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
    URI = "neo4j://localhost"
    AUTH = ("neo4j", "neo4jadmin")

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()

        # Get the name of all 42 year-olds
        records, summary, keys = driver.execute_query(
            """MATCH (a:Person {name:'Tom Hanks'})-[:ACTED_IN]->(m)<-[:ACTED_IN]-(coActors),
                (coActors)-[:ACTED_IN]->(m2)<-[:ACTED_IN]-(cocoActors)
            WHERE NOT (a)-[:ACTED_IN]->()<-[:ACTED_IN]-(cocoActors) AND a <> cocoActors
            RETURN cocoActors.name AS Recommended, count(*) AS Strength ORDER BY Strength DESC""",
            # age=42,
            database_="neo4j",
        )

        # Loop through results and do something with them
        for person in records:
            print(person)

        # Summary information
        print("The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query, records_count=len(records),
            time=summary.result_available_after,
        ))