
# Currently not used!
# Reformulate as decorator?
def optimize_batch_size(session, initial_batch_size, max_retries=5):
    batch_size = initial_batch_size
    retries = 0

    while retries < max_retries:
        print(f"Testing with batch size: {batch_size}")
        try:
            result = session.run(group_query, group_list=group_registry, batch_size=batch_size, set_attrs=True)
            for record in result:
                print(
                    f"--- Batch Summary ---\n"
                    f"Batches Processed : {record['batches']}\n"
                    f"Total Entries     : {record['total']}\n"
                    f"Time Taken (ms)   : {record['timeTaken']}\n"
                    f"Failed Operations : {record['failedOperations']}\n"
                    f"------------------------------------"
                )
            # If successful, try a larger batch size
            batch_size *= 2
            retries = 0  # Reset retries if successful
        except neo4j.exceptions.TransientError as e:
            print(f"Memory error with batch size {batch_size}: {e}")
            # Reduce batch size and retry
            batch_size //= 2
            retries += 1

    print(f"Final optimized batch size: {batch_size}")
    return batch_size
