from cassandra.cluster import Cluster
import time

# Database connection details
db_config = {
    'contact_points': ['localhost']
}

# Function to establish a database connection
def connect_to_database():
    try:
        cluster = Cluster(**db_config)
        connection = cluster.connect()
        return connection
    except Exception as error:
        print("Error connecting to the database:", error)

# Function to execute a query and fetch results
def execute_query(session, query):
    try:
        result = session.execute(query)
        return result
    except Exception as error:
        print("Error executing query:", error)
        return None

# Connect to Cassandra
session = connect_to_database()

# Specify the keyspace
keyspace = 'curriculum'
session.set_keyspace(keyspace)

# Query: Fetch all prof_ids and campus from courses2 table where campus is Papardo
query_prof_ids = "SELECT prof_id, campus FROM courses2 WHERE campus = 'Papardo' ALLOW FILTERING;"
start_time = time.time()

# Execute the query to get prof_ids
prof_ids_result = execute_query(session, query_prof_ids)

# Extract prof_ids from the query result
prof_ids = [(row.prof_id, row.campus) for row in prof_ids_result]

# Ensure we have prof_ids to query
if not prof_ids:
    print("No professors found at Papardo campus.")
else:
    print(f"Professor IDs at Papardo campus: {prof_ids}")

    # Construct query to get professor details using the prof_ids
    prof_ids_str = ', '.join(map(lambda x: str(x[0]), prof_ids))
    query_prof_details = f"SELECT prof_id, prof_name FROM professors2 WHERE prof_id IN ({prof_ids_str})"

    # Execute the query to get professor details
    prof_details_result = execute_query(session, query_prof_details)

    # Create a dictionary for professor names
    prof_details_dict = {prof.prof_id: prof.prof_name for prof in prof_details_result}

    # Print the professor details along with campus
    if prof_details_dict:
        print("Professors working at Papardo campus:")
        for prof_id, campus in prof_ids:
            if prof_id in prof_details_dict:
                print(f"Professor Name: {prof_details_dict[prof_id]}, Campus: {campus}")
    else:
        print("No professor details found.")

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the Cassandra session
session.shutdown()
