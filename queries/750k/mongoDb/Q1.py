from pymongo import MongoClient
import time

# Database connection details
mongo_config = {
    'host': 'localhost',
    'port': 27017,
    'database': 'curriculum'
}

start_time = time.time()

# Function to establish a database connection
def connect_to_database():
    try:
        client = MongoClient(mongo_config['host'], mongo_config['port'])
        return client[mongo_config['database']]
    except Exception as error:
        print("Error connecting to the database:", error)

# Function to execute a query and fetch results
def execute_query(collection, query):
    try:
        result = collection.find(query)
        return result
    except Exception as error:
        print("Error executing query:", error)

# Connect to the database
database = connect_to_database()

# Query: Fetch all schedules where the semester is like 'Spring%'
query = {
    'semester': {'$regex': '^Spring'}
}

# Execute the query
results = execute_query(database['schedules3'], query)  # Changed from 'schedules2' to 'schedules3'
for result in results:
    print(result)

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the database connection
database.client.close()
