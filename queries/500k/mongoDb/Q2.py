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

# Step 1: Fetch all courses2 taught at Papardo
course_query = {'campus': 'Papardo'}
courses = execute_query(database['courses2'], course_query)

# Extract the professor IDs from the courses
prof_ids = {course['prof_id'] for course in courses}

# Step 2: Fetch the names of professors2 with the extracted prof_ids
professor_query = {'prof_id': {'$in': list(prof_ids)}}
professors = execute_query(database['professors2'], professor_query)

# Prepare the list of professor names and campus
professor_details = []

# Print the names of professors and their campus
for professor in professors:
    # Handle the case where 'prof_name' might not exist
    if 'prof_name' in professor:
        professor_details.append({
            'prof_name': professor['prof_name'],
            'campus': 'Papardo'  # Since we are only fetching professors who teach at Papardo
        })

# Print the professor names and campus
for detail in professor_details:
    print(f"Professor Name: {detail['prof_name']}, Campus: {detail['campus']}")

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the database connection
database.client.close()
