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

# Step 1: Fetch all schedules for Spring semesters
schedule_query = {'semester': {'$regex': '^Spring'}}
schedules = execute_query(database['schedules'], schedule_query)

# Extract the course IDs from the schedules
course_ids = {schedule['course_id'] for schedule in schedules}

# Step 2: Fetch courses taught at Papardo Campus with the extracted course_ids
course_query = {'campus': 'Papardo', 'course_id': {'$in': list(course_ids)}}
courses = execute_query(database['courses'], course_query)

# Extract the professor IDs from the courses
prof_ids = {course['prof_id'] for course in courses}

# Step 3: Fetch the names of professors with the extracted prof_ids
professor_query = {'prof_id': {'$in': list(prof_ids)}}
professors = execute_query(database['professors'], professor_query)

# Prepare the list of professor names
professor_names = []

# Print the names of professors
for professor in professors:
    # Print the entire document for debugging
    print(professor)
    # Handle the case where 'prof_name' might not exist
    if 'prof_name' in professor:
        professor_names.append(professor['prof_name'])
    else:
        print("prof_name not found in document:", professor)

# Print the professor names
print("Professors teaching at Papardo in Spring:", professor_names)

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the database connection
database.client.close()
