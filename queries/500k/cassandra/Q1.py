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

# Query: Fetch all course_ids and semesters from schedules2 table
query_all_courses = "SELECT course_id, semester FROM schedules2 ALLOW FILTERING;"
start_time = time.time()

# Execute the query to get all course_ids and semesters
all_courses_result = execute_query(session, query_all_courses)

# Filter for Spring semester in application code
spring_courses = [(row.course_id, row.semester) for row in all_courses_result if row.semester.startswith('Spring')]

# Ensure we have spring courses to display
if not spring_courses:
    print("No courses found for Spring semester.")
else:
    print("Courses offered in the Spring semester:")
    for course_id, semester in spring_courses:
        print(f"Course ID: {course_id}, Semester: {semester}")

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the Cassandra session
session.shutdown()
