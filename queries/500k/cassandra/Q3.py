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

# Step 1: Query the schedules2 table for all course_ids and semesters
query_all_courses = "SELECT course_id, semester FROM schedules2 ALLOW FILTERING;"
start_time = time.time()
all_courses_result = execute_query(session, query_all_courses)

# Step 2: Filter for Spring semester in application code
spring_course_ids = [(row.course_id, row.semester) for row in all_courses_result if row.semester.startswith('Spring')]

# Step 3: Query the courses2 table for prof_ids and campus for the above course_ids where campus is Papardo
prof_ids_and_campuses = []
for course_id, semester in spring_course_ids:
    query_course = f"SELECT prof_id, campus FROM courses2 WHERE course_id = {course_id} AND campus = 'Papardo' ALLOW FILTERING;"
    course_result = execute_query(session, query_course)
    for row in course_result:
        prof_ids_and_campuses.append((row.prof_id, row.campus, semester))

# Ensure we have prof_ids to query
if not prof_ids_and_campuses:
    print("No professors found at Papardo campus in Spring semester.")
else:
    # Extract unique prof_ids
    prof_ids = list(set([prof_id for prof_id, _, _ in prof_ids_and_campuses]))
    prof_ids_str = ', '.join(map(str, prof_ids))

    # Step 4: Query the professors2 table for prof_name using the prof_ids
    query_prof_details = f"SELECT prof_id, prof_name FROM professors2 WHERE prof_id IN ({prof_ids_str})"
    prof_details_result = execute_query(session, query_prof_details)

    # Create a dictionary for professor names
    prof_details_dict = {prof.prof_id: prof.prof_name for prof in prof_details_result}

    # Step 5: Print the professor details along with campus and semester
    print("Professors working at Papardo campus in Spring semester:")
    for prof_id, campus, semester in prof_ids_and_campuses:
        if prof_id in prof_details_dict:
            print(f"Professor Name: {prof_details_dict[prof_id]}, Campus: {campus}, Semester: {semester}")

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the Cassandra session
session.shutdown()
