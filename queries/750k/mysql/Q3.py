import mysql.connector
import time

# Database connection details
db_config = {
    'user': 'root',
    'password': '',
    'host': 'localhost',
    'database': 'curriculum'
}

# Function to establish a database connection
def connect_to_database():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except mysql.connector.Error as error:
        print("Error connecting to the database:", error)

# Function to execute a query and fetch results
def execute_query(connection, query):
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(query)
        result = cursor.fetchall()
        cursor.close()
        return result
    except mysql.connector.Error as error:
        print("Error executing query:", error)

# Connect to the database
connection = connect_to_database()

# Query: fetch the names of professors who teach at 'Papardo Campus' in any Spring semester
query = '''
    SELECT DISTINCT p.prof_name
    FROM curriculum.professors3 p
    JOIN curriculum.courses3 c ON p.prof_id = c.prof_id
    JOIN curriculum.schedules3 s ON c.course_id = s.course_id
    WHERE c.campus = 'Papardo' AND s.semester LIKE 'Spring%'
'''
start_time = time.time()
# Execute the query
results = execute_query(connection, query)
for result in results:
    print(result)
# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the database connection
connection.close()
