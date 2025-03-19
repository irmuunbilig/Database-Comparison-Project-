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
        return None

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
        return None

# Connect to the database
connection = mysql.connector.connect(**db_config)

if connection:
    # Step 1: Check data in professors2
    query = 'SELECT * FROM professors2 LIMIT 5'
    results = execute_query(connection, query)
    print("Data in professors2:", results)

    # Step 2: Check data in courses2
    query = 'SELECT * FROM courses2 LIMIT 5'
    results = execute_query(connection, query)
    print("Data in courses2:", results)

    # Step 3: Check data in schedules2
    query = 'SELECT * FROM schedules2 LIMIT 5'
    results = execute_query(connection, query)
    print("Data in schedules2:", results)

    # Step 4: Check join between professors2 and courses2
    query = '''
        SELECT p.prof_name, c.course_id
        FROM professors2 p
        JOIN courses2 c ON p.prof_id = c.prof_id
        WHERE c.campus = 'Papardo'
        LIMIT 5
    '''
    results = execute_query(connection, query)
    print("Join between professors2 and courses2:", results)

    # Step 5: Check join between professors2, courses2, and schedules2
    query = '''
        SELECT p.prof_name, c.course_id, s.semester
        FROM professors2 p
        JOIN courses2 c ON p.prof_id = c.prof_id
        JOIN schedules2 s ON c.course_id = s.course_id
        WHERE c.campus = 'Papardo' AND s.semester LIKE 'Spring%'
        LIMIT 5
    '''
    results = execute_query(connection, query)
    print("Join between professors2, courses2, and schedules2:", results)

    # Step 6: Final query to fetch the names of professors who teach at 'Papardo Campus' in any Spring semester
    query = '''
        SELECT DISTINCT p.prof_name
        FROM professors2 p
        JOIN courses2 c ON p.prof_id = c.prof_id
        JOIN schedules2 s ON c.course_id = s.course_id
        WHERE c.campus = 'Papardo' AND s.semester LIKE 'Spring%'
    '''
    start_time = time.time()
    results = execute_query(connection, query)

    if results:
        for result in results:
            print(result)
    else:
        print("No results found or error executing query.")

    # Calculate and print the execution time
    execution_time = time.time() - start_time
    print("Execution time:", execution_time, "seconds")

    # Close the database connection
    connection.close()
else:
    print("Failed to connect to the database.")