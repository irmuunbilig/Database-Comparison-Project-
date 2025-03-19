from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
import csv
import time
from concurrent.futures import ThreadPoolExecutor

# Establish connection to the Cassandra cluster
cluster = Cluster(['localhost'])
session = cluster.connect()

# Drop the tables if they exist to ensure clean schema creation
session.execute("DROP TABLE IF EXISTS curriculum.professors")
session.execute("DROP TABLE IF EXISTS curriculum.schedules")
session.execute("DROP TABLE IF EXISTS curriculum.courses")

# Create keyspace and tables
session.execute("""
CREATE KEYSPACE IF NOT EXISTS curriculum 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

session.set_keyspace('curriculum')

session.execute("""
CREATE TABLE IF NOT EXISTS professors (
    prof_id int PRIMARY KEY,
    prof_name text,
    major text
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS schedules (
    course_id int,
    semester text,
    day_of_week text,
    start_time text,
    end_time text,
    PRIMARY KEY (course_id)
)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS courses (
    course_id int PRIMARY KEY,
    course_name text,
    prof_id int,
    credit_num int,
    campus text
)
""")

# File paths of the datasets
dataset_files = {
    'professors': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/professors_250k.csv',
    'schedules': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/schedules_250k.csv',
    'courses': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/courses_250k.csv'
}

# Corresponding table names in the Cassandra database
table_names = {
    'professors': 'professors',
    'schedules': 'schedules',
    'courses': 'courses'
}

def import_dataset(file_path, table_name):
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        start_time = time.time()

        # Print the column names to verify
        print(f"Columns in CSV file {file_path}: {reader.fieldnames}")

        # Rename 'day' to 'day_of_week' in schedules dataset
        if table_name == 'schedules':
            reader.fieldnames = ['course_id', 'semester', 'day_of_week', 'start_time', 'end_time']

        # Prepare the insert query
        insert_query = f"INSERT INTO {table_name} ({', '.join(reader.fieldnames)}) VALUES ({', '.join(['?']*len(reader.fieldnames))})"
        prepared_insert = session.prepare(insert_query)

        batch_size = 50  # Reduce batch size to avoid "Batch too large" error
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        count = 0
        
        for row in reader:
            # Convert appropriate columns to integers
            for column in reader.fieldnames:
                if column.endswith("_id") or column == "credit_num":
                    row[column] = int(row[column])

            # Add row to batch
            batch.add(prepared_insert.bind(row.values()))
            count += 1

            # Execute batch if batch size is reached
            if count % batch_size == 0:
                session.execute(batch)
                batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        # Execute any remaining rows in the batch
        if count % batch_size != 0:
            session.execute(batch)

    execution_time = time.time() - start_time
    print(f"Imported {table_name} dataset in {execution_time:.2f} seconds")

def parallel_import():
    with ThreadPoolExecutor(max_workers=5) as executor:  # Increase number of threads
        futures = []
        for table, file_path in dataset_files.items():
            futures.append(executor.submit(import_dataset, file_path, table_names[table]))
        for future in futures:
            future.result()  # Wait for all threads to complete

# Import each dataset
print("Starting parallel import...")
parallel_import()

# Close the Cassandra session
session.shutdown()
cluster.shutdown()
