from neo4j import GraphDatabase
import csv
import time

# Database connection details
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"

# File paths of the datasets
dataset_files = {
    'professors': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/professors_250k.csv',
    'schedules': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/schedules_250k.csv',
    'courses': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/courses_250k.csv'
}

BATCH_SIZE = 1000  # Adjust batch size as needed

class Neo4jImporter:
    
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.create_indexes()

    def create_indexes(self):
        with self.driver.session() as session:
            session.run("CREATE INDEX IF NOT EXISTS FOR (p:Professor) ON (p.prof_id)")
            session.run("CREATE INDEX IF NOT EXISTS FOR (c:Course) ON (c.course_id)")
            session.run("CREATE INDEX IF NOT EXISTS FOR (s:Schedule) ON (s.course_id)")

    def close(self):
        self.driver.close()

    def import_data(self, dataset, import_function):
        with self.driver.session() as session:
            with open(dataset_files[dataset], 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                batch = []
                total_records = 0
                for row in reader:
                    batch.append(row)
                    if len(batch) == BATCH_SIZE:
                        session.execute_write(import_function, batch)
                        total_records += len(batch)
                        print(f"Uploaded {BATCH_SIZE} {dataset[:-1].capitalize()} records.")
                        batch = []
                if batch:
                    session.execute_write(import_function, batch)
                    total_records += len(batch)
                    print(f"Uploaded remaining {len(batch)} {dataset[:-1].capitalize()} records.")
                print(f"Total {dataset[:-1].capitalize()} records uploaded: {total_records}")

    @staticmethod
    def _create_professor(tx, batch):
        query = """
        UNWIND $rows AS row
        CREATE (p:Professor {prof_id: toInteger(row.prof_id), prof_name: row.prof_name, major: row.major})
        """
        tx.run(query, rows=batch)

    @staticmethod
    def _create_schedule(tx, batch):
        query = """
        UNWIND $rows AS row
        CREATE (s:Schedule {course_id: toInteger(row.course_id), semester: row.semester, day: row.day, start_time: row.start_time, end_time: row.end_time})
        """
        tx.run(query, rows=batch)

    @staticmethod
    def _create_course(tx, batch):
        query = """
        UNWIND $rows AS row
        CREATE (c:Course {course_id: toInteger(row.course_id), course_name: row.course_name, prof_id: toInteger(row.prof_id), credit_num: toInteger(row.credit_num), campus: row.campus})
        """
        tx.run(query, rows=batch)

    @staticmethod
    def _create_relationships(tx, batch):
        query = """
        UNWIND $rows AS row
        MATCH (p:Professor {prof_id: toInteger(row.prof_id)}), (c:Course {course_id: toInteger(row.course_id)})
        CREATE (p)-[:TEACHES]->(c)
        WITH row, c
        MATCH (s:Schedule {course_id: toInteger(row.course_id)})
        CREATE (c)-[:HAS_SCHEDULE]->(s)
        """
        tx.run(query, rows=batch)

# Instantiate the importer and run the import process
importer = Neo4jImporter(uri, user, password)

start_time = time.time()
importer.import_data('professors', Neo4jImporter._create_professor)
elapsed_time_professors = time.time() - start_time

start_time = time.time()
importer.import_data('schedules', Neo4jImporter._create_schedule)
elapsed_time_schedules = time.time() - start_time

start_time = time.time()
importer.import_data('courses', Neo4jImporter._create_course)
elapsed_time_courses = time.time() - start_time

# Establish relationships in batches
start_time = time.time()
with open(dataset_files['courses'], 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    batch = []
    total_records = 0
    with importer.driver.session() as session:
        for row in reader:
            batch.append(row)
            if len(batch) == BATCH_SIZE:
                session.execute_write(Neo4jImporter._create_relationships, batch)
                total_records += len(batch)
                print(f"Created relationships for {BATCH_SIZE} courses.")
                batch = []
        if batch:
            session.execute_write(Neo4jImporter._create_relationships, batch)
            total_records += len(batch)
            print(f"Created relationships for remaining {len(batch)} courses.")
    print(f"Total relationships created for courses: {total_records}")
elapsed_time_relationships = time.time() - start_time

importer.close()

# Print the elapsed time for each table
print(f"Importing professors took {elapsed_time_professors:.2f} seconds.")
print(f"Importing schedules took {elapsed_time_schedules:.2f} seconds.")
print(f"Importing courses took {elapsed_time_courses:.2f} seconds.")
print(f"Creating relationships took {elapsed_time_relationships:.2f} seconds.")