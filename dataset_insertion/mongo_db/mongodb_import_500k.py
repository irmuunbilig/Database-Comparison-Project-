import csv
from pymongo import MongoClient, InsertOne
import time

# Connect to the MongoDB server and return a MongoClient object
def configure_mongo_client(mongodb_cfg):
    return MongoClient(host=mongodb_cfg['host'], port=mongodb_cfg['port'])

# Retrieve a database object from the MongoClient
def get_database(client, database_name):
    return client[database_name]

# Retrieve a collection object from the database
def get_collection(database, collection_name):
    return database[collection_name]

# Import data from a CSV file into a MongoDB collection with type casting and bulk write
def import_data(file_path, collection, key_fields):
    start_time = time.time()

    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        operations = []
        for row in reader:
            for key in key_fields:
                row[key] = str(row[key])  # Ensure key fields are strings
            operations.append(InsertOne(row))

    collection.bulk_write(operations)

    execution_time = time.time() - start_time
    print(f"Imported dataset from {file_path} in {execution_time:.2f} seconds")

if __name__ == "__main__":
    mongodb_cfg = {
        'host': 'localhost',
        'port': 27017,
        'database': 'curriculum'
    }

    dataset_files = {
        'professors': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/500k/professors_500k.csv',
        'schedules': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/500k/schedules_500k.csv',
        'courses': '/Users/irmuunbilig/Desktop/DB_pro/Dataset/500k/courses_500k.csv'
    }

    collection_names = {
        'professors': 'professors2',
        'schedules': 'schedules2',
        'courses': 'courses2',
    }

    key_fields = {
        'professors': ['prof_id'],
        'schedules': ['course_id'],
        'courses': ['course_id', 'prof_id']
    }

    client = configure_mongo_client(mongodb_cfg)
    database = get_database(client, mongodb_cfg['database'])

    for collection_key, file_path in dataset_files.items():
        collection = get_collection(database, collection_names[collection_key])
        import_data(file_path, collection, key_fields[collection_key])

    client.close()
