import redis
import csv
import time

# Redis connection details
redis_host = 'localhost'
redis_port = 6379
redis_db = 0

# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# File paths of the datasets
professors_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/professors_250k.csv'
schedules_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/schedules_250k.csv'
courses_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/250k/courses_250k.csv'

def import_data(filename, key_prefix):
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        pipeline = r.pipeline()
        for row in reader:
            unique_id = row[next(iter(row))]  # Get the value of the first column (assuming it's the unique ID)
            key = f"{key_prefix}:{unique_id}"
            for field, value in row.items():
                pipeline.hset(key, field, value)
        pipeline.execute()

# Import professors data
print("Importing professors data...")
start_time = time.time()
import_data(professors_filename, "professors:professor")
elapsed_time = time.time() - start_time
print("Professors data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Import schedules data
print("Importing schedules data...")
start_time = time.time()
import_data(schedules_filename, "schedules:schedule")
elapsed_time = time.time() - start_time
print("Schedules data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Import courses data
print("Importing courses data...")
start_time = time.time()
import_data(courses_filename, "courses:course")
elapsed_time = time.time() - start_time
print("Courses data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Close the Redis connection
r.close()


