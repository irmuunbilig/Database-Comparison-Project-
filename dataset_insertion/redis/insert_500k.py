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
professors_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/500k/professors_500k.csv'
schedules_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/500k/schedules_500k.csv'
courses_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/500k/courses_500k.csv'

def import_data(filename, key_prefix):
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            unique_id = row[next(iter(row))]  # Get the value of the first column (assuming it's the unique ID)
            key = f"{key_prefix}:{unique_id}"
            for field, value in row.items():
                r.hset(key, field, value)

# Import professors2 data
print("Importing professors2 data...")
start_time = time.time()
import_data(professors_filename, "professors2:professor")
elapsed_time = time.time() - start_time
print("Professors2 data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Import schedules2 data
print("Importing schedules2 data...")
start_time = time.time()
import_data(schedules_filename, "schedules2:schedule")
elapsed_time = time.time() - start_time
print("Schedules2 data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Import courses2 data
print("Importing courses2 data...")
start_time = time.time()
import_data(courses_filename, "courses2:course")
elapsed_time = time.time() - start_time
print("Courses2 data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Close the Redis connection
r.close()
