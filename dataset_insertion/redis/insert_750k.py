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
professors_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/750k/professors_750k.csv'
schedules_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/750k/schedules_750k.csv'
courses_filename = '/Users/irmuunbilig/Desktop/DB_pro/Dataset/750k/courses_750k.csv'

def import_data(filename, key_prefix):
    with open(filename, 'r', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            unique_id = row[next(iter(row))]  # Get the value of the first column (assuming it's the unique ID)
            key = f"{key_prefix}:{unique_id}"
            for field, value in row.items():
                r.hset(key, field, value)

# Import professors3 data
print("Importing professors3 data...")
start_time = time.time()
import_data(professors_filename, "professors3:professor")
elapsed_time = time.time() - start_time
print("Professors3 data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Import schedules3 data
print("Importing schedules3 data...")
start_time = time.time()
import_data(schedules_filename, "schedules3:schedule")
elapsed_time = time.time() - start_time
print("Schedules3 data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Import courses3 data
print("Importing courses3 data...")
start_time = time.time()
import_data(courses_filename, "courses3:course")
elapsed_time = time.time() - start_time
print("Courses3 data imported successfully.")
print(f"Execution time: {elapsed_time:.2f} seconds")

# Close the Redis connection
r.close()
