import redis
import time

# Redis connection configuration
redis_host = 'localhost'
redis_port = 6379
redis_db = 0

# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Query: Fetch the names of professors who teach at 'Papardo'
start_time = time.time()

# Step 1: Fetch all courses2 taught at Papardo
course_keys = r.keys("courses2:course:*")
papardo_courses = []

for key in course_keys:
    course_data = r.hgetall(key)
    
    # Decode the values from bytes to strings
    decoded_data = {field.decode('utf-8'): value.decode('utf-8') for field, value in course_data.items()}
    
    if decoded_data.get('campus') == 'Papardo':
        papardo_courses.append(decoded_data)

# Extract the professor IDs from the Papardo courses
prof_ids = {course['prof_id'] for course in papardo_courses}

# Step 2: Fetch the names of professors2 with the extracted prof_ids
professor_details = []

for prof_id in prof_ids:
    professor_key = f"professors2:professor:{prof_id}"
    professor_data = r.hgetall(professor_key)
    
    if professor_data:
        prof_name = professor_data.get(b'prof_name').decode('utf-8')
        professor_details.append({
            'prof_name': prof_name,
            'campus': 'Papardo'
        })

# Print the result
for detail in professor_details:
    print(f"Professor Name: {detail['prof_name']}, Campus: {detail['campus']}")

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the Redis connection
r.close()
