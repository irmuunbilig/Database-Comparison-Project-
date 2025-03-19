import redis
import time

# Redis connection configuration
redis_host = 'localhost'
redis_port = 6379
redis_db = 0

# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Query: Fetch all schedules2 where the semester is like 'Spring%'
query = "schedules2:*"

start_time = time.time()

# Execute the query and fetch results
schedule_keys = r.keys(query)
spring_schedules = []

for key in schedule_keys:
    schedule_data = r.hgetall(key)
    
    # Decode the values from bytes to strings
    decoded_data = {field.decode(): value.decode() for field, value in schedule_data.items()}
    
    semester = decoded_data.get('semester')
    
    if semester and semester.startswith('Spring'):
        spring_schedules.append(decoded_data)

# Print the result
for schedule in spring_schedules:
    print(schedule)

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the Redis connection
r.close()
