import redis
import time

# Redis connection configuration
redis_host = 'localhost'
redis_port = 6379
redis_db = 0

# Connect to Redis
r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)

# Query: Fetch the names of professors who teach at 'Papardo Campus' in any Spring semester
start_time = time.time()

# Step 1: Fetch all schedules for Spring semesters
schedule_keys = r.keys("schedules:*")
spring_courses = {}

for key in schedule_keys:
    schedule_data = r.hgetall(key)
    
    # Decode the values from bytes to strings
    decoded_data = {field.decode(): value.decode() for field, value in schedule_data.items()}
    
    semester = decoded_data.get('semester')
    
    if semester and semester.startswith('Spring'):
        course_id = decoded_data['course_id']
        if course_id not in spring_courses:
            spring_courses[course_id] = []
        spring_courses[course_id].append(semester)

# Debugging: Print extracted spring courses
print(f"Spring Courses: {spring_courses}")

# Step 2: Fetch courses taught at Papardo Campus with the extracted course_ids
course_keys = r.keys("courses:*")
papardo_professors = {}

for key in course_keys:
    course_data = r.hgetall(key)
    
    # Decode the values from bytes to strings
    decoded_data = {field.decode(): value.decode() for field, value in course_data.items()}
    
    course_id = decoded_data['course_id']
    if decoded_data.get('campus') == 'Papardo' and course_id in spring_courses:
        prof_id = decoded_data['prof_id']
        if prof_id not in papardo_professors:
            papardo_professors[prof_id] = []
        papardo_professors[prof_id].extend(spring_courses[course_id])

# Debugging: Print extracted professor IDs and semesters
print(f"Papardo Professors: {papardo_professors}")

# Step 3: Fetch the names of professors with the extracted prof_ids
professor_details = []

for prof_id, semesters in papardo_professors.items():
    professor_key = f"professors:professor:{prof_id}"
    professor_data = r.hgetall(professor_key)
    
    if professor_data:
        prof_name = professor_data.get(b'prof_name').decode()
        professor_details.append({
            'prof_name': prof_name,
            'campus': 'Papardo',
            'semesters': semesters
        })

# Print the result
for detail in professor_details:
    semesters_str = ', '.join(detail['semesters'])
    print(f"Professor Name: {detail['prof_name']}, Campus: {detail['campus']}, Semesters: {semesters_str}")

# Calculate and print the execution time
execution_time = time.time() - start_time
print("Execution time:", execution_time, "seconds")

# Close the Redis connection
r.close()
