from faker import Faker
import csv

fake = Faker()

# Define the dataset sizes
dataset_sizes = [250000, 500000, 750000]    # 250000, 500000, 750000

# Generate and export data for each dataset size
for size in dataset_sizes:
    # 
    professors = []
    courses = []
    schedules = []

    # professors table
    for prof_id in range(1, size + 1):
        prof = {
            'prof_id': prof_id,       #Primary key
            'prof_name': fake.name(),
            'major': fake.random_element(elements=('Physics', 'Mathematics', 'English', 'Chemistry', 'Biology')),
        }
        professors.append(prof)

    # courses table
    prof_ids = list(range(1, size + 1))   # making a list to connect
    for course_id in range(1, size + 1):
        prof_id = fake.random_element(elements=prof_ids)    #connecting course with the corresponding prof
        prof_ids.remove(prof_id)
        course = {
            'course_id': course_id,    #primary key
            'course_name': fake.random_element(elements=('Subject 1', 'Subject 2', 'Subject 3', 'Subject 4')),  #course catalog
            'prof_id': prof_id,        #foreign key
            'campus': fake.random_element(elements=('Annunziata', 'Papardo', 'Policlinic', 'Central')),
            'credit_num': fake.random_element(elements=(3,6,9,12))
        }
        courses.append(course)

        
    # schedule table
    course_ids = list(range(1, size + 1))    # making a list to connect
    for schedule_id in range(1, size+1):
        course_id = fake.random_element(elements=course_ids)   #creating a schedule with the corresponding course
        course_ids.remove(course_id)
        schedule = {
            'course_id': course_id,  # foreign key & the primary key
            'semester': fake.random_element(elements=('Spring', 'Autumn', 'Winter')),
            'day': fake.random_element(elements=("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")),
            "start_time": fake.time(pattern="%I:%M %p"),
            "end_time": fake.time(pattern="%I:%M %p")
        }
        schedules.append(schedule)


    # Export the generated data to CSV files
    professors_filename = f'professors_{size}.csv'
    courses_filename = f'courses_{size}.csv'
    schedules_filename = f'schedules_{size}.csv'

    with open(professors_filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=professors[0].keys())
        writer.writeheader()
        writer.writerows(professors)

    with open(courses_filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=courses[0].keys())
        writer.writeheader()
        writer.writerows(courses)

    with open(schedules_filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=schedules[0].keys())
        writer.writeheader()
        writer.writerows(schedules)

    print(f'Datasets with {size} records generated and exported to {professors_filename}, {courses_filename}, {schedules_filename}')
