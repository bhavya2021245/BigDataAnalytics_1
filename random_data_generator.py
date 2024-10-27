import random
from datetime import datetime, timedelta

# Parameters
num_students = 1700
num_courses = 100
num_instructors = 50
num_departments = 5

# Helper functions to generate random data
def generate_name(prefix, id_num):
    return f"{prefix}_{id_num}"

def generate_contact():
    return f"{random.randint(9000000000, 9999999999)}"

# Random date generator for enrollments
def random_date(start, end):
    return start + timedelta(days=random.randint(0, int((end - start).days)))

# Insert into Departments
departments = ["CSE", "AI", "CB", "MTH", "SSH"]
department_values = []

for i, name in enumerate(departments, start=1):
    department_values.append(f"('{name}')")

# Combine department inserts into a single command
department_insert = f"INSERT INTO Departments (name) VALUES {', '.join(department_values)};"

# Insert into Instructors
# Keep track of which department each instructor belongs to
instructor_values = []
instructor_department_map = {}  # {instructor_id: department_id}
for i in range(1, num_instructors + 1):
    department_id = random.randint(1, num_departments)
    name = generate_name("Instructor", i)
    instructor_values.append(f"('{name}', {department_id})")
    instructor_department_map[i] = department_id

# Combine instructor inserts into a single command
instructor_insert = f"INSERT INTO Instructors (name, departmentid) VALUES {', '.join(instructor_values)};"

# Insert into Courses (without courseid)
course_values = []
course_department_map = {}  # {course_id: department_id}
course_id_map = {}  # To map course index to actual course_id (used in enrollments)
used_course_names = set()  # Ensure course names are unique

# Assign random courses to departments and instructors
for i in range(1, num_courses + 1):
    department_id = random.randint(1, num_departments)
    
    # Choose a random instructor from the same department
    eligible_instructors = [inst_id for inst_id, dept_id in instructor_department_map.items() if dept_id == department_id]
    instructor_id = random.choice(eligible_instructors)
    
    # Generate a unique course name
    course_name = f"Course_{i}"

    # Ensure the course name is unique across all departments
    while course_name in used_course_names:
        course_name = f"Course_{i}_{random.randint(1000, 9999)}"  # Ensure uniqueness
    
    used_course_names.add(course_name)  # Track used course names

    # Add course values
    course_values.append(f"('{course_name}', {department_id}, {instructor_id}, 'Core')")
    course_department_map[i] = department_id
    course_id_map[i] = i  # Store the course_id for use in enrollments

# Combine course inserts into a single command (without courseid)
course_insert = f"INSERT INTO Courses (name, departmentid, instructorid, coursetype) VALUES {', '.join(course_values)};"

# Insert into Students
student_values = []
for i in range(1, num_students + 1):
    name = generate_name("Student", i)
    age = random.randint(18, 25)
    department_id = random.randint(1, num_departments)
    student_values.append(f"('{name}', {age}, {department_id})")

# Combine student inserts into a single command
student_insert = f"INSERT INTO Students (name, age, departmentid) VALUES {', '.join(student_values)};"

# Insert into Enrollments (with enrollmentdate)
enrollment_values = []
enrollments_set = set()  # To ensure no duplicate enrollments (student, course)
start_date = datetime(2020, 1, 1)
end_date = datetime(2024, 1, 1)

# Ensure every course gets some enrollments
students_per_course = num_students // num_courses  # Average number of students per course
course_enrollments = {course_id: [] for course_id in range(1, num_courses + 1)}

# Distribute students to each course evenly
all_students = list(range(1, num_students + 1))
random.shuffle(all_students)

for course_idx in range(1, num_courses + 1):
    num_course_enrollments = random.randint(3, 10)  # Each course gets between 3 and 10 enrollments initially
    selected_students = all_students[:num_course_enrollments]
    course_enrollments[course_idx].extend(selected_students)
    all_students = all_students[num_course_enrollments:]  # Remove selected students from the pool

    for student_id in selected_students:
        # Check for duplicate enrollment
        if (student_id, course_idx) not in enrollments_set:
            course_id = course_id_map[course_idx]  # Use the actual course_id
            enrollment_date = random_date(start_date, end_date).strftime('%Y-%m-%d')
            enrollment_values.append(f"({student_id}, {course_id}, '{enrollment_date}')")
            enrollments_set.add((student_id, course_idx))  # Mark this combination as enrolled

# Assign remaining students to random courses to fill up enrollments
for student_id in all_students:
    num_enrollments = random.randint(2, 5)  # Each remaining student takes 2 to 5 courses
    enrolled_courses = random.sample(range(1, num_courses + 1), num_enrollments)
    for course_idx in enrolled_courses:
        # Check for duplicate enrollment
        if (student_id, course_idx) not in enrollments_set:
            course_id = course_id_map[course_idx]  # Use the actual course_id
            enrollment_date = random_date(start_date, end_date).strftime('%Y-%m-%d')
            enrollment_values.append(f"({student_id}, {course_id}, '{enrollment_date}')")
            course_enrollments[course_idx].append(student_id)
            enrollments_set.add((student_id, course_idx))  # Mark this combination as enrolled

# Combine enrollment inserts into a single command (including enrollmentdate)
enrollment_insert = f"INSERT INTO Enrollments (studentid, courseid, enrollmentdate) VALUES {', '.join(enrollment_values)};"

# Combine all SQL insert statements
all_inserts = [department_insert, instructor_insert, course_insert, student_insert, enrollment_insert]

# Write to an SQL file
with open("large_data_insert.sql", "w") as f:
    for insert in all_inserts:
        f.write(insert + "\n")

print("SQL data generation complete. File 'large_data_insert.sql' created.")
