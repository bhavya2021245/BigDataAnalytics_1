import psycopg2
from pymongo import MongoClient
from datetime import date, datetime

# PostgreSQL connection details
pg_config = {
    'database': 'university',
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',
    'port': '5432'
}

# MongoDB connection details
mongo_client = MongoClient("mongodb://localhost:27017/")  # Adjust this if using a remote server
mongo_db = mongo_client["university_mongo"]

# Connect to PostgreSQL
try:
    pg_conn = psycopg2.connect(**pg_config)
    pg_cursor = pg_conn.cursor()
    print("Connected to PostgreSQL")
except Exception as e:
    print(f"Failed to connect to PostgreSQL: {e}")
    exit()

# Fetch data helper
def fetch_data(query):
    pg_cursor.execute(query)
    return pg_cursor.fetchall()

# Function to handle datetime conversion for MongoDB
def convert_date(pg_date):
    if isinstance(pg_date, date):
        return datetime(pg_date.year, pg_date.month, pg_date.day)
    return pg_date

# Migrate Departments (Create a separate collection for departments)
def migrate_departments():
    print("Migrating Departments...")
    departments = fetch_data("SELECT DepartmentID, Name FROM Departments")
    
    # Insert each department as a separate document in MongoDB
    for dept in departments:
        mongo_db.departments.insert_one({
            "DepartmentID": dept[0],
            "Name": dept[1]
        })
    
    # Create a dictionary for later embedding in other collections
    department_dict = {dept[0]: dept[1] for dept in departments}  # Store department data in a dictionary for later use
    print(f"Migrated {len(departments)} departments.")
    return department_dict

# Migrate Instructors (Create a separate collection for instructors)
def migrate_instructors():
    print("Migrating Instructors...")
    instructors = fetch_data("SELECT InstructorID, Name, DepartmentID FROM Instructors")
    
    # Insert each instructor as a separate document in MongoDB
    for instructor in instructors:
        mongo_db.instructors.insert_one({
            "InstructorID": instructor[0],
            "Name": instructor[1],
            "DepartmentID": instructor[2]
        })
    
    # Create a dictionary for embedding in courses
    instructor_dict = {instructor[0]: instructor[1] for instructor in instructors}  # Store instructor data for later use
    print(f"Migrated {len(instructors)} instructors.")
    return instructor_dict

# Migrate Students (Create a separate collection for students)
def migrate_students(department_dict):
    print("Migrating Students...")
    students = fetch_data("SELECT StudentID, Name, Age, DepartmentID FROM Students")
    
    student_dict = {}
    for student in students:
        student_id, student_name, age, department_id = student
        
        # Insert student as a separate document in MongoDB
        mongo_db.students.insert_one({
            "StudentID": student_id,
            "Name": student_name,
            "Age": age,
            "DepartmentID": department_id,
            "DepartmentName": department_dict[department_id]  # Embed department name with student
        })
        
        # Create a lookup dictionary for embedding in courses
        student_dict[student_id] = {
            "StudentID": student_id,
            "Name": student_name,
            "Age": age,
            "DepartmentID": department_id,
            "DepartmentName": department_dict[department_id]
        }
    
    print(f"Migrated {len(students)} students.")
    return student_dict

# Migrate Courses with Embedded Data for Enrollments, Department, Instructor, and Full Student Data
def migrate_courses(department_dict, instructor_dict, student_dict):
    print("Migrating Courses...")
    courses = fetch_data("SELECT CourseID, Name, DepartmentID, InstructorID, CourseType FROM Courses")
    
    for course in courses:
        course_id, course_name, department_id, instructor_id, course_type = course
        
        # Fetch enrollments for this course
        enrollments = fetch_data(f"SELECT StudentID, EnrollmentDate FROM Enrollments WHERE CourseID = {course_id}")
        
        # Prepare a list of enrolled students with full student details
        enrolled_students = []
        for enrollment in enrollments:
            student_id, enrollment_date = enrollment
            # Retrieve full student details from the dictionary
            student_info = student_dict.get(student_id, {})
            enrolled_students.append({
                **student_info,  # Embed full student details
                "EnrollmentDate": convert_date(enrollment_date)
            })
        
        # Insert course into MongoDB with embedded students and department info
        mongo_db.courses.insert_one({
            "CourseID": course_id,
            "Name": course_name,
            "DepartmentID": department_id,
            "DepartmentName": department_dict[department_id],  # Denormalize department name
            "InstructorID": instructor_id,
            "InstructorName": instructor_dict[instructor_id],  # Denormalize instructor name
            "CourseType": course_type,  # Core or Elective
            "EnrolledStudents": enrolled_students,
            "TotalEnrollments": len(enrolled_students)  # Keep a count of enrollments for each course
        })
    
    print(f"Migrated {len(courses)} courses.")

# Run migrations
try:
    # Migrate departments and create a dictionary for embedding
    department_dict = migrate_departments()
    
    # Migrate instructors and create a dictionary for embedding
    instructor_dict = migrate_instructors()
    
    # Migrate students and create a dictionary for embedding in courses
    student_dict = migrate_students(department_dict)
    
    # Migrate courses with embedded student, instructor, and department data
    migrate_courses(department_dict, instructor_dict, student_dict)
    
    print("Data migration complete.")
except Exception as e:
    print(f"Error during migration: {e}")
finally:
    # Close PostgreSQL connection
    pg_cursor.close()
    pg_conn.close()
    print("Closed PostgreSQL connection.")
    # Close MongoDB connection
    mongo_client.close()
    print("Closed MongoDB connection.")
