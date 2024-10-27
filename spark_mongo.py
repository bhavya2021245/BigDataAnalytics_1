import time
import psutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, avg, countDistinct, array_contains

def print_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    print(f"Memory Usage: {memory_info.rss / (1024 ** 2):.2f} MB")  # Resident Set Size in MB

# Helper function to measure time taken
def measure_time(start_time):
    end_time = time.time()
    print(f"Time taken: {end_time - start_time:.2f} seconds")
    
# Step 1: Create Spark Session with MongoDB Connector
spark = SparkSession.builder \
    .appName("MongoDBSparkQueries") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/university_mongo") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/university_mongo") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0") \
    .getOrCreate()

database_name = "university_mongo"  # Replace with your actual database name

# Load collections
students_df = spark.read.format("mongodb") \
    .option("database", database_name) \
    .option("collection", "students") \
    .load()

courses_df = spark.read.format("mongodb") \
    .option("database", database_name) \
    .option("collection", "courses") \
    .load()

instructors_df = spark.read.format("mongodb") \
    .option("database", database_name) \
    .option("collection", "instructors") \
    .load()

departments_df = spark.read.format("mongodb") \
    .option("database", database_name) \
    .option("collection", "departments") \
    .load()

# Query 1: Fetching all students enrolled in a specific course
course_id = 7  # Replace with the actual course_id
start_time = time.time()
students_enrolled_in_course = courses_df \
    .filter(col("CourseID") == course_id) \
    .select("EnrolledStudents.Name", "EnrolledStudents.Age")

students_enrolled_in_course.show()
measure_time(start_time)
print_memory_usage()

# Query 2: Calculating the average number of students enrolled in courses offered by a particular instructor
instructor_id = 1  # Replace with the actual instructor_id
start_time = time.time()
courses_by_instructor = courses_df.filter(col("InstructorID") == instructor_id)
courses_with_enrollment_count = courses_by_instructor \
    .withColumn("num_students", size(col("EnrolledStudents"))) \
    .select("CourseID", "num_students")

avg_students_per_course = courses_with_enrollment_count.agg(avg("num_students").alias("avg_students")).show()
measure_time(start_time)
print_memory_usage()
# Query 3: Listing all courses offered by a specific department
department_id = 1  # Replace with the actual department_id
start_time = time.time()
courses_in_department = courses_df.filter(col("DepartmentID") == department_id).select("Name")
courses_in_department.show()
measure_time(start_time)
print_memory_usage()

# Query 4: Finding the total number of students per department
students_by_department = students_df.groupBy("DepartmentID").agg(countDistinct("StudentID").alias("total_students"))
students_by_department.show()
measure_time(start_time)
print_memory_usage()
# Query 5: Finding instructors who have taught all the BTech CSE core courses
core_course_type = "Core"
department_name = "Computer Science"
start_time = time.time()


# Filter core courses in the BTech CSE department
core_courses = courses_df.filter((col("CourseType") == core_course_type) & (col("DepartmentName") == department_name))

# Get instructors who have taught all core courses
instructors_who_taught_all_core_courses = instructors_df \
    .join(core_courses, instructors_df.InstructorID == core_courses.InstructorID, "inner") \
    .groupBy(instructors_df.InstructorID, instructors_df.Name) \
    .agg(countDistinct(core_courses.CourseID).alias("num_core_courses")) \
    .filter(col("num_core_courses") == core_courses.count())

instructors_who_taught_all_core_courses.show()
measure_time(start_time)
print_memory_usage()


# Query 6: Finding top-10 courses with the highest enrollments
start_time = time.time()
top_10_courses_by_enrollment = courses_df \
    .withColumn("num_students", size(col("EnrolledStudents"))) \
    .select("CourseID", "Name", "num_students") \
    .orderBy(col("num_students").desc()) \
    .limit(10)

top_10_courses_by_enrollment.show()
measure_time(start_time)
print_memory_usage()


# Stop the Spark session
# spark.stop()
