# University RDBMS Migration to MongoDB and Apache Spark Workload Analysis

## Project Overview

This project involved migrating a traditional relational database (RDBMS) containing university student information from PostgreSQL to MongoDB, a NoSQL database, and executing complex analytics queries using Apache Spark. The project focuses on the following key aspects:

- **Data Modeling:** Designing an efficient document-based schema in MongoDB that supports the university’s data requirements.
- **Data Migration:** Implementing a seamless ETL (Extract, Transform, Load) pipeline to ensure data integrity and consistency.
- **Query Execution with Apache Spark:** Running analytical queries on MongoDB data via Apache Spark.
- **Performance Optimization:** Enhancing query performance using techniques like indexing in MongoDB and query optimizations in Spark.


## Requirements

- **PostgreSQL**: Source database for student information system.
- **MongoDB**: Target NoSQL database.
- **Apache Spark**: Framework for executing queries on MongoDB.
- **Python**: Used for ETL processes and data transformations.
- **Jupyter Notebook**: For implementing and documenting Spark queries.

## Data Model & Schema Design

The project transitioned from a relational model to a document-based model in MongoDB. Key transformations included denormalization to store related information within single documents for optimized querying. Here are some core query requirements that influenced the schema design:

1. **Student Enrollment in a Specific Course**  
2. **Average Enrollment in Instructor-Offered Courses**  
3. **Courses by Department**  
4. **Total Students by Department**  
5. **Instructors for BTech CSE Core Courses**  
6. **Top-10 Courses by Enrollment**

## Data Migration Process

The data migration pipeline consists of:

1. **Extract**: Retrieving data from PostgreSQL tables.
2. **Transform**: Structuring data to match the MongoDB document schema, applying necessary transformations.
3. **Load**: Inserting transformed data into MongoDB, maintaining integrity and consistency.

## Query Implementation with Apache Spark

Using Apache Spark, we implemented the following queries directly on MongoDB data:

1. **Fetch all students in a specific course**
2. **Calculate average students per instructor for specific courses**
3. **List courses by department**
4. **Find total students per department**
5. **Identify instructors for core courses**
6. **List top-10 courses by enrollment**

## Performance Analysis & Optimization

After initial query testing, two main optimization strategies were applied:

1. **Indexing in MongoDB** for frequently accessed fields, significantly reducing query times.
2. **Spark Query Optimization** through partitioning and caching, enhancing data retrieval speeds.

## Results and Observations

- Achieved **faster data retrieval** and improved scalability by transitioning to MongoDB.
- **Optimized queries** with Apache Spark reduced response times, demonstrating the effectiveness of NoSQL for this workload.

## Setup and Usage

1. **Install PostgreSQL, MongoDB, and Apache Spark**.
2. Run the migration scripts in `scripts/` to extract, transform, and load data.
3. Execute `spark_queries.ipynb` to run queries and view performance analysis.

## Contributing

Contributions are welcome! Please fork this repository, make changes, and submit a pull request.

## License

This project is licensed under the MIT License. See `LICENSE` for more information.

