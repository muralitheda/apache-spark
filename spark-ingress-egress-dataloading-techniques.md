# PySpark Ingestion + Egress + Data loading Techniques

```python
from pyspark.sql import SparkSession 

mysql_connector_path = "/home/hduser/install/mysql-connector-java.jar"
spark = SparkSession.builder\
    .appName("Ingress-Egress")\
    .config("spark-jars", mysql_connector_path)\
    .getOrCreate()

print(f"spark session:{spark}")

```

## 1. Converting Unstructured/Semi Structured Data into Structured Data using RDD then to DF

```python
#Case1: Users entered course data in Unstructured format. We would like to know the distinct number of course, which course mostly wanted?**

#Sample data => samplecourse.log
"""
Docker Java JavaScript React Spark Kafka SQL Git Go Python AI

AWS Azure TensorFlow PyTorch Android iOS Rust AI
Python Cloud AI Docker Kubernetes Rust
"""

#1. Covert the unstructured to structured data using RDD
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
unstruct_rdd1 = sc.textFile("file:///home/hduser/samplecourse.log")
struct_rdd2 = unstruct_rdd1.flatMap(lambda row:row.split(" ")) #['Python', 'Cloud', 'AI', 'Docker', 'Kubernetes']
struct_schema_rdd3 = struct_rdd2.map(lambda word:[word]) #[['Python'], ['Cloud'], ['AI'], ['Docker'], ['Kubernetes']]

#2. Covert the RDD into DF for further analysis
df1 = struct_schema_rdd3.toDF()
df1.createOrReplaceTempView("course_view")
df_view = spark.sql("select distinct _1 as distinct_courses from course_view")
df_view.show(5)

df_view = spark.sql("select _1 as cource, count(*) as no_of_people_interested from course_view group by _1")
df_view.show(5)

```

```
+----------------+
|distinct_courses|
+----------------+
|            Rust|
|          Docker|
|      JavaScript|
|         PyTorch|
|             AWS|
+----------------+
only showing top 5 rows

+----------+-----------------------+
|    cource|no_of_people_interested|
+----------+-----------------------+
|      Rust|                 104000|
|    Docker|                 104000|
|JavaScript|                  52000|
|   PyTorch|                  52000|
|       AWS|                  52000|
+----------+-----------------------+
only showing top 5 rows
```

## 2. Reading a CSV data and write into MySQL database using JDBC option

```python
###### Reading CSV data and write into DataFrame #######

# Sample Customer Info Data
"""
cd /home/hduser/custinfo.csv

4000001,Kristina,Chung,55,Pilot
4000002,Paige,Chen,77,Teacher
4000003,Sherri,Melton,34,Firefighter
4000004,Gretchen,Hill,66,Computer hardware engineer
4000005,Karen,Puckett,74,Lawyer
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.22").getOrCreate()

# Schema Definition
custinfo_schema = StructType([\
    StructField('custid', IntegerType(), True)\
    , StructField('first_name', StringType(), True)\
    , StructField('last_name', StringType(), True)\
    , StructField('age', IntegerType(), True)\
    , StructField('profession', StringType(), True)])

# CSV Data Read and storing it in DataFrame
df1 = spark.read.csv(\
    path="file:///home/hduser/custinfo.csv"\
    ,header=False\
    ,sep=","\
    ,inferSchema=False\
    ,schema=custinfo_schema)

df1.show(truncate=False,n=5)
print(f"[INFO] df1.count() = {df1.count()}")
```

```
+-------+----------+---------+---+--------------------------+
|custid |first_name|last_name|age|profession                |
+-------+----------+---------+---+--------------------------+
|4000001|Kristina  |Chung    |55 |Pilot                     |
|4000002|Paige     |Chen     |77 |Teacher                   |
|4000003|Sherri    |Melton   |34 |Firefighter               |
|4000004|Gretchen  |Hill     |66 |Computer hardware engineer|
|4000005|Karen     |Puckett  |74 |Lawyer                    |
+-------+----------+---------+---+--------------------------+
only showing top 5 rows

[INFO] df1.count() = 9999
```

```python
###### Write the data into MySql DB ######

# JDBC Options
url1='jdbc:mysql://127.0.0.1:3306/stocksdb?createDatabaseIfNotExist=true'
dbproperties={'user':'root','password':'Root123$','driver':'com.mysql.cj.jdbc.Driver'}

# Write into DB
df1.write.jdbc(url=url1,properties=dbproperties,table="custinfo",mode="overwrite")
print("[INFO] CSV file data write into MySQL DB is successful.")
```

```python
###### Optimized way to read the data from any RDBMS DB using JDBC ######

#Question: How to improve performance for JDBC?
#partition, fetchsize, caching, pushdown optimization etc.,
#partitionColumn:, numberOfPartitions:, upperBound:, lowerBound, predicates, fetchsize..

# JDBC Options for performance optimization
url1='jdbc:mysql://127.0.0.1:3306/stocksdb'
dbproperties = {
    'user': 'root',
    'password': 'Root123$',
    'driver': 'com.mysql.cj.jdbc.Driver',
    # Performance optimization options (values as strings):
    'partitionColumn': 'custid', # Column used to divide data into sections for parallel processing.
    'lowerBound': '4000001',     # Minimum value for the partition column to start reading data.
    'upperBound': '4009000',     # Maximum value for the partition column to start reading data.
    'numPartitions': '3',
    'pushDownPredicate': 'true', # Sends filters (WHERE clauses) to the database for early processing.
    'pushDownAggregate': 'true', # Sends aggregations (SUM, COUNT) to the database for early processing.
    'queryTimeout': '120',       # Maximum time (in seconds) a database query can run before timing out.
    'fetchSize': '10',           # Number of rows retrieved from the database in each batch.
    'isolationLevel': 'READ_COMMITTED' # Ensures only committed data is visible during a transaction.
}

# Read the data from RDBMS using query instead of direct table
table_query = "(select * from stocksdb.custinfo order by custid) as tablename"
df2_db = spark.read.jdbc(url=url1,properties=dbproperties,table=table_query)
df2_db.show(truncate=False,n=5)
print("[INFO] Partition wise record count:",df2_db.rdd.glom().map(len).collect())
```
```
+-------+----------+---------+---+--------------------------+
|custid |first_name|last_name|age|profession                |
+-------+----------+---------+---+--------------------------+
|4000001|Kristina  |Chung    |55 |Pilot                     |
|4000002|Paige     |Chen     |77 |Teacher                   |
|4000003|Sherri    |Melton   |34 |Firefighter               |
|4000004|Gretchen  |Hill     |66 |Computer hardware engineer|
|4000005|Karen     |Puckett  |74 |Lawyer                    |
+-------+----------+---------+---+--------------------------+
only showing top 5 rows

[INFO] Partition wise record count: [3000, 2999, 4000]

```


🔹 Spark JDBC Partitioning Example for UpperBound & LowerBound Calculation

| Parameter / Step       | Description / Formula                         | Value / Example                                       |
| ---------------------- | --------------------------------------------- | ----------------------------------------------------- |
| **partitionColumn**    | Column used to divide data for parallel reads | `custid`                                              |
| **lowerBound**         | Minimum value for the partition column        | `4000001`                                             |
| **upperBound**         | Maximum value for the partition column        | `4009000`                                             |
| **numPartitions**      | Number of parallel partitions (tasks)         | `3`                                                   |
| **Formula for stride** | `(upperBound - lowerBound) / numPartitions`   | `(4009000 - 4000001) / 3 = 8999 / 3 = 2999.67 ≈ 2999` |
| **Stride (approx)**    | Range of values per partition                 | `2999`                                                |

🔹 Partition Details:

| Partition No. | Start (inclusive) | End (exclusive) | WHERE Clause (applied on MySQL)          |
| ------------- | ----------------- | --------------- | ---------------------------------------- |
| 0             | 4000001           | 4003000         | `custid < 4003000`                       |
| 1             | 4003000           | 4006000         | `custid >= 4003000 AND custid < 4006000` |
| 2             | 4006000           | 4009000         | `custid >= 4006000`                      |


## 3. Schema Evoluation/Growing handling using columner file formats ORC/Parquet

## 4. Reading a JSON data with various options

## 5. Reading a CSV data with various options

## 6. ORC & Parquet file formats for Performance Optimization

## 7. PySpark & Hive Integration : Data Ingestion and Table Creation

