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

```python
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

```python
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
```python
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

```python
#ORC/PARQUET Other Properties

#Source is sending data on a daily basis, once in a while the schema of the data is evolving/growing
  #Example (Day1): exch~stock~price
  #Example (Day2): exch~stock~price~buyer
  #Example (Day3): stock~price~seller

#**mergeSchema: Orc/Parquet read all the datafiles headers and merge them into one header
```

```python
# Sample data
day1 = """
exch~stock~price
NYSE~CLI~36.3
NYSE~ABC~36.3
"""

day2 = """
exch~stock~price~buyer
NYSE~CLI~37.3~Alan
NYSE~ABC~37.3~Harpar
"""

day3 = """
stock~price~seller
CLI~37.3~Jack
ABC~37.3~Ross
"""

"""
/home/hduser/stockdata_csv/
├── part-00000-01f262bb-27a7-465d-95ca-4fdb6e1986aa-c000.csv
└── _SUCCESS
"""

# Write the same data into CSV + Read the CSV + Write into ORC format (Append) + Read the ORC data (MergeSchema=True)

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Day 1: exch~stock~price
lines_day1 = day1.strip().split('\n')
header_day1 = lines_day1[0].split('~')
data_rows_day1 = [line.split('~') for line in lines_day1[1:]]
df1 = spark.createDataFrame(data_rows_day1, header_day1)
df1.coalesce(1).write.csv(path="file:///home/hduser/stockdata_csv/",mode="overwrite",sep="~",header=True)

df_csv = spark.read.csv(path="file:///home/hduser/stockdata_csv/",pathGlobFilter="part-*.csv",sep="~",header=True)
print("[INFO] Day1 : Source CSV data")
df_csv.show()
df_csv.coalesce(1).write.orc(path="file:///home/hduser/stockdata_orc/",mode="overwrite") # Overwrite for the first time
df_orc = spark.read.orc(path="file:///home/hduser/stockdata_orc/",mergeSchema=True) # Schema Evoluation
print("[INFO] Day1 : ORC data read")
df_orc.show()

# Day 2: exch~stock~price~buyer
lines_day2 = day2.strip().split('\n')
header_day2 = lines_day2[0].split('~')
data_rows_day2 = [line.split('~') for line in lines_day2[1:]]
df2 = spark.createDataFrame(data_rows_day2, header_day2)
df2.coalesce(1).write.csv(path="file:///home/hduser/stockdata_csv/",mode="overwrite",sep="~",header=True)

df_csv = spark.read.csv(path="file:///home/hduser/stockdata_csv/",pathGlobFilter="part-*.csv",sep="~",header=True)
print("[INFO] Day2 : Source CSV data")
df_csv.show()
df_csv.coalesce(1).write.orc(path="file:///home/hduser/stockdata_orc/",mode="append") # Append for the Schema Evoluation
df_orc = spark.read.orc(path="file:///home/hduser/stockdata_orc/",mergeSchema=True) # Schema Evoluation
print("[INFO] Day2 : ORC data read with evolved schema")
df_orc.show()

# Day 3: stock~price~seller
lines_day3 = day3.strip().split('\n')
header_day3 = lines_day3[0].split('~')
data_rows_day3 = [line.split('~') for line in lines_day3[1:]]
df3 = spark.createDataFrame(data_rows_day3, header_day3)
print("[INFO] Day3 : Source CSV data")
df3.show()
df3.coalesce(1).write.csv(path="file:///home/hduser/stockdata_csv/",mode="overwrite",sep="~",header=True)

df_csv = spark.read.csv(path="file:///home/hduser/stockdata_csv/",pathGlobFilter="part-*.csv",sep="~",header=True)
df_csv.coalesce(1).write.orc(path="file:///home/hduser/stockdata_orc/",mode="append") # Append for the Schema Evoluation
df_orc = spark.read.orc(path="file:///home/hduser/stockdata_orc/",mergeSchema=True) # Schema Evoluation
print("[INFO] Day3 : ORC data read evolved schema")
df_orc.show()

"""
$ ls -ltr /home/hduser/stockdata_orc/
total 12
-rw-r--r--. 1 hduser hduser 538 Nov  8 18:04 part-00000-d84c9a15-a0c3-4028-bcee-1be0e4b4b0c1-c000.snappy.orc
-rw-r--r--. 1 hduser hduser 665 Nov  8 18:04 part-00000-72318d44-2b2c-4a9d-a3f9-b44367ed6aee-c000.snappy.orc
-rw-r--r--. 1 hduser hduser   0 Nov  8 18:04 _SUCCESS
-rw-r--r--. 1 hduser hduser 537 Nov  8 18:04 part-00000-7ececc67-8fa5-44cf-bbb6-943997a24d2c-c000.snappy.orc

"""
```
```python
[INFO] Day1 : Source CSV data
+----+-----+-----+
|exch|stock|price|
+----+-----+-----+
|NYSE|  CLI| 36.3|
|NYSE|  ABC| 36.3|
+----+-----+-----+

[INFO] Day1 : ORC data read
+----+-----+-----+
|exch|stock|price|
+----+-----+-----+
|NYSE|  CLI| 36.3|
|NYSE|  ABC| 36.3|
+----+-----+-----+

[INFO] Day2 : Source CSV data
+----+-----+-----+------+
|exch|stock|price| buyer|
+----+-----+-----+------+
|NYSE|  CLI| 37.3|  Alan|
|NYSE|  ABC| 37.3|Harpar|
+----+-----+-----+------+

[INFO] Day2 : ORC data read with evolved schema
+----+-----+-----+------+
|exch|stock|price| buyer|
+----+-----+-----+------+
|NYSE|  CLI| 37.3|  Alan|
|NYSE|  ABC| 37.3|Harpar|
|NYSE|  CLI| 36.3|  NULL|
|NYSE|  ABC| 36.3|  NULL|
+----+-----+-----+------+

[INFO] Day3 : Source CSV data
+-----+-----+------+
|stock|price|seller|
+-----+-----+------+
|  CLI| 37.3|  Jack|
|  ABC| 37.3|  Ross|
+-----+-----+------+

[INFO] Day3 : ORC data read evolved schema
+----+-----+-----+------+------+
|exch|stock|price| buyer|seller|
+----+-----+-----+------+------+
|NYSE|  CLI| 37.3|  Alan|  NULL|
|NYSE|  ABC| 37.3|Harpar|  NULL|
|NYSE|  CLI| 36.3|  NULL|  NULL|
|NYSE|  ABC| 36.3|  NULL|  NULL|
|NULL|  CLI| 37.3|  NULL|  Jack|
|NULL|  ABC| 37.3|  NULL|  Ross|
+----+-----+-----+------+------+

```


## 4. Reading a JSON data with various options

```python
from pyspark.sql.types import DecimalType,BooleanType,ArrayType,DateType,TimestampType

# Data
samplejson = """
[
  {
    "id": 1,
    "name": "Alice",
    "age": 30,
    "salary": 50000.50,
    "isActive": true,
    "comments": "This is a comment.",
    "tags": ["A", "B"],
    "address": {
      "street": "123 Main St",
      "city": "Anytown"
    }
  },
  {
    "id": 2,
    "name": "Bob",
    "age": 25,
    "salary": 45000.75,
    "isActive": false,
    "comments": "Another comment.",
    "tags": ["C"],
    "address": {
      "street": "456 Oak Ave",
      "city": "Otherville"
    }
  },
  {
    "id": 3,
    "name": "Charlie",
    "age": null,
    "salary": null,
    "isActive": true,
    "comments": "Invalid JSON",
    "tags": ["D", "E"]
  },
  {
    "id": 4,
    "name": "David",
    "age": 40,
    "salary": 60000.00,
    "isActive": true,
    "date_joined": "2023-01-15",
    "timestamp_event": "2023-01-15 10:30:00.123"
  },
  {
    "id": 5,
    "name": "Eve",
    "age": 35,
    "salary": 55555.555,
    "isActive": true,
    "comments": "This has 'single quotes'.",
    "field with space": "value"
  },
  {
    "id": 6,
    "name": "Frank",
    "age": 28,
    "salary": 12345.678,
    "isActive": true,
    "comments": "Escaped chars: \\n\\t\\r",
    "tags": ["F"],
    "decimal_val": 12345.678
  },
  {
    "invalid":"invalid"
  }  
]
"""

# JSON file creation
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

import os
spark=SparkSession.builder.getOrCreate()

file_path = "/home/hduser/employe_json/sample.json"
directory = os.path.dirname(file_path)
os.makedirs(directory, exist_ok=True)
with open("/home/hduser/employe_json/sample.json", "w") as f:
    f.write(samplejson)

# Define a custom schema
custom_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DecimalType(10, 3), True),
    StructField("isActive", BooleanType(), True),
    StructField("comments", StringType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True)
    ])),
    StructField("date_joined", DateType(), True),
    StructField("timestamp_event", TimestampType(), True),
    StructField("corrupted_record", StringType(), True),
])

# Read JSON with multiple options and inline comments
df = spark.read.json(
    path="file:///home/hduser/employe_json/",  # Specifies the location of the JSON file(s) to read.
    schema=custom_schema,  # Defines a custom schema to avoid automatic inference.
    primitivesAsString=False,  # Treats all primitive values (int, float, bool) as strings if True.
    prefersDecimal=True,  # Infers floating-point numbers as DecimalType instead of DoubleType.
    allowComments=True,  # Allows Java/C++ style comments (//, /* */) in JSON.
    allowUnquotedFieldNames=True,  # Accepts JSON keys without double quotes.
    allowSingleQuotes=True,  # Accepts single quotes for string values.
    allowBackslashEscapingAnyCharacter=True,  # Allows any character to be escaped with a backslash.
    mode="PERMISSIVE",  # Defines how to handle corrupt records (PERMISSIVE, DROPMALFORMED, FAILFAST).
    columnNameOfCorruptRecord="corrupted_record",  # Stores malformed JSON strings in a specified column.
    dateFormat="yyyy-MM-dd",  # Specifies the format for parsing date strings.
    timestampFormat="yyyy-MM-dd HH:mm:ss.SSS",  # Specifies the format for parsing timestamp strings.
    multiLine=True,  # Treats the entire file as a single JSON object (for pretty-printed or array JSON).
    allowUnquotedControlChars=True,  # Allows control characters (e.g., \n, \t) to appear unquoted.
    lineSep="\n",  # Defines a custom line separator between JSON records. If multiline=True then it is not required.
    samplingRatio=1.0,  # Sets the fraction of data used for schema inference.
    encoding="UTF-8",  # Specifies the character encoding (e.g., UTF-8, UTF-16).
    locale="en-US",  # Sets the locale for parsing locale-sensitive data like dates.
    pathGlobFilter="*.json",  # Filters files using glob patterns (e.g., *.json).
    recursiveFileLookup=True  # Enables recursive search in subdirectories.
)

df.printSchema()
df.show(truncate=False)
```

```python
root
 |-- id: integer (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- salary: decimal(10,3) (nullable = true)
 |-- isActive: boolean (nullable = true)
 |-- comments: string (nullable = true)
 |-- tags: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- address: struct (nullable = true)
 |    |-- street: string (nullable = true)
 |    |-- city: string (nullable = true)
 |-- date_joined: date (nullable = true)
 |-- timestamp_event: timestamp (nullable = true)
 |-- corrupted_record: string (nullable = true)

+----+-------+----+---------+--------+-------------------------+------+-------------------------+-----------+-----------------------+----------------+
|id  |name   |age |salary   |isActive|comments                 |tags  |address                  |date_joined|timestamp_event        |corrupted_record|
+----+-------+----+---------+--------+-------------------------+------+-------------------------+-----------+-----------------------+----------------+
|1   |Alice  |30  |50000.500|true    |This is a comment.       |[A, B]|{123 Main St, Anytown}   |NULL       |NULL                   |NULL            |
|2   |Bob    |25  |45000.750|false   |Another comment.         |[C]   |{456 Oak Ave, Otherville}|NULL       |NULL                   |NULL            |
|3   |Charlie|NULL|NULL     |true    |Invalid JSON             |[D, E]|NULL                     |NULL       |NULL                   |NULL            |
|4   |David  |40  |60000.000|true    |NULL                     |NULL  |NULL                     |2023-01-15 |2023-01-15 10:30:00.123|NULL            |
|5   |Eve    |35  |55555.555|true    |This has 'single quotes'.|NULL  |NULL                     |NULL       |NULL                   |NULL            |
|6   |Frank  |28  |12345.678|true    |Escaped chars: \n\t\r    |[F]   |NULL                     |NULL       |NULL                   |NULL            |
|NULL|NULL   |NULL|NULL     |NULL    |NULL                     |NULL  |NULL                     |NULL       |NULL                   |NULL            |
+----+-------+----+---------+--------+-------------------------+------+-------------------------+-----------+-----------------------+----------------+
```

## 5. Reading a CSV data with various options

## 6. ORC & Parquet file formats for Performance Optimization

## 7. PySpark & Hive Integration : Data Ingestion and Table Creation

