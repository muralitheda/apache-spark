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

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructType,StructField,StringType,IntegerType,DateType,TimestampType
import os

spark = SparkSession.builder.getOrCreate()

# Data
data = """
symbol,exchange,date,timestamp,price,volume
AAPL,NYSE,2023-08-01,2023-08-01 09:30:00,195.25,1200000
GOOGL,NYSE,2023-08-01,2023-08-01 09:30:00,2735.55,850000
MSFT,NYSE,2023-08-01,2023-08-01 09:30:00,-1,950000
TSLA,NYSE,2023-08-01,2023-08-01 09:30:00,Inf,1100000
AMZN,NYSE,na,2023-08-01 09:30:00,134.25,na
MSFK,NYSE,2023-08-01,2023-08-01 09:30,100.01,950000
INVALID_ROW_WITHOUT_PROPER_FIELDS       
"""

# CSV file creation
file_path = "/home/hduser/employe_csv/sample.csv"
directory = os.path.dirname(file_path)
os.makedirs(directory, exist_ok=True)
with open(file_path, "w") as f:
    f.write(data)

# Define custom schema
customschema = StructType([
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("date", DateType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("corrupted_data", StringType(), True)
])

# Read CSV with various options
df1 = spark.read.csv(
    path="file:///home/hduser/employe_csv/sample.csv",
    sep=',',  # Column separator used in the CSV file
    header=True,  # First line of the file contains column headers
    schema=customschema,  # Custom schema to define data types and structure
    columnNameOfCorruptRecord='corrupted_data',  # Stores malformed rows in this column
    encoding='UTF-8',  # Character encoding used to read the file
    quote="'",  # Defines single quote as the string quoting character
    comment='-',  # Lines starting with '-' are treated as comments and ignored
    ignoreTrailingWhiteSpace=True,  # Trims trailing whitespace from fields
    ignoreLeadingWhiteSpace=True,  # Trims leading whitespace from fields
    nullValue='na',  # Treats 'na' as a null value
    nanValue='-1',  # Treats '-1' as NaN (Not a Number)
    positiveInf='Inf',  # Treats 'Inf' as positive infinity
    dateFormat='yyyy-MM-dd',  # Format used to parse date fields
    timestampFormat='yyyy-MM-dd HH:mm:ss',  # Format used to parse timestamp fields
    maxColumns=40  # Maximum number of columns allowed in the file
)

# Show first 10 rows
df1.show(10, False)

print("[INFO] Corruputed Rows")
# Cache and filter corrupted rows
df2 = df1.cache().where("corrupted_data is not null")
df2.show(10, False)  # Display malformed rows for RCA
```

```python
+---------------------------------+--------+----------+-------------------+--------+-------+---------------------------------------------------+
|symbol                           |exchange|date      |timestamp          |price   |volume |corrupted_data                                     |
+---------------------------------+--------+----------+-------------------+--------+-------+---------------------------------------------------+
|AAPL                             |NYSE    |2023-08-01|2023-08-01 09:30:00|195.25  |1200000|NULL                                               |
|GOOGL                            |NYSE    |2023-08-01|2023-08-01 09:30:00|2735.55 |850000 |NULL                                               |
|MSFT                             |NYSE    |2023-08-01|2023-08-01 09:30:00|NaN     |950000 |NULL                                               |
|TSLA                             |NYSE    |2023-08-01|2023-08-01 09:30:00|Infinity|1100000|NULL                                               |
|AMZN                             |NYSE    |NULL      |2023-08-01 09:30:00|134.25  |NULL   |NULL                                               |
|MSFK                             |NYSE    |2023-08-01|NULL               |100.01  |950000 |MSFK,NYSE,2023-08-01,2023-08-01 09:30,100.01,950000|
|INVALID_ROW_WITHOUT_PROPER_FIELDS|NULL    |NULL      |NULL               |NULL    |NULL   |INVALID_ROW_WITHOUT_PROPER_FIELDS                  |
+---------------------------------+--------+----------+-------------------+--------+-------+---------------------------------------------------+

[INFO] Corruputed Rows
+---------------------------------+--------+----------+---------+------+------+---------------------------------------------------+
|symbol                           |exchange|date      |timestamp|price |volume|corrupted_data                                     |
+---------------------------------+--------+----------+---------+------+------+---------------------------------------------------+
|MSFK                             |NYSE    |2023-08-01|NULL     |100.01|950000|MSFK,NYSE,2023-08-01,2023-08-01 09:30,100.01,950000|
|INVALID_ROW_WITHOUT_PROPER_FIELDS|NULL    |NULL      |NULL     |NULL  |NULL  |INVALID_ROW_WITHOUT_PROPER_FIELDS                  |
+---------------------------------+--------+----------+---------+------+------+---------------------------------------------------+

```
## 6. ORC & Parquet file formats for Performance Optimization

```python
# Data SCHEMA MIGRATION from csv (Struct) to ORC/Parquet (serialized-binary) and load into our DATALAKE

# Data
data =""" 
stock_symbol,exchange,date,timestamp,price,volume
AAPL,NYSE,2023-08-01,2023-08-01 09:30:00,195.25,1200000
GOOGL,NYSE,2023-08-01,2023-08-01 09:30:00,2735.55,850000
MSFT,NYSE,2023-08-01,2023-08-01 09:30:00,-1,950000
TSLA,NYSE,2023-08-01,2023-08-01 09:30:00,Inf,1100000
TSLA,NYSE,2023-08-02,2023-08-02 09:30:00,Inf,1100000
AMZN,NYSE,na,2023-08-01 09:30:00,134.25,na
MSFK,NYSE,2023-08-01,2023-08-01 09:30,100.01,950000
INVALID_ROW_WITHOUT_PROPER_FIELDS 
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructType,StructField,StringType,IntegerType,DateType,TimestampType
import os

spark = SparkSession.builder.getOrCreate()

with open("/tmp/nyse_header_options2.csv", "w") as f:
    f.write(data)
  
# Define custom schema
customschema = StructType([
    StructField("symbol", StringType(), True),
    StructField("exchange", StringType(), True),
    StructField("date", DateType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", IntegerType(), True),
    StructField("corrupted_data", StringType(), True)
])
 
# Read CSV with various options
df1 = spark.read.csv(
    'file:///tmp/nyse_header_options2.csv',
    sep=',',  # Column separator used in the CSV file
    header=True,  # First line of the file contains column headers
    schema=customschema,  # Custom schema to define data types and structure
    columnNameOfCorruptRecord='corrupted_data',  # Stores malformed rows in this column
    encoding='UTF-8',  # Character encoding used to read the file
    quote="'",  # Defines single quote as the string quoting character
    comment='-',  # Lines starting with '-' are treated as comments and ignored
    ignoreTrailingWhiteSpace=True,  # Trims trailing whitespace from fields
    ignoreLeadingWhiteSpace=True,  # Trims leading whitespace from fields
    nullValue='na',  # Treats 'na' as a null value
    nanValue='-1',  # Treats '-1' as NaN (Not a Number)
    positiveInf='Inf',  # Treats 'Inf' as positive infinity
    dateFormat='yyyy-MM-dd',  # Format used to parse date fields
    timestampFormat='yyyy-MM-dd HH:mm:ss',  # Format used to parse timestamp fields
    maxColumns=40  # Maximum number of columns allowed in the file
 
)
 
# Show first 10 rows
print("[INFO] Scrubbed Data")
df1.show(10, False)
 
# Cache and filter corrupted rows
df2 = df1.cache().where("corrupted_data is null")
print("[INFO] Cureated Data")
df2.show(10, False)  
 
```

```python
+---------------------------------+--------+----------+-------------------+--------+-------+---------------------------------------------------+
|symbol                           |exchange|date      |timestamp          |price   |volume |corrupted_data                                     |
+---------------------------------+--------+----------+-------------------+--------+-------+---------------------------------------------------+
|AAPL                             |NYSE    |2023-08-01|2023-08-01 09:30:00|195.25  |1200000|NULL                                               |
|GOOGL                            |NYSE    |2023-08-01|2023-08-01 09:30:00|2735.55 |850000 |NULL                                               |
|MSFT                             |NYSE    |2023-08-01|2023-08-01 09:30:00|NaN     |950000 |NULL                                               |
|TSLA                             |NYSE    |2023-08-01|2023-08-01 09:30:00|Infinity|1100000|NULL                                               |
|AMZN                             |NYSE    |NULL      |2023-08-01 09:30:00|134.25  |NULL   |NULL                                               |
|MSFK                             |NYSE    |2023-08-01|NULL               |100.01  |950000 |MSFK,NYSE,2023-08-01,2023-08-01 09:30,100.01,950000|
|INVALID_ROW_WITHOUT_PROPER_FIELDS|NULL    |NULL      |NULL               |NULL    |NULL   |INVALID_ROW_WITHOUT_PROPER_FIELDS                  |
+---------------------------------+--------+----------+-------------------+--------+-------+---------------------------------------------------+

[INFO] Cureated Data
+------+--------+----------+-------------------+--------+-------+--------------+
|symbol|exchange|date      |timestamp          |price   |volume |corrupted_data|
+------+--------+----------+-------------------+--------+-------+--------------+
|AAPL  |NYSE    |2023-08-01|2023-08-01 09:30:00|195.25  |1200000|NULL          |
|GOOGL |NYSE    |2023-08-01|2023-08-01 09:30:00|2735.55 |850000 |NULL          |
|MSFT  |NYSE    |2023-08-01|2023-08-01 09:30:00|NaN     |950000 |NULL          |
|TSLA  |NYSE    |2023-08-01|2023-08-01 09:30:00|Infinity|1100000|NULL          |
|AMZN  |NYSE    |NULL      |2023-08-01 09:30:00|134.25  |NULL   |NULL          |
+------+--------+----------+-------------------+--------+-------+--------------+

```

```python
# Data SCHEMA MIGRATION from csv (Struct) to ORC (serialized-binary) and load into our DATALAKE
# Write the clean data in ORC format for internal teams data querying furpose
df2.write.orc('file:///tmp/stocks_orc',mode='overwrite') #(Datalake) hdfs:///user/hduser/custorcout
df2.write.orc('file:///tmp/stocks_orc_lzo',mode='ignore',compression='lzo')
df2.write.orc('file:///tmp/stocks_orc_lzo_part',mode='overwrite',partitionBy='date')

# Reading the data from ORC
df_orc = spark.read.orc("file:///tmp/stocks_orc_lzo_part")
print("[INFO] Reading data from ORC")
df_orc.show(truncate=False)
 
df_orc_sql = spark.sql("select * from orc.`file:///tmp/stocks_orc_lzo_part` where date is not null")
print("[INFO] Reading data from ORC using SQL")
df_orc_sql.show(truncate=False)
```

```python
[INFO] Reading data from ORC
+------+--------+-------------------+--------+-------+--------------+----------+
|symbol|exchange|timestamp          |price   |volume |corrupted_data|date      |
+------+--------+-------------------+--------+-------+--------------+----------+
|GOOGL |NYSE    |2023-08-01 09:30:00|2735.55 |850000 |NULL          |2023-08-01|
|MSFT  |NYSE    |2023-08-01 09:30:00|NaN     |950000 |NULL          |2023-08-01|
|TSLA  |NYSE    |2023-08-01 09:30:00|Infinity|1100000|NULL          |2023-08-01|
|AAPL  |NYSE    |2023-08-01 09:30:00|195.25  |1200000|NULL          |2023-08-01|
|AMZN  |NYSE    |2023-08-01 09:30:00|134.25  |NULL   |NULL          |NULL      |
+------+--------+-------------------+--------+-------+--------------+----------+

[INFO] Reading data from ORC using SQL
+------+--------+-------------------+--------+-------+--------------+----------+
|symbol|exchange|timestamp          |price   |volume |corrupted_data|date      |
+------+--------+-------------------+--------+-------+--------------+----------+
|GOOGL |NYSE    |2023-08-01 09:30:00|2735.55 |850000 |NULL          |2023-08-01|
|MSFT  |NYSE    |2023-08-01 09:30:00|NaN     |950000 |NULL          |2023-08-01|
|TSLA  |NYSE    |2023-08-01 09:30:00|Infinity|1100000|NULL          |2023-08-01|
|AAPL  |NYSE    |2023-08-01 09:30:00|195.25  |1200000|NULL          |2023-08-01|
+------+--------+-------------------+--------+-------+--------------+----------+

```

```python
# Data SCHEMA MIGRATION from csv (Struct) to PARQUET (serialized-binary) and load into our DATALAKE
# Write the clean data in ORC format for internal teams data querying furpose
df2.write.parquet('file:///tmp/stocks_parquet',mode='overwrite') #(Datalake) hdfs:///user/hduser/custorcout
df2.write.parquet('file:///tmp/stocks_parquet_lzo',mode='ignore',compression='snappy')
df2.write.parquet('file:///tmp/stocks_parquet_part',mode='overwrite',partitionBy='date')
 
# Reading the data from Parquet
df_orc = spark.read.parquet("file:///tmp/stocks_parquet_part")
print("[INFO] Reading data from Parquet")
df_orc.show(truncate=False)
 
df_orc_sql = spark.sql("select * from parquet.`file:///tmp/stocks_parquet_part` where date is not null")
print("[INFO] Reading data from Parquet using SQL")
df_orc_sql.show(truncate=False)
```

```python
[INFO] Reading data from Parquet
+------+--------+-------------------+--------+-------+--------------+----------+
|symbol|exchange|timestamp          |price   |volume |corrupted_data|date      |
+------+--------+-------------------+--------+-------+--------------+----------+
|GOOGL |NYSE    |2023-08-01 09:30:00|2735.55 |850000 |NULL          |2023-08-01|
|MSFT  |NYSE    |2023-08-01 09:30:00|NaN     |950000 |NULL          |2023-08-01|
|TSLA  |NYSE    |2023-08-01 09:30:00|Infinity|1100000|NULL          |2023-08-01|
|AAPL  |NYSE    |2023-08-01 09:30:00|195.25  |1200000|NULL          |2023-08-01|
|TSLA  |NYSE    |2023-08-02 09:30:00|Infinity|1100000|NULL          |2023-08-02|
|AMZN  |NYSE    |2023-08-01 09:30:00|134.25  |NULL   |NULL          |NULL      |
+------+--------+-------------------+--------+-------+--------------+----------+

[INFO] Reading data from Parquet using SQL
+------+--------+-------------------+--------+-------+--------------+----------+
|symbol|exchange|timestamp          |price   |volume |corrupted_data|date      |
+------+--------+-------------------+--------+-------+--------------+----------+
|GOOGL |NYSE    |2023-08-01 09:30:00|2735.55 |850000 |NULL          |2023-08-01|
|MSFT  |NYSE    |2023-08-01 09:30:00|NaN     |950000 |NULL          |2023-08-01|
|TSLA  |NYSE    |2023-08-01 09:30:00|Infinity|1100000|NULL          |2023-08-01|
|AAPL  |NYSE    |2023-08-01 09:30:00|195.25  |1200000|NULL          |2023-08-01|
|TSLA  |NYSE    |2023-08-02 09:30:00|Infinity|1100000|NULL          |2023-08-02|
+------+--------+-------------------+--------+-------+--------------+----------+
```

## 7. PySpark & Hive Integration : Data Ingestion and Table Creation

```python
# This script demonstrates various methods for writing data from a PySpark
# DataFrame into Hive tables, highlighting best practices, limitations,
# and common use cases.
from pyspark.sql.types import ShortType, StringType,IntegerType,StructType, StructField
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Placeholder for the data and schema
cust_schema = StructType([
    StructField('cid', IntegerType(), nullable=False),
    StructField('fname', StringType()),
    StructField('lname', StringType()),
    StructField('age', ShortType()),
    StructField('profession', StringType())
])

df1 = spark.read.csv(
     'file:///home/hduser/custinfo.csv',
     schema=cust_schema,
     header=False,
     sep=',',
     mode='dropmalformed'
)

df1.show(5)
```

```python
# ==============================================================================
# Method 1: Using the `insertInto` function
#
# This method loads data into an existing table. It's less common for initial
# table creation and data loading, as it does not create a new table schema.
# ==============================================================================
print("--- Method 2: Inserting data into an existing table using insertInto ---")
# This requires the 'default.customers' table to already exist.
# The schema of the DataFrame must match the table schema.
# df1.write.insertInto('wholesale.customers', overwrite=True)
```

```python
# ==============================================================================
# Method 2: Storing as CSV (PySpark only)
#
# This method creates a table with data stored in CSV format. This table is
# typically only accessible and readable via PySpark, not directly via HiveQL.
# The SerDe (Serializer/Deserializer) for Spark-written CSVs is not
# compatible with Hive's default TextFile SerDe.
# ==============================================================================
print("--- Method 3: Creating a CSV table (PySpark-only access) ---")
# Creates a managed table with data stored as CSV files.
# Hive CLI will not be able to read this table correctly.
df1.write.saveAsTable(
    'default.customers_csv',
    format='csv',
    sep=',',
    mode='overwrite'
)
```

```python
# ==============================================================================
# Method 4: Using Hive Literal Syntax
#
# This approach uses direct HiveQL statements to create a table that is
# fully compatible with both Hive and PySpark. It is the proper way to
# meet a requirement for a TextFile table with a specific delimiter.
# ==============================================================================
print("--- Method 4: Creating a TextFile table using HiveQL (interoperable) ---")
# Step 1: Create the table using HiveQL with the specified row format.
spark.sql("""
   CREATE TABLE default.customers_text (
        id INT,
        fname STRING,
        lname STRING,
        age INT,
        prof STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
""")

# Step 2: Load the data into the newly created table.
# Note: 'local' means the file is on the driver's local filesystem.

spark.sql(
    "LOAD DATA LOCAL INPATH 'file:///home/hduser/custinfo.csv' OVERWRITE INTO TABLE default.customers_text"
)
```

```python
# ==============================================================================
# Method 5: Marrying DataFrame to a Hive Table using a View and Insert Select
#
# This is a common pattern to load data from a DataFrame into an existing
# Hive table, providing full interoperability.
# ==============================================================================
print("--- Method 5: Using Insert Select from a temporary view ---")
# Step 1: Create a temporary view from the DataFrame.
df1.createOrReplaceTempView("view1")

# Step 2: Insert data from the view into an existing Hive table.
# Assumes 'default.customers_text' exists and has a compatible schema.
spark.sql("INSERT OVERWRITE TABLE default.customers_csv SELECT * FROM view1")
```

```python
# ==============================================================================
# Creating External Tables
#
# Demonstrates creating external tables, where the data is not managed by Hive,
# using both non-partitioned and partitioned approaches.
# ==============================================================================
print("--- Creating a non-partitioned external table ---")
spark.sql("""
    CREATE EXTERNAL TABLE default.customers_text_ext (
        id INT,
        fname STRING,
        lname STRING,
        age INT,
        prof STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE 
    LOCATION '/user/hduser/customer_ext_table'
""")
# Loading data into the external table.
spark.sql(
    "LOAD DATA LOCAL INPATH 'file:///home/hduser/custinfo.csv' OVERWRITE INTO TABLE default.customers_text_ext"
)

print("--- Creating a partitioned external table ---")
spark.sql("""
    CREATE EXTERNAL TABLE default.customers_text_ext_part (
        id INT,
        fname STRING,
        lname STRING,
        age INT
    )
    PARTITIONED BY (prof STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE 
    LOCATION '/user/hduser/customer_ext_table_part'
""")

# Note: `LOAD DATA` does not support dynamic partitioning.
# Instead, we use `INSERT OVERWRITE` with dynamic partitioning enabled.
spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic") # Use Spark setting for partition overwrite mode
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")

# Insert data dynamically into the partitioned table.
spark.sql(
    "INSERT OVERWRITE TABLE default.customers_text_ext_part PARTITION(prof) SELECT id, fname, lname, age, prof FROM view1"
)
```

```python
# ==============================================================================
# Spark and Hive Bucketing Incompatibility
#
# This is a critical point about a known limitation. Spark and Hive use
# different hashing algorithms for bucketing, making them incompatible.
# ==============================================================================
print("--- Demonstrating Spark-Hive Bucketing Incompatibility ---")
# Drop the table if it already exists
spark.sql("DROP TABLE IF EXISTS default.customers_text_ext_part_bucket")

# Create a bucketed table using HiveQL
spark.sql("""
    CREATE EXTERNAL TABLE default.customers_text_ext_part_bucket (
        id INT,
        fname STRING,
        lname STRING,
        age INT,
        prof STRING
    )
    CLUSTERED BY (id) INTO 10 BUCKETS
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    STORED AS TEXTFILE 
    LOCATION '/user/hduser/customer_ext_table_part_bucket'
""")

# Attempting to load data from a DataFrame into this table will fail
# because Spark's bucketing algorithm is not compatible with Hive's.
# The following line will raise an AnalysisException.
# spark.sql("INSERT OVERWRITE TABLE wholesale.customers_text_ext_part_bucket SELECT * FROM view1")
```

```python
# ==============================================================================
# Solution for High Performance: Combining Partitioning, Bucketing, and Columnar Format
#
# This code shows how to create a highly optimized table entirely within
# PySpark, which is best for Spark-native queries.
# Default format is Parquet to saving the data and Snappy codec for compression.
# ==============================================================================
print("--- Creating a highly performant table using PySpark native methods ---")
df1.write.bucketBy(10, 'cid').sortBy("cid").\
    saveAsTable(
        'default.customers_part_buck_parquet_snappy',
        mode='overwrite',
        partitionBy='profession'
    )

print("--- Conclusion on PySpark and Hive Integration ---")
print("PySpark and Hive have strong integration, but it's crucial to understand")
print("the differences in their internal implementations, especially concerning")
print("file formats and bucketing algorithms, to ensure interoperability and performance.")
```