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
# Case1: Users entered course data in Unstructured format. We would like to know the distinct number of course, which course mostly wanted?

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

"""
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
"""
```

## 2. Reading a CSV data and write into MySQL database using JDBC option

## 3. Schema Evoluation/Growing handling using columner file formats ORC/Parquet

## 4. Reading a JSON data with various options

## 5. Reading a CSV data with various options

## 6. ORC & Parquet file formats for Performance Optimization

## 7. PySpark & Hive Integration : Data Ingestion and Table Creation

