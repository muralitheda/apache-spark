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

## 2. Reading a CSV data and write into MySQL database using JDBC option

## 3. Schema Evoluation/Growing handling using columner file formats ORC/Parquet

## 4. Reading a JSON data with various options

## 5. Reading a CSV data with various options

## 6. ORC & Parquet file formats for Performance Optimization

## 7. PySpark & Hive Integration : Data Ingestion and Table Creation

