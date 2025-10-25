from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DemoApp") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
print(sc)   # SparkContext info
print(spark)   # SparkContext info

spark.stop()

from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("DemoApp") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
print(sc)   # SparkContext info
print(spark)   # SparkContext info

spark.stop()