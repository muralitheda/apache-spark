# PySpark Core Program Techniques

## 1. How to create SparkSession object creation & sparkContext reference?

```
SparkSession is the entry point for accessing the mypyspark cluster operation.
SparkSession object will instantiate SparkContex, SqlContext, HiveContext Objects.
|__SparkSession is a Class
      |__builder is a Class variable/object/attribute to call the SparkSession class methods like master(), appName(), enableHiveSupport(), getOrCreate()
           |__master('yarn')           => method to help us submit the mypyspark application to the respective cluster manager
           |__appName('program-name')  => mypyspark program name that help us identify the jobs runs in a cluster   
           |__enableHiveSupport()      => HiveQueryLanguage(HQL) method help us to create Catalog, UDFs, etc.,
           |__getOrCreate()            => method to create a new SparkSession object or referring to existing SparkSession
```

```python
# Imports 
from pyspark.sql.session import SparkSession

# SparkSession Object Creation
spark=SparkSession.builder.getOrCreate()
print(f"spark:{spark}")

# Accessing the underlying sparkContext with the Spark Session
sc=spark.sparkContext
print(f"sparkContext:{sc}")

# Accessing the underlying SQLContext with the Spark Session.
# FutureWarning: Deprecated in 3.0.0. Use SparkSession.builder.getOrCreate() instead.
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
print(f"sqlContext:{sqlContext}")

# Accessing the underlying the HiveContext with the Spark Session. 
# FutureWarning: HiveContext is deprecated in Spark 2.0.0. Please use SparkSession.builder.enableHiveSupport().getOrCreate() instead.
from pyspark.sql import HiveContext
hiveContext = HiveContext(sc)
print(f"hiveContext:{hiveContext}")

"""
spark:<pyspark.sql.session.SparkSession object at 0xffff676ffe80>
sparkContext:<SparkContext master=local[*] appName=pyspark-shell>
sqlContext:<pyspark.sql.context.SQLContext object at 0xffff74883fa0>
hiveContext:<pyspark.sql.context.HiveContext object at 0xffff676fffa0>
"""
```

## 2. RDD (Resilient Distributed Dataset)

## 3. What is Transformation and Action in RDD?

## 4. Performance Optimization Basics

## 5. Main Program to covert all the Spark core programming concepts 