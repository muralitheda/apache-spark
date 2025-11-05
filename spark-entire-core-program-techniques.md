# PySpark Core Program Techniques

## 1. How to create SparkSession object creation & sparkContext reference?

```text
SparkSession is the entry point for accessing the mypyspark cluster operation.
SparkSession object will instantiate SparkContex, SqlContext, HiveContext Objects.
|__SparkSession is a Class
      |__builder is a Class variable/object/attribute to call the SparkSession class methods like master(), appName(), enableHiveSupport(), getOrCreate()
           |__master('yarn')           => method to help us submit the mypyspark application to the respective cluster manager
           |__appName('program-name')  => mypyspark program name that help us identify the jobs runs in a cluster   
           |__enableHiveSupport()      => HiveQueryLanguage(HQL) method help us to create Catalog, UDFs, etc.,
           |__getOrCreate()            => method to create a new SparkSession object or referring to existing SparkSession
```

```

## 2. RDD (Resilient Distributed Dataset)

## 3. What is Transformation and Action in RDD?

## 4. Performance Optimization Basics

## 5. Main Program to covert all the Spark core programming concepts 