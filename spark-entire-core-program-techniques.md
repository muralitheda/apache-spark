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

### Q1. Can we have more than SparkContext in a same application? 
✅ **Ans:**  Only one active SparkContext is allowed per `application`. Creating more causes an error. Use multiple SparkSessions instead—they share the same SparkContext.

```python
from pyspark.sql import SparkSession

# Create first SparkSession
spark1 = SparkSession.builder.appName("App1").getOrCreate()

try:
    sc1 = spark1.sparkContext()
except Exception as e:
    print(f"Exception Occured: {e}") # Exception Occured: 'SparkContext' object is not callable
    
# Create Second SparkSession (returns the same session)
spark2 = SparkSession.builder.appName("App2").getOrCreate()

# Create a new isolated session (shares same SparkContext)
spark3 = spark1.newSession() # Or spark1.stop(); spark3 = SparkSession.builder.appName("App1").getOrCreate()

print("spark1 is spark2:",spark1 is spark2) # True -> same SparkSession
print("spark1 is spark3:",spark1 is spark3) # False -> new SparkSession
print("spark1.sparkContext is spark3.sparkContext:",spark1.sparkContext is spark3.sparkContext) # True -> shared SparkContext

```

## 2. RDD (Resilient Distributed Dataset)

### Q1. Spark Terminologies?
    1. RDD            :Resilient(can be rebuild) Distribuited(across multiple nodes memory) Dataset (can come from anywhere)
    2. DAG            :(Direct Acyclic Graph)
    3. Transformation :
    4. Action         :
    5. Lineage        :(Direct relation between transformation and action)

### Q2. What is RDD?
    Resilient Distributed Dataset, Lazily evaluated and executed, Immutable, Core Spark Abstraction, Fundamental unit of data, Lineage to rebuild.

### Q3. What are ways to create RDDs?
    1. RDD from any sources(different filesystems)
    2. RDDs/DFs can be created programatically
    3. RDDs/DFs from another RDD/DF
    4. RDD/DF from memory 

```python
"""
download the csv dataset "custs" to linux and copy it to hdfs

ls /home/hduser/custs
hadoop fs -put /home/hduser/custs /user/hduser/
hadoop fs -ls /home/hduser/custs

"""
#1. RDD creation from different filesystems
file_rdd1 = spark.sparkContext.textFile("file:///home/hduser/custs") # linux file system
hdfs_rdd1 = spark.sparkContext.textFile("hdfs:///user/hduser/custs") # hdfs file system

#2. RDD creation programatically
program_rdd1 = spark.sparkContext.parallelize(range(1,1000))
salary_list = [20000,30000,40000,50000]
salary_list_rdd1 = spark.sparkContext.parallelize(salary_list,2) # Creating distributed RDD referencing 2 memory location (partitions)
print(f"salary_list_rdd1.collect():{salary_list_rdd1.collect()}") # Collect Action: Consolidate all the partitions and produce one result
print(f"salary_list_rdd1.glom().collect():{salary_list_rdd1.glom().collect()}") # Collect Action: Partition wise collect output

#3. RDD/DFs from another RDD/DF
revised_salary_rdd1 = salary_list_rdd1.map(lambda sal:sal+5000) # rdd created from another rdd
print(f"revised_salary_rdd1.collect():{revised_salary_rdd1.collect()}") # [25000, 35000, 45000, 55000]

#4. RDD/DF from memory
revised_salary_rdd1.cache() # value will be persist in the memory till the program complete exit
revised_salary_rdd2 = revised_salary_rdd1.map(lambda sal:sal+100) # rdd is created from memory
print(f"revised_salary_rdd2.collect():{revised_salary_rdd2.collect()}") # [25100, 35100, 45100, 55100]

```

## 3. What is Transformation and Action in RDD?

### Q1. Q1. What is Transformation & Action in RDD?
        Transformation:  If a function/method returns another RDD.   Operations => map(), flatMap(), filter(), distinct(), union()
        Action        :  If a function/method returns RESULT(VALUE). Operations => collect(), count(), take(3), reduce(), saveAsTextFile()

```python
rdd1 = spark.sparkContext.parallelize([20000,30000,15000,40000,50000],2)
rdd2 = rdd1.map(lambda sal:sal+1000) #Transformation (MAP returns another RDD)
print(rdd2.count())                  #Action (COUNT trigers computation and returns RESULT)
```

### Q2. What are types of Transformation?
        Active:  If the output number of elements of a given RDD is different from the input number of element of an RDD.
        Passive: If the output number of elements of a given RDD is same      from the input number of element of an RDD.
```python
"""
#/home/hduser/custs
4000001,Kristina,Chung,55,Pilot
4000002,Paige,Chen,77,Teacher
4000003,Sherri,Melton,34,Firefighter
4000004,Gretchen,Hill,66,Computer hardware engineer
"""
rdd1 = spark.sparkContext.textFile("file:///home/hduser/custs")
rdd2 = rdd1.map(lambda row:row.upper())         # Passive Transformation: map       - input passed 4 and output is 4

rdd3 = rdd2.filter(lambda row:'4000002' in row) # Active  Transformation: filter    - input passed 4 and output is 1
print(f"rdd3=>{rdd3.collect()}") # ['4000002,PAIGE,CHEN,77,TEACHER']

rdd4 = rdd3.flatMap(lambda row:row.split(','))    # Active  Transformation: filterMap - input passed 1 row and output is 5 rows
print(f"rdd4=>{rdd4.collect()}") # ['4000002', 'PAIGE', 'CHEN', '77', 'TEACHER']

```

### Q3.
### Q4.
### Q5.
### Q6.
### Q7.
### Q8.
### Q9.
### Q10.
### Q11.
### Q12.


## 4. Performance Optimization Basics

## 5. Main Program to covert all the Spark core programming concepts 