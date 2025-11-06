# PySpark Core Program Techniques

## 1. How to create SparkSession object creation & sparkContext reference?

**📁 Package structure (inside PySpark)**

    A *module* = any single `.py` file that contains Python code.
    A *package* = a folder that contains an `__init__.py` file (and possibly several modules).

**Here’s a simplified version of how `pyspark.sql` looks internally:**
```markdown
pyspark/               ← Package because it has (__init__.py) file
│
├── __init__.py            
└── sql/               ← Package because it has (__init__.py) file 
    ├── __init__.py    
    ├── session.py     ← this is a module
    ├── dataframe.py
    ├── functions.py
    └── ...
```

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

### Q1. What is Transformation & Action in RDD?
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

### Q3.  `map()` (Active/Passive) transformation charactertics?
    #Map is higher order of method. It takes function as an input argument and returns a RDD.
    #Map used to apply any functionality/transformation on every element of a given RDD.
    #Map is like a for/while loop but it is distributed & parallel function that can run on rdd partitions concurrently.
    #SQL side example: 
        select firstname from customer        => select is a map function
        select upper(firstname) from customer => upper(firstname) is a lambda function

```python
map_func_upper1=lambda row:row.upper()
rdd2=rdd1.map(map_func_upper1) #HOF, select=map, upper(cols)=lambda function
print(rdd2.top(5)) 

"""
['4009999,RAY,HEWITT,64,CARPENTER', '4009998,TRACEY,BULLOCK,60,COMPUTER HARDWARE ENGINEER', '4009997,RON,GRIMES,36,COMPUTER HARDWARE ENGINEER', '4009996,TONYA,MCINTOSH,56,ENGINEERING TECHNICIAN', '4009995,REBECCA,DENNIS,37,TEACHER']
"""
```

### Q4. `flatMap()` (Active) transformation charactertics?
    #flatMAP is higher order of method.
    #flatMAP used to iterate on the given list of values and flatten it or expose it like explode function in DB
    #flatMAP runs nested 2 for loops when comparing with python programming

    A table with 2 rows:
    [ ["USD","EUR","GBP"], ["MAR","SGD","INR"] ]
    
    #Flattening
    USD
    EUR
    GBP
    MAR
    SGD
    INR

```python

#Structured data :: Flatting

currency_list = [ ["usd","eur","gbp"], ["mar","sgd","inr"] ]
for i in (currency_list):
    for j in i:
        print(j.upper())

rdd1 = spark.sparkContext.parallelize(currency_list)
rdd2 = rdd1.flatMap(lambda x:x)      # exposing two list elements into a single list.
rdd3 = rdd2.map(lambda x:x.upper())
print(rdd3.collect())

"""
USD
EUR
GBP
MAR
SGD
INR
['USD', 'EUR', 'GBP', 'MAR', 'SGD', 'INR']
"""

#Unstructure data :: Flattening

"""
#vi /home/hduser/unstruct.txt
hadoop mypyspark hadoop mypyspark kafka datascience
mypyspark hadoop mypyspark datascience
informatica java aws gcp
gcp aws azure mypyspark
gcp mypyspark hadoop hadoop
"""

rdd1 = spark.sparkContext.textFile('file:///home/hduser/unstruct.txt')
flatmap_rdd1 = rdd1.flatMap(lambda x:x.split(' '))
print(flatmap_rdd1.collect())

"""
['hadoop', 'spark', 'hadoop', 'spark', 'kafka', 'datascience', 'spark', 'hadoop', 'spark', 'datascience', 'informatica', 'java', 'aws', 'gcp', 'gcp', 'aws', 'azure', 'spark', 'gcp', 'pyspark', 'hadoop', 'hadoop']
"""

```

### Q5. `filter()` (Active) transformation charactertics?
    #Filter is higher order of method. It takes function as an input argument and returns a RDD.
    #Filter used to apply any conditions on every element of a given RDD.
    #Filter is active transformation.
    #Filter is like a for/while loop with if condition but it is distributed & parallel function that can run on rdd partitions concurrently.
    #SQL side example: select statement with a where clause.

```python

filter_func1 = lambda row: 'GCP' in row.upper()
filter_rdd2 = rdd1.filter(filter_func1)
print(filter_rdd2.collect())

"""
['informatica java aws gcp', 'gcp aws azure spark', 'gcp pyspark hadoop hadoop']
"""
```

### Q6. Difference between `map()` and `flatMap()`
    # Map is equivalent to select statement with some functions in the select statement.
    # flatMAP is equivalent to select with EXPLODE function in the select statement.
    # Map will iterate first level (rows), flatMap will iterate two levels (rows and it's each elements(cols) for transposing/flattening)
    # Map can be applied structured data. flatMap will be applied both structured and unstructured data also.

### Q7. `distinct()` & `union()` transformation functions.

```python
rdd1 = spark.sparkContext.parallelize([1,2,3,4])
rdd2 = spark.sparkContext.parallelize([3,4,5,6])
distinct_union_rdd = rdd1.union(rdd2).distinct()
print(distinct_union_rdd.collect()) # [1, 2, 3, 4, 5, 6]
```
### Q8. Key-Value Pair RDDs: Computation based on Keys
        Transformation : mapValues(), flatMapValues()
        Action         : reduceByKey()/aggregateByKey(), groupByKey(), sortByKey(), join()

```python

#1. mapValues()
paired_rdd = spark.sparkContext.parallelize([("hr",10000),("mkt",20000),("hr",30000),("mkt",40000)])
mapped_values_rdd = paired_rdd.mapValues(lambda val:val+100)
print(mapped_values_rdd.collect())
"""
[('hr', 10100), ('mkt', 20100), ('hr', 30100), ('mkt', 40100)]
"""

#2. flatMapValues()
paired_rdd = spark.sparkContext.parallelize([("hr",[10000,100]),("mkt",[20000,100]),("hr",[30000,100]),("mkt",[40000,100])])
flat_mapped_values_rdd = paired_rdd.flatMapValues(lambda x:x)
print(flat_mapped_values_rdd.collect())
"""
[('hr', 10000), ('hr', 100), ('mkt', 20000), ('mkt', 100), ('hr', 30000), ('hr', 100), ('mkt', 40000), ('mkt', 100)]
"""
flat_mapped_values_rdd = flat_mapped_values_rdd.mapValues(lambda val:val+100)
print(flat_mapped_values_rdd.collect())
"""
[('hr', 10100), ('hr', 200), ('mkt', 20100), ('mkt', 200), ('hr', 30100), ('hr', 200), ('mkt', 40100), ('mkt', 200)]
"""

#1. reduceByKey()
reduced_rdd = flat_mapped_values_rdd.reduceByKey(lambda mindvalue,fingervalue:mindvalue+fingervalue)
print(reduced_rdd.collect())
"""
[('mkt', 60600), ('hr', 40600)]
"""

#2. groupByKey()
grouped_rdd = flat_mapped_values_rdd.groupByKey().mapValues(lambda val:len(val))
print(grouped_rdd.collect())
"""
[('mkt', 4), ('hr', 4)]
"""

#3. sortByKey()
sorted_rdd = flat_mapped_values_rdd.sortByKey()
print(sorted_rdd.collect())
"""
[('hr', 10100), ('hr', 200), ('hr', 30100), ('hr', 200), ('mkt', 20100), ('mkt', 200), ('mkt', 40100), ('mkt', 200)]
"""

#6. join()
dept_sal_rdd = spark.sparkContext.parallelize((['hr',1000],['mkt',2000]))
dept_emp_rdd = spark.sparkContext.parallelize((['hr','101,102,103'],['mkt','201,202,203,204']))
dept_emp_sal_rdd = dept_emp_rdd.join(dept_sal_rdd)
print(dept_emp_sal_rdd.collect())
"""
[('mkt', ('201,202,203,204', 2000)), ('hr', ('101,102,103', 1000))]
"""
```


### Q9. Write a word count program using mypyspark core? How to identify the occurance of the given words in a unstructured dataset?

```python
"""
#coursedata.txt
Python Cloud AI Docker Kubernetes Rust

Docker Java JavaScript React Spark Kafka SQL Git Go Python AI

AWS Azure TensorFlow PyTorch Android iOS Rust AI
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

file_rdd = spark.sparkContext.textFile("file:///home/hduser/coursedata.txt")
words_rdd = file_rdd.flatMap(lambda x:x.split(" "))
words_pair_rdd = words_rdd.map(lambda x: (x,1))
words_count_rdd = words_pair_rdd.reduceByKey(lambda mind,finger:mind+finger).sortByKey()
print(words_count_rdd.collect())

"""
[('', 2), ('AI', 3), ('AWS', 1), ('Android', 1), ('Azure', 1), ('Cloud', 1), ('Docker', 2), ('Git', 1), ('Go', 1), ('Java', 1), ('JavaScript', 1), ('Kafka', 1), ('Kubernetes', 1), ('PyTorch', 1), ('Python', 2), ('React', 1), ('Rust', 2), ('SQL', 1), ('Spark', 1), ('TensorFlow', 1), ('iOS', 1)]
"""

```

### Q10.
### Q11.
### Q12.


## 4. Performance Optimization Basics

## 5. Main Program to covert all the Spark core programming concepts 