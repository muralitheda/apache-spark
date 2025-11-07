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

### Q9. How to materialize Actions in RDDs?
        - Action is an RDD function/method used to return the result/value to the driver or the storage layer
        - Collect() action used to collect the rdd elements as a result to the driver from executors.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

rdd1 = spark.sparkContext.parallelize(range(1,10),2)
print('rdd1.collect():',rdd1.collect())
print('rdd1.take(2):',rdd1.take(2))
print('rdd1.first():',rdd1.first())
print('rdd1.count():',rdd1.count())
print('rdd1.top(2):',rdd1.top(2))
print('rdd1.takeOrdered(1):',rdd1.takeOrdered(2))

"""
rdd1.collect(): [1, 2, 3, 4, 5, 6, 7, 8, 9]
rdd1.take(2): [1, 2]
rdd1.first(): 1
rdd1.count(): 9
rdd1.top(2): [9, 8]
rdd1.takeOrdered(1): [1,2]
"""
```

| Action           | Description                                                           | Output (for this RDD) | Notes                                                |
| ---------------- | --------------------------------------------------------------------- | --------------------- | ---------------------------------------------------- |
| `collect()`      | Returns **all elements** from all partitions as a list.               | `[1,2,3,4,5,6,7,8,9]` | Brings all data to driver → avoid on huge RDDs       |
| `take(2)`        | Returns **first 2 elements** from the RDD (based on partition order). | `[1, 2]`              | Reads just enough partitions to get 2 elements       |
| `top(2)`         | Returns **2 largest elements**, in descending order.                  | `[9, 8]`              | Uses the default ordering (numeric or lexicographic) |
| `first()`        | Returns **the first element** of the RDD.                             | `1`                   | Shortcut for `take(1)[0]`                            |
| `count()`        | Returns the **number of elements** in the RDD.                        | `9`                   | Simple action that triggers computation              |
| `takeOrdered(1)` | Returns **1 smallest element**, in ascending order.                   | `[1]`                 | Opposite of `top()`                                  |

### Q10. Collect() Action has to be carefully used or avoid using. Why?
        1. Collect brings all data from multiple executor to one driver, hence resource consumption like network and memory are high.
        2. Collect may reduce the performance of the application when used on the large volumne of the data.
        3. Collect may break the application with OOM exception when used on the large volumne of the data.
        4. Alternative for collect() - sampling, storage in disk, take(2), first(), top(2), count() ...
        5. Conclusion: collect() should be used for development, testing or in production (if it is inevitable) 


### Q11. reduce() vs reduceByKey() Actions?
        - reduce() action help us reduce/consoildate/combine the result in any customized way needed the result
        - reduceByKey() is an action, this function will help us apply aggregation operation on the mapped/direct data

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

rdd = spark.sparkContext.parallelize([10000,20000,10000,20000])
print('sum overall salary:',rdd.reduce(lambda mind,finger:mind+finger)) # 60000

paired_rdd = spark.sparkContext.parallelize([['hr',10000], ['mkt',20000], ['hr',10000],['mkt',20000]])
print('dept wise salary:',paired_rdd.reduceByKey(lambda mind,finger:mind+finger).collect()) # [('mkt', 40000), ('hr', 20000)]
```


### Q12. Write a word count program using pyspark core? How to identify the occurance of the given words in a unstructured dataset?

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

### Q13. Write a production ready word count program using pyspark?

```python

#Standard Spark Application
#FileName: practice_program_core_words_count.py

"""
copy the below program and save it as practice_program_core_words_count.py
input file path      => file:///home/hduser/coursedata.txt
Spark Submit Command => mypyspark-submit --master yarn --deploy-mode cluster practice_program_core_words_count.py file:///home/hduser/coursedata.txt file:///home/hduser/sparkprogram/010304
"""

from pyspark.sql.session import SparkSession
import sys
import datetime

def cnt(rdd):
    return rdd.count()

def add_counts(a,b):
    return a + b

def main(arg1,arg2):
    file_rdd1 = spark.sparkContext.textFile(arg1)
    file_reduced_rdd = file_rdd1.filter(lambda x: len(x) > 0)
    print(cnt(file_reduced_rdd))
    flatten_rdd = file_reduced_rdd.flatMap(lambda row: row.split(" "))
    filter_flatten_rdd = flatten_rdd.filter(lambda value: value != 'Java')

    total_words = filter_flatten_rdd.map(lambda word:1).reduce(add_counts)
    print("Total number of words (excluding Java) using reduce:",total_words)

    paired_rdd = filter_flatten_rdd.map(lambda word: [word, 1])
    reduced_pair_rdd = paired_rdd.reduceByKey(lambda mindvalue, keyvalue: mindvalue + keyvalue)
    print('reduced_pair_rdd.collect()', reduced_pair_rdd.collect())
    print('reduced_pair_rdd.count():',reduced_pair_rdd.count())
    print('reduced_pair_rdd.take(3)',reduced_pair_rdd.take(3))
    print('reduced_pair_rdd.first()', reduced_pair_rdd.first())
    print('reduced_pair_rdd.saveAsTextFile()',arg2)
    reduced_pair_rdd.saveAsTextFile(arg2)

if __name__ == '__main__':
    spark = SparkSession.builder.appName("AppWordCountProgram").getOrCreate()
    if len(sys.argv) < 3 :
        print('[WARNING]Input/Output file path is not given. So selecting default path')
        formatted_time= datetime.datetime.now().strftime("%H%M%S")
        input_path  =  'file:///home/hduser/coursedata.txt'
        output_path =  'file:///home/hduser/sparkprogram/'+formatted_time
        main(input_path,output_path)
    else:
        main(sys.argv[1],sys.argv[2])
"""
[WARNING]Input/Output file path is not given. So selecting default path
3
Total number of words (excluding Java) using reduce: 24
reduced_pair_rdd.collect() [('Python', 2), ('Cloud', 1), ('JavaScript', 1), ('Spark', 1), ('SQL', 1), ('Go', 1), ('Azure', 1), ('TensorFlow', 1), ('AI', 3), ('Docker', 2), ('Kubernetes', 1), ('Rust', 2), ('React', 1), ('Kafka', 1), ('Git', 1), ('AWS', 1), ('PyTorch', 1), ('Android', 1), ('iOS', 1)]
reduced_pair_rdd.count(): 19
reduced_pair_rdd.take(3) [('Python', 2), ('Cloud', 1), ('JavaScript', 1)]
reduced_pair_rdd.first() ('Python', 2)
reduced_pair_rdd.saveAsTextFile() file:///home/hduser/sparkprogram/210427
"""
```

### Q14. What are all we learn out of this exercise?
        1. 40% of the Spark concepts are covered.
        2. All Spark Core Concepts: 
                - SparkSession,
                - sparkContext Object,
                - RDD,
                - Paired RDD => reduceByKey(), groupByKey(), sortByKey(), join()
                - DAG => Plan for execution and planning,
                - Lineage,
                - Transformation (Active/Passive) => map(), filter(), flatMap(), distinct(), union()
                - Action (mandatory) => collect(), count(), take(), first(), reduce(), saveAsTextFile()
                - Partitioning
        3. How to write a standard mypyspark application:
                - defining main method
                - evaluating parameters
                - passing parameters
                - seperating entire application
                - referencing of libraries
                - defining/leveraging reusable function
                - controlling the main method
        4. All Spark Terminologies:
                - Cluster Manager(--master yarn/local) => RM
                - Spark Worker                         => NM
                - Deployment Mode(--deploy-mode client/cluster)
                - Spark Driver                         => AM
                    - SparkSession/SparkObject
                - Spark Executor                       => Containers
                - Job(Spark Application)
                - Horizontal Tasks (Partitions)
                - Vertical Stages (Shuffling happens)


## 4. Performance Optimization Basics

### 1. Basic Coding Standards for Performance Optimization
            - Remove/comment all dead codes
            - Comment the actions used intermediately for dev/testing purpose
            - Comment the actions especially brings entire data to the driver
            - For analysis purpose, use actions other than collect() like (take(2),first(),count())

### 2. Partition Creation/Management
            - Spark Partitioning uses all these methodologies:
                - Block Size : 32mb local / 128m hdfs
                - Cores      : no of partitions = no of cores
                - Default    : no of partitions: 2
                - Functions  : coalesce(range list), repartition(round robin)
                
            - Hive: partition (folders of respect columns), clustered by column into no of buckets (files with hash bucketing applied)
            
            - How and all the partitions can be created/managed?
                1. Partitions are defined when creating RDDs/DFs (Organically/Customized)
                2. Increase it before performing Transformation, decrease it after transformation and before performing Action

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

#1. Partitions are defined when creating RDDs/DFs (Organically/Customized)
rdd1 = spark.sparkContext.parallelize(range(1,100000)) # Organically no of partitions = no of cores allocated
print(f"rdd1.getNumPartitions():{rdd1.getNumPartitions()}") # 5

rdd1 = spark.sparkContext.parallelize(range(1,100000),numSlices=4) # Customized no of partitions by passing arguments
print(f"rdd1.getNumPartitions():{rdd1.getNumPartitions()}") # 4

#2. Increase it Before performing Transformation, Decrease it after the Transformation and before Action
rdd2 = rdd1.repartition(6) # Before perform Transformation so that computation will be distributed across the partition nodes
rdd3 = rdd2.map(lambda x:x+10)
rdd4 = rdd3.filter(lambda x: x> 90000)
rdd5 = rdd4.coalesce(2) # Decrease it after the Transformation

import datetime
now = datetime.datetime.now()
onltyime = now.strftime('%H%M%S')

rdd5.coalesce(1).saveAsTextFile("file:///home/hduser/filterdata/"+onltyime) # Reduce the no of partitions before an Action.

```

### 3. What is Partitioning?
            - Partitioning is horizontal devision of data
            - HDFS=blocks, MR=InputSplit, Sqoop=Mapper, Hive=Partition(folders),Bucket(no of files), YARN=(containers), Spark(RDD/DF/View/DStreams)...
            - Partitioning is used for defining the degree of parallelism and distribution
            - Spark Partition help us distribute the data across multiple nodes in memory in a form of RDD partitions
        

### 4. How to control no of partitions?
            - coalesce()     - transformation help us to reduce the number of partitions
                             - range of values in a given partitioning or data size is random/difference in diff paritition
            - repartition()  - transformation help us to increase the number of partitions (internally coalesce(shuffle=True))
                             - round robin partitioning. equal distribution between partitioning using shuffle
        
```python
# 1. When RDDs are created from local file system? 1 partition = 32mb size
'''
du -kh  /home/hduser/txns*
8.1M	/home/hduser/txns
1.2G	/home/hduser/txns_1gb
202M	/home/hduser/txns_233mb
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

file32mb_rdd1 =  sc.textFile('file:///home/hduser/txns') # organically <32mb then default = 2
print('file32mb_rdd1.getNumPartitions():',file32mb_rdd1.getNumPartitions())

file32mb_rdd1 =  sc.textFile('file:///home/hduser/txns',1) # customized (override the default with any number if it is less than <32. otherwise higher number is required)
print('file32mb_rdd1.getNumPartitions():',file32mb_rdd1.getNumPartitions())

file32mb_rdd1 =  sc.textFile('file:///home/hduser/txns',4) # customized (override the default with any number if it is less than <32. otherwise higher number is required)
print('file32mb_rdd1.getNumPartitions():',file32mb_rdd1.getNumPartitions())

file202mb_rdd1 =  sc.textFile('file:///home/hduser/txns_233mb') # organically >32mb so 202mb/32mb = 7 partitions will be created by default
print('file202mb_rdd1.getNumPartitions():',file202mb_rdd1.getNumPartitions())

file1gb_rdd1 =  sc.textFile('file:///home/hduser/txns_1gb') # organically >32mb so 1024mb/32mb = 32 partitions will be created by default
print('file1gb_rdd1.getNumPartitions():',file1gb_rdd1.getNumPartitions())

file1gb_rdd1 =  sc.textFile('file:///home/hduser/txns_1gb',1) #if i give <32, it will not considered. so the only option is coalesce() to reduce the number of partitions
print('file1gb_rdd1.getNumPartitions():',file1gb_rdd1.getNumPartitions())

#Still i need to restrict to 4 partitions
file1gb_rdd1 =  sc.textFile('file:///home/hduser/txns_1gb').coalesce(4)
print('file1gb_rdd1.getNumPartitions():',file1gb_rdd1.getNumPartitions())

# conclusion: local files rdds will go with default 2 partitions if size <32mb
# or per 32 mb one partition
# we can override with the argument > number of partitions returned

"""
file32mb_rdd1.getNumPartitions(): 2
file32mb_rdd1.getNumPartitions(): 1
file32mb_rdd1.getNumPartitions(): 4
file202mb_rdd1.getNumPartitions(): 7
file1gb_rdd1.getNumPartitions(): 38
file1gb_rdd1.getNumPartitions(): 38
file1gb_rdd1.getNumPartitions(): 4
"""

```


```python

# 2. When RDDs are created from HDFS file system? 1 partition = 128mb size
'''
du -kh  /home/hduser/txns*
8.1M	/home/hduser/txns
1.2G	/home/hduser/txns_1gb
202M	/home/hduser/txns_233mb

hadoop fs -put /home/hduser/txns* /user/hduser/
'''

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

file128mb_rdd1 =  sc.textFile('hdfs:///user/hduser/txns') # organically <128mb then default = 2
print('file128mb_rdd1.getNumPartitions():',file128mb_rdd1.getNumPartitions())

file128mb_rdd1 =  sc.textFile('hdfs:///user/hduser/txns',1) # customized (override the default with any number if it is less than <128. otherwise higher number is required)
print('file128mb_rdd1.getNumPartitions():',file128mb_rdd1.getNumPartitions())

file128mb_rdd1 =  sc.textFile('hdfs:///user/hduser/txns',4) # customized (override the default with any number if it is less than <128. otherwise higher number is required)
print('file128mb_rdd1.getNumPartitions():',file128mb_rdd1.getNumPartitions())

file202mb_rdd1 =  sc.textFile('hdfs:///user/hduser/txns_233mb') # organically >128mb so 202mb/128 = 2 partitions will be created by default
print('file202mb_rdd1.getNumPartitions():',file202mb_rdd1.getNumPartitions())

file1gb_rdd1 =  sc.textFile('hdfs:///user/hduser/txns_1gb') # organically >128 so 1024mb/128b = 8 partitions will be created by default
print('file1gb_rdd1.getNumPartitions():',file1gb_rdd1.getNumPartitions())

file1gb_rdd1 =  sc.textFile('hdfs:///user/hduser/txns_1gb',1) #if i give <128, it will not considered. so the only option is coalesce() to reduce the number of partitions
print('file1gb_rdd1.getNumPartitions():',file1gb_rdd1.getNumPartitions())

#Still i need to restrict to 4 partitions
file1gb_rdd1 =  sc.textFile('hdfs:///user/hduser/txns_1gb').coalesce(1)
print('file1gb_rdd1.getNumPartitions():',file1gb_rdd1.count())

"""
file128mb_rdd1.getNumPartitions(): 2
file128mb_rdd1.getNumPartitions(): 1
file128mb_rdd1.getNumPartitions(): 4
file202mb_rdd1.getNumPartitions(): 2
file1gb_rdd1.getNumPartitions(): 10
file1gb_rdd1.getNumPartitions(): 10
file1gb_rdd1.getNumPartitions(): 14385600
"""
```

✅ In short:
coalesce(1) works on a 1 GB file because Spark reads and processes it in chunks (not all in memory),
but it’s not efficient — it removes parallelism and increases risk of memory pressure.

### 5. When the partitions can be increased or decreased in an RDD? 
         -  Scenario 1 # Increase it before performing the transformation(flatmap)     => repartition()
         -  Scenario 2 # Decrease it after performing the transformation (filter)      => coalesce()
         -  Scenario 3 # Decrease it before performation the transformation(map)       => coalesce()

### 6. Before we run an action, can we change the number of partitions? 
            yes

### 7. How many files will be generated? 
            No of files = No of Partitions

### 8. Memory Optimization - using Cache()/Persist() once RDD is created
            - cache() & persist(diffierent StorageLevel's) - transformations are used to ask GC, not to purge the data from Executor memory
            - unpersist() - action is used to ask GC to purge the data from the Executor memory
            - If the underlying data in RDD is not changed and used multiple times then go for cache()/persist()
            - If the underlying data in RDD is keep changing(streaming apps) then refesh the cache frequently (unpersist()) and cache() it again
            - Consider: Volumne of data, availability or resources, time taken for serialization/deserialization, GC time, etc.,
            - Right type of cache in the name of persist() is supposed to be considered.  
            - Persist()=> StorageLevel Options: # memory(2)/disk(3)/both(2)/replica/serialization(1)/off_heap(1) 
    
### 9. Broadcasting:
            - It is commonly used in Spark SQL joins.
            - Spark Broadcasting a special static variable that can broadcast once for all from driver to worker(executors)
            - Hence Spark RDD/DF partitions rows can refer that broadcasted variable locally rather than getting it from the driver for every iteration
            - How much rows a rdd or variable can be broadcasted? default is 10mb.

### 10. Accumulator:
            - Accumulator is special incremental variable used for accumulating the number of tasks performed by the executors.
            - Accumulator is used to identify the progress completion of tasks running in the respective executors.
            - Accumulator used in Spark framework for creating job counters.
            - Example: logging/task completition percent/number of tasks completed.

## 5. Main Program to covert all the Spark core programming concepts 

