# 🐘 Hadoop, 🧵 YARN & ⚡ Spark

### Q1. What Happens When you Submit a Job in YARN?

<details>
  <summary> Click to view YARN architecture diagram</summary>
  <img src="images/img.png" alt="Diagram">
</details>

#### 1️⃣ Contact with Resource Manager (RM)

* The **primary POC** will be the **Resource Manager (RM)** 🗂️
* Program will be called `job.submit`.
* In the background, RM contacts **NameNode (NN)** 🏷️ to get **metadata** info of the data.

#### 2️⃣ RM Response to Client

* RM responds with:

  * **Max(Job\_ID)** 🆔
  * **Metadata information** (which DataNodes contain the blocks).

#### 3️⃣ Input Split & Job Resources Preparation

* Client calculates the **input split** using metadata info.
* Copies the following into **HDFS temp location** 📂 `hdfs://tmp/staging/job_id_101/...`:

  * Input split specification
  * `mr.jar` program
  * Additional libraries 📚
  * Property files ⚙️
  * Configuration files 📝

#### 4️⃣ Submit Application

* Client contacts RM again and calls `submit.application`.

#### 5️⃣ Application Master (AM) Creation

* RM (via **Schedulers: Fair, Capacity, FIFO ⏳**) provides approval.
* RM requests a **Node Manager (NM)** 🖥️ to create a container.
* The **Application Master program** 🧑‍💻 gets started inside that container.

#### 6️⃣ AM Registers with RM

* Application Master (AM) 📡 registers itself with Resource Manager.

#### 7️⃣ AM Copies Job Resources

* AM retrieves job resources from **HDFS temp location** created in step 3.

#### 8️⃣ AM Negotiates Resources

* AM negotiates with RM 🤝 and gets approval to contact the respective Node Managers.

#### 9️⃣ Launching Containers

* AM works with NM to **launch containers** with given specifications.

#### 🔟 Containers Creation

* Node Manager creates **containers** 🏗️ where **Mapper/Reducer programs** will run.

#### 1️⃣1️⃣ Copy Job Resources into Containers

* Copy of job resources from common HDFS location → inside the container 📦.
* Prepares to start Mapper/Reducer program.

#### 1️⃣2️⃣ Mapper Execution & Status Report

* Mapper(s) 🗂️ send status reports to AM.
* AM forwards status updates 📡 to the Job Client.

#### 1️⃣3️⃣ Job Completion

* Once all **Mappers & Reducers** ✅ finish, job is marked as complete.

---


### Q2. What Happens When I Submit a Spark Job in **Cluster** Mode?

⚡ Spark Cluster Mode — Detailed Flow

```pgsql
🧑‍💻 1. Client (spark-submit)
    └─> 📦 Application Master (YARN) / Cluster Manager
          • Client uploads app jars & staging (HDFS) then can safely disconnect
          • RM/AM take over lifecycle of the Driver
          |
          v
🚗 2. Driver (runs inside cluster)
   • Driver is launched inside AM container in cluster
   • Builds DAG, schedules tasks, tracks job state (inside cluster)
          |
          v
     🔄 Build DAG (stages & tasks)
          • Plan execution (stages, tasks, partitions)
          |
          v
🤝 3. Driver contacts Resource Manager (YARN / K8s / Mesos)
          • Driver requests executor containers/resources via RM
          |
          v
🖥️ 4. Node Managers (across cluster)
    └─> 📦 Executors launched
          • Executors are long-lived JVMs on worker nodes
          • Launched with required jars/configs from staging
          |
          v
🖥️ 5. Executors register with Driver (inside cluster)
          • Fast local network registration (no external client hop)
          |
          v
 🚗6. Driver schedules tasks → Executors run them
          • Driver assigns tasks based on locality & resources
          • Tasks execute in parallel on executors
          |
          v
🗄️ 7. Executors process data (HDFS / external sources)
          • Read HDFS/DBs/S3, cache partitions in memory/disk, do shuffle
          |
          v
📡 8. Executors send status & results → Driver (in cluster)
          • Progress, metrics, task failures reported to Driver/AM/RM
          |
          v
✅ 9. Job Completion
    └─> Executors shut down
    └─> Driver exits (in cluster)
    └─> AM deregisters from RM; resources released

```
---

### Q3. What Happens When I Submit a Spark Job in **Client** Mode?

⚡ Spark Client Mode — Detailed Flow

```pgsql
🧑‍💻 Client (spark-submit)
   └─> 🚗 Driver (runs on client machine)
          • Driver is a local JVM: builds DAG, schedules tasks, tracks job state
          • Client machine must remain reachable and healthy
          |
          v
     🔄 Build DAG (stages & tasks)
          • Plan execution (stages, tasks, partitions)
          |
          v
🤝 Driver contacts Resource Manager (YARN / K8s / Mesos)
          • Requests executor containers/resources
          • May upload jars/configs to staging on HDFS
          |
          v
🖥️ Node Managers (across cluster)
   └─> 📦 Executors launched
          • Executors are long-lived JVMs on worker nodes
          • Launched with required jars/configs
          |
          v
📂 Executors register with Driver (over network)
          • Executors open RPC/Netty connections to Driver
          • Heartbeats & registration info flow over network
          |
          v
⚡ Driver schedules tasks → Executors run them
          • Driver assigns tasks based on locality & resources
          • Tasks execute in parallel on executors
          |
          v
🗄️ Executors process data (HDFS / external sources)
          • Read HDFS/DBs/S3, cache partitions in memory/disk, do shuffle
          |
          v
📡 Executors send status & results → Driver (on client)
          • Progress, metrics, task failures sent to Driver
          • Driver may retry failed tasks / reschedule
          |
          v
✅ Job Completion
   └─> Executors shut down
   └─> Driver exits (on client)
   └─> Resources released by RM

```

---

### Q4. Comparision between YARN MapReduce vs Spark Cluster Mode vs Spark Client Mode

| Aspect                       | 🗂️ **YARN MapReduce (MR)**                            | ⚡ **Spark — Cluster Mode**                                                                 | 💻 **Spark — Client Mode**                                                                        |
| ---------------------------- | ------------------------------------------------------ | ------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| **Submit command**           | `hadoop jar myjob.jar ...`                             | `spark-submit --master yarn --deploy-mode cluster my_app.jar ...`                          | `spark-submit --master yarn --deploy-mode client my_app.jar ...`                                  |
| **Primary coordinator**      | **ResourceManager (RM)** + **ApplicationMaster (AM)**. | **Driver** (inside cluster, started by AM).                                                | **Driver** runs on client machine; AM only manages executors.                                     |
| **Who schedules tasks**      | **ApplicationMaster** (MR job AM).                     | **Driver** schedules DAG stages & tasks.                                                   | **Driver** (on client) schedules tasks; sends them to cluster executors.                          |
| **Runtime units**            | Short-lived JVMs for **Map/Reduce tasks**.             | Long-lived **Executors** (JVMs) running many tasks.                                        | Same long-lived **Executors**; Driver stays on client side.                                       |
| **Where driver runs**        | Not applicable (no driver concept; AM is coordinator). | **Inside cluster** (AM container).                                                         | **On client machine** (local JVM process).                                                        |
| **Resource allocation flow** | Client → RM → AM → NMs → containers → MR tasks.        | `spark-submit` → RM launches AM+Driver → Driver requests executors → NMs launch executors. | `spark-submit` launches Driver locally → Driver requests RM for executors → NMs launch executors. |
| **Network dependency**       | Client only submits, then can disconnect safely.       | Client submits and exits; Driver inside cluster continues independently.                   | Client must stay alive throughout; Driver is on client → failure of client = job failure.         |
| **Data locality**            | Strong — tasks scheduled close to HDFS blocks.         | Good — executors try to run near HDFS blocks.                                              | Same as cluster mode for executors, but Driver might be remote.                                   |
| **Execution model**          | Two-phase (map → shuffle → reduce).                    | DAG of stages; pipelined.                                                                  | Same DAG execution model.                                                                         |
| **Shuffle handling**         | Disk-based shuffle.                                    | Memory/disk shuffle, optimized with external shuffle service.                              | Same as cluster mode.                                                                             |
| **Fault tolerance**          | Rerun failed MR tasks.                                 | Recompute lost partitions via lineage.                                                     | Same as cluster mode (executors recompute).                                                       |
| **Performance**              | Disk I/O heavy; slower for iterative workloads.        | Faster for iterative, ML, streaming (in-memory).                                           | Similar performance, but higher latency if Driver-client network is far from cluster.             |
| **Startup latency**          | JVM per task (higher overhead).                        | Executors are long-lived → amortized.                                                      | Executors long-lived → but job start tied to client Driver.                                       |
| **Monitoring UI**            | YARN RM / AM logs.                                     | Spark Web UI (Driver inside cluster) accessible via YARN.                                  | Spark Web UI runs on Driver (client machine); less accessible to others.                          |
| **Client reliability**       | After submission, client can disconnect.               | Client can disconnect, Driver inside cluster continues.                                    | Client must stay connected — Driver on client is critical.                                        |
| **Use cases**                | Batch ETL, legacy MR.                                  | Production workloads, long-running apps, jobs submitted remotely.                          | Development, interactive jobs (e.g., notebooks, testing locally).                                 |


#### 🧭 Quick Flow Differences

* **YARN MR:** Client → RM → AM schedules Mappers/Reducers → Job complete.
* **Spark Cluster Mode:** Client → RM → AM launches Driver inside cluster → Driver schedules tasks → Executors run tasks → Job complete.
* **Spark Client Mode:** Client → launches Driver locally → Driver requests executors from RM → Executors run tasks → Driver collects results → Job complete.

---


### Q5. I would like to read the csv file from a HDFS location, then remove duplicates and write into Hive table using Spark program.   What happens internally if we submit this spark job?

 ⚡ Spark Internals — Cluster Mode with Catalyst & Tungsten

#### 1. **Job Submission** 📨

* `spark-submit` in **cluster mode**.
* Client uploads:

  * 📦 Application JAR / Py file
  * ⚙️ Config files, Hive configs, dependencies
* Client hands off control to **🗂️ Resource Manager (RM)** and can safely disconnect. ✅



#### 2. **Driver Launch** 🚗

* **📦 Application Master (AM)** starts a **Driver** container in the cluster.
* Driver acts as **master orchestrator**:

  * 📜 Reads application code
  * 🧩 Builds **logical plan** from DataFrame/Dataset operations



#### 3. **Logical Plan Creation** 🧠

* Program:

```python
df = spark.read.csv("hdfs://...")  
df_dedup = df.dropDuplicates()  
df_dedup.write.saveAsTable("hive_table")
```

* Driver parses it into **logical plan**:

  * `ReadCSV → Deduplicate → WriteHiveTable`
  * Represents **abstract operations**, no physical execution yet
* DAG (Directed Acyclic Graph) for Spark is **implicitly created at this stage**, representing the transformations as nodes:

  * Node 1: Read CSV
  * Node 2: Deduplicate
  * Node 3: Write Hive Table



#### 4. **Catalyst Optimizer** ✨

* Catalyst applies transformations to the logical plan:

  * 🔍 **Analysis**: resolves column names, data types, Hive metadata
  * ♻️ **Logical Optimizations**:

    * Filter pushdown (if any)
    * Projection pruning
    * Remove redundant computations
* Result: **optimized logical plan**
* DAG is **updated with optimized operations**, ready for physical planning ✅

#### 5. **Physical Plan Generation** 🏗️

* Catalyst converts optimized logical plan into **physical plan(s)**:

  * Maps logical nodes to **RDD/DataFrame transformations**

    * Example: `CSV → Rows → Deduplicate → Write to HDFS`
  * Decides:

    * Stage boundaries
    * Task partitioning
    * Shuffles and data movement
* **Driver chooses the most efficient plan** for execution
* DAG now **represents stages**:

  * Stage 1: Read CSV
  * Stage 2: Shuffle & Deduplicate
  * Stage 3: Write to Hive

#### 6. **Tungsten Execution** ⚡

* Optimizes physical execution:

  * 🧠 **Memory Management**: off-heap storage reduces GC overhead
  * 🏎️ **Code Generation**: Java bytecode for transformations
  * 🗄️ **Binary Processing**: efficient in-memory row format
* Tasks running on executors are now **highly optimized** for CPU & memory efficiency
* DAG nodes now correspond to **actual physical tasks** executed by executors


#### 7. **Reading CSV from HDFS** 🗂️

* Driver schedules **tasks** from Stage 1 to **Executors**
* Executors:

  * Read HDFS blocks (data-locality optimized)
  * Parse CSV rows into **internal row objects** (Tungsten binary format)
* DAG shows **parallelism per partition**, with tasks mapped to executors


#### 8. **Deduplication** 🔄

* `dropDuplicates()` triggers **Stage 2** and a **shuffle**:

  * Partitions rows by hash of all columns
  * Executors exchange rows over network
  * Tungsten optimizes in-memory aggregation & minimizes serialization
* DAG visualizes **shuffle edges** between stages, representing data movement

#### 9. **Writing to Hive Table** 🏛️

* Stage 3 of DAG executes:

  * Executors write partitions to HDFS (managed/external table)
  * Hive Metastore updated by Driver/Hive connector
  * Tungsten optimizes serialization and write buffers
* DAG edges from Stage 2 → Stage 3 ensure **task dependencies** are respected

#### 10. **Execution Tracking** 📊

* Driver tracks:

  * Task progress
  * Stage completion
  * Retries for failed tasks
  * Maintains **Spark UI** with DAG visualization
* Executors:

  * Run physical plan tasks
  * Send metrics & status to Driver
* DAG allows **Driver to schedule tasks efficiently** while monitoring execution


#### 11. **Job Completion** ✅

* Executors shut down
* Driver exits (inside cluster)
* AM deregisters from **Resource Manager**
* DAG execution is complete, job finishes successfully


#### Key Internals Summary

| Component                     | Role in CSV → Dedup → Hive                                                               |
| ----------------------------- | ---------------------------------------------------------------------------------------- |
| 🧠 **Logical Plan**           | Abstract representation: ReadCSV → Deduplicate → WriteHiveTable                          |
| ✨ **Catalyst Optimizer**      | Resolves columns/types, optimizes operations (filter pushdown, projection pruning)       |
| 🏗️ **Physical Plan**         | Maps logical plan to real operations: RDD/DataFrame transformations, shuffle, partitions |
| ⚡ **Tungsten**                | Low-level execution engine: off-heap memory, code generation, binary row format          |
| 🚗 **Driver**                 | Builds plans, schedules tasks, coordinates execution, talks to Hive Metastore            |
| 📦 **Executors**              | Read HDFS blocks, run transformations, shuffle for dedup, write results, report status   |
| 🗂️ **Resource Manager / AM** | Allocates containers, manages Driver lifecycle                                           |

![img.png](images/img3.png)

---

# ⚡ Common & Important Spark Internals (with Examples)

## 1. Have you written RDD transformations?

Yes. RDDs support **transformations** (lazy ops) and **actions** (trigger execution).

### Example: `map` vs `flatMap`

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Demojob").enableHiveSupport().getOrCreate()

def main():
    # map -> one input -> one output
    rdd = spark.sparkContext.parallelize(["apple","banana","orange"])
    mapped = rdd.map(lambda x: (x, len(x)))
    print("[INFO] mapped:", mapped.collect())
    #[INFO] mapped: [('apple', 5), ('banana', 6), ('orange', 6)]

    # flatmap -> one input -> multiple output
    rdd2 = spark.sparkContext.parallelize(["1  2","3 4  5","6  7 8  9"])
    flatmapped = rdd2.flatMap(lambda x: x.split(" "))
    print("[INFO] flatmapped:",flatmapped.collect())
    #[INFO] flatmapped: ['1', '', '2', '3', '4', '', '5', '6', '', '7', '8', '', '9']

if __name__ =="__main__":
    main()
    spark.stop()
```

👉 In real projects, developers prefer **DataFrames/SQL** because Catalyst Optimizer + Tungsten provide automatic optimizations.

---

## 2. How to schedule a Spark job?

* **On-Prem**: cron, autosys, Control-M, Oozie, Airflow
* **Cloud**: GCP Cloud Composer (Airflow), Dataproc workflow template, Databricks Jobs

### Example: simple cron job

```bash
crontab -l
crontab -e

0 2 * * * spark-submit --master yarn --deploy-mode client ~/demojob.py
```

---

## 3. How to orchestrate a Spark job?

* **On-Prem**: Oozie (Spark action), Airflow DAG, Shell Scripts
* **Cloud**: Dataproc Workflow, Databricks Jobs API, Cloud Composer (Airflow)

### Example: Airflow (Python DAG)

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG("spark_job_dag", start_date=datetime(2024,1,1), schedule="@daily") as dag:
    spark_task = SparkSubmitOperator(
        task_id="spark_job",
        application="/path/to/app.py",
        conn_id="spark_default",
        conf={"spark.yarn.maxAppAttempts": "2"}
    )
```

---

## 4. How to set number of attempts in `spark-submit`?

```bash
spark-submit \
  --conf spark.yarn.maxAppAttempts=2 \
  demojob.py
```

👉 In **Airflow**, use `retries=2`.

---

## 5. How do you define SparkContext / SparkSession?

* **SparkContext**: entry point for low-level RDD API.
* **SparkSession**: unified entry point for DataFrame/SQL API (wraps `SparkContext`).

### Example

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DemoApp") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
print(sc)   # SparkContext info

spark.stop()
```

---

## 6. Can we have multiple SparkContexts?

* ❌ No, only **one active SparkContext** at a time.
* ✅ You can stop and recreate with new configs.

---

## 7. Can we run Spark without SparkSession?

* ✅ Yes, using `SparkContext` only (RDD API).
* ❌ But for SQL/DF API, we **must use SparkSession**.

---

### Example: RDD program without SparkSession

```python
from pyspark import SparkContext

sc = SparkContext("local", "RDDOnlyApp")
rdd = sc.parallelize([1, 2, 3, 4])
print(rdd.map(lambda x: x * 2).collect())
sc.stop()
```

---

## 8. What happens when a Spark SQL/DSL program runs?

👉 Flow:
SQL/DSL → Logical Plan → Optimized Logical Plan → Physical Plan → RDD DAG → Stages → Tasks → Executors

### Example: SQL → Plan

```python
# SQL/DSL -> Logical Plan -> Optimized Logical Plan -> Physical Plan -> RDD DAG -> Stages -> Tasks -> Executors
df = spark.createDataFrame(data=[(1,"A"),(2,"B"),(3,"C")], schema=["id","name"])
df.createOrReplaceTempView("view1")

spark.sql("select * from view1 where id=1").explain(extended=True)

"""
== Parsed Logical Plan ==
'Project [*]
+- 'Filter ('id = 1)
   +- 'UnresolvedRelation [view1], [], false

== Analyzed Logical Plan ==
id: bigint, name: string
Project [id#0L, name#1]
+- Filter (id#0L = cast(1 as bigint))
   +- SubqueryAlias view1
      +- View (`view1`, [id#0L,name#1])
         +- LogicalRDD [id#0L, name#1], false

== Optimized Logical Plan ==
Filter (isnotnull(id#0L) AND (id#0L = 1))
+- LogicalRDD [id#0L, name#1], false

== Physical Plan ==
*(1) Filter (isnotnull(id#0L) AND (id#0L = 1))
+- *(1) Scan ExistingRDD[id#0L,name#1]
"""
```

---

## 9. PySpark DSL vs SQL

| **Aspect**      | **PySpark DSL**                                           | **PySpark SQL**                                                       |
| --------------- | --------------------------------------------------------- | --------------------------------------------------------------------- |
| **Style**       | Functional programming with method chaining on DataFrames | Declarative SQL-like syntax                                           |
| **Ease of Use** | Steeper learning curve, suits programmers                 | Easier for SQL users, readable                                        |
| **Use Case**    | Complex, reusable, programmatic transformations           | Simple, ad-hoc analytical queries                                     |
| **Integration** | Works well with Python logic and functions                | Keeps SQL logic separate from Python code                             |
| **Performance** | Optimized by Catalyst and Tungsten (same as SQL)          | Optimized by Catalyst and Tungsten (same as DSL)                      |
| **Flexibility** | High – allows chaining, reusable DataFrames               | Moderate – limited to SQL expressions                                 |
| **Portability** | Spark-specific, not platform independent                  | Platform independent (works across BigQuery, Synapse, Redshift, etc.) |
| **Best For**    | ETL pipelines, data transformations, reusable logic       | Reporting, analytics, one-time queries                                |
| **Conclusion**  | Flexible and suited for complex transformations           | Easier, ideal for ad-hoc queries and cross-platform use               |


### Example

```python
"""
demojob.py

custs
4000001,Kristina,Chung,55,Pilot
4000002,Paige,Chen,77,Teacher
4000003,Sherri,Melton,34,Firefighter
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
spark = SparkSession.builder.appName("demojob").enableHiveSupport().getOrCreate()

def main():
    custs_schema = StructType([
        StructField("custid",IntegerType(),True),
        StructField("fname",StringType(),True),
        StructField("lname",StringType(),True),
        StructField("age",IntegerType(),True),
        StructField("prof",StringType(),True)])
    #DSL
    df = spark.read.csv(path="hdfs:///home/hduser/custs",header=False,schema=custs_schema)
    df.filter(df.age > 40).select(df.custid,df.fname,df.lname,df.age,df.prof).orderBy(df.fname).show(5,truncate=False)

    #SQL
    df.createOrReplaceTempView("customers")
    spark.sql("select custid, fname, lname,age,prof from customers order by fname").show(5, truncate=False)

if __name__ =="__main__":
    main()
    spark.stop()
```
---

## 10. Repartition, Coalesce, Cache

| **Aspect**                      | **Repartition**                                                                   | **Coalesce**                                                         | **Cache / Persist**                                                       |
| ------------------------------- | --------------------------------------------------------------------------------- | -------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| **Purpose**                     | Increases or decreases the number of partitions for parallelism                   | Reduces the number of partitions without a full shuffle              | Stores DataFrame in memory (or memory + disk) for faster access           |
| **Shuffle**                     | **Yes** – performs a **full shuffle** across the cluster                          | **No (or minimal)** – tries to merge partitions locally              | **No shuffle** – just caches data in memory                               |
| **When to Use**                 | When you need **more partitions** for parallel processing or balanced workload    | When you need **fewer partitions** to optimize small output datasets | When the same DataFrame is used **multiple times** in a job               |
| **Performance Cost**            | **High** – due to full data shuffle                                               | **Low** – merges existing partitions                                 | **Low/Medium** – depends on memory availability                           |
| **Example**                     | `df.repartition(8)`                                                               | `df.coalesce(2)`                                                     | `df.cache()` or `df.persist(StorageLevel.MEMORY_AND_DISK)`                |
| **Typical Use Case**            | Before wide transformations (e.g., joins, aggregations) to distribute data evenly | After filtering or reducing data volume to minimize partitions       | Before reusing a DataFrame in multiple actions (e.g., count, show, write) |
| **Effect on Data Distribution** | Redistributes data evenly across partitions                                       | Does not guarantee even distribution                                 | No effect on partitioning or data distribution                            |
| **Action Triggered?**           | No – lazy operation                                                               | No – lazy operation                                                  | No – lazy operation (materialized on first action)                        |
| **Persistence Type**            | Restructures data                                                                 | Merges partitions                                                    | Keeps data in memory/disk for reuse                                       |
| **Conclusion**                  | Use for **balancing and scaling up**                                              | Use for **scaling down efficiently**                                 | Use for **reusing results quickly**                                       |


```python
df = spark.range(1, 100)
print(df.rdd.getNumPartitions()) # default: 5

df = df.repartition(10) # increase partitions (full shuffle across the cluster)
print(df.rdd.getNumPartitions())  # 10

df = df.repartition(5) # reduce partitions (full shuffle across the cluster)
print(df.rdd.getNumPartitions())  # 5

df = df.coalesce(2) # reduce partitions (no shuffle. only merge the partitions locally)
print(df.rdd.getNumPartitions())  # 2

df.cache()
print(df.rdd.getStorageLevel()) # Serialized 1x Replicated
```
---

## 11. What are all the activities we can’t do in a DF?

| **S.No** | **Activity**                                                       | **Reason / Explanation**                                                                                                                                                                                                          |
| -------- | ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1**    | **Cannot modify a DataFrame directly**                             | DataFrames in Spark are **immutable**. Once created, they can’t be updated or modified in place. To change data, you must create a **new DataFrame** with transformations. (Delta Lake enables updates and deletes later.)        |
| **2**    | **Cannot find the number of partitions directly from a DataFrame** | The DataFrame API does not provide a direct method like `df.numPartitions`. To get partition info, you must use the **RDD API** — e.g., `df.rdd.getNumPartitions()`.                                                              |
| **3**    | **Cannot broadcast a DataFrame directly**                          | Spark’s **broadcast function** works on variables, not on DataFrames. To broadcast, you must convert the DataFrame to an **RDD or use broadcast joins** via Spark SQL or the `broadcast()` function from `pyspark.sql.functions`. |

Here’s a simple example of broadcasting in PySpark 👇

### ❌ Not allowed (Direct broadcast on DataFrame)

```python
# This will NOT work
df_broadcast = sc.broadcast(df)   # ❌ Error: Can't broadcast DataFrame
```

### ✅ Correct way (Using broadcast join)

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
spark = SparkSession.builder.appName("demojob").enableHiveSupport().getOrCreate()

def main():
    # Sample DataFrames
    df_large = spark.createDataFrame(data=[(1,"A"),(2,"B"),(3,"C")], schema=["id","value"])
    df_small = spark.createDataFrame(data=[(1, "X"), (2, "Y")], schema=["id", "desc"])

    # Use Broadcast Join
    df_result = df_large.join(broadcast(df_small),"id")
    df_result.show()

if __name__ =="__main__":
    main()
    spark.stop()
    
"""
+---+-----+----+
| id|value|desc|
+---+-----+----+
|  1|    A|   X|
|  2|    B|   Y|
+---+-----+----+
"""
```

**Explanation:**

* The small DataFrame (`df_small`) is **broadcasted** to all worker nodes.
* This avoids shuffling large data during joins, improving performance.


---

## 12. How to find number of jobs running?

```bash
yarn application -list | grep "RUNNING"

#OR

yarn application -list | grep "RUNNING" | awk '{print $1}' > job_list.txt
```

or via Spark History UI.

---

## 13. What is the total number of jobs/applications runs in your cluster per day?

| **Cluster Type**              | **Number of Nodes / Environment** | **Share of Jobs**       | **Job Frequency**              | **Notes**                                 | **Resource Utilization**                  | **Example Use Cases**                                          |
| ----------------------------- | --------------------------------- | ----------------------- | ------------------------------ | ----------------------------------------- | ----------------------------------------- | -------------------------------------------------------------- |
| **Long-running / On-prem**    | 120 nodes, Dataproc cluster       | 50% of total Spark jobs | Hourly, Daily, Weekly, Monthly | Supports continuous data processing 24/7  | 60–70% average, 30–40% buffer for spikes  | ETL pipelines, scheduled batch jobs, continuous streaming      |
| **Ephemeral Clusters**        | Short-lived / on-demand           | 20% of total Spark jobs | Ad-hoc / on-demand             | Runs temporary workloads and experiments  | Scales based on demand                    | Ad-hoc analytics, testing new pipelines, temporary experiments |
| **Serverless Spark Clusters** | Managed serverless environment    | 30% of total Spark jobs | Hourly / Daily / Weekly        | Auto-scales, no cluster management needed | Optimized for spikes and high concurrency | Event-driven processing, auto-scaling jobs, variable workloads |

**Overall Cluster Activity:**

* **Total jobs per day (all clusters):** 300–500+
* **Jobs in your project:** ~80–120+ per day

---

## 14. How many Jobs/Pipelines you developed?

| **Category**                   | **Details**                                              |
| ------------------------------ | -------------------------------------------------------- |
| **Total Pipelines Developed**  | ~40–60 end-to-end data pipelines                         |
| **Development vs Maintenance** | Developed: 20+ <br> Maintained: 20–40+                   |
| **Pipeline Type**              | Batch: 80% <br> Streaming / Near Real-time: 20%          |
| **Data Sources / Ingestion**   | On-prem databases, APIs, flat files, GCS, Pub/Sub, Kafka |
| **Transformations**            | PySpark, Spark SQL, Dataflow                             |
| **Data Quality**               | Validation and quality checks integrated                 |
| **Data Storage / Publishing**  | BigQuery, Hive, downstream APIs                          |
| **Orchestration**              | Airflow / Cloud Composer                                 |

**Key Points:**

* Developed **end-to-end pipelines** covering ingestion, transformation, validation, and publishing.
* Hands-on experience with both **batch and streaming** workflows.
* Integrated **workflow orchestration** and **data quality checks** into pipelines.

---

## 15. How to identifying slow jobs, tracking failures, and optimizing Spark jobs

| **Category**                                                   | **Method / Command**                       | **Purpose / Details**                                               | **Comments / Examples**                                                        |
| -------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| **Identify Slow / Stale Jobs**                                 | `yarn application -list -appStates RUNNING \| awk '$5 > 7200000'` | Lists running jobs exceeding 2 hours (7,200,000 ms)                | `# Helps detect jobs running longer than expected, possibly stale`             |
|                                                                | `yarn application -list \| grep -v "churn" \| grep "default" \| grep "RUNNING" \| awk '{print $1}' > job_list.txt` | Find long-running jobs in a specific queue                            | `# Generates list of job IDs in default queue for monitoring or debugging`     |
|                                                                | ResourceManager / Application Master UI    | Monitor running jobs, resource usage, and executor stats            | `# RM UI (http://<rm_host>:8088) shows live cluster metrics`                   |
|                                                                | Spark UI                                   | Visualize DAG, stages, tasks, shuffle, and executor performance     | `# Identify bottleneck stages, straggler tasks, or skewed data partitions`     |
|                                                                | Oozie / Airflow Console                    | Track workflow-level progress and detect stuck tasks                | `# Useful for jobs orchestrated via Airflow/Oozie; highlights workflow delays` |
| **Track Job Failures**                                         | `yarn application -list 2>/dev/null \| grep "<queue_name>" \| grep "FAILED" \| awk '{print $1}' > job_list.txt` | List failed jobs in a specific queue                                  | `# Captures application IDs of failed jobs for further investigation`         |
|                                                                | `yarn logs -applicationId <app_id>`        | View logs from Application Master / executors                       | `# Helps determine root cause of job failure`                                  |
|                                                                | Spark History Server                       | Review completed job stages, metrics, and failed tasks              | `# Useful for post-mortem analysis of batch jobs`                              |
|                                                                | Airflow / Oozie logs                       | Check task-level failures in workflows                              | `# Pinpoints exact step causing failure in orchestrated pipelines`             |
| **Optimize Spark Job Performance (Multi-node vs Single-node)** | Executor Configuration                     | Tune number of executors, cores, and memory per executor            | `# Avoid too many small executors; match resources to data volume`             |
|                                                                | Partitioning / Shuffling                   | Adjust `spark.sql.shuffle.partitions` or repartition DataFrames     | `# Reduces shuffle overhead; prevents excessive small tasks and stragglers`    |
|                                                                | Task Parallelism                           | Ensure even data distribution; use broadcast joins for small tables | `# Prevents skewed partitions; reduces shuffle and task time`                  |
|                                                                | Caching / Persisting                       | Cache intermediate DataFrames that are reused                       | `# Avoids recomputation for multiple actions on same data`                     |
|                                                                | Spark UI Monitoring                        | Inspect stage execution time, shuffle read/write, and executor logs | `# Identify bottleneck stages and optimize transformations accordingly`        |


### **Additional Comments / Best Practices**

* Slow jobs can be caused by **skewed data, too many small executors, or shuffle-heavy operations**.
* Tracking failed jobs requires **combination of CLI commands, Spark UI, and logs**.
* Optimization often includes **right-sizing executors, tuning shuffle partitions, and caching reused datasets**.
* Using **broadcast joins** and checking **resource alignment with data volume** can significantly reduce execution time.

This approach ensures you can **monitor, troubleshoot, and optimize jobs effectively** across Hadoop/YARN and Spark clusters.

---

## 16. What errors have you faced while debugging your spark code?


| **Error Type**                   | **Description / Cause**                                           | **Example / Notes**                                                       | **Resolution / Fix**                                                                    |
| -------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Data Issues                      | Inconsistent, missing, or corrupt data causing runtime errors     | Null values in non-nullable columns, missing partitions                   | Validate and clean data before processing, use `na.fill()`, `drop()`, or default values |
| Format / Type Issues             | Schema mismatch or wrong data type for transformations            | Trying to cast a string column to integer without proper validation       | Cast columns explicitly, use `withColumn()` with proper type conversion                 |
| Memory Issues                    | Out of Memory (OOM) or executor memory errors                     | `java.lang.OutOfMemoryError: Java heap space`                             | Increase executor memory, reduce shuffle partitions, persist/cache intermediate data    |
| Data Availability Issues         | Missing files or inaccessible data sources                        | `java.io.FileNotFoundException` when reading from HDFS or GCS             | Verify file paths, check HDFS/GCS permissions, ensure data exists                       |
| Class Not Found                  | Spark cannot find required classes for UDFs or external libraries | `java.lang.ClassNotFoundException: org.apache.spark.sql.hive.HiveContext` | Add required JARs to classpath, check Spark submit `--jars` or `--packages`             |
| Dependency / JAR Issues          | Conflicts between Spark and external libraries                    | Version mismatch causing runtime errors                                   | Resolve version conflicts, shade dependencies, use compatible library versions          |
| Performance / Skew Issues        | Skewed data causing straggler tasks and slow stages               | Uneven partition sizes leading to some executors taking longer            | Use `repartition()`, `salting`, or broadcast small tables                               |
| Job Failures / Task Retry Errors | Failures in stages due to network, node failure, or retry limits  | Spark retries a stage multiple times before failing                       | Check Spark/YARN logs, handle errors in code, adjust retry configs                      |

---

## 17. After performing a **join**, **groupBy**, or **aggregation** operation in Spark, does the **number of partitions** in the resulting DataFrame change? If yes, what determines the new number of partitions and how can it be tuned for better performance?

**Expected Answer (Short):**
Yes — after wide transformations like join, groupBy, or aggregation, Spark triggers a **shuffle**, and the number of output partitions is reset based on the **`spark.sql.shuffle.partitions`** setting (default = **200**).
You can tune it using:

```python
spark.conf.set("spark.sql.shuffle.partitions", <new_value>)
```
to optimize performance based on data volume and cluster resources.

---
## 18. What are the performance challenges you faced in Spark? Have you done any performance tuning by debugging in Spark UI?

| **Category / Challenge**                           | **Symptoms Observed (Spark UI / Cluster)**                               | **Root Cause Analysis**                                                       | **Tuning / Resolution Applied (Key Techniques)**                                                                                                      |
| -------------------------------------------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Skewed Data (Uneven Partition Distribution)** | Some tasks took significantly longer; high shuffle read on few executors | Data skew on join/group keys — few keys have majority of data                 | Applied **salting** on skewed keys; enabled **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled=true`); used **broadcast joins** for small tables |
| **2. Too Many Small Tasks / Partitions**           | 1000+ very small tasks completing too fast (<100 ms)                     | Over-partitioned data; default `spark.sql.shuffle.partitions=200` not optimal | Enabled **AQE** (`spark.sql.adaptive.enabled=true`) to auto-adjust partitions; manually tuned shuffle partitions based on input size                  |
| **3. Memory / OOM Errors (Executor Lost)**         | Job failed with `OutOfMemoryError`; executor lost                        | Executor memory too low or too many concurrent tasks                          | Increased `spark.executor.memory`, adjusted `spark.memory.fraction`; reduced executor cores for better GC efficiency                                  |
| **4. Excessive Shuffling**                         | High shuffle read/write time in Spark UI                                 | Too many wide transformations (joins, groupBy) causing shuffle                | Combined transformations; reused cached DataFrames; tuned `spark.sql.shuffle.partitions`; avoided unnecessary shuffles                                |
| **5. Inefficient Join Strategy**                   | Large shuffle during joins despite one small table                       | Spark didn’t use broadcast join automatically                                 | Used **broadcast(df_small)**; tuned `spark.sql.autoBroadcastJoinThreshold`; re-ordered join inputs                                                    |
| **6. Uncached Reused DataFrames**                  | Same DataFrame recomputed for multiple actions                           | Lazy evaluation leads to recomputation                                        | Used `.cache()` or `.persist(StorageLevel.MEMORY_AND_DISK)` to reuse results                                                                          |
| **7. Inefficient Python UDFs**                     | High CPU time, slow task execution                                       | Python UDF serialization overhead                                             | Replaced with **Spark SQL built-in** or **Pandas UDFs** for vectorized execution                                                                      |
| **8. Unbalanced Executor Utilization**             | Some executors idle while others overloaded                              | Poor partitioning or data locality imbalance                                  | Used `.repartition()` / `.coalesce()` for better load balance; validated partition distribution                                                       |
| **9. Long Shuffle Read Waits**                     | Stage time dominated by “fetch wait time”                                | Large shuffle blocks and high network I/O                                     | Tuned shuffle parameters (`spark.shuffle.io.maxRetries`, compression); minimized shuffle data size                                                    |
| **10. Small Files Problem**                        | Thousands of tiny input files; slow job startup                          | Input data fragmentation                                                      | Compacted files upstream (merge job) or after read using `.coalesce()` / `.repartition()`                                                             |
| **11. Suboptimal Executor Configuration**          | High GC time, underutilized cores                                        | Misconfigured executor cores/memory                                           | Tuned `--num-executors`, `--executor-memory`, `--executor-cores` for optimal resource usage                                                           |
| **12. Lack of Adaptive Query Execution (AQE)**     | Static shuffle partitions leading to inefficient parallelism             | AQE disabled in config                                                        | Enabled `spark.sql.adaptive.enabled=true` to let Spark dynamically tune partitions and join strategies                                                |
| **13. Poor File Format Choice**                    | Slow read/write, large disk usage                                        | Text/CSV instead of columnar format                                           | Migrated to **Parquet/ORC**, enabled compression (`snappy`), and optimized schema projection                                                          |
| **14. Data Skew in Joins (Advanced)**              | Stage running 10× slower due to skewed key                               | Heavy concentration of one key in join                                        | Added **random salt key**, used **salting + union** technique to rebalance data                                                                       |
| **15. Lack of Caching Between Expensive Stages**   | Repeated recomputation of intermediate results                           | No caching used across multiple actions                                       | Persisted reused data at strategic checkpoints using `.cache()` or `.persist()`                                                                       |
| **16. Non-vectorized Reads/Writes**                | Slow Parquet read/write throughput                                       | Vectorization disabled                                                        | Enabled vectorized reader: `spark.sql.parquet.enableVectorizedReader=true`                                                                            |


### 🎯 How Spark UI Helped

| **Spark UI Section** | **Insights Used for Debugging**                                          |
| -------------------- | ------------------------------------------------------------------------ |
| **Stages Tab**       | Identified shuffle-heavy stages, straggler tasks, and skewed partitions  |
| **SQL Tab**          | Verified physical plan — join types (BroadcastHashJoin vs SortMergeJoin) |
| **Executors Tab**    | Checked memory usage, GC overhead, task failures                         |
| **Environment Tab**  | Reviewed applied Spark configurations                                    |
| **Storage Tab**      | Validated whether cached/persisted DataFrames are in memory              |

### 🎯 Key Takeaways / Outcomes

* **30–40% reduction** in overall job execution time
* **50% decrease** in shuffle data after enabling AQE and broadcast joins
* Improved **executor utilization** and reduced OOM failures
* Enhanced **cluster cost efficiency and throughput**

---

## 19. How do you get the size of a DataFrame or RDD in memory (in bytes) in Spark?

### **Explanation:**

There is **no direct API** in PySpark to get the in-memory size of a DataFrame.
However, there are **two main approaches**:

| **Method**                                                            | **Approach**            | **Description**                                                                                     | **Example / Notes**                                                             |
| --------------------------------------------------------------------- | ----------------------- | --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| **1. Convert DF → RDD → Java RDD → use `SizeEstimator` (Scala/Java)** | JVM-based estimation    | Uses Spark’s internal `SizeEstimator.estimate()` method to get approximate size of RDD/DF in memory | More accurate, but needs Scala/Java interop — not available directly in PySpark |
| **2. Estimate via Python using `sys.getsizeof()`**                    | Python-based estimation | Collects size of partitions in bytes and sums them up                                               | Simpler and works directly in PySpark; gives approximate result                 |

### **Example: Estimate Size of DataFrame in Bytes (PySpark)**

```python
from pyspark.sql import SparkSession
from sys import getsizeof

def main():
    # Sample DataFrame
    df = spark.read.csv(path="hdfs:///home/hduser/custs",header=False,inferSchema=True).toDF("custno","firstname","lastname","age","profession")

    # Convert to RDD and estimate partition wise size
    rdd_sizes = df.rdd.glom().map(lambda part: getsizeof(part)).collect()

    # Total estimated size
    total_size_bytes = sum(rdd_sizes)
    print(f"Estimated total size:{total_size_bytes} bytes")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("demojob").enableHiveSupport().getOrCreate()
    main()
```

### **Notes / Key Points**

* This gives an **approximate size**, not an exact value.
* Spark data is **distributed** across executors; each partition’s memory usage may vary.
* For practical monitoring, you can also check the **Spark UI → Storage Tab**, which shows cached DataFrames and their memory footprint.
* Estimating memory size helps in tuning **`executor-memory`** and **cache persistence levels**.

---

## 20. How do you submit a Spark job with optimized memory and CPU parameters using spark-submit?

**Suggested `spark-submit` (based on above integer sizing):**

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 39 \
  --executor-cores 5 \
  --executor-memory 12g \
  --queue default \
  your_spark_job.py
```

| **Metric / Step**                      | **Formula / Explanation**                                                                 | **Value (Example)**         | **Cluster Utilization (Approx.)** |
|----------------------------------------|-------------------------------------------------------------------------------------------|-----------------------------|-----------------------------------|
| **Nodes**                              | Number of worker nodes in the cluster                                                     | **10**                      | —                                 |
| **RAM per Node**                       | Total RAM available on each node                                                          | **64 GB**                   | Total cluster RAM = 64 × 10 = 640 GB |
| **Cores per Node**                     | Total CPU cores available on each node                                                    | **24 cores**                | Total cluster cores = 24 × 10 = 240 cores |
| **Reserved Cores (for OS/HDFS)**       | Reserve 1 core per node for system/HDFS operations                                        | **1 core/node**             | Reserved cores = 1 × 10 = 10 cores |
| **Total Cores (Cluster)**              | `(Cores per Node × Nodes) - (Reserved Cores × Nodes)`                                     | (24×10) - (1×10) = **230 cores** | Usable cores ≈ **230**            |
| **Cores per Executor** (`--executor-cores`) | Number of cores assigned to each executor (optimal 3–7)                                   | **5 cores**                 | —                                 |
| **Executors per Node (float)**         | `(Cores per Node - Reserved) ÷ Cores per Executor`                                        | (24 - 1) ÷ 5 = **4.6**      | —                                 |
| **Executors per Node (rounded)**       | Round down to nearest integer (must be whole number)                                      | **4 executors/node**        | Executors consume (4 × 5) = 20 cores/node |
| **Total Executors (`--num-executors`)**| `(Executors per Node × Nodes) - 1 (for Application Master)`                               | (4×10) - 1 = **39 executors** | **Total executors = 39**         |
| **Allocated Cores (job)**              | `Total Executors × Cores per Executor`                                                    | 39 × 5 = **195 cores**      | CPU utilization = 195 ÷ 230 ≈ **85%** |
| **RAM per Executor (`--executor-memory`)** | `(RAM per Node ÷ Executors per Node) × (1 - overhead%)`, overhead ≈ 7%                    | (64 ÷ 4.6) × 0.93 ≈ **12 GB** | Memory per node used = 4 × 12 = 48 GB/node |
| **Total Executor Memory (Cluster)**    | `Total Executors × RAM per Executor`                                                      | 39 × 12 = **468 GB**        | RAM utilization = 468 ÷ 640 ≈ **73%** |
| **Data Size (Example)**                | Estimated input dataset size                                                              | **230 GB**                  | Data-to-core guideline: ~1 core per GB ⇒ need ≈230 cores |
| **Data-to-Core Ratio (Rule of Thumb)** | Rough guideline — ~1 Core per GB for balanced performance                                 | 230 GB ⇒ **≈230 cores**     | For this job: allocated cores = 195 (slightly under guideline) |
| **Practical Ranges / Notes**           | Cores/executor: **3–7** │ Memory/executor: **10–40 GB** │ Reserve 1 core/node & ~7–10% RAM for OS | Use Spark UI to tweak; leave buffer for other services |



### Quick interpretation / tips

* **CPU ~85%** and **RAM ~73%** utilization: good utilization with a safe buffer left for OS/Hadoop daemons and other services.
* If you need strict data-to-core parity (1 core per GB for 230 GB), increase executors/cores to reach ~230 cores — but monitor GC and shuffle overhead.
* Use Spark UI (Executors/Stages/Storage tabs) to validate actual utilization and adjust `--num-executors` / `--executor-memory` accordingly.

---

## 21. How to handle variable data volume efficiently in Spark? — i.e., when source data size (or record count) changes frequently?


### 🎯 Core Techniques to Handle Variable Source Data Size

| Category                                | Technique                                       | Explanation                                                                                                                                                                                                                                                                                                                                            |
| --------------------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **1️⃣ Partition Tuning**                | **`repartition()` / `coalesce()`**              | Adjusts the number of partitions at runtime based on data size.  <br>• Use `repartition(n)` to **increase** partitions (causes shuffle).  <br>• Use `coalesce(n)` to **reduce** partitions (avoids shuffle).                                                                                                                                           |
|                                         | **`spark.sql.shuffle.partitions`**              | Controls the default number of partitions for shuffle operations (e.g., joins, aggregations). <br>✅ Example: `spark.conf.set("spark.sql.shuffle.partitions", 4)`                                                                                                                                                                                       |
|                                         | **Custom Input Splitting**                      | When reading from sources like HDFS, GCS, or S3, you can define block size or number of partitions. <br>✅ Example: `spark.read.option("maxPartitionBytes", "128MB")`                                                                                                                                                                                   |
| **2️⃣ Resource Scaling**                | **YARN Dynamic Allocation**                     | Enables Spark to automatically **scale executors up or down** based on workload. <br>Key configs: <br>`\n--conf spark.dynamicAllocation.enabled=true\n--conf spark.dynamicAllocation.minExecutors=2\n--conf spark.dynamicAllocation.maxExecutors=5\n--conf spark.shuffle.service.enabled=true\n--conf spark.dynamicAllocation.executorIdleTimeout=30s` |
| **3️⃣ Data Skew / Small Data Handling** | **Broadcast Joins**                             | If one dataset is small, broadcast it to all executors to avoid shuffle. <br>✅ Example: `broadcast(df_small)`                                                                                                                                                                                                                                          |
| **4️⃣ Compression and Speculation**     | **Compression + Speculative Execution**         | Ensures better performance and fault tolerance for variable workloads. <br>✅ Example: `--conf spark.shuffle.compress=true` and `--conf spark.speculation=true`                                                                                                                                                                                         |
| **5️⃣ Monitoring**                      | **Adaptive Query Execution (AQE)** *(Spark 3+)* | Automatically optimizes shuffle partitions, join strategy, etc., at runtime. <br>✅ `spark.sql.adaptive.enabled=true`                                                                                                                                                                                                                                   |


### 🎯 Example Breakdown (Your spark-submit Command)

```bash
spark-submit --master yarn --deploy-mode client \
--jars gs://spark-lib/bigquery/spark-3.1-bigquery-0.32.2.jar \
--queue default \
--driver-memory 512m \
--executor-cores 2 \
--executor-memory 512m \
--conf spark.shuffle.compress=true \
--conf spark.speculation=true \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.initialExecutors=2 \
--conf spark.dynamicAllocation.minExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=5 \
--conf spark.dynamicAllocation.executorIdleTimeout=30s \
--conf spark.shuffle.service.enabled=true \
--conf spark.sql.shuffle.partitions=4 \
gs://xxyy/etl_job.py
```

### 🎯 What this achieves:

| Feature                    | Purpose                                                              |
| -------------------------- | -------------------------------------------------------------------- |
| `dynamicAllocation.*`      | Dynamically increases or decreases executors depending on data size. |
| `sql.shuffle.partitions=4` | Reduces shuffle overhead for smaller datasets.                       |
| `speculation=true`         | Avoids long-running tasks from slowing down job completion.          |
| `shuffle.compress=true`    | Compresses shuffle data to reduce network I/O.                       |


### 🎯 Optional Enhancements

| Enhancement                                                       | Why                                                                 |
| ----------------------------------------------------------------- | ------------------------------------------------------------------- |
| `spark.sql.adaptive.enabled=true`                                 | Enables AQE (auto-adjusts partitions at runtime).                   |
| `spark.sql.adaptive.shuffle.targetPostShuffleInputSize=134217728` | Targets ~128 MB per shuffle partition.                              |
| Monitor via Spark UI                                              | Track stage/task execution to tune partition and executor settings. |


### 🎯 Example Logic Inside Script

You can even make it dynamic in code:

```python
df = spark.read.parquet(input_path)
num_rows = df.count()

if num_rows > 10_000_000:
    df = df.repartition(200)
elif num_rows > 1_000_000:
    df = df.repartition(50)
else:
    df = df.coalesce(5)
```

### 🎯 **Summary**

> **To handle variable data sizes efficiently:**

* Scale **partitions** (`repartition`/`coalesce`/`sql.shuffle.partitions`)
* Scale **resources** (YARN dynamic allocation)
* Use **broadcast joins** for small datasets
* Enable **compression, speculation, and AQE** for adaptive performance

---


## 22. Why Spark Job is not uniformly running various every time? Sometimes the job is completing faster and sometimes it slow?

### 🎯 **Symptom**

Same job finishes fast sometimes, but slow at other times.  
Not applicable for Spark Serverless or BQ ondemand.

### 🎯 **Causes**

| Cause                             | Description                                                        |
| --------------------------------- | ------------------------------------------------------------------ |
| **Resource contention**           | Other jobs using cluster CPU/memory; your job waits in YARN queue. |
| **Variable data size**            | Input volume changes daily → more shuffle & memory use.            |
| **Uneven partitions / data skew** | Some tasks take longer due to heavier partitions.                  |
| **Dynamic allocation behavior**   | Executors added/removed too aggressively.                          |
| **Shuffle/network load**          | High I/O during heavy jobs or peak hours.                          |

### 🎯 **Fixes**

| Area              | Recommendation                                                             |
| ----------------- | -------------------------------------------------------------------------- |
| **Scaling**       | Enable **Dynamic Allocation** & **AQE** to auto-tune partitions/resources. |
| **Scheduling**    | Run job during **off-peak hours** or limit other heavy jobs.               |
| **Resources**     | Increase executor cores/memory or container size for spikes.               |
| **Skew handling** | Use **repartition**, **salting**, or **broadcast joins**.                  |
| **Reliability**   | Enable speculation: `spark.speculation=true`.                              |


### 🎯 **Example Settings**

```bash
--conf spark.dynamicAllocation.enabled=true
--conf spark.dynamicAllocation.maxExecutors=10
--conf spark.sql.adaptive.enabled=true
--conf spark.speculation=true
```

### 🎯 **Summary**

> Job speed variation mainly comes from **cluster contention** or **data size changes**.
> Use **dynamic allocation**, **adaptive execution**, and **better scheduling** to stabilize performance.

---



## 23. A daily Spark job that processes around 90 million records fails midway. How would you perform a Root Cause Analysis (RCA) and decide how to rerun the job efficiently?

### 🎯 **Common Failure Reasons**

| Category                                  | Examples                                                                                                      |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| ✅ **Data Issues**                         | Nulls in mandatory columns, invalid date formats, field length overflow, special characters, encoding errors. |
| ✅ **Resource Constraints**                | Executor OOM, CPU exhaustion, timeouts, insufficient YARN memory, cluster contention.                         |
| ✅ **Code Errors**                         | Null pointer exceptions, unhandled edge cases, missing dependencies, bad UDF logic.                           |
| 🟨 **Infrastructure / External Failures** | Temporary network, storage (HDFS/GCS/S3), or BigQuery connectivity issues.                                    |



### 🎯 **RCA Steps**

| Step                         | Action                                                                                                                        |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| ✅ **1. Check Logs**          | Examine Spark UI / YARN History Server logs to identify failing stage or exception type.                                      |
| ✅ **2. Identify Root Cause** | Based on the error: <br>• Data issue → bad records <br>• Resource issue → OOM/timeouts <br>• Code issue → review stack trace. |
| ✅ **3. Data Handling**       | For minor data errors, apply null handling, reject logic, or pre-validation (data audit checks).                              |
| ✅ **4. Upstream Correction** | If data corruption is major or mandatory fields missing → request source data resend.                                         |
| 🟨 **5. Track Failed Stage** | Note the exact stage/partition ID or marker to decide partial vs. full rerun.                                                 |


### 🎯 **Rerun Strategy**

| Scenario                           | Strategy                                                                                                      |
| ---------------------------------- |---------------------------------------------------------------------------------------------------------------|
| ✅ **Full Restart (slow but safe)** | If job is interdependent (joins, overwrites, deletes) — **delete old partition** and restart end-to-end.      |
| ✅ **Partial Restart (fast)**       | If pipeline is modular (ingestion → staging → transform → load), rerun only failed step using persisted outputs. |
| ✅ **Airflow Rerun Commands**       | airflow tasks clear <dag_id> <task_id>  <br/> airflow tasks run <dag_id> <task_id> <execution_date>           |
| 🟨 **Use Step Markers / Flags**    | Maintain completion flags or metadata tables to enable rerun from last successful checkpoint.                 |


### 🎯 **Preventive & Design Measures**

| Area                                    | Recommendation                                                                        |
| --------------------------------------- | ------------------------------------------------------------------------------------- |
| ✅ **Error Handling**                    | Add validations, try/except blocks, and known data fixes.                             |
| ✅ **Intermediate Outputs**              | Save results after major stages (Parquet + Snappy).                                   |
| ✅ **Performance Optimization**          | Tune executor memory, partitions, and enable dynamic allocation/AQE.                  |
| ✅ **Partial Rerun Enablement**          | Use flags, checkpoints, or modular design.                                            |
| ✅ **Workflow Management**               | Split large jobs into multiple **Airflow tasks** for better retry granularity.        |
| 🟨 **Monitoring & Alerts**              | Configure alerting (email/Slack) on task failure with error snapshot for quick RCA.   |
| 🟨 **Version Control for Code Changes** | Maintain job version to trace whether failure correlates to a recent code deployment. |

### 🎯 **Summary**

> When a Spark job fails midway, first identify *why* (data, resource, or code issue) via Spark/YARN logs, then choose the right rerun approach:
> **Full restart** if dependent transformations are affected, or **partial rerun** if modular checkpoints exist.
> Long term, add **error handling, step tracking, and job modularization (Airflow)** to reduce rerun effort.

---


## 24. If we face a performance or data issue in a Spark job, how do you perform RCA (Root Cause Analysis) beyond just checking logs?

### 🎯 **Answer:**

When logs alone are not sufficient, we follow a structured RCA approach combining environment checks, simulation, and controlled testing:

| Step                                           | Action                                                                                                                                                                                           | Purpose                                                                                                                                    |
| ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **1️⃣ Analyze Logs & Metrics**                 | Review **YARN AppMaster**, **container logs**, and **Spark driver/executor logs** for stage/task-level errors and memory usage.                                                                  | Identify initial failure point or resource bottleneck.                                                                                     |
| **2️⃣ Perform EDA / Simulation in Production** | With Prod support help, we can start a **REPL (Spark shell or PySpark session)** on the **same production cluster configuration** and **rerun the failing logic** using the problematic dataset. | This reproduces the issue in the same environment — a **foolproof way** to confirm if the problem is data-specific or environment-related. |
| **3️⃣ Reproduce in Development Environment**   | Replicate the same code in **DEV or QA**, using a smaller or masked subset of data (since Prod data usually can’t be copied).                                                                    | Helps isolate **code logic** or transformation issues independent of environment.                                                          |
| **4️⃣ Compare Resource & Data Stats**          | Compare data volume, skew, null count, partition distribution, and executor metrics between successful and failed runs.                                                                          | Detects whether the root cause is **data growth/skew** or **infrastructure performance**.                                                  |


### 🎯 **Extra Best Practices**

* Maintain **historical metrics** (record counts, partition sizes, run durations) for anomaly detection.
* Automate **data validation** before processing (schema, null, range checks).
* Involve **onshore Prod support** early for secure Prod-level testing access.

### 🎯 **Summary**

> RCA for Spark issues isn’t limited to reading logs — it combines **log analysis, live simulation in Prod (with care)**, and **code replication in lower environments** to isolate whether the issue lies in **data, environment, or logic**.

---

## 25. You are working on a data pipeline that loads data from multiple source systems into BigQuery.  During the load, you observe frequent data quality issues — for example, missing account numbers, invalid date formats, and null numeric fields.  The source system owners are not willing to fix these issues at the source. How would you design your ETL pipeline to handle these issues gracefully without blocking the load into BigQuery?

### 🎯 1. Identify and Classify Data Issues

First, categorize the types of issues:

| Type                     | Example                           | Typical Impact  | Possible Fix            |
| ------------------------ | --------------------------------- | --------------- | ----------------------- |
| Missing mandatory fields | `account_number` is null          | Record rejected | Derive or default       |
| Invalid format           | `date` = "32-13-2024"             | Parsing fails   | Clean/normalize         |
| Duplicates               | Repeated transaction IDs          | Double-counting | Deduplicate             |
| Referential integrity    | `customer_id` not in master table | Join fails      | Soft skip or flag       |
| Schema drift             | New columns added/removed         | Schema mismatch | Dynamic schema handling |


### 🎯 2. Apply Code-Level Patches (Data Cleaning Layer)

Create a **data pre-processing layer** before loading into BigQuery (BQ).
Examples (in PySpark or SQL transformations):

#### a. Handle Missing Fields

```python
df = df.withColumn(
    "account_number",
    F.when(F.col("account_number").isNull(),
           F.concat_ws("_", F.col("phone_number"), F.col("dob")))
     .otherwise(F.col("account_number"))
)
```

#### b. Handle Invalid Dates

```python
df = df.withColumn(
    "txn_date",
    F.when(F.col("txn_date").rlike("^\d{4}-\d{2}-\d{2}$"), F.col("txn_date"))
     .otherwise(F.lit(None))
)
```

#### c. Default or Flag Invalid Data

Add an error flag column instead of dropping the record:

```python
df = df.withColumn(
    "error_flag",
    F.when(F.col("account_number").isNull(), "MISSING_ACC_NO")
     .otherwise("VALID")
)
```

### 🎯 3. Implement a Quarantine or Error Table

Send bad or unverifiable data to a separate **BQ table** (e.g., `error_records`) for audit and later correction.

```sql
INSERT INTO bq_dataset.error_records
SELECT * FROM staging_table WHERE error_flag != 'VALID';
```

Then, only valid data moves to the final table:

```sql
INSERT INTO bq_dataset.main_table
SELECT * FROM staging_table WHERE error_flag = 'VALID';
```

### 🎯 4. Use Data Quality Rules (DQ Layer)

Add automated checks using tools or scripts:

* **Great Expectations**, **Deequ**, or **custom PySpark checks**
* Example: Validate all numeric fields or enforce date ranges

### 🎯 5. Communicate Upstream and Document Workarounds

Even though the source won’t fix it, maintain:

* A **data issue log** (timestamp, issue type, workaround applied)
* **Versioned patch scripts** for traceability
* Regular feedback loops to revisit root causes later

### 🎯 Example Summary

| Problem                  | Impact                 | Mitigation                    |
| ------------------------ | ---------------------- | ----------------------------- |
| Missing `account_number` | Rejects during BQ load | Derive using phone + DOB      |
| Invalid date format      | Parse errors           | Normalize date or set to null |
| Null numeric fields      | Aggregation errors     | Replace with 0 or flag        |
| Unmatched foreign key    | Join failure           | Move to quarantine table      |

---

## 26. How can you change the number of partitions and cache/persist a DataFrame in PySpark?  Also, how do you check its storage level and partition count?

### 🎯 **PySpark — Partitions, Cache & Storage Levels (Quick Reference)**

| **Command**                 | **Purpose**                      | **Shuffle** | **Example**                            |
| --------------------------- | -------------------------------- | ----------- | -------------------------------------- |
| `df.repartition(n)`         | Increase or rebalance partitions | ✅           | `df = df.repartition(6)`               |
| `df.coalesce(n)`            | Reduce partitions (efficient)    | ❌           | `df = df.coalesce(3)`                  |
| `df.rdd.getNumPartitions()` | Get partition count              | ❌           | `df.rdd.getNumPartitions()`            |
| `df.cache()`                | Cache in memory + disk (default) | ❌           | `df.cache()`                           |
| `df.persist(level)`         | Custom cache level               | ❌           | `df.persist(StorageLevel.MEMORY_ONLY)` |
| `df.unpersist()`            | Remove cache                     | ❌           | `df.unpersist()`                       |
| `df.rdd.getStorageLevel()`  | Show cache level                 | ❌           | `df.rdd.getStorageLevel()`             |


### 🎯 Storage Levels

```
StorageLevel(useDisk, useMemory, useOffHeap, deserialized, replication)
```
| **Output**                       | **Equivalent Constant** | **Meaning**                  |
| -------------------------------- | ----------------------- | ---------------------------- |
| `(True, True, False, False, 1)`  | `MEMORY_AND_DISK_SER`   | Serialized, in memory + disk |
| `(False, True, False, False, 1)` | `MEMORY_ONLY_SER`       | Serialized, memory only      |
| `(False, True, False, True, 1)`  | `MEMORY_ONLY`           | Deserialized, memory only    |
| `(True, False, False, False, 1)` | `DISK_ONLY`             | Stored on disk only          |


### 🎯 Example

```python
ffrom pyspark.sql import SparkSession
from pyspark import StorageLevel

spark = SparkSession.builder.getOrCreate()

# Load data
df = spark.read.csv("hdfs:///home/hduser/custs", header=False, inferSchema=True)

# Check initial partitions
print("Initial partitions:", df.rdd.getNumPartitions())

# Increase partitions (with shuffle)
df = df.repartition(6)
print("After repartition:", df.rdd.getNumPartitions())

# Decrease partitions (without shuffle)
df = df.coalesce(3)
print("After coalesce:", df.rdd.getNumPartitions())

# Cache or persist
df.cache()   # same as persist(StorageLevel.MEMORY_AND_DISK)
df.count()   # triggers cache

# Check cache info
print("Is Cached?:", df.is_cached)
print("[Storage Level]")
level = df.rdd.getStorageLevel()
print(" useDisk:", level.useDisk)
print(" useMemory:", level.useMemory)
print(" useOffHeap:", level.useOffHeap)
print(" deserialized:", level.deserialized)
print(" replication:", level.replication)

# Custom persist example
df.unpersist()
df.persist(StorageLevel.MEMORY_ONLY)
print("[Updated Storage Level]")
level = df.rdd.getStorageLevel()
print(" useDisk:", level.useDisk)
print(" useMemory:", level.useMemory)
print(" useOffHeap:", level.useOffHeap)
print(" deserialized:", level.deserialized)
print(" replication:", level.replication)
```

```pqsql
Initial partitions: 2
After repartition: 6
After coalesce: 3
Is Cached?: True
Storage Level: StorageLevel(True, True, False, False, 1)
Updated Storage Level: StorageLevel(False, True, False, False, 1)

```
---

## 27. How is a Spark application's code distributed and executed across the Driver and Executor nodes?

### 🎯 Spark Code Execution Location

| Location | Primary Responsibility | Key Operations |
| :--- | :--- | :--- |
| **Driver Node** | **Coordination & Planning** | * **`main()`** method, **`SparkSession`/`SparkContext`** creation. |
| | | * **`DAGScheduler`** and **`TaskScheduler`** (Job/Stage planning). |
| | | * Defining RDD lineage/definitions. |
| | | * **Consolidation Actions:** **`collect()`**, **`take()`**, final **`count()`** aggregation. |
| **Executor Node** | **Data Processing & Execution** | * Running assigned **Tasks**. |
| | | * **Transformations:** **`map`**, **`filter`**, **`join`**, **`groupBy`**, etc. |
| | | * Storing and using **broadcasted data**. |
| | | * Writing results to storage or sending intermediate results to the Driver. |
| **Driver & Executor** | **Interaction & Shared State** | * **`count()`:** Executors do local counts, Driver sums them. |
| | | * **`accumulator()`:** Driver initializes, Executors update. |
| | | * **`broadcast()`:** Driver sends the data, Executors cache and use it. |

### 🎯 Example
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf

# ---- DRIVER ----
spark = SparkSession.builder.appName("DriverExecutorDFExample").getOrCreate()
sc = spark.sparkContext

# Create sample DataFrame
data = [(1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E")]
df = spark.createDataFrame(data, ["id", "category"])

# Create broadcast variable (created in DRIVER → used in EXECUTOR)
multiplier = sc.broadcast(10)

# Create accumulator (created in DRIVER → updated in EXECUTOR)
acc = sc.accumulator(0)

# ---- EXECUTOR ----
# Define transformation using DataFrame DSL
# Transformations like withColumn, filter run on executors
df_transformed = (
    df.withColumn("value", col("id") * lit(multiplier.value))
      .filter(col("value") > 20)
)

# Accumulator update using UDF (executed in EXECUTOR)
def update_acc(x):
    acc.add(1)
    return x

update_acc_udf = udf(update_acc)

df_final = df_transformed.withColumn("updated_value", update_acc_udf(col("value")))

# ---- DRIVER + EXECUTOR ----
# Action (collect) triggers computation:
result = df_final.collect()  # Executors execute; Driver gathers result

# ---- DRIVER ----
print("✅ Final Result:", result)
print("✅ Accumulator Value:", acc.value)

```

```
Final Result: [Row(id=3, category='C', value=30, updated_value='30'), Row(id=4, category='D', value=40, updated_value='40'), Row(id=5, category='E', value=50, updated_value='50')]
Accumulator Value: 3
```
---


## 28. What happens when cached data doesn’t fit entirely into executor memory in Spark?

### 🎯 **Short Answer:**

If the data doesn’t fully fit in memory, **only some partitions get cached**.
The remaining partitions are **not cached** and will be **recomputed(using RDD lineage) on demand** when accessed again.
Even cached data can be **evicted** if new data needs memory space — so caching acts as a *hint*, not a strict guarantee.

### 🎯 **Detailed Behavior**

| **Scenario**                                 | **What Happens**                                               | **Result**                              |
| -------------------------------------------- |----------------------------------------------------------------| --------------------------------------- |
| Data < Executor Memory                       | Entire RDD/DataFrame cached in memory                          | Fast re-use from memory                 |
| Data > Executor Memory                       | Only part of RDD cached; rest recomputed on access             | Partial recomputation overhead          |
| New data cached later                        | Older cached blocks evicted (LRU policy - Least Resently Used) | Eviction of least-used data             |
| Cache + Disk persistence (`MEMORY_AND_DISK`) | Overflow data written to disk                                  | Prevents recomputation cost             |
| Cache + Serialization (`MEMORY_ONLY_SER`)    | Data stored in serialized form to save memory                  | Reduced memory footprint, slower access |

### 🎯 Example

```python
# MEMORY_ONLY -> If data doesn't fit, uncached partitions recomputed
df.persist(StorageLevel.MEMORY_ONLY)

# MEMORY_AND_DISK -> If data doesn't fit, spills to disk
df.persist(StorageLevel.MEMORY_AND_DISK)
```

### 🎯 **Key Takeaway**

Caching in Spark is **best-effort**, not guaranteed.
If memory is insufficient, Spark:

1. Keeps what it can.
2. Recomputes missing partitions.
3. May evict old cache blocks as needed.
---

## 29. When Out of Memory (OOM) Occurs in Spark?

Spark can throw an **OutOfMemoryError** when the allocated **JVM heap space** (either on the **Driver** or **Executor**) is not enough for the workload being processed.

### 🎯 1. Driver OOM (Out Of Memory in Driver Node)

| Cause                                                          | Explanation                                                                                    | Example                                       | Prevention / Fix                                                                                    |
| -------------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | --------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| **`rdd.collect()` or `df.collect()` on large datasets**        | Brings all data from executors to the driver, exceeding driver memory.                         | `df.collect()` on millions of rows.           | Use `take(n)`, `sample()`, or write to file instead of collecting full data.                        |
| **`sparkContext.broadcast()` of large data**                   | Broadcasting large datasets from driver to executors causes driver memory pressure.            | Broadcasting large lookup tables.             | Use small reference data for broadcast; store large data in distributed storage (e.g., HDFS, Hive). |
| **Low driver memory configuration**                            | Insufficient memory allocated using `--driver-memory`.                                         | e.g., Only 1 GB driver memory for large jobs. | Increase memory: `--driver-memory 4g` or higher.                                                    |
| **Large Job Plans / DAGs**                                     | Extremely complex query plans (too many transformations) consume high memory for DAG creation. | Complex joins, long lineage RDDs.             | Use checkpoints or persist intermediate data.                                                       |
| **Collecting metadata / results in actions like `toPandas()`** | Converts Spark DataFrame to local Pandas DF — all data moves to driver.                        | `df.toPandas()` on big data.                  | Use Spark operations for aggregations instead of moving to Pandas.                                  |


### 🎯 2. Executor OOM (Out Of Memory in Worker Nodes)

| Cause                                                         | Explanation                                                                                        | Example                                      | Prevention / Fix                                                                    |
| ------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------- | ----------------------------------------------------------------------------------- |
| **Large shuffles (joins, groupBy, reduceByKey, repartition)** | Intermediate shuffle data exceeds executor memory during aggregation.                              | `df.groupBy("key").agg(...)` on skewed keys. | Tune partitions, use broadcast joins, or increase `executor-memory`.                |
| **Data skew**                                                 | One or few partitions contain much more data than others.                                          | Key-based skew during joins.                 | Use salting or skew join optimization (`spark.sql.adaptive.skewJoin.enabled=true`). |
| **Caching / persisting large data**                           | Persisting big DataFrames without enough memory causes spilling or OOM.                            | `.cache()` multiple big DFs.                 | Cache only required DFs, or use `DISK_ONLY` storage level.                          |
| **Improper memory split (storage vs execution)**              | Spark divides memory between storage (cache) and execution (shuffle). Poor tuning can lead to OOM. | Default ratio not suitable for job workload. | Adjust with `spark.memory.fraction`, `spark.memory.storageFraction`.                |
| **User-defined functions (UDFs)**                             | UDFs that load big data into memory or return large objects.                                       | UDF reading external data.                   | Optimize UDF logic, or use native Spark SQL functions.                              |


### 🎯 3. Shuffle Memory Pressure

| Issue                                            | Description                                         | Mitigation                                                                             |
| ------------------------------------------------ | --------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Large shuffle files written to disk or in memory | Joins, groupBy, reduce cause heavy memory use.      | Increase `spark.executor.memoryOverhead`, or enable `spark.sql.adaptive.enabled=true`. |
| Serialization overhead                           | Large objects during shuffle need to be serialized. | Use `KryoSerializer` for better memory efficiency.                                     |


### 🎯 4. General OOM Prevention Tips

1. **Avoid collect() on large data** – use `limit`, `take`, or store to disk.
2. **Broadcast only small dataframes** (tens of MBs max).
3. **Use caching judiciously** — unpersist when not needed.
4. **Enable adaptive execution** (`spark.sql.adaptive.enabled=true`).
5. **Monitor Spark UI → Executors tab** to check memory usage.
6. **Adjust memory configs**:

   ```bash
   --driver-memory 4g
   --executor-memory 8g
   --executor-cores 4
   --conf spark.memory.fraction=0.8
   ```
7. **Handle data skew** using salting or AQE (`adaptive skew join handling`).


### 🎯 Summary Table

| Component    | Common Triggers                                             | Example Symptoms                                      |
| ------------ | ----------------------------------------------------------- | ----------------------------------------------------- |
| **Driver**   | collect(), broadcast large RDD, small driver memory         | Driver JVM crash, "OutOfMemoryError: Java heap space" |
| **Executor** | shuffle-heavy ops, data skew, caching large data            | Stage failure, repeated task retries                  |
| **Both**     | too many wide transformations or large intermediate results | Long GC pauses, slow job progress, OOM errors         |

---

# ⚡ Spark Functionalities (with Examples)

## 1. Difference between select(), selectExpr() and withColumn() functions in Spark DF?

| Operation                 | Syntax Style      | Adds / Modifies Column | Supports SQL Functions | Common Use                          |
| ------------------------- | ----------------- | ---------------------- | ---------------------- | ----------------------------------- |
| `withColumn()`            | DataFrame         | ✅ Yes                  | ⚠️ Limited             | Add/modify specific columns         |
| `select()`                | DataFrame         | ✅ Yes (if aliased)     | ⚠️ Limited             | Select subset of columns            |
| `selectExpr()`            | Hybrid (DF + SQL) | ✅ Yes (if aliased)     | ✅ Yes                  | SQL-style transformations inside DF |
| `spark.sql("SELECT ...")` | Pure SQL          | ✅ Yes                  | ✅ Yes                  | Full SQL flexibility                |

```python
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import *
from pyspark.sql.functions import col,lit

# --------------------------------------------------------
# 1️⃣ Create SparkSession
# --------------------------------------------------------
spark = SparkSession.builder.appName("Select_vs_SelectExpr_vs_withColumn").getOrCreate()

# --------------------------------------------------------
# 2️⃣ Create sample dataset
# --------------------------------------------------------
dataset1 = [
    Row("James", 34, "2006-01-01", "true", "M", 3000.60),
    Row("Michael", 33, "1980-01-10", "true", "F", 3300.80),
    Row("Robert", 37, "1992-06-01", "false", "M", 5000.50)
]

simpleSchema = StructType([
    StructField("firstName", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("jobStartDate", StringType(), True),
    StructField("isGraduated", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", DoubleType(), True)
])

df = spark.createDataFrame(dataset1, simpleSchema)
print("=== Original DataFrame ===")
df.printSchema()
df.show(truncate=False)

# --------------------------------------------------------
# 3️⃣ withColumn(): Add / Modify Columns using DF API
# --------------------------------------------------------
df_withcolumn = (
    df.withColumn("age1", col("age").cast(StringType()))
      .withColumn("isGraduated", col("isGraduated").cast(BooleanType()))
      .withColumn("jobStartDate", col("jobStartDate").cast(DateType()))
      .withColumn("deptid", lit(0).cast(IntegerType()))
)
print("=== Using withColumn() ===")
df_withcolumn.printSchema()
df_withcolumn.show(truncate=False)

# --------------------------------------------------------
# 4️⃣ select(): Select or rename columns
# --------------------------------------------------------
df_select = df_withcolumn.select("firstName", "age", "salary")
print("=== Using select() (select few columns) ===")
df_select.printSchema()   
df_select.show(truncate=False)

# You can also use expressions with select()
df_select_expr = df_withcolumn.select(
    col("firstName"),
    (col("salary") * 1.1).alias("increased_salary")
)
print("=== Using select() with column expressions ===")
df_select_expr.printSchema()
df_select_expr.show(truncate=False)

# --------------------------------------------------------
# 5️⃣ selectExpr(): Apply SQL expressions directly on columns
# --------------------------------------------------------
df_selectexpr = df_withcolumn.selectExpr(
    "cast(age as int) as age",
    "cast(isGraduated as string) as isGraduated",
    "cast(jobStartDate as string) as jobStartDate",
    "salary * 1.15 as bonus_salary",
    "concat(firstName, '-', gender) as fullId"
)
print("=== Using selectExpr() ===")
df_selectexpr.printSchema()
df_selectexpr.show(truncate=False)

# --------------------------------------------------------
# 6️⃣ SQL SELECT: Use full SQL syntax
# --------------------------------------------------------
df_withcolumn.createOrReplaceTempView("CastExample")

df_sql = spark.sql("""
    SELECT 
        firstName,
        STRING(age) AS age,
        BOOLEAN(isGraduated) AS isGraduated,
        DATE(jobStartDate) AS jobStartDate,
        salary,
        year(jobStartDate) AS joining_year,
        jobStartDate + INTERVAL 2 hours AS added_hours
    FROM CastExample
""")
print("=== Using spark.sql() ===")
df_sql.printSchema()
df_sql.show(truncate=False)

# --------------------------------------------------------
# 7️⃣ Comparison Summary
# --------------------------------------------------------
print("""
🧾 Summary:
- withColumn(): Add or modify column using DataFrame API.
- select(): Choose specific columns or simple expressions.
- selectExpr(): Apply SQL expressions inline on DataFrame.
- SQL SELECT: Full SQL capabilities (functions, intervals, joins, etc.)
""")

spark.stop()

```
---

## 2. How to print Schema, Column Names, and Data Types in Spark?

| Task                     | Command                                  |
| ------------------------ | ---------------------------------------- |
| Print schema             | `df3.printSchema()`                      |
| Get column names         | `df3.columns`                            |
| Get number of columns    | `len(df3.columns)`                       |
| Get datatypes            | `df3.dtypes`                             |
| Describe temp view       | `spark.sql("DESCRIBE view_name").show()` |

---

## 3.Schema Evolution using unionByName() in PySpark?

🎯 Scenario
You have employee data coming monthly —  
Jan data (old schema) → no salary column  
Feb data (new schema) → added salary column  
We need to combine both datasets safely even though their schemas differ.  

```python
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SchemaEvolutionExample").getOrCreate()

# --- January data (old schema)
data_jan = [Row(id=1, name="James"),
            Row(id=2, name="Maria")]
df_jan = spark.createDataFrame(data_jan)

# --- February data (new schema: added 'salary')
data_feb = [Row(id=3, name="Robert", salary=5000),
            Row(id=4, name="Jen", salary=6000)]
df_feb = spark.createDataFrame(data_feb)

print("=== January Data ===")
df_jan.show()
print("=== February Data ===")
df_feb.show()

# --- Schema Evolution using unionByName
df_union = df_jan.unionByName(df_feb, allowMissingColumns=True)

print("=== Combined Data After unionByName ===")
df_union.show()
df_union.printSchema()

# --- Write as Parquet (simulating monthly folders)
df_jan.write.mode("overwrite").parquet("hdfs:///home/hduser/employees/2023-01/")
df_feb.write.mode("overwrite").parquet("hdfs:///home/hduser/employees/2023-02/")

# --- Read back with automatic schema merging
df_parquet = spark.read.option("mergeSchema", "true").parquet("hdfs:///home/hduser/employees/*/")
print("=== Parquet Schema Merged Automatically ===")
df_parquet.printSchema()
df_parquet.show()

```
---
# Resilient Distributed Datasets (RDDs):
  1. Concept: The original low-level abstraction in Spark. Understand it conceptually, though you'll primarily use Dataframes.
  2. Chracteristics: Immutable, distributed collection of objects, fault-tolerant.
  3. Transformations (Lazy): `map(), filter(), flatMap(), distinct(), union()`.
  4. Actions (Trigger computation): `collect(), count(), take(), first(), reduce(), saveAsTextFile()`.
  5. Lazy Evaluation: Understand why transformations aren't executed until an action is called.
  6. Key-Value Pair RDDs: Operations like `reduceByKey(), groupByKey(), sortByKey(), join(), aggregateByKey()`.
---

## 4. Difference between Map vs FlatMap in Spark

| Aspect                  | map(func)                                                                            | flatMap(func)                                                                                         |
| ----------------------- | ------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| Definition              | Returns a new distributed dataset formed by passing each element through a function. | Similar to map, but each input can produce 0 or more output elements (function returns a collection). |
| Nature                  | One-to-one (Passive)                                                                 | One-to-many (Active)                                                                                  |
| Transformation Function | One element in → One element out                                                     | One element in → 0 or more elements out                                                               |
| Use Case                | Apply or validate fields like a SELECT statement (e.g., check field count).          | Used in frustration scoring or intent identification; explode or pivot data like SQL EXPLODE.         |

**Example:**

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MapVsFlatMap").getOrCreate()
rdd = spark.sparkContext.parallelize(["a b", "c d"])

# Using map(): keeps the structure (one input → one output list).
print(rdd.map(lambda x: x.split(" ")).collect())
# Output: [['a', 'b'], ['c', 'd']]

# Using flatMap(): flattens nested results (one input → multiple outputs).
print(rdd.flatMap(lambda x: x.split(" ")).collect())
# Output: ['a', 'b', 'c', 'd']
```
---

## 5. Difference between reduceByKey(), aggregateByKey() and groupByKey()


| **Aspect**               | **reduceByKey**                       | **aggregateByKey**                    | **groupByKey**                          |
| ------------------------ | ------------------------------------- | ------------------------------------- | --------------------------------------- |
| **Combiner Usage**       | ✅ Uses Combiner                       | ✅ Uses Combiner                       | ❌ Does not use Combiner                 |
| **Performance**          | High (less shuffle, combines locally) | High (less shuffle, combines locally) | Low (shuffles all data before grouping) |
| **Input vs Output Type** | Same                                  | Can differ                            | Same                                    |
| **Operation Type**       | Simple aggregation (sum, max, count)  | Complex aggregation (average, ratio)  | Grouping only                           |
| **Typical Use Case**     | Find total sales, max value, etc.     | Compute averages or ratios            | Group data like city → list of users    |

### 🎯 Input RDD

```python
rdd = sc.parallelize([('a', 2), ('b', 3), ('a', 4)])
```

### 🎯 1. reduceByKey → Combine values directly

```python
rdd.reduceByKey(lambda x, y: x + y).collect()
# Output: [('a', 6), ('b', 3)]
```

**Explanation:**
`reduceByKey` merges values using the same function (`+`) within and across partitions — total sum per key.

| Key | Values | Result |
| --- | ------ | ------ |
| 'a' | [2, 4] | 6      |
| 'b' | [3]    | 3      |


### 🎯 2. aggregateByKey → Flexible aggregation (e.g., average)

```python
rdd.aggregateByKey((0, 0),
                   lambda acc, v: (acc[0] + v, acc[1] + 1),   # within partition
                   lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])   # across partitions
                  ).mapValues(lambda x: x[0] / x[1]).collect()
# Output: [('a', 3.0), ('b', 3.0)]
```

**Explanation:**
Keeps track of both **sum and count** per key → calculates **average**.

| Key | Sum | Count | Avg |
| --- | --- | ----- | --- |
| 'a' | 6   | 2     | 3.0 |
| 'b' | 3   | 1     | 3.0 |


### 🎯 3. groupByKey → Groups values into a list

```python
rdd.groupByKey().mapValues(list).collect()
# Output: [('a', [2, 4]), ('b', [3])]
```

**Explanation:**
Simply groups all values per key — no computation yet.

| Key | Values |
| --- | ------ |
| 'a' | [2, 4] |
| 'b' | [3]    |

---

## 6. Converting Existing RDDs to Spark DataFrame

| **Method**                       | **Description**                                                                                              |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| **1. Using `toDF()`**            | Converts an RDD of tuples or `Row` objects into a DataFrame using implicit column naming.                    |
| **2. Using `createDataFrame()`** | Uses `SparkSession.createDataFrame()` to explicitly create a DataFrame from an RDD (with or without schema). |


```python
from pyspark.sql import SparkSession, Row

# Initialize SparkSession
spark = SparkSession.builder.appName("RDDtoDataFrameExample").getOrCreate()

# ---------------------------
# Method 1: Using toDF()
# ---------------------------
rdd1 = spark.sparkContext.parallelize([("James", 25), ("Anna", 28)])
df1 = rdd1.toDF(["name", "age"])

print("DataFrame using toDF():")
df1.show()

# ---------------------------
# Method 2: Using createDataFrame()
# ---------------------------
rdd2 = spark.sparkContext.parallelize([
    Row(name="John", age=30),
    Row(name="Mary", age=22)
])
df2 = spark.createDataFrame(rdd2)

print("DataFrame using createDataFrame():")
df2.show()

```

```pqsql
DataFrame using toDF():
+-----+---+
| name|age|
+-----+---+
|James| 25|
| Anna| 28|
+-----+---+

DataFrame using createDataFrame():
+----+---+
|name|age|
+----+---+
|John| 30|
|Mary| 22|
+----+---+

```

 🎯 **Summary**

* `toDF()` → Simple and direct (good for small or tuple-based RDDs).
* `createDataFrame()` → More control, supports schema definition and Row objects.

---

## 7. Which all kinds of data processing are supported by Spark?

| **Type**                   | **Description**                                                    | **Example / Tool**                                                     |
| -------------------------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------- |
| **Batch Processing**       | Processes large volumes of static data in batches.                 | ETL jobs, data transformations using **Spark Core** or **Spark SQL**   |
| **Interactive Processing** | Allows users to query and analyze data interactively in real time. | **Spark Shell**, **Spark SQL**, **Zeppelin**, **Databricks Notebooks** |
| **Stream Processing**      | Handles continuous data streams in near real time.                 | **Spark Streaming**, **Structured Streaming**                          |

---

## 8. Which all are the ways to configure Spark properties and order them from least priority to most priority?

| **Priority Level**       | **Configuration Method**                              | **Description / Usage**                                                                     |
| ------------------------ | ----------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| **3️⃣ Least Priority**   | **`conf/spark-defaults.conf`**                        | Default configuration file; applies to all Spark applications unless overridden.            |
| **2️⃣ Medium Priority**  | **`--conf` (command-line option)**                    | Used with `spark-submit` or `spark-shell` to override defaults for a specific job.          |
| **1️⃣ Highest Priority** | **`SparkConf` (inside code or SparkSession builder)** | Programmatically set Spark properties inside the application; overrides all other settings. |

Step 1: Set Default in spark-defaults.conf
```bash
## spark-defaults.conf
spark.executor.memory    2g
spark.executor.cores     2
```

Step 2: Override via Command Line
```bash
spark-submit \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=3 \
  my_app.py

```

Step 3: Override Again in Code (Highest Priority)
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PriorityExample") \
    .config("spark.executor.memory", "6g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()

print(spark.sparkContext.getConf().getAll())
print(spark.sparkContext.getConf().get("spark.default.parallelism"))

"""
[‘spark.app.name’, ‘PriorityExample’]
[‘spark.driver.extraJavaOptions’, ‘-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions —add-opens=java.base/java.lang=ALL-UNNAMED —add-opens=java.base/java.lang.invoke=ALL-UNNAMED —add-opens=java.base/java.lang.reflect=ALL-UNNAMED —add-opens=java.base/java.io=ALL-UNNAMED —add-opens=java.base/java.net=ALL-UNNAMED —add-opens=java.base/java.nio=ALL-UNNAMED —add-opens=java.base/java.util=ALL-UNNAMED —add-opens=java.base/java.util.concurrent=ALL-UNNAMED —add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED —add-opens=java.base/jdk.internal.ref=ALL-UNNAMED —add-opens=java.base/sun.nio.ch=ALL-UNNAMED —add-opens=java.base/sun.nio.cs=ALL-UNNAMED —add-opens=java.base/sun.security.action=ALL-UNNAMED —add-opens=java.base/sun.util.calendar=ALL-UNNAMED —add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false’]
[‘spark.executor.id’, ‘driver’]
[‘spark.driver.port’, ‘36223’]
[‘spark.app.submitTime’, ‘1762178769811’]
[‘spark.executor.cores’, ‘4’]
[‘spark.executor.memory’, ‘6g’]
[‘spark.app.id’, ‘local-1762178770274’]
[‘spark.app.startTime’, ‘1762178769890’]
[‘spark.rdd.compress’, ‘True’]
[‘spark.executor.extraJavaOptions’, ‘-Djava.net.preferIPv6Addresses=false -XX:+IgnoreUnrecognizedVMOptions —add-opens=java.base/java.lang=ALL-UNNAMED —add-opens=java.base/java.lang.invoke=ALL-UNNAMED —add-opens=java.base/java.lang.reflect=ALL-UNNAMED —add-opens=java.base/java.io=ALL-UNNAMED —add-opens=java.base/java.net=ALL-UNNAMED —add-opens=java.base/java.nio=ALL-UNNAMED —add-opens=java.base/java.util=ALL-UNNAMED —add-opens=java.base/java.util.concurrent=ALL-UNNAMED —add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED —add-opens=java.base/jdk.internal.ref=ALL-UNNAMED —add-opens=java.base/sun.nio.ch=ALL-UNNAMED —add-opens=java.base/sun.nio.cs=ALL-UNNAMED —add-opens=java.base/sun.security.action=ALL-UNNAMED —add-opens=java.base/sun.util.calendar=ALL-UNNAMED —add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED -Djdk.reflect.useDirectMethodHandle=false’]
[‘spark.driver.host’, ‘192.168.189.001’]
[‘spark.serializer.objectStreamReset’, ‘100’]
[‘spark.master’, ‘local[*]’]
[‘spark.submit.pyFiles’, ‘’]
[‘spark.submit.deployMode’, ‘client’]
[‘spark.ui.showConsoleProgress’, ‘true’]
"""
```

## 9. What is the default level of parallelism in Spark?

The **default level of parallelism** in Spark is determined by the **number of partitions** created when reading data or by Spark’s internal settings when not explicitly defined by the user.

| **Basis**                               | **Explanation**                                                                                                             |
| --------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| **HDFS / File-based Input**             | Equal to the **number of HDFS blocks**, typically with a **default block size of 128 MB**.                                  |
| **RDD Operations (like `parallelize`)** | Determined by the **`spark.default.parallelism`** configuration property.                                                   |
| **Default Behavior**                    | If not specified, Spark uses **the number of available CPU cores on the cluster** (i.e., total cores across all executors). |

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckDefaultParallelism").getOrCreate()
sc = spark.sparkContext

# Method 1
print("Default Parallelism:", sc.defaultParallelism)

# Method 2
print(spark.sparkContext.getConf().get("spark.default.parallelism"))

# Method 3
"""
You can also check it in the Spark Web UI under:
Environment → Runtime Environment → spark.default.parallelism
"""
```

✅ **Summary:**
**Default parallelism = number of partitions (based on 128 MB block size or CPU cores)**  
→ Controlled by **`spark.default.parallelism`** when not explicitly set.

---

## 10. Is it possible to have multiple SparkContext in a single JVM?

No, by default Spark **does not allow multiple SparkContexts in a single JVM**.
Only **one SparkContext** can run per **Driver JVM** because it manages all cluster communication and resources.

If you try to create another, Spark throws:

```
org.apache.spark.SparkException: Only one SparkContext may be running in this JVM
```

```python
from pyspark import SparkContext

# First SparkContext
sc1 = SparkContext(appName="App1")

# Attempt to create second SparkContext
sc2 = SparkContext(appName="App2")

```

You can override this (not recommended) by setting:

```
spark.driver.allowMultipleContexts = true
```

```python
from pyspark import SparkConf, SparkContext

# Enable multiple SparkContexts
conf = SparkConf() \
    .setAppName("MultipleContextsExample") \
    .setMaster("local") \
    .set("spark.driver.allowMultipleContexts", "true")

# First SparkContext
sc1 = SparkContext(conf=conf)
print("First SparkContext created:", sc1.appName)

# Stop the first context (recommended before creating another)
sc1.stop()

# Second SparkContext
sc2 = SparkContext(conf=conf)
print("Second SparkContext created:", sc2.appName)

sc2.stop()


```

## 11. In what situation do we terminate one SparkSession or SparkContext and create a new one within the same program?

We terminate an existing SparkSession or SparkContext and create a new one **when we need to start another session with different configurations** that cannot be modified at runtime (e.g., memory, shuffle partitions, or environment settings).

**Example:**

```python
from pyspark.sql import SparkSession

# First session
spark = SparkSession.builder.appName("Session1").getOrCreate()
print("First session created")

# Stop the first session
spark.stop()

# Create a new session with different config
spark2 = SparkSession.builder \
    .appName("Session2") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()

print("New session started with modified configuration")
```
> 💡 **Note:** Only one active SparkContext is allowed per JVM, so always stop the previous one before creating another.

### 🎯 **Typical Use Cases**

| **Scenario**                                                                 | **Reason to Create a New Session / Context**                                        | **Example**                                                                            |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| 🔧 **Change in Spark configuration** (e.g., memory size, shuffle partitions) | Spark configs are **immutable** after startup — new settings require a new session. | Changing `"spark.sql.shuffle.partitions"` from `200` to `50` for a smaller dataset.    |
| 🌐 **Switching between environments** (e.g., Dev → QA → Prod)                | Each environment may need **different cluster or database connections.**            | Switching Spark’s Hive warehouse from `/user/dev_warehouse` to `/user/prod_warehouse`. |
| 🧩 **Running independent workloads** in the same application                 | To **isolate configs, temporary views, or catalogs** across jobs.                   | Running one Spark job on CSV data and another on a JDBC source independently.          |

---

## 12. Can we share a DataFrame across multiple Spark sessions within a single application?  Can we share it across different Spark applications?  Or what is the difference between **Temp View** and **Global Temp View** in Spark (v2.3+)?

| **Aspect**    | **Temp View**                                         | **Global Temp View**                                                                    | **Across Applications**                                     |
| ------------- | ----------------------------------------------------- | --------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| **Scope**     | Accessible **only within the same SparkSession**.     | Accessible **across multiple SparkSessions** but **within the same Spark Application**. | ❌ Not possible directly.                                    |
| **Lifetime**  | Exists until the **SparkSession is terminated**.      | Exists until the **Spark Application ends**.                                            | Data must be persisted externally.                          |
| **Namespace** | Uses the current database context (no prefix needed). | Stored under the **system database `global_temp`**.                                     | Not applicable.                                             |
| **Use Case**  | Temporary queries inside one session.                 | Share data across sessions in same application.                                         | Share data between jobs by saving it (e.g., Parquet, Hive). |


### 🎯 **Example**

```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("TempViewExample").getOrCreate()

# Read DataFrame
df = spark.read.option("header", "false") \
               .option("delimiter", ",") \
               .option("inferschema", "true") \
               .csv("hdfs:///home/hduser/custs") \
               .toDF("custno", "firstname", "lastname", "age", "profession")

# -------------------------
# 1️⃣ Create a Session-level TEMP VIEW
# -------------------------
df.createOrReplaceTempView("view_temp")

print("\nSession-level Temp View (accessible only in same session):")
spark.sql("SELECT * FROM view_temp LIMIT 5").show()

# -------------------------
# 2️⃣ Create a GLOBAL TEMP VIEW
# -------------------------
df.createOrReplaceGlobalTempView("view_global")

print("\nGlobal Temp View (accessible across sessions in same app):")
spark.sql("SELECT * FROM global_temp.view_global LIMIT 5").show()

# -------------------------
# 3️⃣ Access Global Temp View from a NEW SESSION
# -------------------------
new_spark = spark.newSession()
print("\nAccessing Global Temp View from new session:")
new_spark.sql("SELECT * FROM global_temp.view_global LIMIT 5").show()
```

### 🎯 **Summary**

| **Type**                | **Accessible In**              | **Lifetime**                    | **Access Syntax**                           |
| ----------------------- | ------------------------------ | ------------------------------- | ------------------------------------------- |
| **Temp View**           | Same SparkSession              | Until session ends              | `SELECT * FROM view_temp`                   |
| **Global Temp View**    | All sessions in same Spark app | Until app ends                  | `SELECT * FROM global_temp.view_global`     |
| **Across Applications** | ❌ Not supported directly       | Until external storage deletion | Use saved data (e.g., Parquet, Hive, NoSQL) |

---

## 13. What is the advantage of broadcasting values or broadcasting a DataFrame across a Spark cluster?

Broadcasting in Spark allows sending a read-only copy of a variable or small DataFrame from the driver to all executors only once.  
This avoids repeatedly transferring the same data over the network and improves performance — especially for joins or lookups with small reference datasets.

| **Benefit**                 | **Explanation**                                                |
| --------------------------- | -------------------------------------------------------------- |
| 🚀 **Improved performance** | Reduces shuffle and network I/O during joins or lookups.       |
| 💾 **Lower driver load**    | Data is distributed once and reused on executors.              |
| 🔄 **Efficient joins**      | Ideal for joining a large DataFrame with a small lookup table. |

Example: Dataframe broadcasting using Join 
```python
"""
vi ~/transactions.csv
txn_id,cust_id,amount,country_code,txn_date
1001,C001,250.75,US,2025-11-01
1002,C002,180.00,IN,2025-11-01
1003,C003,99.50,UK,2025-11-02
1004,C004,310.25,IN,2025-11-02
1005,C005,520.00,AU,2025-11-03

vi ~/countries.csv
country_code,country_name
US,United States
IN,India
UK,United Kingdom
AU,Australia

hadoop fs -put ~/transactions.csv /home/hduser/
hadoop fs -put ~/countries.csv /home/hduser/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("BroadcastExample").getOrCreate()

# Large DataFrame
transactions = spark.read.csv("hdfs:///home/hduser/transactions.csv", header=True, inferSchema=True)

# Small lookup DataFrame
countries = spark.read.csv("hdfs:///home/hduser/countries.csv", header=True, inferSchema=True)

# Broadcast join
result = transactions.join(broadcast(countries), "country_code")
result.show()

result.explain()

```

Example: Variable Broadcasting
```python
from pyspark.sql import SparkSession

# 1️⃣ Create Spark session
spark = SparkSession.builder.appName("BroadcastVariableDemo").getOrCreate()
sc = spark.sparkContext

# 2️⃣ Small lookup dictionary (can be broadcast)
country_lookup = {
    "US": "United States",
    "IN": "India",
    "UK": "United Kingdom",
    "AU": "Australia"
}

# 3️⃣ Broadcast the dictionary
broadcast_country = sc.broadcast(country_lookup)

# 4️⃣ Create RDD (simulating transactions)
transactions = [
    (1001, "C001", 250.75, "US"),
    (1002, "C002", 180.00, "IN"),
    (1003, "C003", 99.50, "UK"),
    (1004, "C004", 310.25, "IN"),
    (1005, "C005", 520.00, "AU"),
]

rdd = sc.parallelize(transactions)

# 5️⃣ Use broadcast variable inside transformation
result = rdd.map(lambda x: (x[0], x[1], x[2], x[3], broadcast_country.value.get(x[3], "Unknown")))

# 6️⃣ Show result
for record in result.collect():
    print(record)

```
---

## 14. How can we distribute dependency JARs to workers?

Spark provides multiple ways to distribute dependency JARs to all worker nodes in the cluster. The JARs are automatically sent from the driver to executors when the job starts.

| **Method**                     | **Description**                                              | **Example**                                              |
| ------------------------------ | ------------------------------------------------------------ | -------------------------------------------------------- |
| `--jars` (with `spark-submit`) | Distributes JARs to all worker nodes at job submission time  | `spark-submit --jars /path/myutils.jar,myudf.jar app.py` |
| `SparkContext.addJar()`        | Dynamically adds JARs at runtime from within your Spark code | `sc.addJar("hdfs:///libs/myudf.jar")`                    |

✅ **Effect:**
These JARs are **copied to all executors** automatically, making their classes and functions available for use in transformations or actions across the cluster.

---

## 15. Which scheduler is used by SparkContext by default?
**Ans:** By default, **SparkContext** uses two internal schedulers to manage job execution:

| **Scheduler**     | **Purpose / Function**                                                                                                                   |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| **DAGScheduler**  | Breaks the job into **stages** based on RDD lineage (i.e., shuffle boundaries) and creates a **DAG (Directed Acyclic Graph)** of stages. |
| **TaskScheduler** | Takes each stage from the DAGScheduler and schedules the **individual tasks** to run on executors.                                       |

✅**In short:**
`DAGScheduler` handles *stage-level scheduling*, while `TaskScheduler` handles *task-level scheduling* within each stage.

---

## 16. How would you allocate the amount of memory to each executor?

**Ans:** The amount of memory allocated to each executor can be configured in two ways — either programmatically or through the `spark-submit` command.

| **Method**              | **Configuration Property** | **Example**                                | **Description**                                                    |
| ----------------------- | -------------------------- | ------------------------------------------ | ------------------------------------------------------------------ |
| 🔧 **spark-submit**     | `--executor-memory`        | `spark-submit --executor-memory 4G app.py` | Allocates 4 GB of memory to each executor.                         |
| 💻 **Programmatically** | `spark.executor.memory`    | `conf.set("spark.executor.memory", "4g")`  | Sets executor memory inside the SparkConf or SparkSession builder. |

Example: Setting Executor Memory in Code
```python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# 1️⃣ Create SparkConf and set executor memory
conf = SparkConf() \
    .setAppName("ExecutorMemoryExample") \
    .setMaster("local[*]") \
    .set("spark.executor.memory", "4g") \
    .set("spark.driver.memory", "2g")

# 2️⃣ Create SparkContext using the above configuration
sc = SparkContext(conf=conf)

# 3️⃣ Optionally create SparkSession (if using DataFrames)
spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()

# 4️⃣ Print configuration values
print("Executor Memory:", spark.sparkContext.getConf().get("spark.executor.memory"))
print("Driver Memory:", spark.sparkContext.getConf().get("spark.driver.memory"))

# 5️⃣ Simple operation
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)
print("RDD Sum:", rdd.sum())

# 6️⃣ Stop Spark
spark.stop()

```
**In short:**
You can control executor memory using either `--executor-memory` in the submission command or the configuration property `spark.executor.memory` in code.

---

## 17. How would you control the number of partitions of an RDD?

**Ans:** You can control or change the number of partitions in an RDD using **`repartition()`** or **`coalesce()`** operations.

| **Method**       | **Usage**                                                  | **When to Use**                                                 | **Example**                 |
| ---------------- | ---------------------------------------------------------- | --------------------------------------------------------------- | --------------------------- |
| `repartition(n)` | Increases or decreases partitions (creates a full shuffle) | When you want to **increase partitions** for better parallelism | `rdd2 = rdd.repartition(6)` |
| `coalesce(n)`    | Reduces partitions (avoids full shuffle)                   | When you want to **reduce partitions** efficiently              | `rdd3 = rdd.coalesce(2)`    |

**In short:**

* Use **`repartition()`** → for **increasing** partitions (involves shuffle).
* Use **`coalesce()`** → for **decreasing** partitions (no shuffle, faster).

---

## 18. What are the possible operations on RDD?

**Ans:** RDDs support two main types of operations — **Transformations** and **Actions**.

| **Type**            | **Description**                                                                                        | **Examples**                                                                                 |
| ------------------- | ------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------- |
| **Transformations** | Lazy operations that create a **new RDD** from an existing one. They are **not executed immediately**. | `map()`, `flatMap()`, `filter()`, `groupByKey()`, `reduceByKey()`, `join()`, `repartition()` |
| **Actions**         | Trigger the **actual computation** and return a value to the driver or write data to storage.          | `collect()`, `count()`, `first()`, `take()`, `saveAsTextFile()`, `reduce()`                  |


**In short:**

* **Transformations** define *what to do*.
* **Actions** tell Spark *to do it*.

---

## 19. How does RDD help in parallel job processing?

**Ans:**
RDDs enable **parallel processing** by dividing data into **partitions**. Each partition is processed **independently and in parallel** across multiple executors or nodes in a Spark cluster.

| **Concept**                     | **Explanation**                                                                                                      |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Partitioning**                | RDD is split into multiple partitions so that each partition can be processed on a separate executor thread or node. |
| **Parallel Execution**          | Spark schedules tasks for each partition to run concurrently across the cluster.                                     |
| **Sequential Inside Partition** | Within a single partition, records are processed sequentially.                                                       |

✅**In short:**
RDDs enable **data parallelism**, allowing Spark to process large datasets **faster and efficiently** across multiple nodes.

---

## 20. What is a transformation?

**Ans:**
A **transformation** is a **lazy operation** on an RDD that produces a **new RDD** from an existing one. Transformations are **not executed immediately** — they are executed **only when an action** is called.

| **Key Point**       | **Explanation**                                                                                |
| ------------------- | ---------------------------------------------------------------------------------------------- |
| **Lazy Evaluation** | Spark builds a logical execution plan (DAG) instead of running the transformation immediately. |
| **Output**          | Always returns a new RDD.                                                                      |
| **Examples**        | `map()`, `flatMap()`, `filter()`, `reduceByKey()`, `join()`, `cogroup()`                       |

✅**In short:**
Transformations define **what to do** with the data — actual execution happens only when an **action** triggers the computation.

---

## 21. How do you define actions?

**Ans:**
An **action** is an operation that **triggers the execution** of all previously defined **RDD transformations** and **returns a result** to the Spark driver or writes data to external storage.

| **Key Point** | **Explanation**                                                             |
| ------------- | --------------------------------------------------------------------------- |
| **Purpose**   | Executes the RDD lineage (DAG) and performs the actual computation.         |
| **Result**    | Returns data to the driver or writes output to storage (HDFS, S3, etc.).    |
| **Examples**  | `collect()`, `count()`, `first()`, `take()`, `saveAsTextFile()`, `reduce()` |


**In short:**
Actions are like a **valve** — until an action is triggered, transformations are just plans.  
Only actions **materialize** the computation and produce actual results.

---

## 22. How can you create an RDD for a text file?

**Ans:**
You can create an RDD from a text file using the **`SparkContext.textFile()`** method. It reads data line by line from a file (local, HDFS, S3, etc.) and returns an RDD of strings.

| **Method**          | **Description**                                                                  | **Example**                                    |
| ------------------- | -------------------------------------------------------------------------------- | ---------------------------------------------- |
| `sc.textFile(path)` | Reads a text file and creates an RDD where each element is one line of the file. | `rdd = sc.textFile("hdfs:///data/sample.txt")` |

**Example:**

```python
from pyspark import SparkContext

sc = SparkContext(appName="TextFileExample")

rdd = sc.textFile("hdfs:///home/hduser/transactions.csv")
print("Number of lines:", rdd.count())

sc.stop()
```

**In short:**
`textFile()` is the standard way to **load text data** as an RDD in Spark.

---
## 23. How does execution start and end in an RDD or Spark job?

**Ans:**
Execution in Spark starts from the **earliest RDDs** (those without dependencies or that are cached) and proceeds through the **DAG of transformations**, ending with the **RDD that produces the result of an action**.

| **Stage**                      | **What Happens**                                                                                                        |
| ------------------------------ | ----------------------------------------------------------------------------------------------------------------------- |
| **1️⃣ DAG Creation**           | When transformations are applied, Spark builds a logical **Directed Acyclic Graph (DAG)** of RDDs showing dependencies. |
| **2️⃣ Action Trigger**         | When an **action** (like `collect()` or `count()`) is called, Spark’s **DAGScheduler** starts execution.                |
| **3️⃣ Stage & Task Execution** | The DAG is split into **stages**, and each stage is executed as **tasks** on worker nodes.                              |
| **4️⃣ Result Collection**      | Once all tasks finish, results are **returned to the driver** (or written to storage).                                  |

**In short:**
Execution starts from **source RDDs → transformations → actions**,
and ends when **the action produces a result or output.**

---

## 24. Give examples of transformations (or actions) that trigger Spark jobs.

**Ans:**
Normally, **transformations** are *lazy* and don’t trigger a Spark job until an **action** is called.
However, there are a few **special transformations** that internally require executing a job to compute intermediate results.

| **Category**                           | **Examples**                                                  | **Why They Trigger a Job**                                                                                                |
| -------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **Transformations that trigger a job** | `sortBy()`, `zipWithIndex()`, `repartition()`, `countByKey()` | These need to compute data metrics (like partition sizes or element ordering), which require executing tasks immediately. |
| **Actions that trigger a job**         | `collect()`, `count()`, `saveAsTextFile()`, `reduce()`        | Actions always trigger job execution since they materialize the computation.                                              |

**In short:**
While most transformations are lazy, a few like **`sortBy()`** or **`zipWithIndex()`** internally run Spark jobs to prepare metadata before continuing.

---

## 25. Data is spread across all nodes of the cluster — how does Spark process this data?

**Ans:**
Spark processes distributed data efficiently by leveraging **data locality** and **parallelism**.

| **Concept**             | **Explanation**                                                                                                                   |
| ----------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| **Data Locality**       | Spark tries to schedule tasks on the same node where the data resides (or nearby) to **reduce data transfer** across the network. |
| **Partitioning**        | Data is split into **partitions**, and each partition is processed as a **task** on a cluster node.                               |
| **Parallel Processing** | Multiple tasks run **simultaneously** across executors on different nodes, enabling parallel computation.                         |

**In short:**
Spark optimizes processing by **running tasks close to where data lives** and **processing partitions in parallel** across the cluster.

---

## 26. How would you hint the minimum number of partitions during a transformation?

You can specify the **minimum number of partitions** as a **second parameter** in many Spark transformations or when creating an RDD. This helps control parallelism during data loading or transformation.

| **Method**                            | **Example**                                       | **Description**                                                                   |
| ------------------------------------- | ------------------------------------------------- | --------------------------------------------------------------------------------- |
| `sc.parallelize(data, numPartitions)` | `sc.parallelize(range(1, 101), 2)`                | Creates an RDD with **2 partitions**.                                             |
| `sc.textFile(path, numPartitions)`    | `rdd = sc.textFile("hdfs:///data/file.txt", 400)` | Loads the file into **400 partitions**, determined by Hadoop’s `TextInputFormat`. |

**In short:**
Set the **number of partitions** in the transformation or data load itself — Spark will create that many **tasks** to read and process data in **parallel**, improving performance and control over workload distribution.

---

## 27. How many concurrent tasks can Spark run for an RDD partition?

**Ans:**
Spark can run **only one concurrent task per RDD partition**, and the total number of tasks running in parallel is limited by the **number of available cores** in the cluster.

| **Concept**                  | **Explanation**                                                                                  |
| ---------------------------- | ------------------------------------------------------------------------------------------------ |
| **Task per Partition**       | Each RDD partition is processed by **exactly one task**.                                         |
| **Parallelism Limit**        | Maximum concurrent tasks = **total number of cores** available across the cluster.               |
| **Recommended Partitioning** | Ideally, have **2–3× the number of cores** for better load balancing and resource utilization.   |
| **Get Default Parallelism**  | `sc.defaultParallelism` gives Spark’s default number of partitions (usually equals total cores). |

**In short:**
Spark runs **one task per partition**, and the total parallelism depends on **cluster cores**.
For efficient performance, set partitions ≈ **2–3× total cores** in the cluster.

---

## 28. What limits the maximum size of a partition in Spark?

**Ans:**
The **maximum size of a partition** is mainly limited by the **available memory** and **CPU cores** of the **executor** that processes it.

| **Factor**                           | **Explanation**                                                                                                           |
| ------------------------------------ | ------------------------------------------------------------------------------------------------------------------------- |
| **Executor Memory**                  | Each partition must fit in the executor’s memory; too large a partition can cause **OutOfMemory (OOM)** errors.           |
| **Number of Cores**                  | Each core handles one task (and hence one partition) at a time — limited cores mean limited parallel processing capacity. |
| **Shuffle / Serialization Overhead** | Large partitions also increase network shuffle time and serialization/deserialization overhead.                           |

✅ **In short:**
A partition’s size is effectively limited by **executor memory capacity** and **available cores** — partitions that are too large can lead to **OOM** or poor performance.

---

## 29. ✅When Spark works with `file.txt.gz`, how many partitions can be created?

**Ans:**
When Spark reads a **compressed file** such as `file.txt.gz`, it **cannot split** the file because **Gzip compression is not splittable**.
Hence, **Spark creates only one partition** for that file — meaning the entire file is read by a **single task**.

If you want to increase parallelism, you can **repartition** the RDD after reading:

```python
from pyspark import SparkContext

sc = SparkContext(appName="TextFileExample")
rdd = sc.textFile("file.txt.gz",minPartitions=2) #Even if you specify minPartitions=2, Spark will still create only one partition for .gz files.
rdd = rdd.repartition(100)
```
Now, the RDD will have **100 partitions** of roughly equal size (though data is shuffled during repartitioning).

| **Case**              | **Splittable?** | **Default Partitions**          | **Can Increase Partitions?** |
| --------------------- | --------------- | ------------------------------- | ---------------------------- |
| `.txt` (uncompressed) | ✅ Yes           | Multiple (based on HDFS blocks) | Yes                          |
| `.txt.gz` (Gzip)      | ❌ No            | **1 partition**                 | ✅ Yes, via `repartition()`   |
| `.bz2` (Bzip2)        | ✅ Yes           | Multiple                        | Yes                          |

**In short:**
Gzip files (`.gz`) are **non-splittable**, so Spark will initially create **only 1 partition**, but you can **repartition** the RDD afterward to improve parallelism.

---

## 30. What is coalesce transformation?
`coalesce()` reduces the number of partitions in an RDD or DataFrame.
It avoids full shuffle by default (`shuffle=False`), making it faster but less balanced.
Use it mainly after filters or before saving output.

**Example:**

```python
from pyspark import SparkContext
sc = SparkContext(appName="TextFileExample")

rdd = sc.parallelize(range(1, 101), 10)
rdd2 = rdd.coalesce(5)  # reduce to 5 partitions

rdd3 = rdd.coalesce(5, shuffle=True) # If you need better balance:
```

---

## 31. What is the difference between `cache()` and `persist()` in RDD?

* `cache()` is a shorthand for `persist(StorageLevel.MEMORY_ONLY)`.
* `persist()` allows specifying different storage levels (e.g., memory, disk, or both).
  Both help reuse RDDs across multiple actions without recomputation.

**Example:**

```python
from pyspark import SparkContext
sc = SparkContext(appName="TextFileExample")

rdd = sc.textFile("hdfs:///data/file.txt")

# Using cache() → stores in memory only
rdd.cache()

# Using persist() → stores in memory and disk
from pyspark import StorageLevel
rdd.persist(StorageLevel.MEMORY_AND_DISK)
```

---

## 32. What is Shuffling in Spark?
Shuffling is the process of **repartitioning or redistributing data** across different partitions — often involving data transfer between executors or even nodes over the network.

It happens during wide transformations like `groupByKey()`, `reduceByKey()`, or `join()` and can be **costly** in terms of performance.

**💡 Tips to Reduce Shuffling:**

* Use **combiners** (e.g., `reduceByKey`, `aggregateByKey`) instead of `groupByKey`.
* **Repartition wisely** (use `coalesce()` when reducing partitions).
* **Cache intermediate RDDs** if reused.

**Example:**

```python
rdd = sc.parallelize([(1, 10), (2, 20), (1, 30), (2, 40)], 2)

# Causes shuffle as data with same key moves to same partition
rdd.reduceByKey(lambda x, y: x + y).collect()
# Output: [(1, 40), (2, 60)]
```

---
## 33. Does shuffling change the number of partitions?
**Yes.**

During a shuffle, Spark **redistributes data across partitions**, and by default, the number of shuffle partitions is set by the configuration parameter:

```python
spark.conf.get("spark.sql.shuffle.partitions")
# Default: 200
```

So, when a shuffle occurs (e.g., during `join`, `groupBy`, `reduceByKey`),
Spark **creates 200 output partitions by default**, unless explicitly changed using:

```python
spark.conf.set("spark.sql.shuffle.partitions", 50)
```

**💡 Note:**

* For **RDDs**, the number of partitions after shuffle depends on the transformation used (e.g., `reduceByKey(numPartitions=10)`).
* For **DataFrames**, it’s controlled by `spark.sql.shuffle.partitions`.

---

## 34. Which script will you use to run a Spark Application when not using `spark-shell/pyspark`?

✅ **Answer:**
You use the **`spark-submit`** script to launch a Spark application — it submits your program to a Spark cluster or local mode for execution.

**Example:**

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 2G \
  --total-executor-cores 4 \
  my_spark_app.py
```

**💡Note:**

* `spark-shell`(for Scala) and `pyspark`(for Python) are REPL (Read–Eval–Print Loop) used for **interactive mode**.
* `spark-submit` is used for **production or batch job submissions**.

---

## 35. What is a Task in Spark Job execution?

✅ **Answer:**
A **Task** is the **smallest unit of execution** in Spark — it represents a **computation on a single data partition** that runs on an executor.

| Concept              | Description                                                                      |
| -------------------- | -------------------------------------------------------------------------------- |
| **Definition**       | A Task is a physical unit of work sent by the driver to executors for execution. |
| **Execution**        | Each Task runs on a single partition of an RDD or DataFrame.                     |
| **Stage Dependency** | All Tasks in one stage must finish before the next stage starts.                 |
| **Parallelism**      | Multiple Tasks (one per partition) run in parallel across executors.             |
| **Retry**            | If a Task fails, Spark can re-run it on another executor.                        |

**💡 Example:**
If you have an RDD with **10 partitions**, Spark will create **10 Tasks** — one per partition — which will be distributed to executors for parallel execution.

---
## 36. What is Speculative Execution of a Task (Straggler Tasks)?

✅ **Answer:**
**Speculative Execution** is a mechanism in Spark to handle **slow-running (straggler) tasks** by **launching duplicate copies** of those tasks on other executors.
The first completed copy’s result is accepted, and the others are killed — improving overall stage completion time.

| Concept             | Description                                                                             |
| ------------------- | --------------------------------------------------------------------------------------- |
| **When it happens** | When a task runs significantly slower than the median of other tasks in the same stage. |
| **What Spark does** | Starts a duplicate task (speculative copy) on another executor.                         |
| **Goal**            | Reduce stage delay caused by slow nodes or network issues.                              |
| **Default**         | Disabled by default (`spark.speculation=false`).                                        |

**Example:**

```bash
spark-submit \
  --conf "spark.speculation=true" \
  --conf "spark.speculation.multiplier=5" \
  --class "org.example.MyJob" myapp.jar
```

**💡Note:**
Enable speculation **only when needed** — in large jobs with thousands of tasks, excessive speculative execution can overload the driver.

---

## 37. What is the Master URL in Local Mode?

✅ **Answer:**
In **local mode**, Spark runs on a single machine using one or more threads — useful for testing or development.
The **master URL** defines how many threads Spark should use.

| Master URL | Description                                                                               |
| ---------- | ----------------------------------------------------------------------------------------- |
| `local`    | Runs Spark with **1 thread** (no parallelism).                                            |
| `local[n]` | Runs Spark with **n threads**, simulating parallel execution.                             |
| `local[*]` | Uses **all available cores** on the machine (`Runtime.getRuntime.availableProcessors()`). |

**💡Example:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("LocalModeExample") \
    .getOrCreate()
```

✅ **Use Case:**

* `local` → debugging simple jobs
* `local[4]` → simulate 4 cores
* `local[*]` → maximize CPU usage on your system

**The list of possible values for .master() in Spark**  
local  
local[N]  
local[*]  
local-cluster[N, cores, memory]  
spark://<host>:<port>  
yarn  
yarn-client  
yarn-cluster  
mesos://<host>:<port>  
k8s://https://<api-server>:<port>  

---

## 38. Define Components of YARN?

✅ **Answer:**
YARN (**Yet Another Resource Negotiator**) is the cluster resource management layer in Hadoop that manages resources and schedules jobs.
Its key components are:

| Component                  | Description                                                                                                 |
| -------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **ResourceManager (RM)**   | Master daemon that manages cluster resources, allocates containers, and coordinates with NodeManagers.      |
| **ApplicationMaster (AM)** | Manages a single application’s execution. Requests containers from RM, monitors tasks, and handles retries. |
| **NodeManager (NM)**       | Runs on each worker node. Reports resource usage to RM and launches containers on request.                  |
| **Container**              | A logical unit of resources (CPU + memory) allocated by RM where tasks or AMs run.                          |
| **NameNode**               | (From HDFS, not YARN core) — Stores metadata and manages access to distributed file system data.            |

💡 **In Spark on YARN:**

* **Driver runs inside the ApplicationMaster** (in cluster mode).
* **Executors run inside Containers** on NodeManagers.

---

## 39. What is a Broadcast Variable?

✅ **Answer:**
A **Broadcast Variable** in Spark is a **read-only shared variable** that is **cached on each executor node** instead of being sent with every task.

This reduces communication overhead and improves performance when the same data (like lookup tables or reference data) is needed across multiple tasks.

| Feature       | Description                                         |
| ------------- | --------------------------------------------------- |
| **Type**      | Read-only shared variable                           |
| **Stored On** | Each executor (cached once per node)                |
| **Purpose**   | Avoids repeatedly sending large data to executors   |
| **Benefit**   | Reduces network I/O and task serialization overhead |

**Example:**

```python
broadcastVar = sc.broadcast({"IN": "India", "US": "United States"})
rdd = sc.parallelize(["IN", "US", "IN"])
result = rdd.map(lambda code: broadcastVar.value[code]).collect()
print(result)   # ['India', 'United States', 'India']
```

---

## 40. If we join multiple DataFrames, how do we identify which DataFrame or join is taking more time using Spark UI?

✅ **Answer:**
You can identify which DataFrame or join operation takes more time by analyzing the **SQL tab** and **Stages tab** in the **Spark UI**.

| Step                            | Where to Look                                                    | What to Check                                                                                                    |
| ------------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **1. SQL Tab**                  | Shows each query execution plan.                                 | Check the **execution DAG** and **physical plan** — it reveals join types (Broadcast, Shuffle Hash, Sort Merge). |
| **2. Stage Tab**                | Shows runtime for each stage in the join.                        | Identify the stage that takes the longest — it likely corresponds to a large shuffle or join.                    |
| **3. Tasks Tab (inside Stage)** | Shows task duration, input size, and shuffle read/write metrics. | Helps pinpoint which DataFrame caused heavy shuffle or skew.                                                     |
| **4. SQL Explain Plan**         | (via `df.explain(True)`)                                         | Shows join strategies and cost; adjust join config based on data size.                                           |

**💡 Optimization Tip:**
Use appropriate join strategy based on data volume:

* **Broadcast Join** → for small lookup DataFrames (`spark.sql.autoBroadcastJoinThreshold`).
* **Shuffle Hash / Sort Merge Join** → for large DataFrames.

**Example:**

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)  # 100 MB
```
**Summary:**
🔍 In Spark UI → **SQL Tab → Query → Execution Plan**
→ Identify long-running stages → Optimize join strategy accordingly.

---

## 41. We have to read and process N number of HQL/SQL queries. How can we execute them in parallel instead of sequentially in Spark?

✅ **Answer:**
To **execute multiple HQL/SQL queries in parallel** rather than sequentially in Spark, you can use **multi-threading** (in local mode) or **parallel job submission** (in cluster mode).

Here’s how it works and what the example does 👇

### **Approach**

You can:

1. **Store the SQL/HQLs** (in a file, list, or database table).
2. **Load them into a DataFrame or RDD.**
3. **Process them in parallel** by using:

   * Python’s `threading` (local mode)
   * `spark.scheduler.mode=FAIR` (enables concurrent jobs)
   * Or separate Spark sessions in cluster mode


### **Example Using Threading (Local Mode)**

```python
from pyspark import SparkContext, SparkConf
import threading

def run_query(sc, i):
    print(f"Starting query {i}")
    # Simulate a job (replace with spark.sql(query))
    print(sc.parallelize(range(i * 10000)).count())

def run_multiple_queries():
    conf = SparkConf().setMaster("local[*]").setAppName("ParallelHQLExample")
    conf.set("spark.scheduler.mode", "FAIR")  # Allow parallel scheduling
    sc = SparkContext(conf=conf)

    threads = []
    for i in range(4):  # e.g., 4 queries
        t = threading.Thread(target=run_query, args=(sc, i))
        threads.append(t)
        t.start()
        print(f"Query {i} started")

    for t in threads:
        t.join()

    print("All queries completed.")

run_multiple_queries()
```

### **Explanation**

| Component                   | Purpose                                                   |
| --------------------------- | --------------------------------------------------------- |
| `local[*]`                  | Use all available cores for parallel execution            |
| `spark.scheduler.mode=FAIR` | Enables fair scheduling between concurrent jobs           |
| `threading.Thread()`        | Allows concurrent job submission to the same SparkContext |
| `t.join()`                  | Waits for all threads to complete                         |


### **Cluster Mode Alternative**

If running on **YARN / Kubernetes**, you can:

* Submit multiple Spark jobs **in parallel** via separate `spark-submit` calls.
* Or use **Fair Scheduler Pools** via configuration:

  ```bash
  spark-submit \
    --conf spark.scheduler.mode=FAIR \
    --conf spark.scheduler.allocation.file=/path/to/fairscheduler.xml \
    ...
  ```

### ✅ **Summary**

| Mode                                  | How to Achieve Parallelism                             |
| ------------------------------------- | ------------------------------------------------------ |
| **Local Mode**                        | Use `threading` + `spark.scheduler.mode=FAIR`          |
| **Cluster Mode (YARN/K8s)**           | Use Fair Scheduler or concurrent `spark-submit` jobs   |
| **Databricks / Structured Streaming** | Can use multiple concurrent queries using `async` APIs |

---

## 42. How do we read nested structured data in Spark?


### 🎯 **Sample JSON file (`data.json`):**

```json
[
  {
    "id": 1,
    "name": "Alice",
    "contacts": [
      {"type": "email", "value": "alice@example.com"},
      {"type": "phone", "value": "1234567890"}
    ],
    "address": {
      "city": {
        "name": "Bangalore",
        "pincode": 560001
      },
      "country": "India"
    }
  },
  {
    "id": 2,
    "name": "Bob",
    "contacts": [
      {"type": "email", "value": "bob@example.com"},
      {"type": "phone", "value": "9876543210"}
    ],
    "address": {
      "city": {
        "name": "Chennai",
        "pincode": 600001
      },
      "country": "India"
    }
  }
]
```

### 🎯 **PySpark Code**

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Create Spark session
spark = SparkSession.builder \
    .appName("NestedDataExample") \
    .master("local[*]") \
    .getOrCreate()

# Read the nested JSON file
df = spark.read.option("multiline", "true").json("file:///home/hduser/data.json")

print("Original Schema:")
df.printSchema()

print("Original Data:")
df.show(truncate=False)

# Explode the array field 'contacts'
df_exploded = df.withColumn("contact", explode(col("contacts")))

# Access deeply nested fields using dot notation
df_flattened = df_exploded.select(
    "id",
    "name",
    col("contact.type").alias("contact_type"),
    col("contact.value").alias("contact_value"),
    col("address.city.name").alias("city_name"),
    col("address.city.pincode").alias("pincode"),
    col("address.country").alias("country")
)

print("Flattened Data:")
df_flattened.show(truncate=False)
```

### 🎯 **Output:**

**Schema (Before):**

```
root
 |-- address: struct (nullable = true)
 |    |-- city: struct (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- pincode: long (nullable = true)
 |    |-- country: string (nullable = true)
 |-- contacts: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- type: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- id: long (nullable = true)
 |-- name: string (nullable = true)
```

**Flattened Data (After explode + dot access):**

```
+---+-----+-------------+---------------+----------+-------+-------+
|id |name |contact_type |contact_value  |city_name |pincode|country|
+---+-----+-------------+---------------+----------+-------+-------+
|1  |Alice|email        |alice@example.com|Bangalore|560001|India  |
|1  |Alice|phone        |1234567890     |Bangalore|560001|India  |
|2  |Bob  |email        |bob@example.com|Chennai  |600001|India  |
|2  |Bob  |phone        |9876543210     |Chennai  |600001|India  |
+---+-----+-------------+---------------+----------+-------+-------+
```


✅ **Key Concepts:**

* Use `explode()` to flatten **arrays**.
* Use **dot notation** (`a.b.c`) to access nested **struct fields**.
* Optionally use `alias()` to rename flattened columns for clarity.

---

## 43. Two spark jobs running in parallel and writing in the same hive table what will happen? 

✅ **Answer:**
If two Spark jobs try to **write to the same Hive table at the same time**, the writes will **not happen truly in parallel** — they’ll execute **sequentially** or one job may even **fail** due to **file/lock conflicts**.

However:

* If both jobs write to **different HDFS locations** (for example, different output folders or temporary paths), and
* You then create or refresh an **external Hive table** pointing to those combined locations,

then you can achieve **parallelism** and avoid write contention.

### 🎯 Summary

| Scenario                                                                  | Behavior                                         |
| ------------------------------------------------------------------------- | ------------------------------------------------ |
| Two jobs writing to **same Hive table (managed)**                         | One job waits for the other / potential conflict |
| Two jobs writing to **same path in HDFS**                                 | Data corruption or “file exists” errors          |
| Two jobs writing to **different HDFS paths** and using **external table** | Parallel writes are possible                     |
| Use of **partitioned Hive table** (each job writes different partition)   | Safe and parallelizable                          |


**Best Practice:**
👉 When parallel jobs need to write to Hive, ensure they:

* Write to **distinct partitions** or **separate directories**.
* Use **external tables** or **staging tables**, then perform a final **merge or MSCK REPAIR TABLE** after completion.

--- 

## 44. What is checkpointing, when we go for checkpointing?

✅ **Answer:**
**Checkpointing** is the process of **persisting an RDD’s data** to a **reliable storage system** (like HDFS) and **cutting off its lineage graph** to prevent recomputation in case of driver or node failure. It helps make Spark applications more **fault-tolerant** and **stable**, especially for long-running jobs or streaming workloads.

### 🎯 **When to Use Checkpointing**

Use checkpointing when:

1. 🔄 **Long Lineage Chains** — When an RDD depends on many previous transformations, causing a large lineage graph that risks driver memory overload or recomputation delays.
2. ⚡ **Streaming Applications** — In Spark Streaming, to recover stateful operations (like `updateStateByKey` or `mapWithState`) during driver restarts.
3. 💾 **Non-recomputable Source** — When source data cannot be recomputed (e.g., streaming data or ephemeral sources).
4. 🧱 **Fault Tolerance** — When you need to recover from driver or executor crashes without losing intermediate computations.

### 🎯 **Example**

```python
from pyspark import SparkContext

sc = SparkContext("local[*]", "CheckpointExample")
sc.setCheckpointDir("hdfs:///checkpoints")

rdd = sc.parallelize(range(1, 10)).map(lambda x: (x, x * 2))
rdd.checkpoint()     # Mark for checkpointing
rdd.count()          # Trigger action (saves to checkpoint dir)
```

After checkpointing, Spark will:

* Save the RDD data to the checkpoint directory.
* Remove parent lineage references, reducing recomputation overhead.

### 🎯 **Summary Table**

| Feature                           | Description                                                                                                 |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| **Purpose**                       | Fault tolerance & lineage truncation                                                                        |
| **Storage**                       | Reliable FS (HDFS, S3, etc.)                                                                                |
| **When to Use**                   | Long lineage, streaming, unrecomputable data                                                                |
| **API**                           | `rdd.checkpoint()`                                                                                          |
| **Difference from cache/persist** | Checkpoint stores data *reliably* and truncates lineage; cache/persist stores *temporarily in memory/disk*. |


---

## 45. What happens if we read from a Hive table and write back to the same table using Spark SQL?

✅ **Answer:**
When you **read from and write back to the same Hive table or path in Spark**, you’ll get the error:

> `AnalysisException: Cannot overwrite a path that is also being read from.`

This occurs because Spark detects that the **read and write paths overlap**. To prevent data corruption or inconsistencies, Spark blocks such operations.

### 🎯 **Why It Happens**

Spark builds a **lineage graph** of all transformations.
If the **source and destination paths are the same**, Spark assumes you’re overwriting data that’s still being read — causing potential read/write conflicts. Hence, Spark throws the exception.

### 🎯 **Solutions**

#### 🎯 **Option 1: Use Checkpointing**

Checkpointing **breaks the lineage** so that Spark no longer links the output to the original input path.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CheckpointDemo").enableHiveSupport().getOrCreate()
spark.sparkContext.setCheckpointDir("hdfs:///tmp/checkpoint_dir")

# Read → Checkpoint → Process → Overwrite
df = spark.sql("SELECT * FROM default.customer").checkpoint()
df.write.mode("overwrite").saveAsTable("default.customer")
```

#### 🎯 **Option 2: Write to a Temporary Location**

You can write the intermediate data to a temporary location (like Parquet), reload it, and then safely overwrite the source table.

```python
df = spark.sql("SELECT * FROM default.customer")
df.write.mode("overwrite").parquet("hdfs:///tmp/cust_temp/")

df_temp = spark.read.parquet("hdfs:///tmp/cust_temp/")
df_temp.write.mode("overwrite").saveAsTable("default.customer")
```

### 🎯 **Summary Table**

| Scenario                        | Problem                                                                    | Solution                                              |
| ------------------------------- | -------------------------------------------------------------------------- | ----------------------------------------------------- |
| Read + Write to same Hive table | `AnalysisException: Cannot overwrite a path that is also being read from.` | Break lineage using `checkpoint()` or temporary write |
| Overwriting same path           | Spark prevents overwrite to avoid data corruption                          | Use checkpoint or temp Parquet location               |

---


## 46. 


---
## 12. Schema & Cast Examples

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

data = [("James", 34, "2006-01-01"), ("Michael", 33, "1980-01-10")]
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("dob", StringType(), True)
])

df = spark.createDataFrame(data, schema)

df2 = df.withColumn("age_str", col("age").cast("string"))
df2.show()

```

---

## 13. Example RDD to DataFrame Conversion

```python
from pyspark.sql import Row

rdd = sc.parallelize([Row(name="Alice", age=25), Row(name="Bob", age=30)])
df = spark.createDataFrame(rdd)
df.show()
```

---

## 14. Broadcasting Example

```python
from pyspark.sql.functions import broadcast

df1 = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "val"])
df2 = spark.createDataFrame([(1, "X"), (2, "Y")], ["id", "desc"])

df_join = df1.join(broadcast(df2), "id")
df_join.show()
```

---

## 15. Actions vs Transformations

```python
# Transformation (lazy)
rdd = sc.parallelize([1, 2, 3, 4])
mapped = rdd.map(lambda x: x * 2)

# Action (triggers execution)
print(mapped.collect())
```

---

## 16. Speculative Execution

👉 If one executor is slow, Spark relaunches tasks elsewhere. Enable via:

```bash
--conf spark.speculation=true
```

---

## 17. Cache vs Persist

```python
df = spark.range(1, 1000)
df.cache()   # MEMORY_ONLY
df.persist() # default MEMORY_AND_DISK
```

---

## 18. Union with Schema Evolution

```python
df1 = spark.createDataFrame([(1, "A")], ["id", "name"])
df2 = spark.createDataFrame([(2,)], ["id"])

df_union = df1.unionByName(df2, allowMissingColumns=True)
df_union.show()
```

---

## 19. What is the difference between RDD, DataFrame, and Dataset?

* **RDD**: Low-level, type-unsafe, no schema, transformations & actions.
* **DataFrame**: High-level, schema-based, optimized by Catalyst.
* **Dataset**: Type-safe DataFrame (only in Scala/Java).

```python
rdd = sc.parallelize([("Alice", 25), ("Bob", 30)])
df = rdd.toDF(["name", "age"])
df.show()
```

---

## 20. What are Spark Transformations and Actions?

* **Transformations**: Lazy (e.g., `map`, `filter`)
* **Actions**: Trigger execution (e.g., `collect`, `count`)

```python
rdd = sc.parallelize([1, 2, 3])
mapped = rdd.map(lambda x: x * 2)   # transformation
print(mapped.collect())             # action
```

---

## 21. Explain narrow vs wide transformations.

* **Narrow**: No shuffle (e.g., `map`, `filter`).
* **Wide**: Requires shuffle (e.g., `groupByKey`, `reduceByKey`).

```python
rdd = sc.parallelize([("a",1),("b",2),("a",3)])
print(rdd.reduceByKey(lambda x,y: x+y).collect())
```

---

## 22. Difference between `map` and `flatMap`.

```python
rdd = sc.parallelize(["a b", "c d"])
print(rdd.map(lambda x: x.split(" ")).collect())     # [['a','b'],['c','d']]
print(rdd.flatMap(lambda x: x.split(" ")).collect()) # ['a','b','c','d']
```

---

## 23. Explain Spark Job, Stage, Task.

* **Job**: Triggered by action.
* **Stage**: Split at shuffle boundaries.
* **Task**: Smallest unit, sent to executor.

```python
df = spark.range(1,10)
df.filter("id > 5").count()   # triggers 1 job with multiple tasks
```

---

## 24. Explain Spark DAG.

👉 Directed Acyclic Graph (DAG) built from transformations. Optimized before execution.

```python
df = spark.range(5).withColumn("x2", (df.id * 2))
df.explain(True)   # show DAG/plan
```

---

## 25. Explain Catalyst Optimizer.

* Analyzes query → creates logical plan → optimizes → generates physical plan.

```python
df = spark.range(1,5)
df.filter("id = 2").explain(True)
```

---

## 26. Explain Tungsten Project.

* Memory management + code generation (whole-stage codegen).
* Vectorized execution, CPU-efficient.

---

## 27. Difference between persist and cache.

* `cache()` = MEMORY\_ONLY.
* `persist()` = configurable storage level.

```python
df = spark.range(1,1000)
df.cache()
df.persist()
```

---

## 28. What is checkpointing?

* Writes RDD/DataFrame lineage to HDFS for fault tolerance.

```python
sc.setCheckpointDir("/tmp/checkpoint")
rdd = sc.parallelize(range(10))
rdd.checkpoint()
```

---

## 29. What is broadcast variable?

```python
from pyspark.sql.functions import broadcast

df1 = spark.createDataFrame([(1,"A"),(2,"B")],["id","name"])
df2 = spark.createDataFrame([(1,"X"),(2,"Y")],["id","desc"])
df1.join(broadcast(df2),"id").show()
```

---

## 30. What is accumulator?

```python
acc = sc.accumulator(0)
rdd = sc.parallelize([1,2,3,4])
rdd.foreach(lambda x: acc.add(x))
print(acc.value)   # 10
```

---

## 31. Difference between repartition and coalesce.

* `repartition(n)` → shuffle, can increase partitions.
* `coalesce(n)` → no shuffle, only reduce partitions.

---

## 32. What is shuffle in Spark?

👉 Data redistribution between nodes. Expensive! Happens in `groupByKey`, `join`.

---

## 33. Difference between reduceByKey and groupByKey.

```python
rdd = sc.parallelize([("a",1),("a",2),("b",1)])
print(rdd.reduceByKey(lambda x,y: x+y).collect()) # [('a',3),('b',1)]
print(rdd.groupByKey().mapValues(list).collect()) # [('a',[1,2]),('b',[1])]
```

---

## 34. Difference between DataFrame DSL and SQL.

```python
df = spark.createDataFrame([(1,"A"),(2,"B")],["id","name"])
df.filter(df.id==1).show()       # DSL
df.createOrReplaceTempView("t")
spark.sql("SELECT * FROM t WHERE id=1").show()  # SQL
```

---

## 35. Difference between inner, left, right, full join.

```python
df1 = spark.createDataFrame([(1,"A"),(2,"B")],["id","val"])
df2 = spark.createDataFrame([(1,"X")],["id","desc"])
df1.join(df2,"id","inner").show()
df1.join(df2,"id","left").show()
df1.join(df2,"id","right").show()
df1.join(df2,"id","full").show()
```

---

## 36. Explain union and unionByName.

```python
df1 = spark.createDataFrame([(1,"A")],["id","name"])
df2 = spark.createDataFrame([(2,)],["id"])
df1.unionByName(df2,allowMissingColumns=True).show()
```

---

## 37. Explain schema definition.

```python
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
df = spark.createDataFrame([("Alice",30)], schema)
df.printSchema()
```

---

## 38. Explain withColumn and cast.

```python
from pyspark.sql.functions import col
df = spark.createDataFrame([("Alice",30)],["name","age"])
df.withColumn("age_str", col("age").cast("string")).show()
```

---

## 39. Explain window functions.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df = spark.createDataFrame([("A",100),("B",200),("C",150)],["name","score"])
w = Window.orderBy("score")
df.withColumn("rank", row_number().over(w)).show()
```

---

## 40. Explain cume\_dist() usage.

```python
from pyspark.sql.functions import cume_dist
df = spark.createDataFrame([(1,100),(2,200),(3,300)],["id","score"])
df.withColumn("cume", cume_dist().over(Window.orderBy("score"))).show()
```

---

## 41. Explain partitionBy in writes.

```python
df = spark.createDataFrame([(1,"A"),(2,"B")],["id","val"])
df.write.partitionBy("val").parquet("/tmp/out")
```

---

## 42. Explain bucketing.

```python
df.write.bucketBy(4,"id").saveAsTable("bucketed_table")
```

---

## 43. Difference between shuffle partition & executor core config.

```bash
--conf spark.sql.shuffle.partitions=200
--conf spark.executor.cores=4
```

---

## 44. Explain speculative execution.

```bash
--conf spark.speculation=true
```

---

## 43. Explain skew handling.

* Broadcast join.
* Salting keys.
* Skew join hints.

---

## 44. Difference between narrow and wide join.

👉 Narrow = no shuffle (map-side join). Wide = shuffle required.

---

## 45. Explain save modes.

* `overwrite`, `append`, `ignore`, `errorIfExists`

---

## 46. Explain merge (Delta Lake).

```python
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/tmp/delta")
deltaTable.alias("t").merge(
    source=df.alias("s"),
    condition="t.id = s.id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

---

## 47. Explain dynamic partition pruning.

👉 Spark 3.x feature: reduces scanned partitions in joins.

---

## 48. Explain adaptive query execution (AQE).

👉 Spark 3.x runtime optimizations (e.g., dynamic shuffle partitions).

---

## 49. Difference between collect and take.

```python
print(df.collect())  # all data to driver
print(df.take(5))    # first 5 rows only
```

---

## 50. Difference between show() and display().

* `show()` → prints to console.
* `display()` → Databricks-only interactive table.

---

## 51. Explain UDF in PySpark.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def upper_case(x): return x.upper()
udf_upper = udf(upper_case, StringType())
df.withColumn("name_up", udf_upper("name")).show()
```

---

## 52. Explain SparkContext vs SparkSession.

* SparkContext = RDD API.
* SparkSession = unified entry for SQL/DF.

---

## 53. Multiple SparkContext allowed?

❌ Only 1 active SparkContext.

---

## 54. Run Spark without SparkSession?

✅ Yes, with SparkContext only.

---

## 55. Explain broadcast join hint.

```python
df1.join(df2.hint("broadcast"), "id").show()
```

---

## 56. Explain file formats.

* Parquet (columnar, compressed).
* ORC (optimized for Hive).
* Avro, JSON, CSV.

---

Perfect 👍 Let’s continue the same format (questions + clean explanation + **PySpark snippets where useful**) starting from **Q57**.

---

## 57. What is a Task with regards to Spark Job execution?

* A **Task** = smallest unit of work sent to executors.
* Each partition → 1 task.
* All tasks in a **Stage** must finish before moving to the next stage.
* A job = many tasks grouped in stages.

👉 Example:

```python
rdd = sc.parallelize(range(1,11), 4)  # 4 partitions → 4 tasks
print(rdd.map(lambda x: x*2).collect())
```

Here, Spark will launch **4 tasks** in parallel (one per partition).

---

## 58. What is Speculative Execution of Tasks (Straggler tasks)?

* If a task runs **slower** than peers in the same stage → Spark launches a duplicate on another executor.
* The **faster copy wins**, result is kept.

👉 Enable via:

```bash
spark-submit \
  --conf spark.speculation=true \
  --conf spark.speculation.multiplier=5 \
  job.py
```

⚠️ Use only when stragglers exist — speculation itself adds overhead.

---

## 59. What is the master URL in local mode?

* `local` → 1 thread.
* `local[n]` → n threads.
* `local[*]` → as many threads as CPU cores.

👉 Example:

```python
sc = SparkContext("local[2]", "DemoApp")
```

---

## 60. Define components of YARN.

* **ResourceManager (RM)** → global master, allocates resources.
* **NodeManager (NM)** → manages containers on worker nodes.
* **ApplicationMaster (AM)** → 1 per app; negotiates resources & monitors tasks.
* **Container** → actual resource bundle (CPU + RAM) to run a task.

👉 Spark on YARN:
Driver runs as AM, executors run inside Containers.

---

## 61. What is a Broadcast Variable?

* Read-only variable cached on all executors.
* Avoids shipping copy of same data to every task.

👉 Example:

```python
b = sc.broadcast([1,2,3])
rdd = sc.parallelize([1,2,3,4])
print(rdd.map(lambda x: (x, x in b.value)).collect())
```

---

## 62. If we join multiple DataFrames, how to identify bottleneck in Spark UI?

* Open **Spark UI → SQL Tab**.
* Inspect **query plan**:

  * Shuffle Hash Join
  * Sort Merge Join
  * Broadcast Join

👉 Optimize by forcing broadcast:

```python
df1.join(df2.hint("broadcast"), "id").show()
```

---

## 63. How to process multiple HQL/SQL queries in parallel?

* Store queries in table/file.
* Launch them via **threads** or **FAIR scheduler**.

👉 Example with threading:

```python
import threading
from pyspark import SparkContext, SparkConf

def task(sc, qid):
    print(sc.parallelize(range(qid*10000)).count())

conf = SparkConf().setMaster("local[*]").setAppName("multiJob")
conf.set("spark.scheduler.mode", "FAIR")
sc = SparkContext(conf=conf)

for i in range(4):
    t = threading.Thread(target=task, args=(sc, i))
    t.start()
```

---

## 64. How do we read nested structure data in Spark?

* Use **dot notation** for struct fields.
* Use **explode()** for arrays.

👉 Example:

```python
from pyspark.sql.functions import explode

data = [(1, {"city": "NY", "zip": 10001}, [10,20])]
df = spark.createDataFrame(data, ["id","addr","scores"])

df.select("id", "addr.city", explode("scores")).show()
```

---

## 65. Two Spark jobs writing into the same Hive table in parallel — what happens?

* Hive insert → **serialized execution** (one after another).
* Multiple writers → conflict or overwrite.
* Workaround:

  * Write to **separate HDFS dirs**, then register an **external Hive table**.

---

## 66. What is checkpointing? When do we use it?

* Cuts off **lineage** and saves RDD/DataFrame state to HDFS/local.
* Needed when:

  * Lineage is too long.
  * Source cannot be recomputed (e.g., streaming).

👉 Example:

```python
sc.setCheckpointDir("/tmp/checkpoint")
rdd = sc.parallelize(range(5))
rdd.checkpoint()
print(rdd.count())
```

---

## 67. What if we read & overwrite the same Hive table?

* Spark error: *Cannot overwrite a path that is also being read from*.
* Fix: **checkpoint** or **write to temp location first**.

👉 Example:

```python
spark.sparkContext.setCheckpointDir("/tmp/checkpoint")
df = spark.sql("SELECT * FROM cust").checkpoint()
df.write.mode("overwrite").saveAsTable("cust")
```

---

## 68. How to change Hive execution engine to Spark?

```sql
SET hive.execution.engine=spark;
```

(Available from Hive 1.x onward if Spark integration is enabled.)

---

## 69. How to change column type in RDD, DF & SQL?

* **RDD**: use `map` + type cast.
* **DF**: use `cast()`.
* **SQL**: use `CAST`.

👉 Example:

```python
from pyspark.sql.functions import col

df = spark.createDataFrame([("10",)], ["age"])
df.withColumn("age_int", col("age").cast("int")).show()
```

---

## 70. Have you handled date functions in Spark? Example: extract year.

```python
from pyspark.sql.functions import year, to_date

df = spark.createDataFrame([("2024-09-26",)], ["dt"])
df.withColumn("yr", year(to_date("dt"))).show()
```

---
