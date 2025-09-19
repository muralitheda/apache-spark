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

Perfect 👍 Let’s do the **Spark job submission in Cluster Mode** in the same style as the YARN job flow I gave you earlier.

---

### Q2. What Happens When I Submit a Spark Job in Cluster Mode?

#### 📌 Job Submission Flow

```bash
spark-submit \
  --class pkg.class \
  --master yarn \
  --deploy-mode cluster \
  my_spark_app.jar customer_table temp_path
```

When you submit a **Spark job** to a cluster (YARN/Mesos/K8s in **cluster mode**), here’s the sequence:

#### 1️⃣ Spark Submit Command

* You run `spark-submit` 🖥️
* The **Spark Submit client** prepares:

  * Application jar(s) 📦
  * Dependencies (jars, files, configs) ⚙️
  * Command-line args

#### 2️⃣ Contact with Resource Manager (YARN)

* Since deploy mode = `cluster`, the **driver** itself will run inside the cluster.
* `spark-submit` contacts **YARN Resource Manager** 🗂️ to request resources.
* RM talks to **NameNode** 📑 (for input metadata) if needed.

#### 3️⃣ Launch Application Master / Driver

* RM allocates a container 🏗️ and launches **Application Master (AM)**.
* In Spark, the AM is responsible for starting the **Driver** program 🚗 inside the cluster.
* **Driver = Brain of Spark job** (parsing, DAG building, scheduling).

#### 4️⃣ Driver Initialization

* Driver:

  * Reads job configuration 📝
  * Builds a **DAG (Directed Acyclic Graph)** of transformations & actions 🔗
  * Prepares execution plan

#### 5️⃣ Driver Registers with RM

* Driver program 📡 registers with Resource Manager.
* Requests containers for **executors** from RM.

#### 6️⃣ Executor Containers Allocation

* RM negotiates with **Node Managers** 🖥️ to start **Executor containers**.
* Executors are launched in cluster nodes with:

  * Spark runtime 🏃
  * Required jars & dependencies

#### 7️⃣ Executors Register with Driver

* Executors 📦 register themselves back to the **Driver**.
* Cluster is now ready to execute tasks.

#### 8️⃣ Task Scheduling & Execution

* Driver splits job into **stages** 🪜 and **tasks**.
* Sends tasks to executors for parallel execution ⚡.


#### 9️⃣ Executors Run Tasks

* Executors process data (from HDFS / external sources).
* Store intermediate data in memory/disk 🔄.
* Send results/status back to Driver.

### 🔟 Monitoring & Status Updates

* Executors → Driver → Application Master → Resource Manager → Client 📡.
* Status updates flow continuously.

#### 1️⃣1️⃣ Job Completion

* When all tasks ✅ complete:

  * Executors shut down 📴
  * Driver exits gracefully 🚗💨
  * Application Master de-registers from RM


✅ **Key Difference from YARN MapReduce:**

* In YARN MR → **Application Master manages Mappers/Reducers**.
* In Spark → **Driver manages Executors & tasks (DAG execution)**.

---


### Q3. Comparision between YARN MapReduce vs Spark Cluster Mode vs Spark Client Mode

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
