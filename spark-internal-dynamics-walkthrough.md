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
🧑‍💻 Client (spark-submit)
   └─> 📦 Application Master (YARN) / Cluster Manager
          • Client uploads app jars & staging (HDFS) then can safely disconnect
          • RM/AM take over lifecycle of the Driver
          |
          v
🚗 Driver (runs inside cluster)
   • Driver is launched inside AM container in cluster
   • Builds DAG, schedules tasks, tracks job state (inside cluster)
          |
          v
     🔄 Build DAG (stages & tasks)
          • Plan execution (stages, tasks, partitions)
          |
          v
🤝 Driver contacts Resource Manager (YARN / K8s / Mesos)
          • Driver requests executor containers/resources via RM
          |
          v
🖥️ Node Managers (across cluster)
   └─> 📦 Executors launched
          • Executors are long-lived JVMs on worker nodes
          • Launched with required jars/configs from staging
          |
          v
📂 Executors register with Driver (inside cluster)
          • Fast local network registration (no external client hop)
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
📡 Executors send status & results → Driver (in cluster)
          • Progress, metrics, task failures reported to Driver/AM/RM
          |
          v
✅ Job Completion
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