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


### Q2. What Happens When I Submit a Spark Job in Cluster Mode & Client Mode?

# ⚡ Spark Execution Flow — Cluster Mode vs Client Mode

```
⚡ Cluster Mode                                      💻 Client Mode
------------------------------------------------------------------------------------------------
🧑‍💻 Client (spark-submit)                          🧑‍💻 Client (spark-submit)
   └─> 📦 Application Master (YARN/K8s/Mesos)          └─> 🚗 Driver (runs on client machine)
       • Client uploads jars/configs to staging           • Driver is a local JVM:
       • RM/AM take over lifecycle of Driver                builds DAG, schedules tasks, tracks job state
       • Client can safely disconnect                     • Client must stay alive
       |
       v                                                  |
🚗 Driver (runs inside cluster)                           v
   • Builds DAG, schedules tasks, tracks job state    🔄 Build DAG (stages & tasks)
   • Runs inside AM container                            • Plan execution (stages, tasks, partitions)
       |
       v                                                  |
🔄 Build DAG (stages & tasks)                             v
   • Plan execution (stages, tasks, partitions)       🤝 Driver contacts Resource Manager
                                                        (YARN / K8s / Mesos)
       |                                                • Requests executor containers/resources
       v                                                • May upload jars/configs to HDFS staging
🤝 Driver contacts Resource Manager
   (YARN / K8s / Mesos)                                   |
   • Driver requests executor containers/resources        v
       |                                             🖥️ Node Managers (across cluster)
       v                                                └─> 📦 Executors launched
🖥️ Node Managers (across cluster)                          • Executors are JVMs on worker nodes
   └─> 📦 Executors launched                               • Launched with required jars/configs
       • JVMs on worker nodes
       • Launched with jars/configs from staging           |
       |                                                   v
       v                                             📂 Executors register with Driver (over network)
📂 Executors register with Driver (inside cluster)         • Executors open RPC/Netty connections
   • Fast local network registration                       • Heartbeats & registration info flow
       |
       v                                                   |
⚡ Driver schedules tasks → Executors run them              v
   • Driver assigns tasks based on locality/resources ⚡ Driver schedules tasks → Executors run them
   • Tasks execute in parallel on executors               • Driver assigns tasks based on locality/resources
                                                          • Tasks execute in parallel on executors
       |
       v                                                   |
🗄️ Executors process data (HDFS / external sources)        v
   • Read HDFS/DBs/S3, cache partitions in memory/disk 🗄️ Executors process data (HDFS / external sources)
   • Shuffle intermediate data                            • Read HDFS/DBs/S3, cache partitions in memory/disk
                                                          • Shuffle intermediate data
       |
       v                                                   |
📡 Executors send status & results → Driver                v
   (in cluster)                                      📡 Executors send status & results → Driver
   • Progress, metrics, failures reported                (on client)
   • Driver/AM may retry failed tasks                    • Progress, metrics, failures sent to Driver
                                                          • Driver may retry failed tasks / reschedule
       |
       v                                                   |
✅ Job Completion                                          v
   └─> Executors shut down                           ✅ Job Completion
   └─> Driver exits (in cluster)                        └─> Executors shut down
   └─> AM deregisters from RM                           └─> Driver exits (on client)
                                                         └─> Resources released by RM
```

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