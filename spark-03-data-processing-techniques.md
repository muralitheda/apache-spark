# Full Stack Data Engineer (Multi-Cloud) Program: Transformation & Analytics (BB2)

This document details the core concepts, objectives, and implementation techniques for a **Full Stack Data Engineer (Multi-Cloud)** program, focusing on the module. Successfully completing this program transforms participants into **Data Curation Developers** and **Data Analysts**, with a strong focus on **TRANSFORMATION & ANALYTICS** using **Py-Spark-SQL**.

-----

## Program Objectives and Skill Progression

The curriculum is structured across three levels, moving from basic transformation logic to industrial-grade frameworks and cloud deployment.

| Level | Focus Area | Description |
| :---: | :--- | :--- |
| **Level 1** | **Transformation & Analytics** | Learn to apply transformations, processing, business logics, and conversions using **DSL (DataFrame API)** and **SQL (View)** within the Spark Framework. |
| **Level 2** | **Pipeline Creation** | Focus on creating end-to-end data pipelines by connecting with different sources and targets using various data processing techniques. |
| **Level 3** | **Standardization & Industrialization** | Master techniques to **standardize, modernize, and industrialize** code. This includes creating and consuming generic/reusable functions & frameworks, comprehensive **Testing** (Unit, Peer Review, SIT/Integration, Regression, UAT), **Masking**, **Reusable Transformation** (`munge_data`, `optimize_performance`), **Data Movement Automation (RPA)**, **Quality Suite/Audit Engine**, and **Data/Process Observability**. |
| **Beyond** | **Cloud & Deployment** | Covers key concepts in **Cloud Architecture**, job submission/monitoring/log analysis, application packaging and deployment, **Performance Tuning**, and deploying `mypyspark` applications in **Cloud & other Distributions** (e.g., Hortonworks, Cloudera, Databricks). |

-----

## Data Engineering & Analytics Pipeline Life Cycle

This content addresses essential Data Engineering (DE) interview topics by detailing the life cycle of ETL and Data Engineering & Analytics pipelines, starting with **Data Governance (Security)**, which involves Tagging, Categorization, Classification, and Masking/Filteration.

| Stage | Process Name | Description |
| :---: | :--- | :--- |
| **1.** | **Data Munging** | Transforming and mapping data from its **Raw** form into a **Tidy (usable)** format for subsequent processes (Enrichment, Egress, Analytics, Reporting). |
| **2.** | **Data Enrichment** | The process of making the data rich, detailed, and exhaustive by manipulating field values. |
| **3.** | **Data Customization & Processing** | Application of tailored Business-specific Rules, often through reusable functions. |
| **4.** | **Data Curation** | Transformation, Analysis/Analytics, and Summarization of data. |
| **5.** | **Data Wrangling** | A holistic process of Gathering, Enriching, and Transformation of pre-processed data into usable data. |
| **6.** | **Data Publishing & Consumption (LOAD)** | Enabling the cleansed, transformed, and analyzed data as a **Data Product** (Discovery, Outbound/Egress, Reports/exports, Schema migration). |

-----

## 1\. Data Munging: Detailed Steps

Data Munging is the cornerstone of preparing data for analysis.

### a. Passive Data Munging: Data Discovery (EDA)

This involves **Exploratory Data Analysis (EDA)** at every stage to identify attributes, patterns, and quality issues in the raw data.

1.  **Initial Understanding:** Read all attributes as strings to get row/column counts. Use `inferSchema` to determine natural datatypes.
2.  **Schema Application:** Apply a custom **`StructType`** based on initial analysis.
3.  **Identify Issues:** Check for **Nulls** (`na.drop`, `is null`), **Constraints** (`nullable=False`), **Duplicates** (`distinct`, `dropDuplicates()`), and **Datatype Mismatches** (using `rlike`, `regexp_replace`, `cast`).
4.  **Malformed Data:** Use **`columnNameOfCorruptRecord`** and **`mode="permissive"`** to capture and identify corrupt rows (Rejected Strategy/Audit).

#### PySpark Example: Permissive Read & Audit

```python
from pyspark.sql.session import *
from pyspark.sql.types import StructType,StructField,StringType,ShortType,LongType
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Important-Application").enableHiveSupport().getOrCreate()

# 3. Apply custom schema and Permissive Mode
strt=StructType([StructField("cid",LongType()),StructField("fname",StringType()),
                      StructField("lname",StringType()),StructField("age",ShortType()),
                      StructField("prof",StringType()),
                      StructField("corrupt_row",StringType())])
df1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=strt,
                   columnNameOfCorruptRecord="corrupt_row",mode="permissive")
df1.cache()

# Identify and write corrupted data
df1.filter("corrupt_row is not null").show(10,False)
df1.filter("corrupt_row is not null").coalesce(1).write.csv("file:///home/hduser/custsreject",mode="overwrite")
```

### b. Active Data Munging: Structurizing

This stage handles the physical structure of the data.

  * **Combining Data:** Reading multiple files/paths via **`pathGlobFilter`** or **`recursiveFileLookup=True`**.
  * **Schema Merging/Evolution:** Combining DataFrames with different structures. Use **`unionByName(..., allowMissingColumns=True)`** as the preferred Spark method to handle schema evolution by filling missing columns with nulls.

### c. Active Data Munging: Cleansing & Scrubbing

This involves Validation, Cleansing, and Scrubbing (Preprocessing/Preparation).

  * **Cleansing (Removal):**
      * Remove malformed records (`mode="dropMalformed"`).
      * Remove literal duplicate rows (`distinct()`) or prioritize key-level duplicates (`orderBy` + `dropDuplicates(subset=['key_col'])`).
      * Handle **Nulls** using **`na.drop()`** (`how='any'` or `how='all'`) or the **`thresh`** parameter for minimum non-null columns.
  * **Scrubbing (Repairing/Filling Gaps):**
      * Fill Nulls: Use **`na.fill(value, [columns])`**.
      * Replace Values: Use **`na.replace(old, new, subset=[columns])`**.

### d. Active Data Munging: Standardization

Making data attributes uniform and understandable.

  * **Add/Remove/Rename:** Use **`withColumn()`** or **`select("*", lit(...).alias("new_col"))`** to add derived/static columns. Use **`withColumnRenamed()`** and **`drop()`**.
  * **Uniform Values:** Standardize text using built-in functions like **`initcap(trim("column"))`**.
  * **Datatype Casting & Reordering:** Use **`cast("new_type")`** and **`select()`** to define the final structure and order.

-----

## 2\. Data Enrichment

Enrichment makes the data more detailed and exhaustive.

| Enrichment Type | Key DSL Functions | Note |
| :--- | :--- | :--- |
| **Add/Modify** | **`withColumn()`, `select()`** | Prefer **`select`** over chained `withColumn` calls to optimize performance and avoid generating large execution plans. |
| **Swapping** | `withColumnRenamed()` or `select()` with `col().alias()` | Interchange column names or values. |
| **Merge/Concat** | **`concat(col1, lit(' '), col2)`** | Join multiple columns (e.g., first and last name). |
| **Split** | **`split("column_name", 'delimiter')[index]`** | Break a single column value into multiple derived columns. |
| **Type Casting** | **`cast("date")`, `cast("string")`** | Convert the column's datatype. |
| **Reformatting/Extraction** | **`date_format()`, `year()`** | Change date format or extract date/timestamp components. |

-----

## 3\. Data Customization & Processing

This stage focuses on applying tailored business rules, often achieved by building **Frameworks** and using **User Defined Functions (UDFs)**.

  * **UDF Usage Caution:** Use UDFs only when absolutely necessary, as Spark treats them as a **black box** and cannot apply optimization. **Always prefer a built-in function** if one is available.
  * **UDF Creation:** A Python function must be converted/registered as a UDF to be serializable and usable across Spark executors.

#### PySpark Example: UDF Creation & Application

```python
# Inline Python Function for age categorization
def agecat(agecol):
    if agecol>=0 and agecol<=12:
        return 'Child'
    elif agecol>=13 and agecol<=19:
        return 'Teens'
    elif agecol>=20 and agecol<=50:
        return 'Middle Age'
    else:
        return 'Old Age'
        
# DSL Usage: Convert to UDF
from pyspark.sql.functions import udf
agecatudf=udf(agecat) 
# Apply the UDF
# df.withColumn("agecat", agecatudf("age"))

# SQL Usage: Register as UDF
# spark.udf.register("agecat_sql", agecat)
```

-----

## 4\. Data Curation/Transformation & Analytics

### Filtering and Conditional Logic

  * **Filtering:** Use **`.where()`** or **`.filter()`**.
  * **Conditional Logic:** The **`case` statement** is a vital function for deriving indicators, flags, or metrics based on multiple conditions. This often replaces simpler UDFs.
      * **SQL Syntax:** `case when condition then result else default_value end aliasname`
      * **DSL Syntax (Using `when` and `otherwise`):** `df.when("condition", expression).otherwise("default_value")`

### Grouping and Aggregation

  * **Grouping:** Use **`.groupBy(cols).agg(agg_func().alias(name))`** in DSL.
  * **Filtering Aggregated Results:** Use the **`HAVING`** clause in SQL or a **`where`** clause after aggregation in DSL.

### Wrangling and Advanced Techniques

  * **Joins:** Combine data horizontally (widening/denormalization).
      * **Key Types:** Inner, Right, Left, Full Outer, **Semi** (returns matching values from the left), **Anti** (returns *un-matching* values from the left, useful for identifying new/dormant keys).
  * **Windowing/Statistical & Analytical Processing:** Use **Window Functions** to perform calculations across a related set of rows.
      * Functions include **`row_number()`, `dense_rank()`, `rank()`, `lag()`, `lead()`, `first()`, `last()`**.
      * **Pattern:** `.withColumn("new_col", window_func().over(Window.partitionBy(col).orderBy(col)))`.
  * **Set Operations:** Combine data vertically.
      * **`unionByName()`**: Preferred for combining datasets with different schemas.
      * **`union()`**: Behaves like `union all` (retains duplicates). Use **`union().distinct()`** for a literal union.
      * **`intersect()`** and **`subtract()`**: Costly operations due to required shuffling.

-----

## 6\. Data Publishing & Consumption (LOAD)

The final stage where the curated data is enabled as a **Data Product**.

  * **a. Discovery Layer/Consumption/Publishing (Gold Layer):** Saving the final data for consumers.
    ```python
    df.write.saveAsTable("hive_consumer_table")
    ```
  * **b. Outbound/Egress:** Exporting data to external systems (e.g., JSON).
    ```python
    df.coalesce(1).write.json("file:///home/hduser/consumerdata", mode="overwrite")
    ```
  * **d. Schema Migration/Format Modeling:** Converting data to performance-optimized storage formats (e.g., ORC).
    ```python
    df.write.orc("file:///home/hduser/orcdata", mode="overwrite")
    ```