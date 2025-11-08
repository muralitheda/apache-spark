# Full Stack Data Engineer (Multi-Cloud) Program: Transformation & Analytics

[cite\_start]This document outlines the core concepts, objectives, and implementation details for a **Full Stack Data Engineer (Multi-Cloud)** program, specifically focusing on the content, which transforms participants into **Data Curation Developers & Data Analysts**[cite: 526]. [cite\_start]The focus is on **TRANSFORMATION & ANALYTICS** using **Py-Spark-SQL**[cite: 526].

-----

## Program Objectives (Levels 1-3)

The curriculum covers a progression of skills, from core transformation logic to industrial-grade frameworks:

| Level | Focus Area | Description |
| :---: | :--- | :--- |
| **Level 1** | **Transformation & Analytics** | [cite\_start]How to apply transformations/processing/business logics/functionality/conversion using **DSL (DataFrame)** and **SQL (View)**[cite: 527]. [cite\_start]This involves leveraging the Spark Framework and SQL[cite: 526]. |
| **Level 2** | **Pipeline Creation** | [cite\_start]How to create pipelines using different data processing techniques by connecting with different sources/targets[cite: 528]. |
| **Level 3** | **Standardization & Industrialization** | [cite\_start]How to standardize/modernize/industrialize the code and create/consume generic/reusable functions & frameworks[cite: 529]. [cite\_start]Includes **Testing** (Unit, Peer Review, SIT/Integration, Regression, UAT), **Masking**, **Reusable transformation** (`munge_data`, `optimize_performance`), **Data movement automation** (RPA), **Quality suite/Audit engine**, and **Data/process Observability**[cite: 529]. |
| **Beyond** | **Cloud & Deployment** | [cite\_start]How terminologies/architecture/submit jobs/monitor/log analysis/packaging and deployment works, **Performance Tuning**, and deploying `mypyspark` applications in **Cloud & other Distributions** (Hortonworks/Cloudera/Databricks)[cite: 530, 531]. |

-----

## Interview Focus & ETL Life Cycle

[cite\_start]This program's content is critical for explaining and solving common Data Engineering (DE) interview questions, covering almost all DE features, such as: common transformations performed, daily DE roles, recent business logic implementations, writing an entire `mypyspark` application, DE pipeline stages, and framework creation/usage[cite: 532, 533].

### Life Cycle of ETL and Data Engineering & Analytics Pipelines

[cite\_start]The starting point for all data processes is **Data Governance (Security)**, which includes Tagging, Categorization, Classification, and Masking/Filteration[cite: 533].

1.  [cite\_start]**Data Munging**: Transforming and mapping data from **Raw** form into a **Tidy (usable)** format for downstream purposes like Enrichment, Egress, analytics, and Reporting[cite: 533].
2.  [cite\_start]**Data Enrichment**: Making data rich and detailed[cite: 533].
3.  [cite\_start]**Data Customization & Processing**: Application of tailored Business-specific Rules[cite: 533].
4.  [cite\_start]**Data Curation**: Transformation, Analysis/Analytics, and Summarization[cite: 533].
5.  [cite\_start]**Data Wrangling**: Gathering, Enriching, and Transformation of pre-processed data into usable data[cite: 533].
6.  [cite\_start]**Data Publishing & Consumption (LOAD)**: Enabling the cleansed, transformed, and analyzed data as a **Data Product** (Discovery, Outbound/Egress, Reports/exports, Schema migration)[cite: 533, 541].

-----

## 1\. Data Munging: Deep Dive

[cite\_start]The process of transforming and mapping raw data into a tidy format is crucial for analytics and reporting[cite: 541].

### a. Passive Data Munging: Data Discovery (EDA)

[cite\_start]This involves performing **Exploratory Data Analysis (EDA)** at every layer (ingestion/transformation/analytics/consumption) to identify attributes (columns/datatype) and patterns (format/sequence/alpha/numeric) in the raw data[cite: 534, 541].

  * [cite\_start]**Understanding the Data:** Read all attributes as strings, calculate total rows/columns, and identify natural datatypes using `inferSchema`[cite: 541, 542, 546].
  * [cite\_start]**Schema Application:** Apply a custom `StructType` based on analysis[cite: 542, 546].
  * [cite\_start]**Identify Issues:** Nulls (`na.drop`, `is null`), Constraints (`nullable=False`), Duplicates (`distinct`, `dropDuplicates()`), Datatype mismatches (using `rlike`, `regexp_replace`, `cast`)[cite: 542, 546].
  * [cite\_start]**Malformed Data:** Use `columnNameOfCorruptRecord` and `mode="permissive"` to identify corrupt rows[cite: 548, 551].

#### PySpark Example: Passive EDA & Corrupt Data Handling

```python
from pyspark.sql.session import *
from pyspark.sql.types import StructType,StructField,StringType,ShortType,LongType
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("WE45-BB2-Important-Application").enableHiveSupport().getOrCreate()

# 1. Read data with all columns as string
df1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified")
print(df1.count()) # number of rows 10005 rows
print(len(df1.columns)) # number of columns 5 columns
df1.printSchema()

# 2. Read data with inferSchema to find natural datatypes
df1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",inferSchema=True)
print(df1.schema)

# 3. Apply custom schema and Permissive Mode
strt=StructType([StructField("cid",LongType()),StructField("fname",StringType()),
                      StructField("lname",StringType()),StructField("age",ShortType()),
                      StructField("prof",StringType()),
                      StructField("corrupt_row",StringType())])
df1=spark.read.csv("file:///home/hduser/sparkdata/custsmodified",schema=strt,
                   columnNameOfCorruptRecord="corrupt_row",mode="permissive")
[cite_start]df1.cache() # Caching is required to filter on the corrupt_row column [cite: 551]

# Identify corrupted data (Rejected Strategy/Audit)
[cite_start]df2_permissive.filter("corrupt_row is not null").show(10,False) # corrupted data [cite: 552]
print("corrupted data count",df2_permissive.filter("corrupt_row is not null").count())
[cite_start]df2_permissive.filter("corrupt_row is not null").coalesce(1).write.csv("file:///home/hduser/custsreject",mode="overwrite") [cite: 552]

# Summarize (Statistics)
[cite_start]df2_permissive.describe().show() [cite: 561]
```

### b. Active Data Munging: Structurizing

[cite\_start]This involves **Combining Data** and handling **Schema Evolution/Merging**[cite: 535, 543].

  * [cite\_start]**Combining Data:** Read multiple files/paths using a single `read.csv` call, often with `pathGlobFilter` or `recursiveFileLookup=True`[cite: 563, 564].
  * **Schema Merging/Evolution:** Combine DataFrames with different structures. [cite\_start]The preferred method in Spark is `unionByName(..., allowMissingColumns=True)` to handle differing column sets without manual intervention[cite: 565].

### c. Active Data Munging: Cleansing & Scrubbing

[cite\_start]This is the **Validation, Cleansing, Scrubbing** stage (Preprocessing/Preparation)[cite: 535, 544, 567].

  * **Cleansing (Removal):**
      * [cite\_start]**Level 1:** Remove malformed records using `mode="dropMalformed"`[cite: 566].
      * [cite\_start]**Level 2 & 3 (Duplicates):** Remove literal duplicate rows (`distinct()`) and use a prioritization strategy (e.g., `orderBy` + `dropDuplicates(subset=['key_col'])`) for key-level duplicates[cite: 559, 566].
      * [cite\_start]**Level 4 (Nulls):** Remove rows with nulls using `na.drop(how='any')` (default) or `na.drop(how='all', subset=[...])`[cite: 568]. [cite\_start]The `thresh` parameter can be used to control the minimum number of non-null columns required[cite: 569].
  * **Scrubbing (Repairing/Filling Gaps):**
      * [cite\_start]**Fill Nulls:** Use `na.fill(value, [columns])` to replace null values[cite: 544, 569].
      * [cite\_start]**Replace Values:** Use `na.replace(old, new, subset=[columns])` or a dictionary mapping to replace specific values in columns[cite: 544, 569].

### d. Active Data Munging: Standardization

[cite\_start]This is about making the data attributes uniform and understandable[cite: 536, 545, 570].

  * [cite\_start]**Add Columns:** Use `withColumn()` or `select("*", lit(...).alias("new_col"))` to add derived or static columns (e.g., `sourcesystem`)[cite: 572].
  * [cite\_start]**Rename Columns:** Use `withColumnRenamed("old_name", "new_name")`[cite: 573].
  * [cite\_start]**Remove Columns:** Use `drop("column_name")`[cite: 573].
  * [cite\_start]**Uniform Values (Case/Format):** Use built-in functions like `initcap(trim("column"))` to standardize text case[cite: 574].
  * [cite\_start]**Datatype Casting:** Use `col("col").cast("new_type")` to redefine datatypes[cite: 574].
  * [cite\_start]**Reordering:** Use `select()` to specify the final desired column order[cite: 574].

-----

## 2\. Data Enrichment

[cite\_start]The goal is to make your data richer, detailed, and exhaustive by manipulating field values[cite: 537, 576].

| Enrichment Type | DSL Function | Purpose |
| :--- | :--- | :--- |
| **Add/Modify** | `withColumn()`, `select()` | [cite\_start]Add derived columns (e.g., `load_dt`, `load_ts`)[cite: 578]. [cite\_start]*Note: Prefer `select` over chained `withColumn` calls for performance optimization to avoid generating large plans and `StackOverflowException`*[cite: 579, 580].|
| **Swapping** | Chained `withColumnRenamed()` or `select()` with `col().alias()` | [cite\_start]Interchange the names or values of columns[cite: 582]. |
| **Merge/Concat** | `concat(col1, lit(' '), col2)` | [cite\_start]Join multiple columns (e.g., `fname` and `lname` into `fullname`)[cite: 583]. |
| **Split** | `split("column_name", 'delimiter')[index]` | [cite\_start]Break a column value into multiple derived columns (e.g., split `fullname` into `fname` and `lname`)[cite: 584]. |
| **Type Casting** | `cast("date")`, `cast("string")` | [cite\_start]Convert the column's datatype[cite: 577, 586]. |
| **Reformatting/Extraction** | `date_format()`, `year()` | [cite\_start]Change a date column's display format or extract parts of the date/timestamp[cite: 586]. |

-----

## 3\. Data Customization & Processing

[cite\_start]This stage applies tailored business-specific rules, often achieved using **User Defined Functions (UDFs)** and building **Frameworks & Reusable Functions**[cite: 538].

  * **UDF Usage Caution:** UDFs should only be used when necessary, as Spark treats them as a **black box** and cannot apply optimization to them. [cite\_start]Use a built-in function whenever one is available[cite: 587].
  * [cite\_start]**UDF Creation:** A Python function must be converted/registered as a UDF to be serializable (distributed) and usable across Spark executors[cite: 410, 411, 412].

#### PySpark Example: Inline Python Function & UDF

```python
# Inline Python Function
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
[cite_start]agecatudf=udf(agecat) # Convert the python function to UDF [cite: 411]
# Apply the UDF on the dataframe column
# [cite_start]df_munged_enriched2_customized_final=df_reordered_final.withColumn("agecat",agecatudf("age")) [cite: 411]

# SQL Usage: Register as UDF
# [cite_start]spark.udf.register("agecat_sql", agecat) # Register the python function as UDF into the Metastore [cite: 412]
```

-----

## 4\. Data Curation/Transformation & Analytics

[cite\_start]This involves applying filters, transformations, grouping, and summarization to the data[cite: 539].

### Filtering & Transformation

  * [cite\_start]**Filtering:** Use `.where()` or `.filter()` with SQL-like syntax[cite: 418].
  * [cite\_start]**Case Statement (Conditional Logic):** The `case` statement is a vital function for deriving indicators, metrics, or flags based on conditions[cite: 419]. It can often replace simple UDFs.
      * [cite\_start]**SQL Syntax:** `case when condition then result else default_value end aliasname` [cite: 420]
      * [cite\_start]**DSL Syntax:** `df.when("condition", expression).otherwise("default_value")` [cite: 420]

### Grouping & Aggregation

  * [cite\_start]**SQL:** Use `GROUP BY` and aggregate functions like `avg()`, `sum()`, etc., often with a `HAVING` clause to filter the grouped result[cite: 428].
  * [cite\_start]**DSL:** Use `.groupBy(cols).agg(agg_func().alias(name))`[cite: 428]. [cite\_start]In DSL, the `where` clause is often used in place of `having` after aggregation[cite: 429].

### Wrangling and Advanced Techniques

  * [cite\_start]**Joins:** Used for creating a single view by combining data horizontally (widening/flattening/denormalization)[cite: 440].
      * [cite\_start]**Types:** Inner, Right, Left, Full Outer, Semi (returns matching values from the left table only), Anti (returns un-matching values from the left table only, useful for identifying new/dormant customers)[cite: 453, 462, 472, 474].
      * [cite\_start]**Syntax:** `df1.join(df2, how='inner', on='common_col')`[cite: 453].
  * [cite\_start]**Windowing/Statistical & Analytical Processing:** Use **Window Functions** to perform calculations across a set of table rows that are related to the current row[cite: 488].
      * [cite\_start]Functions include `row_number()`, `dense_rank()`, `rank()`, `lag()`, `lead()`, `first()`, `last()`[cite: 488, 508].
      * [cite\_start]**Pattern:** `.withColumn("new_col", window_func().over(Window.partitionBy(col).orderBy(col)))`[cite: 488].
  * [cite\_start]**Set Operations:** Combine data vertically from two or more data sets[cite: 510].
      * [cite\_start]`unionByName()`: Combines datasets with different schemas, filling missing columns with nulls[cite: 511].
      * [cite\_start]`union()`: Standard union, often behaving like `union all` (retaining duplicates)[cite: 511]. [cite\_start]Use `union().distinct()` to achieve a true `union` (eliminating duplicates)[cite: 511].
      * [cite\_start]`intersect()`, `subtract()`: Identify common data or differences (costly operations due to shuffling)[cite: 511].

-----

## 6\. Data Publishing & Consumption (LOAD)

[cite\_start]This is the final step where the cleansed, transformed, and analyzed data is enabled as a **Data Product**[cite: 541].

  * **a. [cite\_start]Discovery Layer/Consumption/Publishing (Gold Layer):** Data is made available, often by saving to a Hive table[cite: 541].
    ```python
    df.write.saveAsTable("hive_consumer_table")
    ```
  * **b. [cite\_start]Outbound/Egress:** Exporting data to external systems, often in formats like JSON[cite: 541].
    ```python
    df.coalesce(1).write.json("file:///home/hduser/consumerdata", mode="overwrite")
    ```
  * **d. [cite\_start]Schema Migration/Format Modeling:** Converting data into performance-optimized storage formats like ORC[cite: 541].
    ```python
    df.write.orc("file:///home/hduser/orcdata", mode="overwrite")
    ```