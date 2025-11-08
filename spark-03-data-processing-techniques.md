
# 🧩 End-to-End Example: Transformation & Analytics using PySpark

This example covers the **entire lifecycle** — from reading raw data to publishing curated results, implementing every key concept listed in the program.

---

## ✅ Summary: Techniques Covered

| Category                 | Techniques Covered                                                                                                             |
| :----------------------- | :----------------------------------------------------------------------------------------------------------------------------- |
| **Munging**              | `mode="permissive"`, `columnNameOfCorruptRecord`, `unionByName`, `dropDuplicates`, `na.*`, `trim`, `initcap`, `regexp_replace` |
| **Enrichment**           | `concat_ws`, `split`, `to_date`, `year`, `month`, `withColumn`, `select`                                                       |
| **Customization**        | `udf`, `when`, `otherwise`, `cast`, `alias`                                                                                    |
| **Curation & Analytics** | `groupBy().agg()`, `rank`, `row_number`, `unionByName`, `subtract`                                                             |
| **Publishing**           | `saveAsTable`, `write.json`, `write.orc`, `write.parquet`, `compression`                                                       |

---


## 🪣 Stage 1 — Data Munging (Discovery → Structuring → Cleansing → Standardization)

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ShortType, LongType
from pyspark.sql.functions import (
    col, trim, initcap, lit, when, split, concat, current_timestamp,
    regexp_replace, year, month, date_format
)
from pyspark.sql.window import Window

# Step 1: Initialize Spark Session
spark = (SparkSession.builder
         .appName("FullStackDataEngineer_BB2")
         .enableHiveSupport()
         .getOrCreate())

# Step 2: Define custom schema
cust_schema = StructType([
    StructField("cid", LongType(), True),
    StructField("fname", StringType(), True),
    StructField("lname", StringType(), True),
    StructField("age", ShortType(), True),
    StructField("prof", StringType(), True),
    StructField("city", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("doj", StringType(), True),
    StructField("corrupt_row", StringType(), True)
])

# Step 3: Passive Data Munging — Permissive Read & Audit
df_raw = (spark.read
          .option("header", True)
          .option("mode", "permissive")
          .option("columnNameOfCorruptRecord", "corrupt_row")
          .schema(cust_schema)
          .csv("file:///home/hduser/sparkdata/customers"))

# Cache for reuse
df_raw.cache()

# Step 4: Identify and write corrupted data (Reject Handling)
df_reject = df_raw.filter(col("corrupt_row").isNotNull())
df_reject.write.mode("overwrite").csv("file:///home/hduser/rejects")

# Step 5: Active Munging — Structuring and Schema Merging
# Simulate multiple datasets
df_extra = (spark.read.option("header", True)
            .csv("file:///home/hduser/sparkdata/customers_new"))

df_combined = df_raw.unionByName(df_extra, allowMissingColumns=True)

# Step 6: Cleansing & Scrubbing
df_clean = (df_combined
            .dropDuplicates(["cid"])                # remove duplicate customer IDs
            .na.drop(subset=["cid", "fname"])       # drop null key rows
            .na.fill({"city": "Unknown", "salary": 0})
            .na.replace("N/A", None, subset=["prof"]))

# Step 7: Standardization
df_std = (df_clean
          .withColumn("fname", initcap(trim(col("fname"))))
          .withColumn("lname", initcap(trim(col("lname"))))
          .withColumn("city", initcap(trim(col("city"))))
          .withColumn("doj", regexp_replace("doj", "-", "/"))   # Uniform date format
          .withColumn("created_ts", current_timestamp())
          .select("cid", "fname", "lname", "prof", "age", "city", "salary", "doj", "created_ts"))
```

✅ **Covers:**

* `mode="permissive"` / `columnNameOfCorruptRecord`
* `unionByName(allowMissingColumns=True)`
* `dropDuplicates`, `na.drop`, `na.fill`, `na.replace`
* `trim`, `initcap`, `regexp_replace`, `withColumn`, `select`

---

## 🧬 Stage 2 — Data Enrichment

Add new columns, transformations, and derived fields.

```python
from pyspark.sql.functions import concat_ws, split, year, month, to_date

df_enriched = (df_std
               # Merge fields: full name
               .withColumn("fullname", concat_ws(" ", col("fname"), col("lname")))
               # Split job title into primary role (if composite like "Data Engineer-Lead")
               .withColumn("role_main", split(col("prof"), "-")[0])
               # Convert date string to date type and extract year/month
               .withColumn("doj_date", to_date(col("doj"), "yyyy/MM/dd"))
               .withColumn("join_year", year(col("doj_date")))
               .withColumn("join_month", month(col("doj_date"))))
```

✅ **Covers:**

* `concat_ws`, `split`, `to_date`, `year`, `month`
* Derived/modified columns

---

## ⚙️ Stage 3 — Data Customization & Business Rules (UDF / Conditional Logic)

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Define Python function
def age_category(age):
    if age is None:
        return "Unknown"
    elif age <= 12:
        return "Child"
    elif age <= 19:
        return "Teen"
    elif age <= 50:
        return "Adult"
    else:
        return "Senior"

# Register as UDF
agecat_udf = udf(age_category, StringType())
spark.udf.register("agecat_sql", age_category, StringType())

# Apply in DSL
df_custom = (df_enriched
             .withColumn("age_category", agecat_udf("age"))
             .withColumn("salary_band",
                         when(col("salary") < 30000, "Low")
                         .when(col("salary").between(30000, 60000), "Mid")
                         .otherwise("High")))
```

✅ **Covers:**

* `udf()` creation & registration
* Conditional logic using `when` / `otherwise`
* Derived categorization logic

---

## 📊 Stage 4 — Data Curation, Transformation & Analytics

Aggregation, Windowing, and Set Operations.

```python
from pyspark.sql.functions import avg, sum as _sum, count, row_number, rank, dense_rank
from pyspark.sql.window import Window

# Grouping & Aggregation
df_region_summary = (df_custom
                     .groupBy("city")
                     .agg(
                         count("*").alias("cust_count"),
                         avg("salary").alias("avg_salary"),
                         _sum("salary").alias("total_salary"))
                     .orderBy(col("total_salary").desc()))

# Window Function: Rank within each city by salary
window_spec = Window.partitionBy("city").orderBy(col("salary").desc())
df_ranked = (df_custom
             .withColumn("rank_in_city", rank().over(window_spec))
             .withColumn("row_num", row_number().over(window_spec)))

# Set Operations: union/intersect/subtract examples
df_union = df_custom.unionByName(df_extra, allowMissingColumns=True)
df_new_customers = df_custom.select("cid").subtract(df_extra.select("cid"))
```

✅ **Covers:**

* `groupBy().agg()`
* Window functions: `rank()`, `row_number()`
* `unionByName`, `subtract()`

---

## 🚀 Stage 5 — Data Publishing & Consumption (LOAD)

```python
# a. Hive Table (Discovery / Gold Layer)
df_ranked.write.mode("overwrite").saveAsTable("consumer.customer_curated")

# b. Outbound (JSON Export)
df_ranked.coalesce(1).write.mode("overwrite").json("file:///home/hduser/outbound/json/customers")

# c. Schema Migration / Optimized Format (ORC)
df_ranked.write.mode("overwrite").orc("file:///home/hduser/outbound/orc/customers")

# d. Parquet Export with Compression
df_ranked.write.mode("overwrite").option("compression", "snappy") \
    .parquet("file:///home/hduser/outbound/parquet/customers_snappy")
```

✅ **Covers:**

* `saveAsTable`, `json`, `orc`, `parquet`
* Partitioning, compression options
* Discovery → Outbound → Performance formats

---

