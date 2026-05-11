# 🚀 Web-Logs-Large-Scale-Analytics-System

**SDS 2412 — Analysis of Large Datasets | Group 3**

> An end-to-end distributed log analytics pipeline built over six milestones, covering batch processing, real-time streaming, machine learning, and deployment orchestration on web server access logs.

-----

## Table of Contents

- [Project Overview](#project-overview)
- [Team](#team)
- [Dataset](#dataset)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Milestones](#milestones)
- [Model Performance](#model-performance)
- [Known Issues & Fixes](#known-issues--fixes)
- [Requirements Checklist](#requirements-checklist)
- [Setup & Installation](#setup--installation)

-----

## Project Overview

This system ingests, processes, streams, and learns from web server access logs (`weblogs.csv`). It is designed around the **Lambda Architecture** — combining a batch layer for historical analysis and a speed layer for real-time anomaly detection, both feeding into a shared serving layer.

**Core objectives:**

- Detect anomalies and suspicious traffic patterns
- Measure traffic volume, endpoint frequency, and error rates
- Monitor system health via real-time streaming
- Train and deploy an ML model for threat classification

-----

## Team

**Group 3 — JKUAT**

|Name               |Student ID           |
|-------------------|---------------------|
|Joy Muthoni        |—                    |
|Christine Nyokabi  |—                    |
|Shawn Kimani       |—                    |
|Borneventure Kinoti|—                    |
|Joy Cheptoo Chesire|SCT213-C002-0087/2022|
|Angel Wangari      |—                    |

**Supervisor:** Samuel Adhola

-----

## Dataset

**File:** `weblogs.csv`  
**Records:** 16,007 rows

|Field         |Type                  |Description                              |
|--------------|----------------------|-----------------------------------------|
|`Timestamp`   |String → TimestampType|ISO 8601 datetime of the HTTP request    |
|`IP`          |StringType            |Source IP address of the client          |
|`Endpoint`    |StringType            |Requested URL path (e.g. `/api/v1/login`)|
|`Status`      |IntegerType           |HTTP response code (200, 404, 500 …)     |
|`ResponseTime`|IntegerType           |Server response latency in milliseconds  |


> **Note:** The raw CSV contains a column header typo — `Staus` instead of `Status`. This is corrected in the pipeline via `df.rename(columns={"Staus": "Status"})` before creating the Spark DataFrame.

-----

## Architecture

The system follows a **Lambda Architecture** with three layers:

```
weblogs.csv
     │
     ├──────────────► [ Kafka Producer ]  linger_ms=20  batch=64KB  lz4  acks=1
     │                        │
     │               [ Kafka Topic: weblogs ]
     │                   ┌────┴────┐
     │                   │         │
  BATCH LAYER       SPEED LAYER (Streaming)
     │                   │
  [ PySpark ]        [ Spark Structured Streaming ]
  Read CSV           readStream from Kafka
  Schema enforce     Parse JSON → StructType
  Clean & enrich     Watermark / micro-batch
  Aggregate          Real-time analytics
     │                   │
     └─────────┬─────────┘
               │
         SERVING LAYER
         Parquet (output/weblogs_clean/)
         MongoDB (weblog_system.predictions)
         Console sinks for streaming views
```

### Scalability (Big-O Analysis)

|Operation            |Complexity    |Notes                         |
|---------------------|--------------|------------------------------|
|CSV read (full scan) |O(N)          |N = total log records         |
|`dropna` cleaning    |O(N)          |Single pass per partition     |
|`groupBy` aggregation|O(N log N)    |Shuffle sort by key           |
|Kafka producer send  |O(1) amortised|Batched with `linger_ms`      |
|Streaming micro-batch|O(B)          |B = records per batch interval|
|Parquet write        |O(N)          |Columnar, partition-parallel  |

-----

## Technology Stack

|Category            |Technology                           |
|--------------------|-------------------------------------|
|Language            |Python 3.x                           |
|Batch Processing    |PySpark (`local[*]`)                 |
|Streaming           |Spark Structured Streaming           |
|Message Broker      |Apache Kafka 2.3.1 (`kafka-python`)  |
|Distributed Storage |Hadoop (HDFS / local), Parquet       |
|Database            |MongoDB (`weblog_system.predictions`)|
|ML Library          |Spark MLlib (Logistic Regression)    |
|Compression         |LZ4                                  |
|Supporting Libraries|pandas, logging                      |

-----

## Milestones

### M1 — Data Foundations & System Architecture *(Weeks 1–3)*

- Defined the problem and characterised data by volume, velocity, and variety
- Designed the Lambda Architecture (batch + speed + serving layers)
- Set up environment variables for PySpark / Hadoop on Windows
- Documented Big-O complexity for all pipeline operations

### M2 — Distributed Batch Processing Pipeline *(Weeks 4–6)*

- Initialised SparkSession with `local[*]` and `shuffle.partitions = 4`
- Applied explicit schema enforcement on CSV ingestion
- Cleaned data (`dropna`), parsed timestamps, and derived `Date`, `Hour`, and `IsError` columns
- Ran `groupBy` aggregations (status code distribution, top endpoints, hourly traffic)
- Persisted cleaned output to Parquet with `repartition(4)`

### M3 — Streaming & Real-Time Systems *(Weeks 7–9)*

- Built an optimised Kafka producer (`linger_ms=20`, `batch_size=65536`, LZ4, `acks=1`)
- Implemented a Spark Structured Streaming consumer reading from the `weblogs` topic
- Ran three concurrent `writeStream` queries for real-time analytics
- Documented approximate algorithms: Bloom Filter (IP deduplication) and Count-Min Sketch (top-K URLs)
- Compared batch vs streaming on latency, throughput, fault tolerance, and state management

### M4 — Scalable Machine Learning & Analytics *(Weeks 10–12)*

- Engineered 6 binary features from URL and endpoint patterns:
  - `url_length`, `time_length`, `has_admin`, `has_login`, `has_php`, `very_long_url`
- Trained a Logistic Regression model via Spark MLlib (`Pipeline.fit()`, `maxIter=20`)
- Evaluated on an 80/20 train-test split
- Extracted feature coefficients for basic explainability (`has_admin` strongest at +1.38)

### M5 — System Optimisation & Deployment *(Weeks 13–14)*

- Built a full `run_pipeline()` orchestration function with real Spark operations (not stubs)
- Applied `.cache()` on `train_df`, `test_df`, and `predictions` to reduce recomputation
- Added Python `logging` module with file output and timestamp tracking
- Integrated MongoDB writes (`collection.insert_many()`) for prediction persistence
- Implemented anomaly rate monitoring with a configurable threshold (40%)

### M6 — Integrated Intelligent System & Capstone *(Week 15)*

- End-to-end pipeline: CSV → Spark → feature engineering → model training → evaluation → MongoDB storage
- Added a probability-weighted threat scoring UDF on the Logistic Regression probability vector
- Final analytical outputs: status code distribution, top-URL frequency, top-threat IP ranking
- System evaluation report generated with all metrics

-----

## Model Performance

|Metric           |Value                            |
|-----------------|---------------------------------|
|Algorithm        |Logistic Regression (Spark MLlib)|
|Training records |~12,806 (80%)                    |
|Test records     |~3,201 (20%)                     |
|**Accuracy**     |**98.59%**                       |
|**AUC**          |**0.9476**                       |
|Strongest feature|`has_admin` (coefficient +1.38)  |

-----

## Known Issues & Fixes

Six documented failures encountered and resolved during development:

|#|Failure                                                                     |Milestone               |Fix                                                                    |
|-|----------------------------------------------------------------------------|------------------------|-----------------------------------------------------------------------|
|1|`NativeIO` / `winutils.exe` IOException (Hadoop on Windows)                 |M2 · Batch Pipeline     |Set `HADOOP_HOME` and `PATH` **before** `SparkSession` creation        |
|2|`SyntaxError` on section divider `---...---`                                |M3 · Streaming Consumer |Added missing `#` prefix to make it a Python comment                   |
|3|`ModuleNotFoundError: No module named 'kafka'`                              |M3 · Kafka Producer     |Ran `pip install kafka_python` in the active conda environment         |
|4|`AnalysisException: Resolved attribute Status missing` (column typo `Staus`)|M4 · Feature Engineering|Renamed column in pandas **before** creating Spark DataFrame           |
|5|`GBTClassifier` fails on small partitions with sparse labels                |M4 · Model Selection    |Switched to `LogisticRegression`; used full 16,007-row dataset         |
|6|`run_pipeline()` contained only `print()` stubs — no real execution         |M5/M6 · Orchestration   |Replaced stubs with actual Spark operations, model calls, and DB writes|

-----

## Requirements Checklist

All 25 milestone requirements are satisfied ✓

<details>
<summary><strong>M1 — Data Foundations & System Architecture</strong></summary>

- ✓ Problem definition and data characterisation (volume, velocity, variety)
- ✓ Data sourcing and ingestion strategy (CSV → Spark, Kafka producer)
- ✓ Complexity and scalability analysis (Big-O table documented)
- ✓ System architecture design (Lambda — batch + speed + serving layers)
- ✓ Initial batch data pipeline (SparkSession + CSV read + schema enforcement)

</details>

<details>
<summary><strong>M2 — Distributed Data Processing</strong></summary>

- ✓ Data partitioning (`repartition(4)`, shuffle partitions = 4, `local[*]`)
- ✓ Batch processing (PySpark `groupBy` / `agg` — MapReduce equivalent)
- ✓ Distributed storage (Parquet write, HDFS-compatible path)
- ✓ Fault tolerance (overwrite mode, Hadoop FileOutputCommitter fix)
- ✓ Job scheduling / execution pipeline (notebook cell ordering + `run_pipeline()`)

</details>

<details>
<summary><strong>M3 — Streaming & Real-Time Systems</strong></summary>

- ✓ Streaming ingestion (optimised Kafka producer — `linger_ms`, `batch_size`, LZ4)
- ✓ Event-driven architecture (Kafka topic `weblogs`, consumer group offsets)
- ✓ Real-time analytics pipeline (Spark Structured Streaming + 3 `writeStream` queries)
- ✓ Approximate algorithms documented (Bloom Filter for IP dedup, Count-Min Sketch for top-K)
- ✓ Batch vs streaming comparison table (latency, throughput, fault tolerance, state)

</details>

<details>
<summary><strong>M4 — Scalable Machine Learning & Analytics</strong></summary>

- ✓ Feature engineering on large datasets (6 features engineered)
- ✓ Model training (Logistic Regression via Spark MLlib, `maxIter=20`)
- ✓ Distributed / parallel training (`local[*]` — all CPU cores, `Pipeline.fit()`)
- ✓ Model validation and evaluation (80/20 split, 98.59% accuracy, 0.9476 AUC, confusion matrix)
- ✓ Basic explainability (logistic regression coefficients extracted)

</details>

<details>
<summary><strong>M5 — System Optimisation & Deployment</strong></summary>

- ✓ Model deployment pipeline (`run_pipeline()` with real function calls, logging, DB write)
- ✓ Monitoring and drift detection (anomaly rate threshold 40%, Python logging to file)
- ✓ Pipeline optimisation (`repartition(4)`, `.cache()` on train/test/predictions)
- ✓ Integration with NoSQL systems (MongoDB — `weblog_system.predictions` collection)
- ✓ Workflow orchestration (`run_pipeline()` with 6 real steps + logging timestamps)

</details>

<details>
<summary><strong>M6 — Integrated Intelligent System & Capstone</strong></summary>

- ✓ Integration of batch + ML components (full pipeline: CSV → Spark → features → model → predictions → MongoDB)
- ✓ End-to-end pipeline (ingestion → feature engineering → training → evaluation → monitoring → storage)
- ✓ System-level innovation (probability-weighted threat scoring via UDF on LR probability vector)
- ✓ Analytical outputs and interpretation (status code distribution, top-URL frequency, top-threat IP ranking)
- ✓ System evaluation (final report: 16,007 records, 98.59% accuracy, 0.9476 AUC, deployment-ready)

</details>

-----

## Setup & Installation

### Prerequisites

|Requirement     |Version            |
|----------------|-------------------|
|Python          |3.x                |
|Java (JDK)      |17                 |
|Apache Kafka    |2.3.1+             |
|Hadoop (Windows)|with `winutils.exe`|
|MongoDB         |Any recent version |

### Environment Variables (Windows)

Set these **before** any Spark imports:

```python
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"]        += os.pathsep + "C:\\hadoop\\bin"
os.environ["JAVA_HOME"]   = "C:\\Program Files\\Java\\jdk-17"
```

### Python Dependencies

```bash
pip install pyspark kafka-python pymongo pandas
```

### SparkSession Initialisation

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("WebLogBatchAnalytics") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
```

### Running the Pipeline

```python
# Full end-to-end execution
run_pipeline()
```

This executes all six steps in sequence: data loading → cleaning → feature engineering → model training → evaluation → MongoDB persistence.

-----

*SDS 2412 · Analysis of Large Datasets | Jomo Kenyatta University of Agriculture and Technology*
