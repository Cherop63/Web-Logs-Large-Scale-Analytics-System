# 🚀 Web-Logs-Large-Scale-Analytics-System

A real-time log analytics pipeline built with **Apache Kafka**, **Apache Spark Structured Streaming**, and **Python**. The system ingests raw web server logs, streams them through a distributed messaging layer, and processes them with live aggregations and enrichment.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Data Specification](#data-specification)
- [Pipeline Stages](#pipeline-stages)
- [Setup & Requirements](#setup--requirements)
- [Usage](#usage)
- [Output](#output)

---

## Overview

This system implements a **Lambda-style streaming pipeline** that:

1. Reads raw web server logs from `weblog.csv`
2. Streams records into a Kafka topic using an optimized producer
3. Consumes and parses the stream with Spark Structured Streaming
4. Enriches and aggregates log data in real time
5. Outputs results to the console (extensible to dashboards or databases)

---

## Architecture

```
[ weblog.csv ]
      │
      ▼
[ Kafka Producer (Python) ]
  - Batching (linger_ms, batch_size)
  - Compression (lz4)
  - Acknowledgements (acks=1)
      │
      ▼
[ Kafka Topic: weblogs ]
  - Distributed log broker
  - Durability & scalability
      │
      ▼
[ Spark Structured Streaming Consumer ]
  - readStream from Kafka
  - Binary → JSON → Structured columns
  - Schema enforcement
      │
      ▼
[ Processing Layer ]
  - Enrichment: Date, Hour, IsError
  - Aggregations: Traffic per endpoint,
    Hourly traffic, Error rates
      │
      ▼
[ Output Layer ]
  - Console (writeStream)
  - Extensible: Dashboards, Databases,
    Monitoring Systems
```

### Layer Breakdown

| Layer | Component | Role |
|---|---|---|
| Producer | Python `KafkaProducer` | Streams weblog records into Kafka |
| Messaging | Apache Kafka | Distributed log broker; durability & scalability |
| Consumer | Spark Structured Streaming | Subscribes to Kafka topic; parses JSON payloads |
| Processing | Spark Transformations | Log enrichment and real-time aggregations |
| Output | Console / External Sinks | Displays or stores processed results |

---

## Data Specification

### Source Dataset

**File:** `weblog.csv` — a structured web server log file containing continuous HTTP event records.

### Schema

| Field | Type | Description |
|---|---|---|
| `Timestamp` | `string → timestamp` | Event datetime |
| `IP` | `string` | Client IP address |
| `Endpoint` | `string` | Requested resource/URL path |
| `Status` | `integer` | HTTP status code (e.g., 200, 404, 500) |
| `ResponseTime` | `integer` | Request latency in milliseconds |

### Derived Fields (added during processing)

| Field | Description |
|---|---|
| `Date` | Extracted date from `Timestamp` |
| `Hour` | Extracted hour from `Timestamp` |
| `IsError` | Boolean flag (`Status >= 400`) |

### Data Characteristics

- **Velocity** — Continuous stream of log events
- **Variety** — Multiple endpoints and HTTP status codes
- **Volume** — Designed to scale to millions of records

---

## Pipeline Stages

### 1. Data Ingestion

**Kafka Producer**
- Reads `weblog.csv` with Pandas
- Streams each record as a message into the `weblogs` Kafka topic
- Optimizations:
  - `linger_ms` and `batch_size` for batching
  - `lz4` compression
  - `acks=1` for balanced durability and throughput

**Kafka Consumer (Spark)**
- Spark session subscribes to the `weblogs` topic
- Uses `readStream` for continuous consumption
- Converts binary Kafka payloads to JSON strings
- Parses JSON into structured DataFrame columns using an enforced schema

### 2. Processing & Enrichment

- Adds `Date` and `Hour` columns extracted from `Timestamp`
- Adds `IsError` flag based on HTTP status code
- Computes aggregations:
  - **Traffic per endpoint** — request counts grouped by `Endpoint`
  - **Hourly traffic** — request counts grouped by `Hour`
  - **Error rates** — proportion of requests where `IsError = true`

### 3. Output

- Results written to the **console** in real time via `writeStream`
- Architecture is extensible to:
  - BI dashboards (e.g., Grafana, Kibana)
  - Databases (e.g., PostgreSQL, Cassandra)
  - Alerting / monitoring systems

---

## Setup & Requirements

### Prerequisites

- Python 3.8+
- Apache Kafka (local or remote cluster)
- Apache Spark 3.x with `pyspark`
- Java 8 or 11 (required by Spark)

### Python Dependencies

```bash
pip install pandas kafka-python pyspark
```

### Kafka Setup

Start Zookeeper and a Kafka broker, then create the topic:

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties

# Create the topic
bin/kafka-topics.sh --create --topic weblogs --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

---

## Usage

### 1. Run the Kafka Producer

```bash
python producer.py --input weblog.csv --topic weblogs --bootstrap-server localhost:9092
```

### 2. Start the Spark Streaming Consumer

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
  consumer.py
```

Or launch the full pipeline from the Jupyter notebook:

```bash
jupyter notebook Large-Scale_Log_Analytics_System.ipynb
```

---

## Output

The pipeline continuously emits aggregated results to the console, for example:

```
+--------------------+-------+
|Endpoint            |Traffic|
+--------------------+-------+
|/api/v1/users       |  4821 |
|/api/v1/products    |  3107 |
|/health             |   892 |
+--------------------+-------+

+----+-------+----------+
|Hour|Traffic|ErrorCount|
+----+-------+----------+
|  9 |  1243 |       87 |
| 10 |  1892 |      112 |
| 11 |  2034 |       95 |
+----+-------+----------+
```

Results can be redirected to any sink supported by Spark Structured Streaming (Kafka, JDBC, files, etc.).

---

## Extending the System

- **Add sinks** — replace or augment `writeStream` with JDBC, Kafka, or file sinks
- **Add alerts** — trigger notifications when error rates exceed a threshold
- **Add dashboards** — connect output to Grafana or a custom web UI
- **Scale horizontally** — increase Kafka partitions and Spark executor count to handle higher throughput
