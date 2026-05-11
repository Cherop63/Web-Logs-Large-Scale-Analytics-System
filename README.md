# 🚀 Web-Logs-Large-Scale-Analytics-System

## 📌 Overview
This project implements a **real-time log analytics pipeline** using **Apache Kafka** and **Apache Spark**. It simulates streaming web log data, processes it efficiently, and extracts meaningful insights such as error rates and user activity patterns.

---

## 🎯 Problem Statement
Modern systems generate massive volumes of log data that must be processed in real time for:

- Monitoring system performance
- Detecting errors quickly
- Understanding user behavior

This project demonstrates how to build a **scalable pipeline** to handle such data efficiently.

---

## 🏗️ System Architecture

1. **Data Source**
   - Web log dataset (`weblog.csv`)

2. **Data Ingestion**
   - Kafka Producer streams log data in real time

3. **Stream Processing**
   - Apache Spark consumes and processes the stream

4. **Analytics**
   - Extract insights such as:
     - Error rates
     - Request patterns

---

## 📊 Dataset Description
The dataset contains simulated web logs with fields such as:

- IP Address
- Timestamp
- Request Type
- Status Code
- URL

---

## ⚙️ Methodology

### 1️⃣ Data Loading
The dataset is loaded using **Pandas**:

```python
df = pd.read_csv('weblog.csv')
