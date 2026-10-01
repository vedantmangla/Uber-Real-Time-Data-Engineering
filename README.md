# **UBER REAL-TIME DATA ENGINEERING PROJECT**


# 🚗 Uber Real-Time Data Engineering Pipeline

An end-to-end **real-time data engineering pipeline** designed to ingest, process, transform, and model Uber ride events using **Azure Event Hubs, Azure Data Factory, Databricks, PySpark Structured Streaming, and Delta Lake**.

The project demonstrates a modern **lakehouse architecture** with Bronze, Silver, and Gold layers, along with downstream analytical modelling using a **star schema**.

---

## 🏗️ Architecture

```text
                ┌──────────────────────┐
                │   Uber Ride Events   │
                │  Python Event/API    │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │    Azure Event Hubs  │
                │   Streaming Ingestion│
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │      Databricks      │
                │ PySpark Structured   │
                │      Streaming       │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │    Bronze Layer      │
                │    Raw Events        │
                │      Delta           │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │    Silver Layer      │
                │ Cleaned & Transformed│
                │       Data           │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │     Gold Layer       │
                │ Business-ready Data │
                │      & Models        │
                └──────────┬───────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │    Star Schema       │
                │ Analytics-ready Data │
                └──────────────────────┘
```

---

## 🎯 Project Objectives

* Build an end-to-end **real-time data pipeline** for ride events.
* Ingest streaming events using **Azure Event Hubs**.
* Process streaming data using **PySpark Structured Streaming**.
* Implement a **Bronze → Silver → Gold Medallion Architecture**.
* Store processed datasets using **Delta Lake**.
* Apply data transformation and quality checks.
* Build analytics-ready datasets using a **star schema**.
* Demonstrate how streaming data can be transformed into structured analytical data.

---

## 🛠️ Technology Stack

| Technology                     | Purpose                                            |
| ------------------------------ | -------------------------------------------------- |
| **Python**                     | Event generation and data processing               |
| **Azure Event Hubs**           | Real-time event ingestion                          |
| **Azure Data Factory**         | Pipeline orchestration                             |
| **Databricks**                 | Data engineering and processing                    |
| **PySpark**                    | Distributed data processing                        |
| **Spark Structured Streaming** | Real-time stream processing                        |
| **Delta Lake**                 | Reliable storage and transactional data processing |
| **SQL**                        | Data transformation and analytical queries         |
| **Git/GitHub**                 | Version control                                    |

---

## 🔄 Data Pipeline

### 1. Event Generation

Ride events are generated in a structured format representing Uber trip activity.

Example event:

```json
{
  "ride_id": "R10001",
  "timestamp": "2026-10-01T10:15:00",
  "customer_id": "C501",
  "driver_id": "D102",
  "pickup_location": "Gurgaon",
  "drop_location": "Noida",
  "fare_amount": 425,
  "status": "completed"
}
```

These events represent the type of data that would continuously enter a real-time ride platform.

---

### 2. Streaming Ingestion

Events are published to **Azure Event Hubs**, which acts as the streaming ingestion layer.

```text
Event Generator
      ↓
Azure Event Hubs
      ↓
Databricks
```

Event Hubs provides the buffer between event producers and downstream processing.

---

### 3. Bronze Layer

The Bronze layer stores the incoming data with minimal transformation.

Purpose:

* Preserve raw data
* Maintain ingestion history
* Provide a recoverable source for downstream processing

```text
Event Hubs → Bronze Delta
```

---

### 4. Silver Layer

The Silver layer performs data cleaning and transformation.

Typical operations include:

* Data type conversion
* Null handling
* Data validation
* Duplicate handling
* Standardization
* Business-rule transformations

```text
Bronze
   ↓
Cleaning
   ↓
Validation
   ↓
Silver
```

---

### 5. Gold Layer

The Gold layer contains business-ready datasets designed for analytics.

Examples of possible analytical metrics:

* Total rides
* Total revenue
* Average fare
* Completed vs cancelled rides
* Location-wise demand
* Driver performance
* Ride volume over time

```text
Silver
   ↓
Business Transformations
   ↓
Gold
```

---

## 🧱 Medallion Architecture

The project follows the **Medallion Architecture**:

### 🥉 Bronze

Raw event data.

**Goal:** Preserve the source data.

### 🥈 Silver

Cleaned and validated data.

**Goal:** Create reliable, standardized datasets.

### 🥇 Gold

Business-ready analytical data.

**Goal:** Serve dashboards, analytics, and downstream applications.

---

## 📊 Data Modelling

The processed data is organized into an analytical **star schema**.

Example:

```text
                 ┌─────────────────┐
                 │  Dim Customer   │
                 └────────┬────────┘
                          │
                          │
┌───────────────┐   ┌─────▼──────┐   ┌────────────────┐
│ Dim Driver    │───│ Fact Rides │───│ Dim Location   │
└───────────────┘   └─────┬──────┘   └────────────────┘
                          │
                          │
                  ┌───────▼────────┐
                  │   Dim Date     │
                  └────────────────┘
```

### Fact Table

Contains measurable ride-level metrics such as:

* Fare amount
* Distance
* Ride duration
* Ride count

### Dimension Tables

Provide descriptive attributes such as:

* Customer
* Driver
* Location
* Date

---

## ⚙️ Key Data Engineering Concepts

This project demonstrates several important Data Engineering concepts:

* Real-time data ingestion
* Streaming data processing
* PySpark
* Distributed processing
* Medallion Architecture
* Delta Lake
* Data transformation
* Data quality
* Incremental processing
* Data modelling
* Star schema
* Pipeline orchestration

---

## 📁 Project Structure

```text
Uber-Real-Time-Data-Engineering/
│
├── data/
│   └── sample_data/
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── producer/
│   ├── streaming/
│   └── transformations/
│
├── pipelines/
│
├── sql/
│
├── docs/
│
├── requirements.txt
│
└── README.md
```

> Update this structure to match the actual folders in your implementation.

---

## 🚀 How the Pipeline Works

The complete flow can be summarized as:

```text
Ride Events
     ↓
Event Generation
     ↓
Azure Event Hubs
     ↓
Spark Structured Streaming
     ↓
Bronze Delta
     ↓
Data Cleaning & Validation
     ↓
Silver Delta
     ↓
Business Transformations
     ↓
Gold Delta
     ↓
Star Schema
     ↓
Analytics
```

---

## 🔍 Engineering Considerations

For a production-oriented implementation, the pipeline can be extended with:

* Checkpointing for streaming recovery
* Watermarking for late-arriving events
* Deduplication using event/ride identifiers
* Data-quality monitoring
* Pipeline failure handling and retries
* Incremental processing
* Structured logging
* Monitoring and alerting

These features help make streaming pipelines more reliable and production-ready.

---

## 📌 Key Learnings

Through this project, the main concepts explored are:

1. How real-time events move through a data pipeline.
2. How Event Hubs can be used for streaming ingestion.
3. How Spark Structured Streaming processes continuous data.
4. How Medallion Architecture organizes a lakehouse.
5. How Delta Lake supports reliable analytical storage.
6. How raw streaming data can be transformed into business-ready datasets.
7. How star schemas support analytical workloads.

---

## 👨‍💻 Author

**Vedant Mangla**

B.Tech — Mathematics & Computing
Delhi Technological University

---

## ⭐ Future Improvements

* Add real-time Power BI/Databricks dashboards.
* Implement advanced watermarking and late-event handling.
* Add automated data-quality monitoring.
* Introduce CI/CD for pipeline deployment.
* Add pipeline observability and alerting.
* Add automated testing for transformations.




