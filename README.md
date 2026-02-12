# Smart City Traffic & Congestion Management System 

## Real-Time Big Data Streaming Pipeline

### Project Overview

The Smart City Traffic & Congestion Management System is a real-time big data pipeline designed to simulate and analyze traffic conditions.
The system ingests live traffic sensor data, processes it using stream analytics, detects congestion events in real time, and generates daily analytical reports to support traffic management decisions.

This project demonstrates the practical use of **Kafka**, **Spark Structured Streaming**, **Airflow**, **PostgreSQL**, and **React** in a modern data engineering architecture.

### System Architecture

```bash
                    Traffic Sensors (Python)
                              |
                              v
                        Apache Kafka
                      (traffic topic)
                              |
                              v
                  Spark Structured Streaming
                    (Windowed Aggregations)
                              |
                    +--------------------+
                    |                    |
                    v                    v
            Kafka Alert Topic     PostgreSQL Database
            (critical-traffic)     (Historical Data)
                                          |
                                +--------------------+
                                |                    |
                                v                    v
                        Apache Airflow       Real-Time Dashboard
                        (Nightly Batch Job)        (React)
                                |
                                v
                    Daily Traffic Report (CSV)
```
### Technology Stack 

Data Ingestion	Apache Kafka	High-throughput, fault-tolerant ingestion of real-time sensor streams
Stream Processing	Apache Spark Structured Streaming	Supports windowing, scalable processing, and Kafka integration
Alerting	Kafka Topic (critical-traffic)	Decouples alert generation from alert consumption
Storage	 PostgreSQL for analytics
Orchestration	Apache Airflow	Industry-standard batch job scheduling
Visualization	React	Real-time dashboards using PostgreSQL as data source
Deployment	Docker Compose	Simplifies multi-service environment setup

### Project Structure

```bash
smart-city-traffic/
│
├── docker-compose.yml
│
├── kafka/
│   ├── traffic_producer.py
│   └── alert_consumer.py
│
├── spark/
│   └── traffic_streaming.py
│
├── airflow/
│   └── dags/
│       └── traffic_nightly_report.py
│
├── reports/
│   └── peak_traffic_report.csv
│
├── sql/
│   └── init.sql
│
└── README.md

```

### How to Run the Project
1. Start Infrastructure
```bash
   docker-compose up -d
```


2. Run Kafka Producer
```bash
   python kafka/sensor_producer.py
```

3. Run Spark Streaming Job

```bash
   spark-submit spark/traffic_streaming.py
```

5. Access Dashboards

React: http://localhost:5173

Airflow: http://localhost:8085