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

#### 1. Start All Services

Start the complete infrastructure using Docker Compose.

```bash
docker-compose up -d
```

This will start:

- PostgreSQL
- Zookeeper
- Kafka
- Spark Master & Worker
- Apache Airflow

---

#### 2. Create Kafka Topics

Create the required Kafka topics for traffic streaming and congestion alerts.

```bash
docker exec -it kafka kafka-topics \
--bootstrap-server kafka:9093 \
--create \
--topic traffic-data \
--partitions 4 \
--replication-factor 1
```

```bash
docker exec -it kafka kafka-topics \
--bootstrap-server kafka:9093 \
--create \
--topic critical-traffic \
--partitions 4 \
--replication-factor 1
```

Verify created topics:

```bash
docker exec -it kafka kafka-topics \
--bootstrap-server kafka:9093 \
--list
```

---

#### 3. Start Traffic Sensor Producer

Run the Python-based IoT traffic sensor simulator.

```bash
python kafka/sensor_producer.py
```

This continuously publishes real-time traffic data to the `traffic-data` Kafka topic.

---

#### 4. Run Spark Structured Streaming Job

Submit the Spark streaming application to the Spark cluster.

```bash
docker exec -it spark-master \
/opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.postgresql:postgresql:42.6.0 \
--conf spark.driver.extraJavaOptions="-Duser.home=/tmp" \
--conf spark.executor.extraJavaOptions="-Duser.home=/tmp" \
/opt/spark-jobs/spark_streaming.py
```

The streaming application:

- Consumes traffic data from Kafka
- Performs window-based analytics
- Detects congestion events
- Stores processed data in PostgreSQL
- Sends critical alerts to Kafka

---

#### 5. Configure Apache Airflow PostgreSQL Connection

Open Airflow UI:

```text
http://localhost:8085
```

Login Credentials:

| Username | Password |
|----------|----------|
| admin | admin123 |

Navigate to:

```text
Admin → Connections
```

Create a new PostgreSQL connection using the following settings:

| Field | Value |
|------|------|
| Conn Id | postgres_traffic_db |
| Conn Type | Postgres |
| Host | postgres |
| Schema | traffic_db |
| Login | smartcity |
| Password | smartcity123 |
| Port | 5432 |

---

#### 6. Trigger Airflow DAG

After configuring the database connection:

1. Open the DAGs page in Airflow
2. Enable the DAG
3. Trigger the DAG manually

The DAG performs:

- Daily traffic aggregation
- Peak-hour analysis
- Congestion analysis
- Intervention report generation
- Visualization report generation

---

#### 7. Access System Dashboards

| Service | URL |
|---------|-----|
| React Dashboard | http://localhost:5173 |
| Airflow UI | http://localhost:8085 |
| Spark Master UI | http://localhost:8080 |
| Grafana Dashboard | http://localhost:3000 |
