# Real-Time Data Lakehouse Pipeline

An end-to-end, containerized streaming data pipeline that ingests real-time events, performs windowed aggregations, and writes the results to an Apache Iceberg Data Lakehouse on AWS S3.

## 🏗️ Architecture Overview


1. **Data Generation:** A Python producer continuously streams mock pageview events (User ID, Postcode, URL, Timestamp) into an **Apache Kafka** topic.
2. **Infrastructure as Code:** **Terraform** automatically provisions the required AWS S3 bucket for the data lake.
3. **Stream Processing:** **Apache Flink** consumes the Kafka stream and runs two parallel jobs:
   * **Raw Ingestion:** Writes the raw, unadulterated JSON events directly into an Iceberg table (`raw_db`).
   * **Windowed Aggregation:** Calculates the total pageviews per postcode over a 1-minute tumbling window, writing the aggregated metrics to a second Iceberg table (`agg_db`).
4. **Storage:** Both Flink jobs utilize the **Apache Iceberg** table format to provide ACID transactions and schema evolution directly on AWS S3.

## 🚀 Prerequisites

* Docker and Docker Compose
* An AWS Account with programmatic access (Access Key & Secret Key)

## ⚙️ Setup Instructions

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/yourusername/real-time-lakehouse-pipeline.git](https://github.com/yourusername/real-time-lakehouse-pipeline.git)
   cd real-time-lakehouse-pipeline
   ```
2. **Configure Environment Variables:** Copy the example environment file and fill in your AWS credentials and desired S3 bucket name.
    ```bash
   cp .env.example .env
   ```
   
## 🏃‍♂️ Running the Pipeline
1. **Start the Infrastructure:**
Run Docker Compose to spin up Kafka, provision the AWS S3 bucket via Terraform, build the Flink cluster, and compile the Java jobs.
```Bash
docker-compose up -d --build
```
2. **Submit the Flink Jobs:**
Once the cluster is running, execute the following commands to deploy the streaming jobs to the Flink JobManager:

Submit the Raw Data Ingestion Job:

```Bash
docker exec -d flink-jobmanager ./bin/flink run -c com.pipeline.streaming.RawDataJob /opt/flink/usrlib/job.jar
```

Submit the Windowed Aggregation Job:

```Bash
docker exec -d flink-jobmanager ./bin/flink run -c com.pipeline.streaming.AggregationJob /opt/flink/usrlib/job.jar
```
3. **Monitor the Pipeline:**
Navigate to the Flink UI at http://localhost:8081 to watch the jobs process the data streams in real-time.


## 🧹 Teardown
To stop the pipeline and destroy the containers:

```Bash
docker-compose down
```