# TP06 – Real-Time Data Processing

## Project Overview

This project focuses on real-time data processing using Apache Spark Structured Streaming and Apache Flink SQL, with Apache Kafka as the messaging system.

The main objective is to collect streaming events, process them continuously, and generate meaningful results using real-time data processing technologies.

The project is divided into two practical laboratories:

- **Lab A:** Real-Time Processing with Apache Spark Structured Streaming.
- **Lab B:** Real-Time Data Processing with Apache Flink SQL.

Both laboratories use Apache Kafka to receive and distribute streaming data.

---

## Technologies Used

| Technology | Version / Description |
|---|---|
| Apache Kafka | 3.9.1 |
| Apache Spark | 4.0.1 |
| Apache Flink | 1.20.2 |
| Python | 3.x |
| Java | 17 |
| Docker | Container-based execution |
| Kafka Python Client | kafka-python |
| Flink Kafka Connector | 3.3.0-1.20 |

### Main Components

- **Apache Kafka:** Distributed messaging platform used to receive and distribute streaming events.
- **Apache Spark Structured Streaming:** Processes continuous streams of data and performs window-based aggregations.
- **Apache Flink SQL:** Executes SQL queries on streaming data using event-time processing and window operations.
- **Python:** Generates simulated events and sends them to Kafka.
- **Docker:** Provides isolated environments for running the required services.

---

## Project Structure

```text
TP06-Real-Time-Processing/
│
├── Lab-A-Spark-Streaming/
│   ├── producer.py
│   ├── spark_streaming.py
│   └── venv/
│
├── Lab-B-Flink-SQL/
│   ├── producer_flink.py
│   │
│   ├── sql/
│   │   ├── create_table.sql
│   │   └── department_checkins.sql
│   │
│   └── lib/
│       └── flink-sql-connector-kafka-3.3.0-1.20.jar
│
├── screenshots/
│   ├── lab-a-fast-streaming.png
│   ├── lab-a-spark-streaming-results.png
│   └── lab-b-flink-sql-results.png
│
├── report/
│   └── TP06_Real_Time_Processing_Report.pdf
│
└── README.md
```

---

# Lab A – Real-Time Processing with Apache Spark

## 1. Objective

The objective of this laboratory is to implement a real-time data processing pipeline using Apache Kafka and Apache Spark Structured Streaming.

A Python producer continuously generates simulated campus events and sends them to a Kafka topic. Spark consumes these events, processes them in real time, and calculates the number of events for each event type within one-minute event-time windows.

## 2. Data Generation

The file `producer.py` generates simulated campus events.

Each event contains the following fields:

| Field | Description |
|---|---|
| studentId | Identifier of the student |
| eventType | Type of campus event |
| eventTime | Timestamp of the event |

The simulated event types include:

- `wifi_connection`
- `course_registration`
- `login`
- `library_entry`

The producer sends events to the Kafka topic:

```text
campus-events
```

## 3. Data Processing with Spark

The file `spark_streaming.py` implements the Spark Structured Streaming application.

The processing pipeline performs the following operations:

1. Connects to the Kafka topic.
2. Reads incoming streaming messages.
3. Deserializes JSON event data.
4. Extracts event fields.
5. Converts event timestamps into timestamp values.
6. Applies event-time windowing.
7. Groups events by their event type.
8. Calculates the number of events in each one-minute window.
9. Displays the aggregated results.

### Window Aggregation

The application uses one-minute tumbling windows.

Each window represents a separate time interval, and events are grouped according to their event type.

The resulting output contains:

| Column | Description |
|---|---|
| window_start | Beginning of the time window |
| window_end | End of the time window |
| eventType | Event category |
| count | Number of events in the window |

## 4. Running Lab A

### Step 1 – Start the Kafka Producer

Navigate to the Lab A directory:

```bash
cd Lab-A-Spark-Streaming
```

Activate the Python virtual environment:

```bash
source venv/bin/activate
```

Run the producer:

```bash
python producer.py
```

The producer continuously sends campus events to Kafka.

### Step 2 – Run Spark Structured Streaming

Open another terminal and navigate to the same directory:

```bash
cd ~/TP06-Real-Time-Processing/Lab-A-Spark-Streaming
```

Run the Spark application using Docker:

```bash
docker run --rm \
  --network host \
  -v "$PWD":/opt/spark/work-dir \
  -w /opt/spark/work-dir \
  apache/spark:4.0.1 \
  /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1 \
  spark_streaming.py
```

Spark will connect to Kafka, consume the incoming events, and display the aggregated results.

## 5. Lab A Results

The application was successfully tested with continuous event generation.

The results demonstrated:

- Successful communication between the Python producer and Kafka.
- Successful consumption of streaming events by Spark.
- Correct event-time window processing.
- Successful aggregation by event type.
- Processing of events under a faster event-generation rate.

The fast-streaming test demonstrated that Spark could process a large number of incoming events.

Screenshots of the execution results are available in the `screenshots/` directory.

---

# Lab B – Real-Time Processing with Apache Flink SQL

## 1. Objective

The objective of this laboratory is to implement a real-time data processing pipeline using Apache Flink SQL and Apache Kafka.

A Python producer generates simulated department check-in events and sends them to Kafka. Flink SQL consumes these events and performs real-time aggregations using event-time tumbling windows.

## 2. Data Generation

The file `producer_flink.py` generates simulated department check-in events.

Each event contains:

| Field | Description |
|---|---|
| department | Name of the department |
| event_time | Event timestamp in ISO-8601 format |

The simulated departments are:

- Computer Science
- Mathematics
- Physics
- Biology

The producer sends events to the Kafka topic:

```text
department-checkins
```

## 3. Flink SQL Table Definition

The SQL table is defined in:

```text
sql/create_table.sql
```

The table connects Flink to the Kafka topic `department-checkins`.

The table contains:

- `department`: Department name.
- `event_time`: Original event timestamp stored as a string.
- `event_time_ts`: A computed timestamp column of type `TIMESTAMP_LTZ(3)`.

The computed timestamp is created using `TO_TIMESTAMP_LTZ` and `UNIX_TIMESTAMP`.

This timestamp column is used for event-time processing.

### Watermark Configuration

The table defines a watermark on `event_time_ts`:

```sql
WATERMARK FOR event_time_ts AS
    event_time_ts - INTERVAL '5' SECOND
```

The watermark allows Flink to handle event-time progress while accounting for events that may arrive slightly late.

## 4. Tumbling Window Aggregation

The SQL query is stored in:

```text
sql/department_checkins.sql
```

The query uses the Flink SQL `TUMBLE` table-valued function with a one-minute interval.

It performs the following operations:

1. Reads streaming records from the `checkins` table.
2. Groups events into one-minute event-time windows.
3. Groups records by department.
4. Counts the number of check-ins in each window.
5. Produces continuous aggregation results.

### SQL Query

```sql
SELECT
    window_start,
    window_end,
    department,
    COUNT(*) AS checkin_count
FROM TABLE(
    TUMBLE(
        TABLE checkins,
        DESCRIPTOR(event_time_ts),
        INTERVAL '1' MINUTE
    )
)
GROUP BY
    window_start,
    window_end,
    department;
```

### Output Columns

| Column | Description |
|---|---|
| window_start | Beginning of the one-minute window |
| window_end | End of the one-minute window |
| department | Department name |
| checkin_count | Number of check-ins |

## 5. Running Lab B

### Step 1 – Start the Python Producer

Navigate to the Lab B directory:

```bash
cd ~/TP06-Real-Time-Processing/Lab-B-Flink-SQL
```

Activate the shared Python virtual environment:

```bash
source ../Lab-A-Spark-Streaming/venv/bin/activate
```

Run the producer:

```bash
python producer_flink.py
```

The producer continuously sends department check-in events to Kafka.

### Step 2 – Start the Flink JobManager

Open another terminal in the Lab B directory.

Run:

```bash
docker run -d \
  --name tp06-flink-jobmanager \
  --network host \
  -e JOB_MANAGER_RPC_ADDRESS=localhost \
  -v "$PWD/lib/flink-sql-connector-kafka-3.3.0-1.20.jar:/opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar" \
  flink:1.20.2-scala_2.12-java17 \
  jobmanager
```

### Step 3 – Start the Flink TaskManager

Run:

```bash
docker run -d \
  --name tp06-flink-taskmanager \
  --network host \
  -e JOB_MANAGER_RPC_ADDRESS=localhost \
  -v "$PWD/lib/flink-sql-connector-kafka-3.3.0-1.20.jar:/opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar" \
  flink:1.20.2-scala_2.12-java17 \
  taskmanager
```

### Step 4 – Start the Flink SQL Client

From the Lab B directory, run:

```bash
docker run --rm -it \
  --network host \
  -v "$PWD/lib/flink-sql-connector-kafka-3.3.0-1.20.jar:/opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar" \
  -v "$PWD/sql:/opt/flink/sql" \
  flink:1.20.2-scala_2.12-java17 \
  /opt/flink/bin/sql-client.sh
```

### Step 5 – Configure the Remote Execution Environment

Inside the Flink SQL Client, execute each statement separately:

```sql
SET 'execution.target' = 'remote';
```

```sql
SET 'rest.address' = 'localhost';
```

```sql
SET 'rest.port' = '8081';
```

### Step 6 – Create the Kafka Source Table

Execute the SQL statement contained in:

```text
sql/create_table.sql
```

The table definition can be entered directly into the Flink SQL Client.

### Step 7 – Execute the Aggregation Query

Execute the SQL statement contained in:

```text
sql/department_checkins.sql
```

The query continuously processes incoming check-in events and displays the one-minute aggregation results.

## 6. Lab B Results

The Flink SQL application was successfully executed.

The results demonstrated:

- Successful generation of department check-in events.
- Successful delivery of events to Kafka.
- Successful connection between Flink SQL and the Kafka source.
- Successful timestamp conversion.
- Successful event-time watermark configuration.
- Successful one-minute tumbling window aggregation.
- Continuous processing of streaming data.

The Flink JobManager REST API was also used to verify the execution status of the streaming job.

The final aggregation query was confirmed to be running successfully.

A screenshot of the query results is available in:

```text
screenshots/lab-b-flink-sql-results.png
```

---

# Screenshots

The `screenshots/` directory contains visual evidence of the practical experiments.

| Screenshot | Description |
|---|---|
| `lab-a-spark-streaming-results.png` | Spark Structured Streaming aggregation results |
| `lab-a-fast-streaming.png` | Spark processing under a faster event-generation rate |
| `lab-b-flink-sql-results.png` | Flink SQL department check-in aggregation results |

---

# Final Project Results

Both laboratories were successfully implemented and tested.

| Feature | Lab A – Spark | Lab B – Flink |
|---|---|---|
| Kafka integration | Yes | Yes |
| Python event producer | Yes | Yes |
| Real-time stream processing | Yes | Yes |
| Event-time processing | Yes | Yes |
| One-minute tumbling windows | Yes | Yes |
| Group-based aggregation | By event type | By department |
| Continuous processing | Yes | Yes |

## Conclusion

This project demonstrates the practical implementation of real-time data processing pipelines using Apache Kafka, Apache Spark Structured Streaming, and Apache Flink SQL.

Lab A demonstrated how Spark can consume and process continuous streams of campus events, while Lab B demonstrated how Flink SQL can perform event-time window aggregations on department check-in data.

The experiments provided practical experience with streaming architectures, Kafka integration, event-time processing, watermarks, and window-based aggregation.

Both implementations successfully achieved their respective processing objectives.

---

# Project Report

The complete project report is available in PDF format:

```text
report/TP06_Real_Time_Processing_Report.pdf
```

It contains the project description, technologies, implementation details, experimental results, and conclusion.

---

**End of Project – TP06 Real-Time Processing**