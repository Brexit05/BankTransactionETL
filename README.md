# Bank Transactions ETL Pipeline with Kafka, Postgres, Airflow & S3

This project demonstrates a **data engineering pipeline** I built to simulate real-time bank transactions, store them in a database, and orchestrate batch ETL jobs for downstream analytics. It combines **streaming ingestion** with **batch processing**, showing how raw events can be cleaned, transformed, and exported to cloud storage for analysis.

##  Features

* **Streaming Ingestion**

  * Fake bank transactions generated with Python (`producer.py`).
  * Kafka Consumer stores transactions into **Postgres** (`bank_transactions` table).

* **Batch Processing with Airflow**

  * ETL DAG orchestrated in Airflow.
  * Extracts data from Postgres, runs SQL transformations (cleaning, aggregations).
  * Loads results back to Postgres (e.g., `daily_summary`) and exports CSVs to **Amazon S3**.

* **Containerized Setup**

  * All services (Kafka, Zookeeper, Postgres, Airflow, Producer, Consumer) run in **Docker Compose**.
  * Single network `bank_network` ensures connectivity.

## Project Structure
My-Kafka/
│── dags/                  # Airflow DAGs
│   └── bank_etl_dag.py
│── producer.py            # Kafka producer (fake bank transactions)
│── consumer.py            # Kafka consumer (writes to Postgres)
│── docker-compose.yml     # All services (Kafka, Postgres, Airflow, etc.)
│── requirements.txt       # Python dependencies

## Tech Stack

* **Apache Kafka** → real-time streaming of events
* **Postgres** → persistent storage
* **Apache Airflow** → batch ETL orchestration
* **Amazon S3** → cloud data lake (CSV exports)
* **Docker Compose** → containerized environment

## How to Run

1. **Clone the repo**

   ```bash
   git clone https://github.com/Brexit05/BankTransactionETL.git
   cd bank-transactions-etl
   ```

2. **Start the pipeline**

   ```bash
   docker compose up -d
   ```

3. **Access services**

   * Airflow Web UI → [http://localhost:8080](http://localhost:8080)
   * Postgres → `localhost:5432` (DB: `bank_db`)
   * Kafka Broker → `localhost:9092`

4. **Run Airflow DAG**

   * Enable `bank_etl_dag` in Airflow UI.
   * Watch transformations run and data exported to S3.


