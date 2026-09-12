# Market Data Orchestrator

A complete, production-ready data pipeline that automatically fetches daily stock market data from the Alpha Vantage API, processes the JSON response, and stores it in a PostgreSQL database using Apache Airflow for orchestration.

## Architecture

```text
                    Docker Compose
                         │
          ┌──────────────┴──────────────┐
          │                             │
       Airflow                      PostgreSQL
          │                             │
          │ schedules                  │
          ▼                             │
   ┌───────────────────┐                │
   │   Airflow DAG     │                │
   └─────────┬─────────┘                │
             │                          │
             ▼                          │
   ┌───────────────────┐                │
   │ Fetch Stock Data  │                │
   │ Python + requests │                │
   └─────────┬─────────┘                │
             │                          │
             ▼                          │
   ┌───────────────────┐                │
   │ Parse & Validate  │                │
   │ JSON Response     │                │
   └─────────┬─────────┘                │
             │                          │
             ▼                          │
   ┌───────────────────┐                │
   │ Transform Records │                │
   └─────────┬─────────┘                │
             │                          │
             ▼                          │
   ┌───────────────────┐                │
   │ Upsert PostgreSQL │────────────────┘
   └───────────────────┘
```

## Technology Stack

* **Python 3.11**: Core logic for the data pipeline.
* **Apache Airflow 2.8**: Workflow orchestration, scheduling, and retry management.
* **PostgreSQL 15**: Relational database for storing the processed stock data and Airflow metadata.
* **Docker & Docker Compose**: Containerisation and local deployment.
* **Requests**: Python library for API interactions.
* **Psycopg2**: Python adapter for PostgreSQL connectivity.

## Project Structure

```text
market-data-orchestrator/
├── docker-compose.yml        # Infrastructure configuration
├── Dockerfile                # Custom Airflow image with pipeline dependencies
├── Makefile                  # Convenience commands (make start, make stop, etc.)
├── requirements.txt          # Python dependencies (requests, psycopg2-binary)
├── .env.example              # Example environment variables (copy to .env)
├── .gitignore                # Git ignore rules
├── README.md                 # Project documentation
├── dags/
│   └── stock_pipeline_dag.py # Airflow DAG definition (Orchestration)
├── scripts/
│   └── stock_pipeline.py     # Core data pipeline logic (Business/Data logic)
└── sql/
    ├── init.sql              # App database schema (stock_prices table)
    └── create_airflow_db.sql # Airflow metadata database creation
```

## Prerequisites

* **Docker Desktop** (Must be open and running in the background)
* Docker Compose (v2)
* An API Key from [Alpha Vantage](https://www.alphavantage.co/support/#api-key) (Free)

> [!IMPORTANT]
> Ensure the Docker Desktop application is open and fully started (the Docker icon says "Engine running") on your machine before running any terminal commands.

## Configuration

1. Clone the repository and navigate to the project directory:
   ```bash
   git clone https://github.com/chv-sneha/market-data-orchestrator.git
   cd market-data-orchestrator
   ```

2. Copy the example environment file to create your local configuration:
   ```bash
   cp .env.example .env
   ```

3. Get a free API key from [Alpha Vantage](https://www.alphavantage.co/support/#api-key), then open `.env` and configure it:
   ```env
   ALPHA_VANTAGE_API_KEY=your_actual_api_key_here
   ```

   *Note: Do not commit your `.env` file.*

## Running the Project

You can start the pipeline using either the `Makefile` (recommended) or raw Docker Compose commands directly — both work identically.

### Start the Pipeline

**Option A — using Make (recommended):**
```bash
make start
```

**Option B — using Docker Compose directly:**
```bash
docker compose up --build -d
```

Both options will build the Docker images and start all containers in the background. With `make start`, a clickable link is also printed in the terminal once everything is ready:

```
=====================================================
  ✅ Pipeline is up and running!

  🌐 Airflow UI  →  http://localhost:8080
     Username : admin
     Password : admin

  💡 Tip: Cmd+Click (Mac) / Ctrl+Click (Windows)
     the link above to open it in your browser.
=====================================================
```

This command will:
1. Start the PostgreSQL database.
2. Initialise the database schemas (both for the app and Airflow).
3. Initialise the Airflow metadata and create the admin user.
4. Start the Airflow Webserver and Scheduler.

### Accessing Airflow

1. Click the `http://localhost:8080` link printed in your terminal after `make start`, or open it manually in your browser.
2. Log in to the UI using the default credentials:
   * **Username:** `admin`
   * **Password:** `admin`
3. Locate the `daily_stock_market_pipeline` DAG.
4. Toggle the switch to unpause the DAG. It will run automatically based on its schedule, or you can trigger it manually by clicking the "Play" button.

### Verifying the Data (PostgreSQL)

You can connect to the PostgreSQL database interactively to verify the ingested data or run your own queries.

1. Open an interactive database shell:
   ```bash
   # Using Make
   make db

   # Using Docker directly
   docker exec -it stock_pipeline_postgres psql -U stockuser -d stockdb
   ```

2. Or run a quick query directly without entering the shell:
   ```bash
   # Using Make
   make db-query

   # Using Docker directly
   docker exec -it stock_pipeline_postgres psql -U stockuser -d stockdb -c "SELECT * FROM stock_prices ORDER BY timestamp DESC LIMIT 10;"
   ```

   This prints the 10 most recent stock price rows.

> **Pro-Tip for PostgreSQL Shell:**
> * If the output is long, you will see a `--More--` prompt at the bottom. Press **`Spacebar`** to scroll down, or press **`q`** to exit the view.
> * To exit the database entirely and return to your normal terminal, type **`\q`** and press Enter.

## Pipeline Workflow

1. **Fetch**: The pipeline calls the Alpha Vantage `TIME_SERIES_DAILY` endpoint for each configured symbol (`STOCK_SYMBOLS` in `.env`).
2. **Validate**: Checks the JSON response for expected structures, API errors, and rate limits.
3. **Parse & Transform**: Extracts the Date, Open, High, Low, Close, and Volume fields. Validates data types and constraints. Converts them into clean, structured dictionaries.
4. **Upsert**: Connects to PostgreSQL and performs an `INSERT ... ON CONFLICT DO UPDATE`. This ensures the operation is idempotent—rerunning the pipeline updates existing records rather than creating duplicates.

## Error Handling & Resilience

The pipeline is designed to be robust:

* **Missing/Malformed Data**: If an individual daily record is malformed or missing fields, it is skipped (and logged), allowing valid records to still be processed.
* **API Errors & Rate Limits**: If the API key is invalid or the free-tier rate limit is hit, an exception is raised. Airflow will catch this and automatically retry the task according to its policy (default: 3 retries, 5-minute delay).
* **Database Failures**: Uses PostgreSQL transactions. If an insert fails, the transaction rolls back, preventing partial data corruption. Airflow handles the retry.
* **Idempotency**: Because of the `UNIQUE(symbol, timestamp)` constraint in PostgreSQL and the `ON CONFLICT` clause, the pipeline can be safely run multiple times without duplicating data.

## Scalability

While this is a foundational pipeline, it is designed with scalability in mind:

* **More Stocks**: Simply add symbols to the `STOCK_SYMBOLS` environment variable (e.g., `STOCK_SYMBOLS=AAPL,MSFT,GOOGL,AMZN,TSLA`). The Python script processes them sequentially, pausing briefly to respect API rate limits.
* **More Frequent Execution**: The Airflow DAG schedule (`@daily`) can easily be changed to `@hourly` in `stock_pipeline_dag.py`.
* **Parallel Processing**: As the list of symbols grows, the Airflow DAG could be refactored to dynamically generate parallel tasks for each symbol (using Airflow TaskMapping or dynamic DAG generation).
* **Distributed Architecture**: The `LocalExecutor` currently used can be swapped for a `CeleryExecutor` or `KubernetesExecutor` in production to run tasks across multiple worker nodes.

## Troubleshooting

* **Container not starting / Port conflicts**: Ensure port `5432` (Postgres) and `8080` (Airflow Webserver) are not being used by other applications on your host.
* **DAG not appearing in UI**: It can take a minute for the scheduler to parse new DAG files. Ensure `stock_pipeline_dag.py` is in the `dags/` folder and has no syntax errors.
* **Task Fails / API Rate Limit**: Alpha Vantage free tier is limited to 25 requests/day. Check the Airflow task logs (click the failed task > Logs) to see if you hit this limit. Airflow will retry automatically.

## Commands Cheat Sheet

All commands are run from the project root (`market-data-orchestrator`). Run `make help` at any time to see this list in your terminal.

| Command | Description |
|---|---|
| `make start` | Build & start all containers. Prints a clickable Airflow UI link. |
| `make stop` | Stop all containers (data is preserved). |
| `make restart` | Stop and fully rebuild everything. |
| `make status` | Show which containers are currently running. |
| `make logs` | Stream live logs from all containers. |
| `make logs-webserver` | Stream Airflow webserver logs only. |
| `make logs-scheduler` | Stream Airflow scheduler logs only. |
| `make db` | Open an interactive PostgreSQL shell. |
| `make db-query` | Print the 10 most recent stock price rows. |
| `make reset` | ⚠️ Stop all containers and **wipe all data** (hard reset). |
| `make help` | Display all available commands in the terminal. |
