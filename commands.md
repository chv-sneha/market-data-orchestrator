# Stock Pipeline Commands Cheat Sheet

This file contains all the useful commands to manage, run, and inspect your Dockerized Airflow pipeline.
You should run these commands from your terminal, while inside the `market-data-orchestrator` directory.

## 1. Start the Pipeline
```bash
docker compose up --build -d
```
**What it does:** 
- Builds the custom Airflow Docker image (with our extra Python packages).
- Starts PostgreSQL, the Airflow Scheduler, and the Airflow Webserver in the background (`-d`).
- Automatically creates the databases and tables if they don't exist yet.

## 2. Check the Status of the Containers
```bash
docker ps
```
**What it does:** 
- Lists all running Docker containers. You should see `stock_pipeline_postgres`, `stock_pipeline_airflow_scheduler`, and `stock_pipeline_airflow_webserver` running.

## 3. View Logs for the Webserver (UI)
```bash
docker compose logs -f airflow-webserver
```
**What it does:** 
- Shows the real-time logs for the Airflow web UI. Press `Ctrl + C` to stop watching the logs.

## 4. View Logs for the Scheduler
```bash
docker compose logs -f airflow-scheduler
```
**What it does:** 
- Shows the real-time logs for the Airflow scheduler (which triggers the Python script). Useful for debugging if the DAG isn't running on time. Press `Ctrl + C` to exit.

## 5. View the Database Data (Verify it worked!)
```bash
docker exec -it stock_pipeline_postgres psql -U stockuser -d stockdb -c "SELECT * FROM stock_prices ORDER BY timestamp DESC LIMIT 10;"
```
**What it does:** 
- Reaches inside the running PostgreSQL container.
- Connects to the database `stockdb` as the user `stockuser`.
- Runs a SQL query to print out the 10 most recent rows of stock data that Airflow successfully downloaded and saved.

## 6. Stop the Pipeline (Gracefully)
```bash
docker compose down
```
**What it does:** 
- Stops all the Airflow and PostgreSQL containers safely.
- It keeps your data intact! If you run `docker compose up -d` again later, your stock data will still be there.

## 7. Stop the Pipeline & Wipe All Data (Hard Reset)
```bash
docker compose down -v
```
**What it does:** 
- Stops all the containers AND deletes the database volumes (`-v`). 
- **Warning:** This deletes all the saved stock data and Airflow history. Use this only if you want to start completely from scratch!
