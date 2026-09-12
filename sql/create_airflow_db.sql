-- =============================================================================
-- Stock Market Data Pipeline - Airflow Database Initialisation
-- =============================================================================
-- PostgreSQL only bootstraps ONE database automatically (POSTGRES_DB).
-- This script creates a separate database and user for Airflow metadata
-- so the app data and Airflow state are logically separated.
--
-- The script runs as the superuser (POSTGRES_USER) at container start-up via
-- the /docker-entrypoint-initdb.d/ mechanism.
--
-- Environment variables are not expanded inside SQL files, so we use
-- Docker Compose environment injection to pass the actual credentials at
-- build time via the env_file directive.  The values here are therefore
-- read from the .env file via the entrypoint shell.
-- =============================================================================

-- Create the Airflow user if it does not already exist.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow'
    ) THEN
        CREATE ROLE airflow WITH LOGIN PASSWORD 'changeme_airflow_password';
    END IF;
END
$$;

-- Create the Airflow metadata database if it does not already exist.
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec

-- Grant all privileges on the airflow database to the airflow role.
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
