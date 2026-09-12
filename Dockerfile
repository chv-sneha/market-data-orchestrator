# =============================================================================
# Stock Market Data Pipeline - Custom Airflow Dockerfile
# =============================================================================
# Extends the official Apache Airflow image with the extra Python packages
# required by the pipeline (requests, psycopg2-binary).
#
# The official image already bundles Airflow and its dependencies;
# we only need to layer our application-specific packages on top.
# =============================================================================

FROM apache/airflow:2.8.1-python3.11

# Switch to root only for system-level operations (none needed here).
# Install extra Python dependencies as the airflow user to respect the
# official image's permission model.
USER airflow

# Copy requirements first so Docker's layer cache is reused when only
# source code changes.
COPY requirements.txt /requirements.txt

# Install without cache to keep the image lean.
RUN pip install --no-cache-dir -r /requirements.txt
