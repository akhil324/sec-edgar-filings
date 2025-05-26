#!/bin/bash

# --- Configuration Variables ---
# BigQuery Configuration
BQ_PROJECT_ID="<Your target BQ Project>"
BQ_DATASET_ID="<Your target BQ Dataset>"

# GCS Configuration
GCS_BUCKET_NAME="<Just the bucket name>" 
GCS_BASE_PATH="<Path within the bucket where your data folders are>"

# Table Names
TABLE_FILING_LEVEL="company_form_data_filing_level"
TABLE_TIME_SERIES="company_form_data_time_series"
TABLE_COMPANY_MASTER="sec_company_master_data"

# --- Derived Variables ---
# Full BigQuery Table IDs
BQ_TABLE_FILING_LEVEL="${BQ_PROJECT_ID}:${BQ_DATASET_ID}.${TABLE_FILING_LEVEL}"
BQ_TABLE_TIME_SERIES="${BQ_PROJECT_ID}:${BQ_DATASET_ID}.${TABLE_TIME_SERIES}"
BQ_TABLE_COMPANY_MASTER="${BQ_PROJECT_ID}:${BQ_DATASET_ID}.${TABLE_COMPANY_MASTER}"

# Full GCS Paths to Parquet file directories (note the wildcard for all parquet files)
GCS_PATH_FILING_LEVEL="gs://${GCS_BUCKET_NAME}/${GCS_BASE_PATH}/${TABLE_FILING_LEVEL}/*.parquet"
GCS_PATH_TIME_SERIES="gs://${GCS_BUCKET_NAME}/${GCS_BASE_PATH}/${TABLE_TIME_SERIES}/*.parquet"
GCS_PATH_COMPANY_MASTER="gs://${GCS_BUCKET_NAME}/${GCS_BASE_PATH}/${TABLE_COMPANY_MASTER}/*.parquet"

# Schemas (as strings for bq load command)
SCHEMA_FILING_LEVEL="cik:STRING,entity_name:STRING,accession_number:STRING,form_type:STRING,filed_date:DATE,fiscal_year:INTEGER,fiscal_period:STRING,all_facts_json_array:STRING"
SCHEMA_TIME_SERIES="cik:STRING,entity_name:STRING,taxonomy:STRING,concept:STRING,label:STRING,unit:STRING,period_end_date:DATE,value:FLOAT64,accession_number:STRING,fiscal_year:INTEGER,fiscal_period:STRING,form_type:STRING,filed_date:DATE,frame:STRING"
SCHEMA_COMPANY_MASTER="cik:STRING,company_name:STRING,sic:STRING,sic_description:STRING,ticker:STRING,exchange:STRING"

# --- Helper Function for Logging ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] - $1"
}

# 1. Delete existing tables (if they exist)
log "Attempting to delete existing BigQuery tables (if they exist)..."
bq rm --force --table "${BQ_TABLE_FILING_LEVEL}"
bq rm --force --table "${BQ_TABLE_TIME_SERIES}"
bq rm --force --table "${BQ_TABLE_COMPANY_MASTER}"
log "Deletion attempts complete."
echo

# 2. Load company_form_data_filing_level
log "Loading data into ${BQ_TABLE_FILING_LEVEL}..."
bq load \
    --source_format=PARQUET \
    --replace \
    --time_partitioning_field=filed_date \
    --time_partitioning_type=MONTH \
    --clustering_fields=cik,form_type \
    "${BQ_TABLE_FILING_LEVEL}" \
    "${GCS_PATH_FILING_LEVEL}" \
    "${SCHEMA_FILING_LEVEL}"

if [ $? -eq 0 ]; then
    log "Successfully loaded data into ${BQ_TABLE_FILING_LEVEL}."
else
    log "ERROR: Failed to load data into ${BQ_TABLE_FILING_LEVEL}."
    # Consider adding 'exit 1' here if you want the script to stop on failure
fi
echo

# 3. Load company_form_data_time_series
log "Loading data into ${BQ_TABLE_TIME_SERIES}..."
bq load \
    --source_format=PARQUET \
    --replace \
    --time_partitioning_field=filed_date \
    --time_partitioning_type=MONTH \
    --clustering_fields=cik,concept,form_type \
    "${BQ_TABLE_TIME_SERIES}" \
    "${GCS_PATH_TIME_SERIES}" \
    "${SCHEMA_TIME_SERIES}"

if [ $? -eq 0 ]; then
    log "Successfully loaded data into ${BQ_TABLE_TIME_SERIES}."
else
    log "ERROR: Failed to load data into ${BQ_TABLE_TIME_SERIES}."
fi
echo

# 4. Load sec_company_master_data
log "Loading data into ${BQ_TABLE_COMPANY_MASTER}..."
bq load \
    --source_format=PARQUET \
    --replace \
    --clustering_fields=cik,sic \
    "${BQ_TABLE_COMPANY_MASTER}" \
    "${GCS_PATH_COMPANY_MASTER}" \
    "${SCHEMA_COMPANY_MASTER}"

if [ $? -eq 0 ]; then
    log "Successfully loaded data into ${BQ_TABLE_COMPANY_MASTER}."
else
    log "ERROR: Failed to load data into ${BQ_TABLE_COMPANY_MASTER}."
fi
echo

log "Script execution finished."