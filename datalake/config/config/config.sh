#!/bin/bash
# -*- coding: utf-8 -*-
#
# Arquivo de configuração central para o projeto GDO
#

# Configurações do Google Cloud
export GCP_PROJECT_ID="development-439017"
export GCS_BUCKET_NAME="repo-dev-gdo-carga"
export GCP_REGION="us-central1"
export GCP_ZONE="us-central1-a"

# Configurações do Dataproc
export DATAPROC_IMAGE_VERSION="2.0-debian10"
export MASTER_MACHINE_TYPE="n1-standard-4"
export WORKER_MACHINE_TYPE="n1-standard-4"
export NUM_WORKERS="2"
export MASTER_BOOT_DISK_SIZE="500"
export WORKER_BOOT_DISK_SIZE="500"
export AUTO_DELETE_CLUSTER="true"
export MAX_IDLE_TIME="30m"

# Configurações do Cloud SQL
export DB_HOST="10.98.169.3"
export DB_NAME="db_eco_tcbf_25"
export DB_USER="db_eco_tcbf_25_user"
export DB_PASSWORD="5HN33PHKjXcLTz3tBC"

# Configurações de diretórios
export BASE_DIR_GCS="gs://${GCS_BUCKET_NAME}"
export SCRIPTS_DIR_GCS="${BASE_DIR_GCS}/scripts"
export INIT_SCRIPTS_DIR_GCS="${SCRIPTS_DIR_GCS}/init"
export JOBS_DIR_GCS="${SCRIPTS_DIR_GCS}/jobs"
export DATA_DIR_GCS="${BASE_DIR_GCS}/data"
export RAW_DIR_GCS="${DATA_DIR_GCS}/raw"
export PROCESSED_DIR_GCS="${DATA_DIR_GCS}/processed"
export CURATED_DIR_GCS="${DATA_DIR_GCS}/curated"

# Configurações específicas para processamento de animais
export ANIMAIS_PENDING_DIR="${RAW_DIR_GCS}/animais/pending"
export ANIMAIS_DONE_DIR="${RAW_DIR_GCS}/animais/done"
export ANIMAIS_ERROR_DIR="${RAW_DIR_GCS}/animais/error"

# Configurações de dependências
export POSTGRESQL_DRIVER_JAR="postgresql-42.2.23.jar"
export POSTGRESQL_DRIVER_PATH="/usr/lib/spark/jars/${POSTGRESQL_DRIVER_JAR}"
export POSTGRESQL_DRIVER_GCS="${BASE_DIR_GCS}/jars/${POSTGRESQL_DRIVER_JAR}"

# Configurações de logs
export LOG_LEVEL="INFO"
