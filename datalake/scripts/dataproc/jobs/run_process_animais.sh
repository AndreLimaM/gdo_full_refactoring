#!/bin/bash
# -*- coding: utf-8 -*-
#
# Script para submeter o job de processamento de animais ao Dataproc
#

# Configurações de ambiente
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# Carrega as configurações
if [ -f "${PROJECT_ROOT}/src/config/config.sh" ]; then
  source "${PROJECT_ROOT}/src/config/config.sh"
else
  echo "Arquivo de configuração não encontrado!"
  exit 1
fi

# Variáveis de ambiente
GCP_PROJECT_ID=${GCP_PROJECT_ID:-"development-439017"}
GCS_BUCKET_NAME=${GCS_BUCKET_NAME:-"repo-dev-gdo-carga"}
DB_HOST=${DB_HOST:-"10.98.169.3"}
DB_NAME=${DB_NAME:-"db_eco_tcbf_25"}
DB_USER=${DB_USER:-"db_eco_tcbf_25_user"}
DB_PASSWORD=${DB_PASSWORD:-"5HN33PHKjXcLTz3tBC"}

# Diretórios de processamento
INPUT_DIR="gs://${GCS_BUCKET_NAME}/data/raw/animais/pending"
DONE_DIR="gs://${GCS_BUCKET_NAME}/data/raw/animais/done"
ERROR_DIR="gs://${GCS_BUCKET_NAME}/data/raw/animais/error"

# Timestamp para o nome do cluster
TIMESTAMP=$(date +%Y%m%d%H%M%S)
CLUSTER_NAME="gdo-process-animais-${TIMESTAMP}"

# Diretório de scripts
INIT_SCRIPTS_DIR="gs://${GCS_BUCKET_NAME}/scripts/init"
JOBS_DIR="gs://${GCS_BUCKET_NAME}/scripts/jobs"

# Faz upload dos scripts para o GCS
echo "Fazendo upload dos scripts para o GCS..."
gsutil cp "${PROJECT_ROOT}/scripts/dataproc/init/init_script.sh" "${INIT_SCRIPTS_DIR}/"
gsutil cp "${PROJECT_ROOT}/scripts/dataproc/init/install_postgresql_driver.sh" "${INIT_SCRIPTS_DIR}/"
gsutil cp "${PROJECT_ROOT}/src/processors/animais/process_animais.py" "${JOBS_DIR}/"
gsutil cp "${SCRIPT_DIR}/run_process_animais.py" "${JOBS_DIR}/"

# Cria o cluster Dataproc
echo "Criando cluster Dataproc ${CLUSTER_NAME}..."
gcloud dataproc clusters create ${CLUSTER_NAME} \
  --project=${GCP_PROJECT_ID} \
  --region=us-central1 \
  --zone=us-central1-a \
  --master-machine-type=n1-standard-4 \
  --master-boot-disk-size=500 \
  --num-workers=2 \
  --worker-machine-type=n1-standard-4 \
  --worker-boot-disk-size=500 \
  --image-version=2.0-debian10 \
  --initialization-actions="${INIT_SCRIPTS_DIR}/init_script.sh","${INIT_SCRIPTS_DIR}/install_postgresql_driver.sh" \
  --metadata="GCS_BUCKET=${GCS_BUCKET_NAME}" \
  --scopes=default,storage-rw \
  --max-idle=30m

# Submete o job
echo "Submetendo job de processamento de animais..."
JOB_ID=$(gcloud dataproc jobs submit pyspark \
  --project=${GCP_PROJECT_ID} \
  --region=us-central1 \
  --cluster=${CLUSTER_NAME} \
  "${JOBS_DIR}/run_process_animais.py" \
  -- \
  --input-dir="${INPUT_DIR}" \
  --done-dir="${DONE_DIR}" \
  --error-dir="${ERROR_DIR}" \
  --db-host="${DB_HOST}" \
  --db-name="${DB_NAME}" \
  --db-user="${DB_USER}" \
  --db-password="${DB_PASSWORD}" \
  --format=json | jq -r '.reference.jobId')

echo "Job submetido com ID: ${JOB_ID}"
echo "Acompanhe o progresso em: https://console.cloud.google.com/dataproc/jobs/${JOB_ID}?project=${GCP_PROJECT_ID}&region=us-central1"

# Aguarda a conclusão do job
echo "Aguardando conclusão do job..."
gcloud dataproc jobs wait ${JOB_ID} --project=${GCP_PROJECT_ID} --region=us-central1

# Verifica o status do job
JOB_STATUS=$(gcloud dataproc jobs describe ${JOB_ID} --project=${GCP_PROJECT_ID} --region=us-central1 --format="value(status.state)")

if [ "${JOB_STATUS}" == "DONE" ]; then
  echo "Job concluído com sucesso!"
else
  echo "Job falhou com status: ${JOB_STATUS}"
  exit 1
fi

# Exclui o cluster se a flag AUTO_DELETE_CLUSTER estiver definida como true
if [ "${AUTO_DELETE_CLUSTER:-true}" == "true" ]; then
  echo "Excluindo cluster ${CLUSTER_NAME}..."
  gcloud dataproc clusters delete ${CLUSTER_NAME} --project=${GCP_PROJECT_ID} --region=us-central1 --quiet
  echo "Cluster excluído com sucesso!"
fi

echo "Processamento de animais concluído!"
