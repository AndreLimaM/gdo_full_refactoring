#!/bin/bash

# Cores para output
VERDE='\033[0;32m'
VERMELHO='\033[0;31m'
AMARELO='\033[0;33m'
NC='\033[0m' # Sem cor

# Paru00e2metros
while [[ $# -gt 0 ]]; do
  case $1 in
    --projeto)
      GCP_PROJECT_ID="$2"
      shift 2
      ;;
    --bucket)
      GCS_BUCKET_NAME="$2"
      shift 2
      ;;
    --input)
      INPUT_PATH="$2"
      shift 2
      ;;
    --tabela)
      TABLE_NAME="$2"
      shift 2
      ;;
    *)
      echo -e "${VERMELHO}Paru00e2metro desconhecido: $1${NC}"
      exit 1
      ;;
  esac
done

# Configurau00e7u00f5es
PROJETO_ID=${GCP_PROJECT_ID:-"development-439017"}
REGIAO="us-east4"
ZONA="us-east4-a"
TIMESTAMP=$(date +"%Y%m%d%H%M%S")
CLUSTER_NAME="gdo-dataproc-proc-${TIMESTAMP}"
BUCKET_NAME=${GCS_BUCKET_NAME:-"repo-dev-gdo-carga"}
INPUT_PATH=${INPUT_PATH:-"gs://${BUCKET_NAME}/dados/animais/*.json"}
TABLE_NAME=${TABLE_NAME:-"bt_animais"}

# Configurau00e7u00f5es do banco de dados
DB_HOST="34.48.11.43"  # IP pu00fablico do Cloud SQL
DB_NAME="db_eco_tcbf_25"
DB_USER="db_eco_tcbf_25_user"
DB_PASSWORD="5HN33PHKjXcLTz3tBC"

echo -e "${VERDE}Configurando ambiente...${NC}"
gcloud config set project ${PROJETO_ID}
echo -e "${VERDE}Ambiente configurado com sucesso.${NC}"

# Verificar se o script de inicializau00e7u00e3o existe no bucket
echo -e "${AMARELO}Verificando script de inicializau00e7u00e3o...${NC}"
gsutil ls gs://${BUCKET_NAME}/scripts/init_script.sh &> /dev/null
if [ $? -ne 0 ]; then
    echo -e "${AMARELO}Script de inicializau00e7u00e3o nu00e3o encontrado. Enviando para o bucket...${NC}"
    gsutil cp ./utils/dataproc/init_script.sh gs://${BUCKET_NAME}/scripts/init_script.sh
    if [ $? -ne 0 ]; then
        echo -e "${VERMELHO}Erro ao enviar o script de inicializau00e7u00e3o.${NC}"
        exit 1
    fi
    echo -e "${VERDE}Script de inicializau00e7u00e3o enviado com sucesso.${NC}"
fi

# Enviar o script de processamento para o bucket
echo -e "${AMARELO}Enviando script de processamento para o bucket...${NC}"
gsutil cp ./utils/dataproc/processar_json_para_sql.py gs://${BUCKET_NAME}/jobs/processar_json_para_sql.py
if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao enviar o script de processamento.${NC}"
    exit 1
fi
echo -e "${VERDE}Script de processamento enviado com sucesso.${NC}"

# Configurar o Cloud SQL para aceitar conexu00f5es do Dataproc
echo -e "${AMARELO}Configurando o Cloud SQL para aceitar conexu00f5es...${NC}"
gcloud sql instances patch database-ecotrace-lake \
    --project=${PROJETO_ID} \
    --authorized-networks="0.0.0.0/0"

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao configurar o Cloud SQL. Continuando mesmo assim...${NC}"
fi

# Criar o cluster Dataproc
echo -e "${AMARELO}Criando cluster Dataproc...${NC}"
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region=${REGIAO} \
    --zone=${ZONA} \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50 \
    --num-workers=2 \
    --worker-machine-type=n1-standard-2 \
    --worker-boot-disk-size=50 \
    --image-version=2.0-debian10 \
    --project=${PROJETO_ID} \
    --initialization-actions=gs://${BUCKET_NAME}/scripts/init_script.sh

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao criar o cluster Dataproc.${NC}"
    exit 1
fi

echo -e "${VERDE}Cluster criado com sucesso.${NC}"
echo -e "${AMARELO}Aguardando 60 segundos para garantir que o cluster esteja pronto...${NC}"
sleep 60

# Executar o job de processamento
echo -e "${AMARELO}Executando job de processamento...${NC}"
gcloud dataproc jobs submit pyspark \
    gs://${BUCKET_NAME}/jobs/processar_json_para_sql.py \
    --cluster=${CLUSTER_NAME} \
    --region=${REGIAO} \
    --project=${PROJETO_ID} \
    --jars=file:///usr/lib/spark/jars/postgresql-42.2.23.jar \
    -- \
    --input-path=${INPUT_PATH} \
    --db-host=${DB_HOST} \
    --db-name=${DB_NAME} \
    --db-user=${DB_USER} \
    --db-password=${DB_PASSWORD} \
    --table-name=${TABLE_NAME}

PROCESSAMENTO_RESULT=$?

# Limpar recursos
echo -e "${AMARELO}Excluindo cluster...${NC}"
gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGIAO} --project=${PROJETO_ID} --quiet

if [ ${PROCESSAMENTO_RESULT} -ne 0 ]; then
    echo -e "${VERMELHO}Processamento falhou.${NC}"
    exit 1
fi

echo -e "${VERDE}Processamento concluu00eddo com sucesso.${NC}"
