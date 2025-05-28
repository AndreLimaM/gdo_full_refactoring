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
CLUSTER_NAME="gdo-dataproc-lista-${TIMESTAMP}"
BUCKET_NAME=${GCS_BUCKET_NAME:-"repo-dev-gdo-carga"}

# Caminho do script no bucket GCS (simula o ambiente real)
SCRIPT_PATH="gs://${BUCKET_NAME}/datalake/utils/listar_tabelas_cloudsql.py"

echo -e "${VERDE}Configurando ambiente...${NC}"
gcloud config set project ${PROJETO_ID}
echo -e "${VERDE}Ambiente configurado com sucesso.${NC}"

# Verificar se o script existe no bucket
echo -e "${AMARELO}Verificando se o script existe no bucket...${NC}"
gsutil ls ${SCRIPT_PATH} &> /dev/null
if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro: O script nu00e3o foi encontrado no caminho ${SCRIPT_PATH}${NC}"
    exit 1
fi
echo -e "${VERDE}Script encontrado no bucket.${NC}"

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

# Criar o cluster Dataproc
echo -e "${AMARELO}Criando cluster Dataproc...${NC}"
gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region=${REGIAO} \
    --zone=${ZONA} \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50 \
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

# Executar o job diretamente do caminho no bucket
echo -e "${AMARELO}Executando listagem de tabelas a partir do script no bucket...${NC}"
gcloud dataproc jobs submit pyspark \
    ${SCRIPT_PATH} \
    --cluster=${CLUSTER_NAME} \
    --region=${REGIAO} \
    --project=${PROJETO_ID} \
    --jars=file:///usr/lib/spark/jars/postgresql-42.2.23.jar

LISTAGEM_RESULT=$?

# Limpar recursos
echo -e "${AMARELO}Excluindo cluster...${NC}"
gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGIAO} --project=${PROJETO_ID} --quiet

if [ ${LISTAGEM_RESULT} -ne 0 ]; then
    echo -e "${VERMELHO}Listagem de tabelas falhou.${NC}"
    exit 1
fi

echo -e "${VERDE}Listagem de tabelas concluu00edda com sucesso.${NC}"
