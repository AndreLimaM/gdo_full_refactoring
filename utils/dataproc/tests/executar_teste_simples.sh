#!/bin/bash

# Cores para output
VERDE='\033[0;32m'
VERMELHO='\033[0;31m'
AMARELO='\033[0;33m'
NC='\033[0m' # Sem cor

# Parâmetros
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
      echo -e "${VERMELHO}Parâmetro desconhecido: $1${NC}"
      exit 1
      ;;
  esac
done

# Configurações
PROJETO_ID=${GCP_PROJECT_ID:-"seu-projeto-gcp"}
REGIAO="us-east4"
ZONA="us-east4-a"
TIMESTAMP=$(date +"%Y%m%d%H%M%S")
CLUSTER_NAME="gdo-dataproc-teste-${TIMESTAMP}"
BUCKET_NAME=${GCS_BUCKET_NAME:-"repo-dev-gdo-carga"}
REDE="default"
SUBREDE="default"

echo -e "${VERDE}Configurando ambiente...${NC}"
gcloud config set project ${PROJETO_ID}
echo -e "${VERDE}Ambiente configurado com sucesso.${NC}"

echo -e "${VERDE}Executando teste de conexão entre Dataproc e Cloud SQL...${NC}"

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
    --initialization-actions=gs://${BUCKET_NAME}/scripts/install_postgresql_driver.sh

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao criar o cluster Dataproc.${NC}"
    exit 1
fi

echo -e "${VERDE}Cluster criado com sucesso.${NC}"

# Fazer upload do script de teste
echo -e "${AMARELO}Enviando script de teste para o bucket...${NC}"
gsutil cp ./utils/dataproc/teste_simples_conexao.py gs://${BUCKET_NAME}/jobs/teste_simples_conexao.py

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao enviar o script de teste.${NC}"
    gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGIAO} --project=${PROJETO_ID} --quiet
    exit 1
fi

echo -e "${VERDE}Script enviado com sucesso.${NC}"

# Executar o job
echo -e "${AMARELO}Executando job de teste...${NC}"
gcloud dataproc jobs submit pyspark \
    gs://${BUCKET_NAME}/jobs/teste_simples_conexao.py \
    --cluster=${CLUSTER_NAME} \
    --region=${REGIAO} \
    --project=${PROJETO_ID} \
    --jars=file:///usr/lib/spark/jars/postgresql-42.2.23.jar

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao executar o job de teste.${NC}"
    echo -e "${AMARELO}Excluindo cluster...${NC}"
    gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGIAO} --project=${PROJETO_ID} --quiet
    exit 1
fi

# Limpar recursos
echo -e "${AMARELO}Excluindo cluster...${NC}"
gcloud dataproc clusters delete ${CLUSTER_NAME} --region=${REGIAO} --project=${PROJETO_ID} --quiet

echo -e "${VERDE}Teste concluído.${NC}"
