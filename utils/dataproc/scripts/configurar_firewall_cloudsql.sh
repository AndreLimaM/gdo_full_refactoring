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
    --instancia-sql)
      SQL_INSTANCE="$2"
      shift 2
      ;;
    --regiao-dataproc)
      DATAPROC_REGION="$2"
      shift 2
      ;;
    --regiao-sql)
      SQL_REGION="$2"
      shift 2
      ;;
    *)
      echo -e "${VERMELHO}Parâmetro desconhecido: $1${NC}"
      exit 1
      ;;
  esac
done

# Valores padrão
PROJETO_ID=${GCP_PROJECT_ID:-"development-439017"}
DATAPROC_REGION=${DATAPROC_REGION:-"us-east4"}
SQL_REGION=${SQL_REGION:-"us-central1"}
SQL_INSTANCE=${SQL_INSTANCE:-"ecotrace-dev-db"}

echo -e "${VERDE}Configurando ambiente...${NC}"
gcloud config set project ${PROJETO_ID}
echo -e "${VERDE}Ambiente configurado com sucesso.${NC}"

# Obter informações da instância Cloud SQL
echo -e "${AMARELO}Obtendo informações da instância Cloud SQL...${NC}"
SQL_INFO=$(gcloud sql instances describe ${SQL_INSTANCE} --project=${PROJETO_ID} --format="json")

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao obter informações da instância Cloud SQL.${NC}"
    exit 1
fi

# Extrair IP público e privado do Cloud SQL
SQL_PUBLIC_IP=$(echo $SQL_INFO | jq -r '.ipAddresses[] | select(.type=="PRIMARY") | .ipAddress')
SQL_PRIVATE_IP=$(echo $SQL_INFO | jq -r '.ipAddresses[] | select(.type=="PRIVATE") | .ipAddress')

echo -e "${VERDE}Informações do Cloud SQL:${NC}"
echo -e "  IP Público: ${SQL_PUBLIC_IP}"
echo -e "  IP Privado: ${SQL_PRIVATE_IP}"

# Obter IPs autorizados atuais
AUTHORIZED_NETWORKS=$(echo $SQL_INFO | jq -r '.settings.ipConfiguration.authorizedNetworks[] | .value' 2>/dev/null)

echo -e "${VERDE}IPs atualmente autorizados:${NC}"
if [ -z "$AUTHORIZED_NETWORKS" ]; then
    echo -e "  Nenhum IP autorizado encontrado."
else
    echo "$AUTHORIZED_NETWORKS" | while read IP; do
        echo -e "  $IP"
    done
fi

# Criar um cluster Dataproc temporário para obter a faixa de IPs
echo -e "${AMARELO}Criando cluster Dataproc temporário para obter a faixa de IPs...${NC}"
TIMESTAMP=$(date +"%Y%m%d%H%M%S")
CLUSTER_NAME="gdo-dataproc-temp-${TIMESTAMP}"

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --region=${DATAPROC_REGION} \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50 \
    --image-version=2.0-debian10 \
    --project=${PROJETO_ID}

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao criar o cluster Dataproc temporário.${NC}"
    exit 1
fi

# Obter informações do cluster Dataproc
echo -e "${AMARELO}Obtendo informações do cluster Dataproc...${NC}"
DATAPROC_INFO=$(gcloud dataproc clusters describe ${CLUSTER_NAME} \
    --region=${DATAPROC_REGION} \
    --project=${PROJETO_ID} \
    --format="json")

# Extrair IPs do cluster Dataproc
MASTER_INSTANCE=$(echo $DATAPROC_INFO | jq -r '.config.masterConfig.instanceNames[0]')
MASTER_ZONE=$(echo $DATAPROC_INFO | jq -r '.config.masterConfig.machineTypeUri' | cut -d'/' -f4)

echo -e "${AMARELO}Obtendo IP do nó master do Dataproc...${NC}"
MASTER_IP=$(gcloud compute instances describe ${MASTER_INSTANCE} \
    --zone=${MASTER_ZONE} \
    --project=${PROJETO_ID} \
    --format="get(networkInterfaces[0].networkIP)")

echo -e "${VERDE}Informações do Dataproc:${NC}"
echo -e "  Nó Master: ${MASTER_INSTANCE}"
echo -e "  IP do Master: ${MASTER_IP}"

# Obter a sub-rede do Dataproc
SUBNET=$(gcloud compute instances describe ${MASTER_INSTANCE} \
    --zone=${MASTER_ZONE} \
    --project=${PROJETO_ID} \
    --format="get(networkInterfaces[0].subnetwork)" | cut -d'/' -f10)

SUBNET_INFO=$(gcloud compute networks subnets describe ${SUBNET} \
    --region=${DATAPROC_REGION} \
    --project=${PROJETO_ID} \
    --format="json")

SUBNET_RANGE=$(echo $SUBNET_INFO | jq -r '.ipCidrRange')

echo -e "  Sub-rede: ${SUBNET}"
echo -e "  Faixa de IPs: ${SUBNET_RANGE}"

# Adicionar a faixa de IPs do Dataproc aos IPs autorizados do Cloud SQL
echo -e "${AMARELO}Adicionando a faixa de IPs do Dataproc aos IPs autorizados do Cloud SQL...${NC}"

# Verificar se a faixa já está autorizada
IS_AUTHORIZED=false
if [ ! -z "$AUTHORIZED_NETWORKS" ]; then
    echo "$AUTHORIZED_NETWORKS" | while read IP; do
        if [ "$IP" == "$SUBNET_RANGE" ]; then
            IS_AUTHORIZED=true
            break
        fi
    done
fi

if [ "$IS_AUTHORIZED" == "true" ]; then
    echo -e "${VERDE}A faixa de IPs ${SUBNET_RANGE} já está autorizada.${NC}"
else
    # Adicionar a faixa de IPs à lista de IPs autorizados
    gcloud sql instances patch ${SQL_INSTANCE} \
        --project=${PROJETO_ID} \
        --authorized-networks=${SUBNET_RANGE}
    
    if [ $? -ne 0 ]; then
        echo -e "${VERMELHO}Erro ao adicionar a faixa de IPs aos IPs autorizados.${NC}"
    else
        echo -e "${VERDE}Faixa de IPs ${SUBNET_RANGE} adicionada com sucesso aos IPs autorizados.${NC}"
    fi
fi

# Adicionar o IP específico do nó master
echo -e "${AMARELO}Adicionando o IP do nó master aos IPs autorizados do Cloud SQL...${NC}"

# Verificar se o IP já está autorizado
IS_AUTHORIZED=false
if [ ! -z "$AUTHORIZED_NETWORKS" ]; then
    echo "$AUTHORIZED_NETWORKS" | while read IP; do
        if [ "$IP" == "$MASTER_IP/32" ]; then
            IS_AUTHORIZED=true
            break
        fi
    done
fi

if [ "$IS_AUTHORIZED" == "true" ]; then
    echo -e "${VERDE}O IP ${MASTER_IP}/32 já está autorizado.${NC}"
else
    # Adicionar o IP à lista de IPs autorizados
    gcloud sql instances patch ${SQL_INSTANCE} \
        --project=${PROJETO_ID} \
        --authorized-networks=${MASTER_IP}/32
    
    if [ $? -ne 0 ]; then
        echo -e "${VERMELHO}Erro ao adicionar o IP aos IPs autorizados.${NC}"
    else
        echo -e "${VERDE}IP ${MASTER_IP}/32 adicionado com sucesso aos IPs autorizados.${NC}"
    fi
fi

# Excluir o cluster temporário
echo -e "${AMARELO}Excluindo o cluster Dataproc temporário...${NC}"
gcloud dataproc clusters delete ${CLUSTER_NAME} \
    --region=${DATAPROC_REGION} \
    --project=${PROJETO_ID} \
    --quiet

if [ $? -ne 0 ]; then
    echo -e "${VERMELHO}Erro ao excluir o cluster Dataproc temporário.${NC}"
else
    echo -e "${VERDE}Cluster Dataproc temporário excluído com sucesso.${NC}"
fi

echo -e "${VERDE}Configuração do firewall do Cloud SQL concluída.${NC}"
echo -e "${VERDE}Agora o Cloud SQL deve aceitar conexões do Dataproc.${NC}"

# Instruções para testar a conexão
echo -e "${AMARELO}Para testar a conexão, execute:${NC}"
echo -e "./utils/dataproc/executar_teste_conectividade.sh --projeto ${PROJETO_ID}"
