#!/bin/bash

# Script para executar o teste de conexão entre Dataproc e Cloud SQL
# Autor: Equipe Windsurf
# Versão: 1.0.0
# Data: 28/05/2025

# Configurações
PROJETO_ID=${GCP_PROJECT_ID:-"seu-projeto-gcp"}
REGIAO="us-east4"
ZONA="us-east4-a"
CLUSTER_NAME="gdo-dataproc-teste"
BUCKET_NAME=${GCS_BUCKET_NAME:-"repo-dev-gdo-carga"}
REDE="default"
SUBREDE="default"

# Cores para output
VERDE='\033[0;32m'
AMARELO='\033[1;33m'
VERMELHO='\033[0;31m'
NC='\033[0m' # Sem cor

# Função para exibir mensagem de ajuda
exibir_ajuda() {
    echo "Uso: $0 [opções]"
    echo ""
    echo "Opções:"
    echo "  -p, --projeto ID      ID do projeto GCP (padrão: $PROJETO_ID)"
    echo "  -r, --regiao REGIAO   Região do cluster (padrão: $REGIAO)"
    echo "  -z, --zona ZONA       Zona do cluster (padrão: $ZONA)"
    echo "  -c, --cluster NOME    Nome do cluster (padrão: $CLUSTER_NAME)"
    echo "  -b, --bucket NOME     Nome do bucket GCS (padrão: $BUCKET_NAME)"
    echo "  -n, --rede NOME       Nome da rede VPC (padrão: $REDE)"
    echo "  -s, --subrede NOME    Nome da sub-rede (padrão: $SUBREDE)"
    echo "  -k, --manter          Não exclui o cluster após os testes"
    echo "  -h, --ajuda           Exibe esta mensagem de ajuda"
    echo ""
    echo "Exemplo:"
    echo "  $0 --projeto meu-projeto-gcp --bucket meu-bucket --manter"
    exit 0
}

# Função para exibir mensagem de erro e sair
erro() {
    echo -e "${VERMELHO}ERRO: $1${NC}" >&2
    exit 1
}

# Função para exibir mensagem de aviso
aviso() {
    echo -e "${AMARELO}AVISO: $1${NC}"
}

# Função para exibir mensagem de sucesso
sucesso() {
    echo -e "${VERDE}$1${NC}"
}

# Função para verificar dependências
verificar_dependencias() {
    echo "Verificando dependências..."
    
    # Verifica se o Python 3 está instalado
    if ! command -v python3 &> /dev/null; then
        erro "Python 3 não encontrado. Por favor, instale o Python 3."
    fi
    
    # Verifica se o pip está instalado
    if ! command -v pip3 &> /dev/null; then
        erro "pip3 não encontrado. Por favor, instale o pip3."
    fi
    
    # Verifica se o gcloud está instalado
    if ! command -v gcloud &> /dev/null; then
        erro "Google Cloud SDK (gcloud) não encontrado. Por favor, instale o Google Cloud SDK."
    fi
    
    # Verifica se está autenticado no gcloud
    if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" &> /dev/null; then
        erro "Não autenticado no Google Cloud. Execute 'gcloud auth login' primeiro."
    fi
    
    # Verifica se as bibliotecas Python necessárias estão instaladas
    echo "Verificando bibliotecas Python..."
    pip3 install --quiet google-cloud-dataproc google-cloud-storage python-dotenv
    
    sucesso "Todas as dependências estão instaladas."
}

# Função para configurar o ambiente
configurar_ambiente() {
    echo "Configurando ambiente..."
    
    # Configura o projeto padrão
    gcloud config set project "$PROJETO_ID"
    
    # Verifica se o bucket existe
    if ! gsutil ls -b "gs://$BUCKET_NAME" &> /dev/null; then
        aviso "Bucket gs://$BUCKET_NAME não encontrado. Criando..."
        gsutil mb -l "$REGIAO" "gs://$BUCKET_NAME"
    fi
    
    sucesso "Ambiente configurado com sucesso."
}

# Função para executar o teste
executar_teste() {
    echo "Executando teste de conexão entre Dataproc e Cloud SQL..."
    
    # Constrói o comando com os parâmetros
    COMANDO="python3 $(dirname "$0")/testar_conexao_dataproc_cloudsql.py"
    COMANDO="$COMANDO --project-id $PROJETO_ID"
    COMANDO="$COMANDO --region $REGIAO"
    COMANDO="$COMANDO --zone $ZONA"
    COMANDO="$COMANDO --cluster-name $CLUSTER_NAME"
    COMANDO="$COMANDO --bucket-name $BUCKET_NAME"
    COMANDO="$COMANDO --network $REDE"
    COMANDO="$COMANDO --subnetwork $SUBREDE"
    
    # Adiciona a flag para manter o cluster, se necessário
    if [ "$MANTER_CLUSTER" = true ]; then
        COMANDO="$COMANDO --keep-cluster"
    fi
    
    # Executa o comando
    echo "Comando: $COMANDO"
    eval "$COMANDO"
    
    # Verifica o resultado
    if [ $? -eq 0 ]; then
        sucesso "Teste de conexão concluído com sucesso!"
    else
        erro "Teste de conexão falhou. Verifique os logs para mais detalhes."
    fi
}

# Processa os argumentos da linha de comando
MANTER_CLUSTER=false
while [[ $# -gt 0 ]]; do
    case $1 in
        -p|--projeto)
            PROJETO_ID="$2"
            shift 2
            ;;
        -r|--regiao)
            REGIAO="$2"
            shift 2
            ;;
        -z|--zona)
            ZONA="$2"
            shift 2
            ;;
        -c|--cluster)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        -b|--bucket)
            BUCKET_NAME="$2"
            shift 2
            ;;
        -n|--rede)
            REDE="$2"
            shift 2
            ;;
        -s|--subrede)
            SUBREDE="$2"
            shift 2
            ;;
        -k|--manter)
            MANTER_CLUSTER=true
            shift
            ;;
        -h|--ajuda)
            exibir_ajuda
            ;;
        *)
            erro "Opção desconhecida: $1"
            ;;
    esac
done

# Verifica se o ID do projeto foi fornecido
if [ "$PROJETO_ID" = "seu-projeto-gcp" ]; then
    aviso "ID do projeto GCP não fornecido. Use a opção --projeto para especificar o ID do projeto."
    read -p "Deseja continuar com o valor padrão? (s/n): " CONTINUAR
    if [[ ! "$CONTINUAR" =~ ^[Ss]$ ]]; then
        erro "Operação cancelada pelo usuário."
    fi
fi

# Executa as funções principais
verificar_dependencias
configurar_ambiente
executar_teste

exit 0
