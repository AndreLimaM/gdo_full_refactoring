#!/bin/bash

# Script para executar o processamento de dados JSON conforme mapeamento específico
# usando Dataproc e Cloud SQL

# Configurações padrão
PROJETO=""
REGIAO="us-central1"
CLUSTER="cluster-dataproc"
BUCKET=""
INPUT_PATH=""
OUTPUT_DONE_PATH=""
OUTPUT_ERROR_PATH=""
DB_HOST=""
DB_NAME="gdo_database"
DB_USER="postgres"
DB_PASSWORD=""
TABLE_NAME="dados_mapeados"
MODE="append"

# Função para exibir ajuda
exibir_ajuda() {
    echo "Uso: $0 [opções]"
    echo ""
    echo "Opções:"
    echo "  --projeto ID           ID do projeto GCP (obrigatório)"
    echo "  --regiao REGIAO        Região do Dataproc (padrão: us-central1)"
    echo "  --cluster NOME         Nome do cluster Dataproc (padrão: cluster-dataproc)"
    echo "  --bucket NOME          Nome do bucket GCS (obrigatório)"
    echo "  --input-path CAMINHO   Caminho GCS para os arquivos JSON de entrada (obrigatório)"
    echo "  --output-done-path CAMINHO   Caminho GCS para os arquivos processados com sucesso (obrigatório)"
    echo "  --output-error-path CAMINHO  Caminho GCS para os arquivos com erro (obrigatório)"
    echo "  --db-host HOST         Host do Cloud SQL (obrigatório)"
    echo "  --db-name NOME         Nome do banco de dados (padrão: gdo_database)"
    echo "  --db-user USUARIO      Usuário do banco de dados (padrão: postgres)"
    echo "  --db-password SENHA    Senha do banco de dados (obrigatório)"
    echo "  --table-name TABELA    Nome da tabela para gravar os dados (padrão: dados_mapeados)"
    echo "  --mode MODO            Modo de gravação: append ou overwrite (padrão: append)"
    echo "  --ajuda                Exibe esta mensagem de ajuda"
    echo ""
    echo "Exemplo:"
    echo "  $0 --projeto meu-projeto --bucket meu-bucket --input-path gs://meu-bucket/pending --output-done-path gs://meu-bucket/done --output-error-path gs://meu-bucket/error --db-host 10.0.0.1 --db-password minhasenha"
    exit 1
}

# Processar argumentos
while [[ $# -gt 0 ]]; do
    case "$1" in
        --projeto)
            PROJETO="$2"
            shift 2
            ;;
        --regiao)
            REGIAO="$2"
            shift 2
            ;;
        --cluster)
            CLUSTER="$2"
            shift 2
            ;;
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --input-path)
            INPUT_PATH="$2"
            shift 2
            ;;
        --output-done-path)
            OUTPUT_DONE_PATH="$2"
            shift 2
            ;;
        --output-error-path)
            OUTPUT_ERROR_PATH="$2"
            shift 2
            ;;
        --db-host)
            DB_HOST="$2"
            shift 2
            ;;
        --db-name)
            DB_NAME="$2"
            shift 2
            ;;
        --db-user)
            DB_USER="$2"
            shift 2
            ;;
        --db-password)
            DB_PASSWORD="$2"
            shift 2
            ;;
        --table-name)
            TABLE_NAME="$2"
            shift 2
            ;;
        --mode)
            MODE="$2"
            shift 2
            ;;
        --ajuda)
            exibir_ajuda
            ;;
        *)
            echo "Opção desconhecida: $1"
            exibir_ajuda
            ;;
    esac
done

# Verificar parâmetros obrigatórios
if [ -z "$PROJETO" ] || [ -z "$BUCKET" ] || [ -z "$INPUT_PATH" ] || [ -z "$OUTPUT_DONE_PATH" ] || [ -z "$OUTPUT_ERROR_PATH" ] || [ -z "$DB_HOST" ] || [ -z "$DB_PASSWORD" ]; then
    echo "Erro: Parâmetros obrigatórios não fornecidos."
    exibir_ajuda
fi

# Verificar se o modo é válido
if [ "$MODE" != "append" ] && [ "$MODE" != "overwrite" ]; then
    echo "Erro: Modo inválido. Use 'append' ou 'overwrite'."
    exibir_ajuda
fi

echo "Iniciando processamento de dados conforme mapeamento..."
echo "Projeto: $PROJETO"
echo "Região: $REGIAO"
echo "Cluster: $CLUSTER"
echo "Bucket: $BUCKET"
echo "Input Path: $INPUT_PATH"
echo "Output Done Path: $OUTPUT_DONE_PATH"
echo "Output Error Path: $OUTPUT_ERROR_PATH"
echo "DB Host: $DB_HOST"
echo "DB Name: $DB_NAME"
echo "Tabela: $TABLE_NAME"
echo "Modo: $MODE"

# Verificar se o cluster existe
if ! gcloud dataproc clusters describe "$CLUSTER" --region="$REGIAO" --project="$PROJETO" &>/dev/null; then
    echo "Erro: Cluster $CLUSTER não encontrado na região $REGIAO."
    echo "Criando cluster Dataproc..."
    
    # Criar o cluster com script de inicialização para instalar o driver PostgreSQL
    gcloud dataproc clusters create "$CLUSTER" \
        --region="$REGIAO" \
        --project="$PROJETO" \
        --bucket="$BUCKET" \
        --initialization-actions="gs://$BUCKET/datalake/utils/dataproc/scripts/init_script.sh" \
        --metadata="postgresql-driver=true" \
        --master-machine-type=n1-standard-4 \
        --worker-machine-type=n1-standard-4 \
        --num-workers=2
    
    if [ $? -ne 0 ]; then
        echo "Erro ao criar o cluster Dataproc."
        exit 1
    fi
fi

# Copiar o script Python para o bucket GCS
echo "Copiando script de processamento para o bucket GCS..."
SCRIPT_PATH="$(dirname "$(readlink -f "$0")")/processar_mapeamento_json.py"
gsutil cp "$SCRIPT_PATH" "gs://$BUCKET/datalake/utils/dataproc/scripts/"

# Executar o job no Dataproc
echo "Executando job de processamento de mapeamento..."
gcloud dataproc jobs submit pyspark "gs://$BUCKET/datalake/utils/dataproc/scripts/processar_mapeamento_json.py" \
    --region="$REGIAO" \
    --project="$PROJETO" \
    --cluster="$CLUSTER" \
    -- \
    --input-path="$INPUT_PATH" \
    --output-done-path="$OUTPUT_DONE_PATH" \
    --output-error-path="$OUTPUT_ERROR_PATH" \
    --db-host="$DB_HOST" \
    --db-name="$DB_NAME" \
    --db-user="$DB_USER" \
    --db-password="$DB_PASSWORD" \
    --table-name="$TABLE_NAME" \
    --mode="$MODE"

# Verificar resultado
if [ $? -eq 0 ]; then
    echo "Processamento de dados conforme mapeamento concluído com sucesso!"
else
    echo "Erro durante o processamento de dados conforme mapeamento."
    exit 1
fi
