#!/bin/bash

# Script de inicialização para instalar o driver PostgreSQL no cluster Dataproc
# Versão: 2.0 - Atualizado em 29/05/2025
# Autor: Equipe Windsurf

# Configurações
JDBC_DRIVER_VERSION="42.2.23"
JDBC_DRIVER_JAR="postgresql-${JDBC_DRIVER_VERSION}.jar"
JDBC_DRIVER_PATH="/usr/lib/spark/jars/${JDBC_DRIVER_JAR}"
BUCKET_NAME="repo-dev-gdo-carga"

echo "Iniciando instalação do driver PostgreSQL e dependências..."

# Função para verificar se um comando existe
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Instala o cliente PostgreSQL
echo "Instalando cliente PostgreSQL..."
try_count=0
max_tries=3

while [ $try_count -lt $max_tries ]; do
  if apt-get update -y && apt-get install -y postgresql-client; then
    echo "Cliente PostgreSQL instalado com sucesso"
    break
  else
    try_count=$((try_count + 1))
    if [ $try_count -lt $max_tries ]; then
      echo "Falha na instalação do PostgreSQL. Tentando novamente (${try_count}/${max_tries})..."
      sleep 5
    else
      echo "Falha na instalação do PostgreSQL após ${max_tries} tentativas. Continuando mesmo assim..."
    fi
  fi
done

# Instala as bibliotecas Python necessárias
echo "Instalando bibliotecas Python..."
if command_exists pip; then
  pip install --no-cache-dir psycopg2-binary pandas google-cloud-storage || echo "Aviso: Falha ao instalar algumas bibliotecas Python"
else
  echo "Aviso: pip não encontrado, pulando instalação de bibliotecas Python"
fi

# Verifica se o driver JDBC já existe
if [ -f "${JDBC_DRIVER_PATH}" ]; then
  echo "Driver JDBC PostgreSQL já existe em ${JDBC_DRIVER_PATH}"
else
  # Tenta baixar o driver JDBC do bucket GCS
  echo "Tentando obter o driver JDBC do bucket GCS..."
  if gsutil cp "gs://${BUCKET_NAME}/jars/${JDBC_DRIVER_JAR}" "${JDBC_DRIVER_PATH}"; then
    echo "Driver JDBC PostgreSQL copiado do bucket GCS para ${JDBC_DRIVER_PATH}"
  else
    # Se falhar, tenta baixar da internet
    echo "Tentando baixar o driver JDBC da internet..."
    if command_exists wget; then
      wget -q "https://jdbc.postgresql.org/download/${JDBC_DRIVER_JAR}" -O "${JDBC_DRIVER_PATH}" && \
      echo "Driver JDBC PostgreSQL baixado da internet para ${JDBC_DRIVER_PATH}"
    elif command_exists curl; then
      curl -s "https://jdbc.postgresql.org/download/${JDBC_DRIVER_JAR}" -o "${JDBC_DRIVER_PATH}" && \
      echo "Driver JDBC PostgreSQL baixado da internet para ${JDBC_DRIVER_PATH}"
    else
      echo "Erro: nem wget nem curl estão disponíveis para baixar o driver JDBC"
      exit 1
    fi
  fi
fi

# Verifica se o driver foi instalado corretamente
if [ -f "${JDBC_DRIVER_PATH}" ]; then
  echo "Driver PostgreSQL JDBC instalado com sucesso em ${JDBC_DRIVER_PATH}"
  # Garante que o driver tenha as permissões corretas
  chmod 644 "${JDBC_DRIVER_PATH}"
else
  echo "Erro: Falha ao instalar o driver PostgreSQL JDBC"
  exit 1
fi

echo "Instalação concluída com sucesso!"
exit 0
