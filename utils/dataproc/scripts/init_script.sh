#!/bin/bash

# Script de inicialização para o cluster Dataproc
# Instala as dependências necessárias para conectar ao Cloud SQL

# Instalar o driver JDBC do PostgreSQL
apt-get update
apt-get install -y postgresql-client

# Baixar o driver JDBC do PostgreSQL
wget -q https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -O /usr/lib/spark/jars/postgresql-42.2.23.jar

# Instalar o Cloud SQL Auth Proxy
wget -q https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O /usr/local/bin/cloud_sql_proxy
chmod +x /usr/local/bin/cloud_sql_proxy

# Instalar bibliotecas Python necessárias
pip install pandas psycopg2-binary google-cloud-storage

# Verificar se o driver foi instalado corretamente
if [ -f /usr/lib/spark/jars/postgresql-42.2.23.jar ]; then
  echo "Driver PostgreSQL instalado com sucesso"
else
  echo "Erro ao instalar o driver PostgreSQL"
  exit 1
fi
