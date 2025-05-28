#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para testar a conexão entre um cluster Dataproc e o Cloud SQL.

Este script cria um cluster Dataproc na mesma região do Cloud SQL (us-east4),
configura a rede para permitir a comunicação entre eles e executa um job PySpark
para testar a conexão com o banco de dados.

Autor: Equipe Windsurf
Versão: 1.0.0
Data: 28/05/2025
"""

import os
import time
import argparse
import logging
from typing import Dict, Any, Optional
from google.cloud import dataproc_v1
from google.cloud import storage
from google.oauth2 import service_account
from dotenv import load_dotenv

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('dataproc-cloudsql-teste')

# Carrega variáveis de ambiente
load_dotenv()

# Configurações padrão
DEFAULT_REGION = "us-east4"
DEFAULT_ZONE = "us-east4-a"
DEFAULT_NETWORK = "default"
DEFAULT_SUBNETWORK = "default"
DEFAULT_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DEFAULT_CLUSTER_NAME = "gdo-dataproc-teste"
DEFAULT_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "repo-dev-gdo-carga")

# Credenciais do banco de dados
DB_HOST = "10.98.169.3"
DB_NAME = "db_eco_tcbf_25"
DB_USER = "db_eco_tcbf_25_user"
DB_PASSWORD = "5HN33PHKjXcLTz3tBC"


def criar_cluster_dataproc(
    project_id: str,
    region: str,
    zone: str,
    cluster_name: str,
    network: str,
    subnetwork: str,
    bucket_name: str
) -> Dict[str, Any]:
    """
    Cria um cluster Dataproc com as configurações necessárias para conectar ao Cloud SQL.
    
    Args:
        project_id: ID do projeto GCP
        region: Região do cluster
        zone: Zona do cluster
        cluster_name: Nome do cluster
        network: Nome da rede VPC
        subnetwork: Nome da sub-rede
        bucket_name: Nome do bucket para armazenamento temporário
        
    Returns:
        Informações do cluster criado
    """
    # Inicializa o cliente Dataproc
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    # Configuração do cluster
    cluster_config = {
        "project_id": project_id,
        "cluster_name": cluster_name,
        "config": {
            "gce_cluster_config": {
                "zone_uri": f"https://www.googleapis.com/compute/v1/projects/{project_id}/zones/{zone}",
                # Usar apenas a sub-rede, que já implica a rede
                "subnetwork_uri": f"https://www.googleapis.com/compute/v1/projects/{project_id}/regions/{region}/subnetworks/{subnetwork}",
                "service_account_scopes": [
                    "https://www.googleapis.com/auth/cloud-platform"
                ],
                # IP público para permitir acesso à internet (necessário para baixar o driver JDBC)
                "internal_ip_only": False,
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 100,
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-4",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 100,
                },
            },
            "software_config": {
                "image_version": "2.0-debian10",
                "properties": {
                    # Configurações para permitir conexão JDBC
                    "dataproc:dataproc.allow.zero.workers": "false",
                    # Adicionar o driver PostgreSQL ao classpath
                    "spark:spark.jars.packages": "org.postgresql:postgresql:42.2.23",
                },
            },
            "initialization_actions": [
                {
                    "executable_file": f"gs://{bucket_name}/scripts/install_postgresql_driver.sh",
                },
            ],
            "config_bucket": bucket_name,
        },
    }
    
    # Cria o cluster
    try:
        logger.info(f"Iniciando criação do cluster com rede {network} e sub-rede {subnetwork}...")
        operation = client.create_cluster(
            request={"project_id": project_id, "region": region, "cluster": cluster_config}
        )

        logger.info(f"Aguardando a criação do cluster (isso pode levar alguns minutos)...")
        # Aumenta o tempo limite para 20 minutos (1200 segundos)
        result = operation.result(timeout=1200)

        logger.info(f"Cluster criado com sucesso: {cluster_name}")
        logger.info(f"Aguardando 60 segundos para garantir que o cluster esteja pronto...")
        time.sleep(60)  # Aguarda um minuto para garantir que o cluster esteja pronto
    except Exception as e:
        logger.error(f"Erro ao criar o cluster: {str(e)}")
        # Verificar se o cluster foi parcialmente criado
        try:
            cluster_info = client.get_cluster(project_id=project_id, region=region, cluster_name=cluster_name)
            logger.info(f"Status do cluster: {cluster_info.status.state}")
        except Exception as get_error:
            logger.error(f"Não foi possível obter informações do cluster: {str(get_error)}")
        raise
    
    return result


def criar_script_inicializacao():
    """
    Cria um script de inicialização para instalar o driver PostgreSQL JDBC e o Cloud SQL Auth Proxy.
    
    Returns:
        str: Conteúdo do script de inicialização
    """
    return """#!/bin/bash

# Script de inicialização para instalar o driver PostgreSQL e o Cloud SQL Auth Proxy
echo "Instalando componentes necessários..."

# Instala o driver PostgreSQL
apt-get update
apt-get install -y postgresql-client

# Baixa o driver JDBC do PostgreSQL
wget https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -P /usr/lib/spark/jars/

# Instala o Cloud SQL Auth Proxy
wget https://dl.google.com/cloudsql/cloud_sql_proxy_x64_linux -O cloud_sql_proxy
chmod +x cloud_sql_proxy
mv cloud_sql_proxy /usr/local/bin/

# Cria diretório para sockets do Cloud SQL Proxy
mkdir -p /var/run/cloudsql
chmod 777 /var/run/cloudsql

echo "Driver PostgreSQL e Cloud SQL Auth Proxy instalados com sucesso!"
"""


def fazer_upload_script_inicializacao(bucket_name: str) -> None:
    """
    Faz upload do script de inicialização para o bucket GCS.
    
    Args:
        bucket_name: Nome do bucket GCS
    """
    script_content = criar_script_inicializacao()
    
    # Inicializa o cliente de armazenamento
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Cria o diretório de scripts se não existir
    scripts_dir_blob = bucket.blob("scripts/")
    if not scripts_dir_blob.exists():
        scripts_dir_blob.upload_from_string("")
    
    # Faz upload do script
    script_blob = bucket.blob("scripts/install_postgresql_driver.sh")
    script_blob.upload_from_string(script_content)
    
    logger.info(f"Script de inicialização enviado para gs://{bucket_name}/scripts/install_postgresql_driver.sh")


def fazer_upload_job_teste(bucket_name: str) -> None:
    """
    Faz upload do script PySpark para testar a conexão com o Cloud SQL.
    
    Args:
        bucket_name: Nome do bucket GCS
    """
    # Conteúdo do script PySpark - Versão ultra simplificada
    script_content = """#!/usr/bin/env python3
from pyspark.sql import SparkSession
import sys
import socket
import subprocess
import os

# Função para executar comandos do sistema
def executar_comando(comando):
    print(f"Executando comando: {comando}")
    resultado = subprocess.run(comando, shell=True, capture_output=True, text=True)
    print(f"Saída: {resultado.stdout}")
    if resultado.stderr:
        print(f"Erro: {resultado.stderr}")
    return resultado

# Verificar informações de rede
print("=== Diagnóstico de Rede ===")
executar_comando("hostname -I")  # Mostrar IPs do host
executar_comando("ip route")    # Mostrar rotas

# Testar conexão com o Cloud SQL
print("=== Teste de Conexão com Cloud SQL ===")
try:
    # Testar conexão com IP público
    print("Testando conexão com IP público (34.48.11.43:5432)...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    resultado = s.connect_ex(("34.48.11.43", 5432))
    if resultado == 0:
        print("Conexão bem-sucedida!")
    else:
        print(f"Falha na conexão. Código de erro: {resultado}")
    s.close()
    
    # Testar conexão com IP privado
    print("Testando conexão com IP privado (10.98.169.3:5432)...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(5)
    resultado = s.connect_ex(("10.98.169.3", 5432))
    if resultado == 0:
        print("Conexão bem-sucedida!")
    else:
        print(f"Falha na conexão. Código de erro: {resultado}")
    s.close()
except Exception as e:
    print(f"Erro ao testar conexão: {e}")

# Inicia a sessão Spark
print("=== Iniciando Sessão Spark ===")
spark = SparkSession.builder \
    .appName("TesteConexaoCloudSQL") \
    .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar") \
    .getOrCreate()

# Configurações de conexão com o banco de dados
# Usando o IP público do Cloud SQL
db_host = "34.48.11.43"  # IP público do Cloud SQL
db_name = "db_eco_tcbf_25"
db_user = "db_eco_tcbf_25_user"
db_password = "5HN33PHKjXcLTz3tBC"
db_url = "jdbc:postgresql://" + db_host + ":5432/" + db_name

try:
    print("Tentando conectar ao Cloud SQL...")
    
    # Primeiro tenta uma consulta simples para verificar a conexão
    test_df = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", db_url) \
        .option("dbtable", "(SELECT 1 as teste) AS test") \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()
    
    print("Conexão básica com o Cloud SQL estabelecida com sucesso!")
    test_df.show()
    
    # Verifica se a tabela bt_animais existe
    print("Verificando tabela bt_animais...")
    
    try:
        # Tenta carregar dados da tabela bt_animais
        df = spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", db_url) \
            .option("dbtable", "bt_animais") \
            .option("user", db_user) \
            .option("password", db_password) \
            .load()
        
        # Exibe a estrutura da tabela
        print("Estrutura da tabela bt_animais:")
        df.printSchema()
        
        # Conta o número de registros
        count = df.count()
        print("Número de registros na tabela: " + str(count))
        
        # Exibe alguns registros
        print("Exemplos de registros:")
        df.show(5, truncate=False)
        
        print("Tabela bt_animais acessada com sucesso!")
    except Exception as table_error:
        print("Erro ao acessar a tabela bt_animais: " + str(table_error))
        
        # Lista as tabelas disponíveis
        print("Listando tabelas disponíveis no banco de dados:")
        tables_df = spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", db_url) \
            .option("dbtable", "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') AS tables") \
            .option("user", db_user) \
            .option("password", db_password) \
            .load()
        
        tables_df.show(100, truncate=False)
        
except Exception as e:
    print("Erro ao conectar ao Cloud SQL: " + str(e))
    sys.exit(1)

# Encerra a sessão Spark
spark.stop()
sys.exit(0)
"""
    
    # Inicializa o cliente de armazenamento
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Cria o diretório de jobs se não existir
    jobs_dir_blob = bucket.blob("jobs/")
    if not jobs_dir_blob.exists():
        jobs_dir_blob.upload_from_string("")
    
    # Faz upload do script
    job_blob = bucket.blob("jobs/testar_conexao_cloudsql.py")
    job_blob.upload_from_string(script_content)
    
    logger.info(f"Script de teste enviado para gs://{bucket_name}/jobs/testar_conexao_cloudsql.py")


def submeter_job_teste(
    project_id: str,
    region: str,
    cluster_name: str,
    bucket_name: str
) -> Dict[str, Any]:
    """
    Submete um job PySpark para testar a conexão com o Cloud SQL.
    
    Args:
        project_id: ID do projeto GCP
        region: Região do cluster
        cluster_name: Nome do cluster
        bucket_name: Nome do bucket com o script
        
    Returns:
        Resultado da execução do job
    """
    # Inicializa o cliente Dataproc
    job_client = dataproc_v1.JobControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    # Configuração do job
    job = {
        "placement": {
            "cluster_name": cluster_name,
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{bucket_name}/jobs/testar_conexao_cloudsql.py",
            "jar_file_uris": ["file:///usr/lib/spark/jars/postgresql-42.2.23.jar"],
            "properties": {
                "spark.jars.packages": "org.postgresql:postgresql:42.2.23",
            },
        },
    }
    
    # Submete o job
    operation = job_client.submit_job_as_operation(
        request={
            "project_id": project_id,
            "region": region,
            "job": job,
        }
    )
    
    logger.info("Job submetido. Aguardando conclusão...")
    result = operation.result()
    
    # Obtém os detalhes do job
    job_id = result.reference.job_id
    job_details = job_client.get_job(
        request={
            "project_id": project_id,
            "region": region,
            "job_id": job_id,
        }
    )
    
    # Verifica o status do job
    status = job_details.status.state
    logger.info(f"Job concluído com status: {status}")
    
    # Obtém os logs do job
    driver_output_uri = job_details.driver_output_resource_uri
    if driver_output_uri:
        logger.info(f"Logs do job disponíveis em: {driver_output_uri}")
    
    return job_details


def excluir_cluster(
    project_id: str,
    region: str,
    cluster_name: str
) -> None:
    """
    Exclui o cluster Dataproc após os testes.
    
    Args:
        project_id: ID do projeto GCP
        region: Região do cluster
        cluster_name: Nome do cluster
    """
    # Inicializa o cliente Dataproc
    client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
    )
    
    # Exclui o cluster
    logger.info(f"Excluindo cluster {cluster_name}...")
    operation = client.delete_cluster(
        request={
            "project_id": project_id,
            "region": region,
            "cluster_name": cluster_name,
        }
    )
    
    # Aguarda a conclusão da operação
    operation.result()
    logger.info(f"Cluster {cluster_name} excluído com sucesso!")


def main():
    """
    Função principal para testar a conexão entre Dataproc e Cloud SQL.
    """
    parser = argparse.ArgumentParser(
        description="Testa a conexão entre um cluster Dataproc e o Cloud SQL."
    )
    parser.add_argument(
        "--project-id",
        default=DEFAULT_PROJECT_ID,
        help=f"ID do projeto GCP (padrão: {DEFAULT_PROJECT_ID})"
    )
    parser.add_argument(
        "--region",
        default=DEFAULT_REGION,
        help=f"Região do cluster (padrão: {DEFAULT_REGION})"
    )
    parser.add_argument(
        "--zone",
        default=DEFAULT_ZONE,
        help=f"Zona do cluster (padrão: {DEFAULT_ZONE})"
    )
    parser.add_argument(
        "--cluster-name",
        default=DEFAULT_CLUSTER_NAME,
        help=f"Nome do cluster (padrão: {DEFAULT_CLUSTER_NAME})"
    )
    parser.add_argument(
        "--network",
        default=DEFAULT_NETWORK,
        help=f"Nome da rede VPC (padrão: {DEFAULT_NETWORK})"
    )
    parser.add_argument(
        "--subnetwork",
        default=DEFAULT_SUBNETWORK,
        help=f"Nome da sub-rede (padrão: {DEFAULT_SUBNETWORK})"
    )
    parser.add_argument(
        "--bucket-name",
        default=DEFAULT_BUCKET_NAME,
        help=f"Nome do bucket GCS (padrão: {DEFAULT_BUCKET_NAME})"
    )
    parser.add_argument(
        "--keep-cluster",
        action="store_true",
        help="Não exclui o cluster após os testes"
    )
    
    args = parser.parse_args()
    
    try:
        # Faz upload dos scripts para o bucket
        logger.info("Preparando scripts...")
        fazer_upload_script_inicializacao(args.bucket_name)
        fazer_upload_job_teste(args.bucket_name)
        
        # Cria o cluster
        cluster = criar_cluster_dataproc(
            project_id=args.project_id,
            region=args.region,
            zone=args.zone,
            cluster_name=args.cluster_name,
            network=args.network,
            subnetwork=args.subnetwork,
            bucket_name=args.bucket_name
        )
        
        # Aguarda um pouco para garantir que o cluster esteja pronto
        logger.info("Aguardando 60 segundos para garantir que o cluster esteja pronto...")
        time.sleep(60)
        
        # Submete o job de teste
        job_result = submeter_job_teste(
            project_id=args.project_id,
            region=args.region,
            cluster_name=args.cluster_name,
            bucket_name=args.bucket_name
        )
        
        # Verifica o resultado do job
        if job_result.status.state == dataproc_v1.JobStatus.State.DONE:
            logger.info("Teste de conexão concluído com sucesso!")
        else:
            logger.error(f"Teste de conexão falhou com status: {job_result.status.state}")
            logger.error(f"Detalhes: {job_result.status.details}")
        
        # Exclui o cluster se não for para mantê-lo
        if not args.keep_cluster:
            excluir_cluster(
                project_id=args.project_id,
                region=args.region,
                cluster_name=args.cluster_name
            )
        else:
            logger.info(f"Cluster {args.cluster_name} mantido conforme solicitado.")
        
    except Exception as e:
        logger.error(f"Erro durante o teste: {str(e)}")
        raise


if __name__ == "__main__":
    main()
