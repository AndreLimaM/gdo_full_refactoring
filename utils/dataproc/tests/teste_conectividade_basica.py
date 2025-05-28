#!/usr/bin/env python3

"""
Teste de conectividade básica entre Dataproc e Cloud SQL

Este script realiza testes básicos de conectividade entre um cluster Dataproc
e uma instância Cloud SQL, usando tanto o IP público quanto o IP privado.
"""

from pyspark.sql import SparkSession
import socket
import sys
import subprocess
import time

# Função para executar comandos do sistema e capturar a saída
def executar_comando(comando):
    print(f"Executando: {comando}")
    processo = subprocess.run(comando, shell=True, capture_output=True, text=True)
    print(f"Saída: {processo.stdout}")
    if processo.stderr:
        print(f"Erro: {processo.stderr}")
    return processo

# Função para testar conectividade TCP básica
def testar_conectividade_tcp(host, porta, descricao):
    print(f"\n=== Testando conectividade TCP com {descricao} ({host}:{porta}) ===")
    try:
        # Criar socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(10)  # Timeout de 10 segundos
        
        # Tentar conectar
        inicio = time.time()
        resultado = s.connect_ex((host, porta))
        fim = time.time()
        
        # Verificar resultado
        if resultado == 0:
            print(f"✅ Conexão bem-sucedida! Tempo: {fim - inicio:.2f} segundos")
        else:
            print(f"❌ Falha na conexão. Código de erro: {resultado}")
            
        # Fechar socket
        s.close()
    except Exception as e:
        print(f"❌ Erro ao testar conexão: {e}")

# Informações do ambiente
print("\n=== Informações do Ambiente ===")
executar_comando("hostname -I")  # IPs do host
executar_comando("ip route")     # Rotas de rede

# Testar conectividade com o Cloud SQL
print("\n=== Testes de Conectividade ===")

# IP público do Cloud SQL
testar_conectividade_tcp("34.48.11.43", 5432, "IP público do Cloud SQL")

# IP privado do Cloud SQL
testar_conectividade_tcp("10.98.169.3", 5432, "IP privado do Cloud SQL")

# Testar outros serviços para referência
testar_conectividade_tcp("8.8.8.8", 53, "DNS do Google (referência externa)")

# Iniciar sessão Spark
print("\n=== Iniciando Sessão Spark ===")
spark = SparkSession.builder \
    .appName("TesteConectividadeCloudSQL") \
    .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar") \
    .getOrCreate()

# Configurações de conexão com o banco de dados
print("\n=== Tentando Conexão JDBC ===")

# Tentar com IP público
db_host_publico = "34.48.11.43"
db_name = "db_eco_tcbf_25"
db_user = "db_eco_tcbf_25_user"
db_password = "5HN33PHKjXcLTz3tBC"
db_url_publico = f"jdbc:postgresql://{db_host_publico}:5432/{db_name}"

print(f"Tentando conexão JDBC com IP público: {db_url_publico}")
try:
    # Consulta simples para testar conexão
    df_publico = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", db_url_publico) \
        .option("dbtable", "(SELECT 1 as teste) AS test") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("connectTimeout", "10") \
        .load()
    
    print("✅ Conexão JDBC com IP público bem-sucedida!")
    df_publico.show()
except Exception as e:
    print(f"❌ Erro na conexão JDBC com IP público: {e}")

# Tentar com IP privado
db_host_privado = "10.98.169.3"
db_url_privado = f"jdbc:postgresql://{db_host_privado}:5432/{db_name}"

print(f"\nTentando conexão JDBC com IP privado: {db_url_privado}")
try:
    # Consulta simples para testar conexão
    df_privado = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", db_url_privado) \
        .option("dbtable", "(SELECT 1 as teste) AS test") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("connectTimeout", "10") \
        .load()
    
    print("✅ Conexão JDBC com IP privado bem-sucedida!")
    df_privado.show()
except Exception as e:
    print(f"❌ Erro na conexão JDBC com IP privado: {e}")

# Encerrar sessão Spark
spark.stop()
print("\n=== Teste concluído ===")
sys.exit(0)
