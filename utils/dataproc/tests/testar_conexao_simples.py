#!/usr/bin/env python3

"""
Script simples para testar a conexu00e3o entre o Dataproc e o Cloud SQL
"""

from pyspark.sql import SparkSession
import socket
import sys
import time

# Iniciar sessu00e3o Spark
print("=== Iniciando Sessu00e3o Spark ===")
spark = SparkSession.builder \
    .appName("TesteConexaoCloudSQL") \
    .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar") \
    .getOrCreate()

# Configurau00e7u00f5es de conexu00e3o com o banco de dados
db_host = "34.48.11.43"  # IP pu00fablico do Cloud SQL
db_name = "db_eco_tcbf_25"
db_user = "db_eco_tcbf_25_user"
db_password = "5HN33PHKjXcLTz3tBC"
db_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"

# Testar conectividade TCP bu00e1sica
print("\n=== Testando conectividade TCP com o Cloud SQL ===")
try:
    # Criar socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)  # Timeout de 10 segundos
    
    # Tentar conectar
    inicio = time.time()
    print(f"Tentando conectar a {db_host}:5432...")
    resultado = s.connect_ex((db_host, 5432))
    fim = time.time()
    
    # Verificar resultado
    if resultado == 0:
        print(f"Conexu00e3o TCP bem-sucedida! Tempo: {fim - inicio:.2f} segundos")
    else:
        print(f"Falha na conexu00e3o TCP. Cu00f3digo de erro: {resultado}")
        
    # Fechar socket
    s.close()
except Exception as e:
    print(f"Erro ao testar conexu00e3o TCP: {e}")

# Testar conexu00e3o JDBC
print("\n=== Testando conexu00e3o JDBC com o Cloud SQL ===")
try:
    # Consulta simples para testar conexu00e3o
    print(f"Tentando conexu00e3o JDBC com: {db_url}")
    inicio = time.time()
    test_df = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", db_url) \
        .option("dbtable", "(SELECT 1 as teste) AS test") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("connectTimeout", "10") \
        .load()
    fim = time.time()
    
    print(f"Conexu00e3o JDBC bem-sucedida! Tempo: {fim - inicio:.2f} segundos")
    print("Resultado da consulta:")
    test_df.show()
    
    # Listar tabelas disponíveis
    print("\n=== Listando tabelas disponíveis no banco de dados ===")
    tables_df = spark.read \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", db_url) \
        .option("dbtable", "(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') AS tables") \
        .option("user", db_user) \
        .option("password", db_password) \
        .load()
    
    print("Tabelas disponíveis:")
    tables_df.show(100, truncate=False)
    
except Exception as e:
    print(f"Erro na conexu00e3o JDBC: {e}")

# Encerrar sessu00e3o Spark
spark.stop()
print("\n=== Teste concluído ===")
sys.exit(0)
