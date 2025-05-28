#!/usr/bin/env python3
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
db_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"

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
        print(f"Número de registros na tabela: {count}")
        
        # Exibe alguns registros
        print("Exemplos de registros:")
        df.show(5, truncate=False)
        
        print("Tabela bt_animais acessada com sucesso!")
    except Exception as table_error:
        print(f"Erro ao acessar a tabela bt_animais: {table_error}")
        
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
    print(f"Erro ao conectar ao Cloud SQL: {e}")
    sys.exit(1)

# Encerra a sessão Spark
spark.stop()
sys.exit(0)
