#!/usr/bin/env python3

"""
Script para processar arquivos JSON e gravar os dados no Cloud SQL

Este script processa arquivos JSON armazenados no Google Cloud Storage,
transforma os dados e os grava em uma tabela PostgreSQL no Cloud SQL.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType
import argparse
import os
import sys
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('processar-json-sql')

def parse_arguments():
    """
    Analisa os argumentos da linha de comando.
    """
    parser = argparse.ArgumentParser(description='Processa arquivos JSON e grava no Cloud SQL')
    parser.add_argument('--input-path', required=True, help='Caminho GCS para os arquivos JSON de entrada')
    parser.add_argument('--db-host', required=True, help='Host do Cloud SQL (IP pu00fablico ou privado)')
    parser.add_argument('--db-name', required=True, help='Nome do banco de dados')
    parser.add_argument('--db-user', required=True, help='Usuu00e1rio do banco de dados')
    parser.add_argument('--db-password', required=True, help='Senha do banco de dados')
    parser.add_argument('--table-name', required=True, help='Nome da tabela para gravar os dados')
    
    return parser.parse_args()

def criar_spark_session():
    """
    Cria e configura a sessu00e3o Spark.
    """
    spark = SparkSession.builder \
        .appName("ProcessarJSONParaSQL") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar") \
        .getOrCreate()
    
    # Configurar o nu00edvel de log para reduzir a verbosidade
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def definir_schema_animal():
    """
    Define o schema para os dados de animais nos arquivos JSON.
    Ajuste este schema de acordo com a estrutura real dos seus arquivos JSON.
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("identificacao", StringType(), True),
        StructField("data_nascimento", TimestampType(), True),
        StructField("sexo", StringType(), True),
        StructField("raca", StringType(), True),
        StructField("peso", DoubleType(), True),
        StructField("propriedade_id", StringType(), True),
        StructField("status", StringType(), True),
        StructField("data_cadastro", TimestampType(), True),
        StructField("data_atualizacao", TimestampType(), True)
    ])

def ler_arquivos_json(spark, input_path, schema):
    """
    Lu00ea os arquivos JSON do Google Cloud Storage.
    """
    logger.info(f"Lendo arquivos JSON de: {input_path}")
    
    try:
        # Ler arquivos JSON com o schema definido
        df = spark.read.schema(schema).json(input_path)
        
        # Mostrar o schema e alguns exemplos
        logger.info("Schema dos dados:")
        df.printSchema()
        
        logger.info("Exemplos de dados:")
        df.show(5, truncate=False)
        
        return df
    except Exception as e:
        logger.error(f"Erro ao ler arquivos JSON: {str(e)}")
        raise

def processar_dados(df):
    """
    Processa e transforma os dados conforme necessu00e1rio.
    Ajuste esta funu00e7u00e3o de acordo com suas necessidades especu00edficas de processamento.
    """
    logger.info("Processando dados...")
    
    try:
        # Exemplo de transformau00e7u00f5es
        # 1. Remover registros com campos obrigatu00f3rios nulos
        df_processado = df.filter(col("id").isNotNull() & col("identificacao").isNotNull())
        
        # 2. Converter campos, se necessu00e1rio
        # df_processado = df_processado.withColumn("peso", col("peso").cast("double"))
        
        # 3. Adicionar campos calculados ou constantes, se necessu00e1rio
        df_processado = df_processado.withColumn("processado_em", lit("now()"))
        
        # Contar registros apu00f3s processamento
        count = df_processado.count()
        logger.info(f"Total de registros apu00f3s processamento: {count}")
        
        return df_processado
    except Exception as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        raise

def gravar_no_cloud_sql(df, db_properties, table_name):
    """
    Grava os dados processados no Cloud SQL.
    """
    logger.info(f"Gravando dados na tabela {table_name}...")
    
    try:
        # Gravar no PostgreSQL usando JDBC
        df.write \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", db_properties["url"]) \
            .option("dbtable", table_name) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("truncate", "false") \
            .option("batchsize", 1000) \
            .mode("append") \
            .save()
        
        logger.info(f"Dados gravados com sucesso na tabela {table_name}")
    except Exception as e:
        logger.error(f"Erro ao gravar dados no Cloud SQL: {str(e)}")
        raise

def testar_conexao_sql(spark, db_properties):
    """
    Testa a conexu00e3o com o Cloud SQL antes de processar os dados.
    """
    logger.info("Testando conexu00e3o com o Cloud SQL...")
    
    try:
        # Executar uma consulta simples para testar a conexu00e3o
        test_df = spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", db_properties["url"]) \
            .option("dbtable", "(SELECT 1 as teste) AS test") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .load()
        
        test_df.show()
        logger.info("Conexu00e3o com o Cloud SQL estabelecida com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Erro ao conectar ao Cloud SQL: {str(e)}")
        return False

def main():
    # Analisar argumentos
    args = parse_arguments()
    
    # Criar sessu00e3o Spark
    spark = criar_spark_session()
    
    # Configurar propriedades de conexu00e3o com o banco de dados
    db_properties = {
        "url": f"jdbc:postgresql://{args.db_host}:5432/{args.db_name}",
        "user": args.db_user,
        "password": args.db_password
    }
    
    # Testar conexu00e3o com o Cloud SQL
    if not testar_conexao_sql(spark, db_properties):
        logger.error("Nu00e3o foi possu00edvel estabelecer conexu00e3o com o Cloud SQL. Abortando.")
        spark.stop()
        sys.exit(1)
    
    try:
        # Definir schema para os dados
        schema = definir_schema_animal()
        
        # Ler arquivos JSON
        df = ler_arquivos_json(spark, args.input_path, schema)
        
        # Processar dados
        df_processado = processar_dados(df)
        
        # Gravar no Cloud SQL
        gravar_no_cloud_sql(df_processado, db_properties, args.table_name)
        
        logger.info("Processamento concluu00eddo com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        sys.exit(1)
    finally:
        # Encerrar a sessu00e3o Spark
        spark.stop()

if __name__ == "__main__":
    main()
