#!/usr/bin/env python3

"""
Script para processar arquivos JSON de animais e gravar os dados no Cloud SQL

Este script processa arquivos JSON de animais armazenados no Google Cloud Storage,
transforma os dados e os grava em uma tabela PostgreSQL no Cloud SQL.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, lit, to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, TimestampType, BooleanType
import argparse
import os
import sys
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('processar-animais-sql')

def parse_arguments():
    """
    Analisa os argumentos da linha de comando.
    """
    parser = argparse.ArgumentParser(description='Processa arquivos JSON de animais e grava no Cloud SQL')
    parser.add_argument('--input-path', required=True, help='Caminho GCS para os arquivos JSON de entrada')
    parser.add_argument('--db-host', required=True, help='Host do Cloud SQL (IP público ou privado)')
    parser.add_argument('--db-name', required=True, help='Nome do banco de dados')
    parser.add_argument('--db-user', required=True, help='Usuário do banco de dados')
    parser.add_argument('--db-password', required=True, help='Senha do banco de dados')
    parser.add_argument('--table-name', required=True, help='Nome da tabela para gravar os dados')
    parser.add_argument('--mode', default='append', choices=['append', 'overwrite'], 
                        help='Modo de gravação: append (adicionar) ou overwrite (sobrescrever)')
    
    return parser.parse_args()

def criar_spark_session():
    """
    Cria e configura a sessão Spark.
    """
    spark = SparkSession.builder \
        .appName("ProcessarAnimaisParaSQL") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar") \
        .getOrCreate()
    
    # Configurar o nível de log para reduzir a verbosidade
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def definir_schema_dados_adicionais():
    """
    Define o schema para os dados adicionais do animal.
    """
    return StructType([
        StructField("vacinado", BooleanType(), True),
        StructField("vacinas", ArrayType(
            StructType([
                StructField("tipo", StringType(), True),
                StructField("data", StringType(), True)
            ])
        ), True),
        StructField("observacoes", StringType(), True)
    ])

def definir_schema_animal():
    """
    Define o schema para os dados de animais nos arquivos JSON.
    """
    return StructType([
        StructField("id_animal", StringType(), True),
        StructField("data_nascimento", StringType(), True),
        StructField("id_propriedade", StringType(), True),
        StructField("sexo", StringType(), True),
        StructField("raca", StringType(), True),
        StructField("peso_nascimento", DoubleType(), True),
        StructField("data_entrada", StringType(), True),
        StructField("data_saida", StringType(), True),
        StructField("status", StringType(), True),
        StructField("dados_adicionais", definir_schema_dados_adicionais(), True)
    ])

def ler_arquivos_json(spark, input_path, schema):
    """
    Lê os arquivos JSON do Google Cloud Storage.
    """
    logger.info(f"Lendo arquivos JSON de animais de: {input_path}")
    
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
    Processa e transforma os dados conforme necessário.
    """
    logger.info("Processando dados de animais...")
    
    try:
        # 1. Remover registros com campos obrigatórios nulos
        df_processado = df.filter(
            col("id_animal").isNotNull() & 
            col("id_propriedade").isNotNull()
        )
        
        # 2. Converter campos de data para timestamp
        df_processado = df_processado \
            .withColumn("data_nascimento", to_timestamp(col("data_nascimento"), "yyyy-MM-dd")) \
            .withColumn("data_entrada", to_timestamp(col("data_entrada"), "yyyy-MM-dd")) \
            .withColumn("data_saida", to_timestamp(col("data_saida"), "yyyy-MM-dd"))
        
        # 3. Adicionar campo de processamento
        df_processado = df_processado.withColumn("data_processamento", current_timestamp())
        
        # 4. Extrair informações de vacinas para uma coluna separada
        df_processado = df_processado.withColumn(
            "vacinas_info", 
            col("dados_adicionais.vacinas").cast("string")
        )
        
        # Contar registros após processamento
        count = df_processado.count()
        logger.info(f"Total de registros após processamento: {count}")
        
        return df_processado
    except Exception as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        raise

def criar_tabela_animais(spark, db_properties):
    """
    Cria a tabela de animais no Cloud SQL se ela não existir.
    """
    logger.info("Verificando/criando tabela de animais...")
    
    try:
        # SQL para criar a tabela
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS animais (
            id SERIAL PRIMARY KEY,
            id_animal VARCHAR(50) NOT NULL,
            data_nascimento TIMESTAMP,
            id_propriedade VARCHAR(50) NOT NULL,
            sexo VARCHAR(1),
            raca VARCHAR(100),
            peso_nascimento DOUBLE PRECISION,
            data_entrada TIMESTAMP,
            data_saida TIMESTAMP,
            status VARCHAR(20),
            vacinas_info TEXT,
            dados_adicionais JSONB,
            data_processamento TIMESTAMP
        )
        """
        
        # Executar a criação da tabela
        spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", db_properties["url"]) \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .option("query", create_table_sql) \
            .load()
        
        logger.info("Tabela de animais verificada/criada com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela de animais: {str(e)}")
        return False

def gravar_no_cloud_sql(df, db_properties, table_name, mode):
    """
    Grava os dados processados no Cloud SQL.
    """
    logger.info(f"Gravando dados na tabela {table_name} com modo {mode}...")
    
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
            .mode(mode) \
            .save()
        
        logger.info(f"Dados gravados com sucesso na tabela {table_name}")
    except Exception as e:
        logger.error(f"Erro ao gravar dados no Cloud SQL: {str(e)}")
        raise

def testar_conexao_sql(spark, db_properties):
    """
    Testa a conexão com o Cloud SQL antes de processar os dados.
    """
    logger.info("Testando conexão com o Cloud SQL...")
    
    try:
        # Executar uma consulta simples para testar a conexão
        test_df = spark.read \
            .format("jdbc") \
            .option("driver", "org.postgresql.Driver") \
            .option("url", db_properties["url"]) \
            .option("dbtable", "(SELECT 1 as teste) AS test") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .load()
        
        test_df.show()
        logger.info("Conexão com o Cloud SQL estabelecida com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Erro ao conectar ao Cloud SQL: {str(e)}")
        return False

def main():
    # Analisar argumentos
    args = parse_arguments()
    
    # Criar sessão Spark
    spark = criar_spark_session()
    
    # Configurar propriedades de conexão com o banco de dados
    db_properties = {
        "url": f"jdbc:postgresql://{args.db_host}:5432/{args.db_name}",
        "user": args.db_user,
        "password": args.db_password
    }
    
    # Testar conexão com o Cloud SQL
    if not testar_conexao_sql(spark, db_properties):
        logger.error("Não foi possível estabelecer conexão com o Cloud SQL. Abortando.")
        spark.stop()
        sys.exit(1)
    
    try:
        # Criar tabela de animais se não existir
        if not criar_tabela_animais(spark, db_properties):
            logger.error("Não foi possível criar/verificar a tabela de animais. Abortando.")
            spark.stop()
            sys.exit(1)
        
        # Definir schema para os dados
        schema = definir_schema_animal()
        
        # Ler arquivos JSON
        df = ler_arquivos_json(spark, args.input_path, schema)
        
        # Processar dados
        df_processado = processar_dados(df)
        
        # Gravar no Cloud SQL
        gravar_no_cloud_sql(df_processado, db_properties, args.table_name, args.mode)
        
        logger.info("Processamento concluído com sucesso!")
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        sys.exit(1)
    finally:
        # Encerrar a sessão Spark
        spark.stop()

if __name__ == "__main__":
    main()
