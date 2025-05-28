#!/usr/bin/env python3

"""
Script para processar arquivos JSON conforme mapeamento específico de campos

Este script processa arquivos JSON da pasta 'pending', grava os dados em uma tabela
respeitando o mapeamento de campos fornecido e move os arquivos processados para
a pasta 'done' ou 'error' em caso de problemas.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, DateType, BooleanType
import argparse
import os
import sys
import logging
import json
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('processar-mapeamento-json')

def parse_arguments():
    """
    Analisa os argumentos da linha de comando.
    """
    parser = argparse.ArgumentParser(description='Processa arquivos JSON conforme mapeamento específico')
    parser.add_argument('--input-path', required=True, help='Caminho GCS para os arquivos JSON de entrada (pasta pending)')
    parser.add_argument('--output-done-path', required=True, help='Caminho GCS para os arquivos processados com sucesso (pasta done)')
    parser.add_argument('--output-error-path', required=True, help='Caminho GCS para os arquivos com erro (pasta error)')
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
        .appName("ProcessarMapeamentoJSON") \
        .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar") \
        .getOrCreate()
    
    # Configurar o nível de log para reduzir a verbosidade
    spark.sparkContext.setLogLevel("WARN")
    
    return spark

def definir_schema_json():
    """
    Define o schema para os dados nos arquivos JSON conforme o mapeamento.
    """
    return StructType([
        # Campos do JSON conforme mapeamento
        StructField("json_certificado", StringType(), True),
        StructField("json_classificacao_ia", StringType(), True),
        StructField("json_caracteristicas", StringType(), True),
        StructField("habilitacoes_etiqueta", StringType(), True),
        StructField("dt_nascimento", StringType(), True),
        StructField("valor_ph", DoubleType(), True),
        StructField("dt_fechamento_camera_abate", TimestampType(), True),
        StructField("dt_abertura_camera_abate", TimestampType(), True),
        StructField("dt_compra", DateType(), True),
        StructField("dt_abate", DateType(), True),
        StructField("lote_abate", IntegerType(), True),
        StructField("id_destino_abate", IntegerType(), True),
        StructField("motivos_dif", StringType(), True),
        StructField("nr_sequencial", IntegerType(), True),
        StructField("nr_banda", IntegerType(), True),
        StructField("peso_vivo", DoubleType(), True),
        StructField("peso_carcaca", DoubleType(), True),
        StructField("hr_ultima_pesagem", TimestampType(), True),
        StructField("flag_contusao", BooleanType(), True),
        StructField("cnpj_industria_abate", StringType(), True),
        StructField("tipo_unidade_abate", StringType(), True),
        StructField("nr_unidade_abate", StringType(), True),
        StructField("prod_cpf_cnpj", StringType(), True),
        StructField("prod_nr_ie", StringType(), True),
        StructField("sexo", StringType(), True),
        StructField("nr_identificacao", StringType(), True),
        StructField("nr_chip", StringType(), True),
        StructField("cod_barra_abate", StringType(), True)
    ])

def ler_arquivos_json(spark, input_path, schema):
    """
    Lê os arquivos JSON do Google Cloud Storage.
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
    Processa e transforma os dados conforme o mapeamento.
    """
    logger.info("Processando dados conforme mapeamento...")
    
    try:
        # Adicionar campos gerados automaticamente
        df_processado = df.withColumn("created_at", current_timestamp()) \
                          .withColumn("updated_at", current_timestamp()) \
                          .withColumn("token_cliente", lit("24ad9d"))
        
        # Tratamento para campos condicionais
        df_processado = df_processado.withColumn(
            "json_habilitacoes",
            when(col("habilitacoes_etiqueta").isNotNull(), col("habilitacoes_etiqueta")).otherwise(lit("{}"))
        )
        
        # Contar registros após processamento
        count = df_processado.count()
        logger.info(f"Total de registros após processamento: {count}")
        
        return df_processado
    except Exception as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        raise

def criar_tabela_mapeada(spark, db_properties, table_name):
    """
    Cria a tabela conforme o mapeamento se ela não existir.
    """
    logger.info(f"Verificando/criando tabela {table_name}...")
    
    try:
        # SQL para criar a tabela conforme o mapeamento
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            json_certificado jsonb,
            json_classificacao_ia jsonb,
            json_caracteristicas jsonb,
            json_habilitacoes jsonb,
            dt_nascimento date,
            valor_ph numeric,
            dt_fechamento_camera_abate timestamp with time zone,
            dt_abertura_camera_abate timestamp with time zone,
            created_at timestamp with time zone,
            updated_at timestamp with time zone,
            dt_compra date,
            dt_abate date,
            nr_lote_abate integer,
            id_destino_abate integer,
            json_motivos_dif jsonb,
            nr_sequencial integer,
            nr_banda smallint,
            peso_vivo numeric,
            peso_carcaca numeric,
            hr_ultima_pesagem timestamp with time zone,
            flag_contusao smallint,
            token_cliente character varying,
            cnpj_industria_abate character varying,
            tipo_unidade_abate character varying,
            nr_unidade_abate character varying,
            prod_cpf_cnpj character varying,
            prod_nr_ie character varying,
            sexo character,
            nr_identificacao text,
            nr_chip character varying,
            codigo_barra_etiqueta character varying
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
        
        logger.info(f"Tabela {table_name} verificada/criada com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar tabela {table_name}: {str(e)}")
        return False

def gravar_no_cloud_sql(df, db_properties, table_name, mode):
    """
    Grava os dados processados no Cloud SQL.
    """
    logger.info(f"Gravando dados na tabela {table_name} com modo {mode}...")
    
    try:
        # Mapear colunas do DataFrame para colunas da tabela
        df_final = df.select(
            col("json_certificado"),
            col("json_classificacao_ia"),
            col("json_caracteristicas"),
            col("json_habilitacoes"),
            col("dt_nascimento"),
            col("valor_ph"),
            col("dt_fechamento_camera_abate"),
            col("dt_abertura_camera_abate"),
            col("created_at"),
            col("updated_at"),
            col("dt_compra"),
            col("dt_abate"),
            col("lote_abate").alias("nr_lote_abate"),
            col("id_destino_abate"),
            col("motivos_dif").alias("json_motivos_dif"),
            col("nr_sequencial"),
            col("nr_banda"),
            col("peso_vivo"),
            col("peso_carcaca"),
            col("hr_ultima_pesagem"),
            col("flag_contusao"),
            col("token_cliente"),
            col("cnpj_industria_abate"),
            col("tipo_unidade_abate"),
            col("nr_unidade_abate"),
            col("prod_cpf_cnpj"),
            col("prod_nr_ie"),
            col("sexo"),
            col("nr_identificacao"),
            col("nr_chip"),
            col("cod_barra_abate").alias("codigo_barra_etiqueta")
        )
        
        # Gravar no PostgreSQL usando JDBC
        df_final.write \
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
        return True
    except Exception as e:
        logger.error(f"Erro ao gravar dados no Cloud SQL: {str(e)}")
        return False

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

def mover_arquivos(spark, input_path, output_done_path, output_error_path, arquivos_processados, arquivos_com_erro):
    """
    Move os arquivos processados para as pastas done ou error.
    """
    logger.info("Movendo arquivos processados...")
    
    # Usar o Hadoop FileSystem para operações de arquivo
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    
    for arquivo in arquivos_processados:
        origem = spark._jvm.org.apache.hadoop.fs.Path(f"{input_path}/{arquivo}")
        destino = spark._jvm.org.apache.hadoop.fs.Path(f"{output_done_path}/{arquivo}")
        
        if fs.exists(origem):
            # Verificar se o arquivo de destino já existe e excluí-lo se necessário
            if fs.exists(destino):
                fs.delete(destino, False)
            
            # Mover o arquivo
            fs.rename(origem, destino)
            logger.info(f"Arquivo {arquivo} movido para a pasta done")
    
    for arquivo in arquivos_com_erro:
        origem = spark._jvm.org.apache.hadoop.fs.Path(f"{input_path}/{arquivo}")
        destino = spark._jvm.org.apache.hadoop.fs.Path(f"{output_error_path}/{arquivo}")
        
        if fs.exists(origem):
            # Verificar se o arquivo de destino já existe e excluí-lo se necessário
            if fs.exists(destino):
                fs.delete(destino, False)
            
            # Mover o arquivo
            fs.rename(origem, destino)
            logger.info(f"Arquivo {arquivo} movido para a pasta error")

def listar_arquivos_json(spark, input_path):
    """
    Lista os arquivos JSON no diretório de entrada.
    """
    # Usar o Hadoop FileSystem para listar arquivos
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(input_path)
    
    arquivos = []
    if fs.exists(path):
        status = fs.listStatus(path)
        for fileStatus in status:
            if not fileStatus.isDirectory() and fileStatus.getPath().getName().lower().endswith('.json'):
                arquivos.append(fileStatus.getPath().getName())
    
    return arquivos

def processar_arquivo(spark, input_path, arquivo, schema, db_properties, table_name, mode):
    """
    Processa um único arquivo JSON.
    """
    logger.info(f"Processando arquivo: {arquivo}")
    
    try:
        # Ler o arquivo JSON
        df = spark.read.schema(schema).json(f"{input_path}/{arquivo}")
        
        if df.count() == 0:
            logger.warning(f"Arquivo {arquivo} está vazio ou não contém dados válidos")
            return False
        
        # Processar os dados
        df_processado = processar_dados(df)
        
        # Gravar no Cloud SQL
        sucesso = gravar_no_cloud_sql(df_processado, db_properties, table_name, mode)
        
        return sucesso
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {arquivo}: {str(e)}")
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
        # Criar tabela se não existir
        if not criar_tabela_mapeada(spark, db_properties, args.table_name):
            logger.error(f"Não foi possível criar/verificar a tabela {args.table_name}. Abortando.")
            spark.stop()
            sys.exit(1)
        
        # Listar arquivos JSON no diretório de entrada
        arquivos = listar_arquivos_json(spark, args.input_path)
        logger.info(f"Encontrados {len(arquivos)} arquivos JSON para processamento")
        
        if not arquivos:
            logger.warning("Nenhum arquivo JSON encontrado para processamento")
            spark.stop()
            sys.exit(0)
        
        # Definir schema para os dados
        schema = definir_schema_json()
        
        # Processar cada arquivo individualmente
        arquivos_processados = []
        arquivos_com_erro = []
        
        for arquivo in arquivos:
            sucesso = processar_arquivo(
                spark, args.input_path, arquivo, schema, 
                db_properties, args.table_name, args.mode
            )
            
            if sucesso:
                arquivos_processados.append(arquivo)
            else:
                arquivos_com_erro.append(arquivo)
        
        # Mover arquivos para as pastas done ou error
        mover_arquivos(
            spark, args.input_path, args.output_done_path, 
            args.output_error_path, arquivos_processados, arquivos_com_erro
        )
        
        logger.info(f"Processamento concluído. Arquivos processados: {len(arquivos_processados)}, Arquivos com erro: {len(arquivos_com_erro)}")
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        sys.exit(1)
    finally:
        # Encerrar a sessão Spark
        spark.stop()

if __name__ == "__main__":
    main()
