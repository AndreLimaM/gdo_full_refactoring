#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script para execução do processamento de dados de animais no Dataproc.

Este script é o ponto de entrada para o job Dataproc que processa
arquivos JSON de animais e insere os dados no banco de dados PostgreSQL.
"""

import os
import argparse
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Importa o processador de animais
import sys
sys.path.append('/home/andre/CascadeProjects/gdo_full_refactoring')
from src.processors.animais.process_animais import process_directory

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """
    Analisa os argumentos da linha de comando.
    
    Returns:
        Objeto com os argumentos analisados
    """
    parser = argparse.ArgumentParser(description='Processamento de dados de animais')
    
    parser.add_argument(
        '--input-dir',
        required=True,
        help='Diretório com arquivos JSON de entrada'
    )
    
    parser.add_argument(
        '--done-dir',
        required=True,
        help='Diretório para arquivos processados com sucesso'
    )
    
    parser.add_argument(
        '--error-dir',
        required=True,
        help='Diretório para arquivos com erro de processamento'
    )
    
    parser.add_argument(
        '--db-host',
        required=True,
        help='Host do banco de dados PostgreSQL'
    )
    
    parser.add_argument(
        '--db-name',
        required=True,
        help='Nome do banco de dados'
    )
    
    parser.add_argument(
        '--db-user',
        required=True,
        help='Usuário do banco de dados'
    )
    
    parser.add_argument(
        '--db-password',
        required=True,
        help='Senha do banco de dados'
    )
    
    return parser.parse_args()


def create_spark_session():
    """
    Cria e configura uma sessão Spark.
    
    Returns:
        Sessão Spark configurada
    """
    # Configuração do Spark
    conf = SparkConf()
    conf.set("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    
    # Cria a sessão Spark
    spark = SparkSession.builder \
        .appName("ProcessamentoAnimais") \
        .config(conf=conf) \
        .getOrCreate()
    
    return spark


def main():
    """
    Função principal do script de processamento.
    """
    start_time = datetime.now()
    logger.info(f"Iniciando processamento de animais em {start_time}")
    
    # Parse argumentos
    args = parse_arguments()
    
    # Configuração do banco de dados
    db_config = {
        "host": args.db_host,
        "database": args.db_name,
        "user": args.db_user,
        "password": args.db_password
    }
    
    # Cria sessão Spark
    spark = create_spark_session()
    
    try:
        # Processa os arquivos
        stats = process_directory(
            spark=spark,
            input_dir=args.input_dir,
            done_dir=args.done_dir,
            error_dir=args.error_dir,
            db_config=db_config
        )
        
        # Log das estatísticas
        logger.info(f"Processamento concluído. Estatísticas:")
        logger.info(f"  Total de arquivos: {stats['total']}")
        logger.info(f"  Arquivos processados com sucesso: {stats['success']}")
        logger.info(f"  Arquivos com erro: {stats['error']}")
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {str(e)}")
        raise
    finally:
        # Encerra a sessão Spark
        spark.stop()
    
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Processamento finalizado em {end_time}. Duração: {duration}")


if __name__ == "__main__":
    main()
