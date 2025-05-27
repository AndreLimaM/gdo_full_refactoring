#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para testar a conexu00e3o com o banco de dados Cloud SQL.

Este script utiliza as credenciais configuradas no arquivo config.yaml
para estabelecer uma conexu00e3o com o banco de dados e executar uma consulta
simples para verificar se a conexu00e3o estu00e1 funcionando corretamente.
"""

import os
import sys
import logging
from db_connector import CloudSQLConnector
from config.config_manager import ConfigManager

# Configurau00e7u00e3o de logging
config = ConfigManager()
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging_level = config.get('logging.level', 'INFO')
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('testar-conexao-db')


def testar_conexao():
    """
    Testa a conexu00e3o com o banco de dados Cloud SQL.
    
    Returns:
        bool: True se a conexu00e3o for bem-sucedida, False caso contru00e1rio.
    """
    try:
        logger.info("Inicializando conector do banco de dados...")
        db_connector = CloudSQLConnector()
        
        # Verificar conexu00e3o
        if db_connector.check_connection():
            logger.info("Conexu00e3o com o banco de dados estabelecida com sucesso!")
            
            # Exibir informau00e7u00f5es de conexu00e3o
            connection_info = db_connector.get_connection_info()
            logger.info(f"Tipo de banco: {connection_info['db_type']}")
            logger.info(f"Banco de dados: {connection_info['database']}")
            logger.info(f"Tabelas disponu00edveis: {connection_info.get('tables', [])}")
            
            # Executar uma consulta simples
            try:
                logger.info("Executando consulta de teste...")
                result = db_connector.execute_query("SELECT current_database(), current_user, version()")
                logger.info(f"Resultado da consulta: {result}")
                
                # Listar schemas
                logger.info("Listando schemas disponu00edveis...")
                schemas = db_connector.execute_query("SELECT schema_name FROM information_schema.schemata")
                logger.info(f"Schemas disponu00edveis: {[s['schema_name'] for s in schemas]}")
                
                # Listar tabelas
                logger.info("Listando tabelas disponu00edveis...")
                tables = db_connector.execute_query(
                    "SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                    "ORDER BY table_schema, table_name"
                )
                
                if tables:
                    logger.info("Tabelas encontradas:")
                    for table in tables:
                        logger.info(f"  - {table['table_schema']}.{table['table_name']}")
                else:
                    logger.info("Nenhuma tabela encontrada.")
                
                return True
            except Exception as e:
                logger.error(f"Erro ao executar consulta: {str(e)}")
                return False
        else:
            logger.error("Falha ao conectar com o banco de dados.")
            return False
    except Exception as e:
        logger.error(f"Erro ao inicializar o conector: {str(e)}")
        return False


def upload_to_dev_folder():
    """
    Faz upload do script para a pasta de desenvolvimento no bucket GCS.
    """
    try:
        from gcs_connector import WindsurfGCSConnector
        
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Caminho de destino no bucket
        dest_path = "datalake/dev/utils/testar_conexao_db.py"
        
        # Fazer upload do arquivo
        connector.upload_file(__file__, dest_path)
        logger.info(f"Script enviado para {dest_path}")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao fazer upload do script: {str(e)}")
        return False


if __name__ == "__main__":
    # Verificar se deve fazer upload do script
    if len(sys.argv) > 1 and sys.argv[1] == "--upload":
        upload_to_dev_folder()
    
    # Testar conexu00e3o
    sucesso = testar_conexao()
    
    if sucesso:
        logger.info("Teste de conexu00e3o concluu00eddo com sucesso!")
        sys.exit(0)
    else:
        logger.error("Teste de conexu00e3o falhou.")
        sys.exit(1)
