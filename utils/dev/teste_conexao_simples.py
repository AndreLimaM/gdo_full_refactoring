#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script simples para testar a conexu00e3o com o banco de dados PostgreSQL.

Este script utiliza a string de conexu00e3o direta para conectar ao banco de dados
e executar consultas simples para verificar o funcionamento da conexu00e3o.
"""

import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor

# Configurau00e7u00e3o de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('teste-conexao-simples')

# String de conexu00e3o direta
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Construir string de conexão a partir de variáveis de ambiente
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_DRIVER = os.getenv('DB_DRIVER', 'postgresql')

CONNECTION_STRING = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Para debug (remover em produção)
# Não exibe a senha real, apenas um placeholder
debug_conn_string = f"{DB_DRIVER}://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"Conectando ao banco de dados: {debug_conn_string}")



def testar_conexao():
    """
    Testa a conexu00e3o com o banco de dados PostgreSQL usando a string de conexu00e3o direta.
    
    Returns:
        bool: True se a conexu00e3o for bem-sucedida, False caso contru00e1rio.
    """
    try:
        logger.info("Conectando ao banco de dados...")
        
        # Conectar ao banco de dados
        conn = psycopg2.connect(CONNECTION_STRING, cursor_factory=RealDictCursor)
        logger.info("Conexu00e3o estabelecida com sucesso!")
        
        # Criar cursor
        with conn.cursor() as cursor:
            # Consulta para obter informau00e7u00f5es do banco de dados
            logger.info("Executando consulta de informau00e7u00f5es do banco...")
            cursor.execute("SELECT current_database(), current_user, version()")
            result = cursor.fetchone()
            logger.info(f"Banco de dados: {result['current_database']}")
            logger.info(f"Usu00e1rio: {result['current_user']}")
            logger.info(f"Versu00e3o: {result['version']}")
            
            # Listar schemas
            logger.info("Listando schemas disponu00edveis...")
            cursor.execute("SELECT schema_name FROM information_schema.schemata")
            schemas = cursor.fetchall()
            logger.info(f"Schemas disponu00edveis: {[s['schema_name'] for s in schemas]}")
            
            # Listar tabelas
            logger.info("Listando tabelas disponu00edveis...")
            cursor.execute(
                "SELECT table_schema, table_name FROM information_schema.tables "
                "WHERE table_schema NOT IN ('pg_catalog', 'information_schema') "
                "ORDER BY table_schema, table_name"
            )
            tables = cursor.fetchall()
            
            if tables:
                logger.info("Tabelas encontradas:")
                for table in tables:
                    logger.info(f"  - {table['table_schema']}.{table['table_name']}")
            else:
                logger.info("Nenhuma tabela encontrada nos schemas de usu00e1rio.")
        
        # Fechar conexu00e3o
        conn.close()
        logger.info("Conexu00e3o fechada.")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao conectar ao banco de dados: {str(e)}")
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
        dest_path = "datalake/dev/utils/teste_conexao_simples.py"
        
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
