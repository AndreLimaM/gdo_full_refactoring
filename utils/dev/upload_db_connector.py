#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para fazer upload do conector de banco de dados para o bucket GCS.

Este script envia o arquivo db_connector.py atualizado para a pasta
datalake/code/common/utils/ no bucket GCS, mantendo a estrutura de pastas
que definimos para o projeto.
"""

import os
import sys
import logging
from gcs_connector import WindsurfGCSConnector

# Configurau00e7u00e3o de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('upload-db-connector')

# Caminho do arquivo local
LOCAL_FILE = os.path.join(os.path.dirname(__file__), 'db_connector.py')

# Caminho de destino no bucket
DEST_PATH = 'datalake/code/common/utils/db_connector.py'


def upload_db_connector():
    """
    Faz upload do conector de banco de dados para o bucket GCS.
    
    Returns:
        bool: True se o upload for bem-sucedido, False caso contru00e1rio.
    """
    try:
        # Verificar se o arquivo existe
        if not os.path.exists(LOCAL_FILE):
            logger.error(f"Arquivo {LOCAL_FILE} nu00e3o encontrado.")
            return False
        
        # Inicializar o conector GCS
        logger.info("Inicializando conector GCS...")
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Fazer upload do arquivo
        logger.info(f"Enviando {LOCAL_FILE} para {DEST_PATH}...")
        connector.upload_file(LOCAL_FILE, DEST_PATH)
        logger.info(f"Upload concluído com sucesso!")
        
        # Verificar se o arquivo foi enviado
        if connector.file_exists(DEST_PATH):
            logger.info(f"Arquivo confirmado no bucket: {DEST_PATH}")
            return True
        else:
            logger.error(f"Não foi possível confirmar o arquivo no bucket: {DEST_PATH}")
            return False
    
    except Exception as e:
        logger.error(f"Erro ao fazer upload do arquivo: {str(e)}")
        return False


def upload_config_file():
    """
    Faz upload do arquivo de configurau00e7u00e3o para o bucket GCS.
    
    Returns:
        bool: True se o upload for bem-sucedido, False caso contru00e1rio.
    """
    try:
        # Caminho do arquivo de configurau00e7u00e3o
        config_file = os.path.join(os.path.dirname(__file__), 'config', 'config.yaml')
        
        # Verificar se o arquivo existe
        if not os.path.exists(config_file):
            logger.error(f"Arquivo de configurau00e7u00e3o {config_file} nu00e3o encontrado.")
            return False
        
        # Inicializar o conector GCS
        logger.info("Inicializando conector GCS...")
        connector = WindsurfGCSConnector()
        
        # Caminho de destino no bucket
        dest_path = 'datalake/code/config/config.yaml'
        
        # Fazer upload do arquivo
        logger.info(f"Enviando {config_file} para {dest_path}...")
        connector.upload_file(config_file, dest_path)
        logger.info(f"Upload do arquivo de configurau00e7u00e3o concluído com sucesso!")
        
        return True
    
    except Exception as e:
        logger.error(f"Erro ao fazer upload do arquivo de configurau00e7u00e3o: {str(e)}")
        return False


def upload_config_manager():
    """
    Faz upload do gerenciador de configurau00e7u00e3o para o bucket GCS.
    
    Returns:
        bool: True se o upload for bem-sucedido, False caso contru00e1rio.
    """
    try:
        # Caminho do arquivo de gerenciador de configurau00e7u00e3o
        config_manager = os.path.join(os.path.dirname(__file__), 'config', 'config_manager.py')
        
        # Verificar se o arquivo existe
        if not os.path.exists(config_manager):
            logger.error(f"Arquivo de gerenciador de configurau00e7u00e3o {config_manager} nu00e3o encontrado.")
            return False
        
        # Inicializar o conector GCS
        logger.info("Inicializando conector GCS...")
        connector = WindsurfGCSConnector()
        
        # Caminho de destino no bucket
        dest_path = 'datalake/code/config/config_manager.py'
        
        # Fazer upload do arquivo
        logger.info(f"Enviando {config_manager} para {dest_path}...")
        connector.upload_file(config_manager, dest_path)
        logger.info(f"Upload do gerenciador de configurau00e7u00e3o concluído com sucesso!")
        
        return True
    
    except Exception as e:
        logger.error(f"Erro ao fazer upload do gerenciador de configurau00e7u00e3o: {str(e)}")
        return False


def upload_test_script():
    """
    Faz upload do script de teste de conexu00e3o para o bucket GCS.
    
    Returns:
        bool: True se o upload for bem-sucedido, False caso contru00e1rio.
    """
    try:
        # Caminho do arquivo de teste
        test_file = os.path.join(os.path.dirname(__file__), 'teste_conexao_simples.py')
        
        # Verificar se o arquivo existe
        if not os.path.exists(test_file):
            logger.error(f"Arquivo de teste {test_file} nu00e3o encontrado.")
            return False
        
        # Inicializar o conector GCS
        logger.info("Inicializando conector GCS...")
        connector = WindsurfGCSConnector()
        
        # Caminho de destino no bucket
        dest_path = 'datalake/dev/utils/teste_conexao_simples.py'
        
        # Fazer upload do arquivo
        logger.info(f"Enviando {test_file} para {dest_path}...")
        connector.upload_file(test_file, dest_path)
        logger.info(f"Upload do script de teste concluído com sucesso!")
        
        return True
    
    except Exception as e:
        logger.error(f"Erro ao fazer upload do script de teste: {str(e)}")
        return False


if __name__ == "__main__":
    # Upload do conector de banco de dados
    success_db = upload_db_connector()
    
    # Upload do arquivo de configurau00e7u00e3o
    success_config = upload_config_file()
    
    # Upload do gerenciador de configurau00e7u00e3o
    success_manager = upload_config_manager()
    
    # Upload do script de teste
    success_test = upload_test_script()
    
    # Verificar resultados
    if success_db and success_config and success_manager and success_test:
        logger.info("Todos os arquivos foram enviados com sucesso!")
        sys.exit(0)
    else:
        logger.error("Alguns arquivos nu00e3o puderam ser enviados.")
        sys.exit(1)
