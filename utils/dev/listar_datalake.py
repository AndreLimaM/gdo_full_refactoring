#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para listar o conteu00fado da pasta datalake no bucket repo-dev-gdo-carga
"""

import os
import logging
from gcs_connector import WindsurfGCSConnector

# Configurau00e7u00e3o de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('listar-datalake')


def verificar_credenciais():
    """Verifica se as credenciais estu00e3o configuradas corretamente."""
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credentials_path:
        logger.error("Variu00e1vel GOOGLE_APPLICATION_CREDENTIALS nu00e3o definida no arquivo .env")
        return False
        
    if not os.path.exists(credentials_path):
        logger.error(f"Arquivo de credenciais nu00e3o encontrado em: {credentials_path}")
        logger.info("Vocu00ea precisa criar um arquivo de credenciais do Google Cloud.")
        logger.info("Instruu00e7u00f5es:")
        logger.info("1. Acesse o Console do Google Cloud: https://console.cloud.google.com/")
        logger.info("2. Navegue atu00e9 IAM & Admin > Service Accounts")
        logger.info("3. Crie ou selecione uma conta de serviu00e7o")
        logger.info("4. Crie uma chave (formato JSON) para esta conta de serviu00e7o")
        logger.info("5. Baixe o arquivo JSON e salve-o como 'credentials.json' no diretu00f3rio do projeto")
        return False
    
    return True


def listar_conteudo_datalake():
    """Lista o conteu00fado da pasta datalake."""
    try:
        # Inicializa o conector GCS
        connector = WindsurfGCSConnector()
        
        # Prefixo para o caminho datalake
        datalake_prefix = "datalake/"
        
        logger.info(f"Listando conteu00fado da pasta {datalake_prefix} no bucket {connector.bucket_name}")
        
        # Listar arquivos e pastas no caminho datalake/
        blobs = connector.client.list_blobs(connector.bucket_name, prefix=datalake_prefix, delimiter='/')
        
        # Listar prefixos (pastas)
        logger.info("\nPastas encontradas:")
        prefixes_count = 0
        for prefix in blobs.prefixes:
            logger.info(f"  - {prefix}")
            prefixes_count += 1
        
        if prefixes_count == 0:
            logger.info("  Nenhuma pasta encontrada")
        
        # Listar arquivos
        logger.info("\nArquivos encontrados:")
        files_count = 0
        for blob in blobs:
            # Ignorar o pru00f3prio diretu00f3rio
            if blob.name != datalake_prefix:
                logger.info(f"  - {blob.name} ({blob.size} bytes)")
                files_count += 1
        
        if files_count == 0:
            logger.info("  Nenhum arquivo encontrado diretamente na raiz")
        
        # Listar conteu00fado recursivamente (opcional)
        logger.info("\nListagem recursiva (todos os arquivos):")
        all_blobs = connector.list_files(prefix=datalake_prefix)
        if all_blobs:
            for blob in all_blobs:
                logger.info(f"  - {blob}")
        else:
            logger.info("  Nenhum arquivo encontrado")
            
        logger.info(f"\nTotal: {prefixes_count} pastas e {files_count} arquivos na raiz, {len(all_blobs)} arquivos no total")
        
    except Exception as e:
        logger.error(f"Erro ao listar conteu00fado do datalake: {str(e)}")


if __name__ == "__main__":
    if verificar_credenciais():
        listar_conteudo_datalake()
    else:
        logger.error("Configurau00e7u00e3o de credenciais incompleta. Corrija os problemas antes de continuar.")
