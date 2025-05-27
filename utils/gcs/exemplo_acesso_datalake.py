#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Exemplo de acesso ao bucket repo-dev-gdo-carga/datalake

Este script demonstra como usar o WindsurfGCSConnector para acessar
arquivos no caminho datalake dentro do bucket repo-dev-gdo-carga.
"""

import os
import logging
from gcs_connector import WindsurfGCSConnector

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('exemplo-datalake')


def main():
    """
    Função principal que demonstra o acesso ao datalake.
    """
    try:
        # Inicializa o conector GCS
        # O nome do bucket é obtido do arquivo .env (GCS_BUCKET_NAME=repo-dev-gdo-carga)
        connector = WindsurfGCSConnector()
        
        # Prefixo para o caminho datalake
        datalake_prefix = "datalake/"
        
        # Lista todos os arquivos no caminho datalake/
        logger.info("Listando arquivos no caminho datalake/")
        files = connector.list_files(prefix=datalake_prefix)
        
        if files:
            logger.info(f"Encontrados {len(files)} arquivos:")
            for file in files:
                logger.info(f"  - {file}")
                
                # Exemplo: obter metadados do primeiro arquivo
                if file == files[0]:
                    metadata = connector.get_metadata(file)
                    logger.info(f"Metadados do arquivo {file}:")
                    for key, value in metadata.items():
                        logger.info(f"  {key}: {value}")
                    
                    # Exemplo: gerar URL assinada para o primeiro arquivo
                    signed_url = connector.get_signed_url(file, expiration=3600)  # 1 hora
                    logger.info(f"URL assinada (válida por 1 hora): {signed_url}")
        else:
            logger.info("Nenhum arquivo encontrado no caminho datalake/")
            
            # Se não houver arquivos, podemos criar uma pasta de exemplo
            logger.info("Criando pasta de exemplo datalake/exemplo/")
            connector.create_folder("datalake/exemplo/")
            
            # E fazer upload de um arquivo de teste
            test_content = "Este é um arquivo de teste criado pelo Windsurf GCS Connector."
            test_file_path = "datalake/exemplo/teste.txt"
            logger.info(f"Criando arquivo de teste em {test_file_path}")
            connector.upload_from_string(test_content, test_file_path)
            
            # Listar novamente para confirmar a criação
            logger.info("Listando arquivos após criação:")
            files = connector.list_files(prefix=datalake_prefix)
            for file in files:
                logger.info(f"  - {file}")
        
    except Exception as e:
        logger.error(f"Erro ao acessar o bucket: {str(e)}")
        logger.error("Verifique se o arquivo de credenciais existe e se tem permissões adequadas.")
        logger.error("Também verifique se o bucket 'repo-dev-gdo-carga' existe e se você tem acesso a ele.")


def verificar_configuracao():
    """
    Verifica se as configurações necessárias estão presentes.
    """
    logger.info("Verificando configuração...")
    
    # Verificar arquivo de credenciais
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credentials_path:
        logger.warning("Variável GOOGLE_APPLICATION_CREDENTIALS não definida no arquivo .env")
        return False
        
    if not os.path.exists(credentials_path):
        logger.warning(f"Arquivo de credenciais não encontrado em: {credentials_path}")
        logger.info("Você precisa criar um arquivo de credenciais do Google Cloud.")
        logger.info("Instruções:")
        logger.info("1. Acesse o Console do Google Cloud: https://console.cloud.google.com/")
        logger.info("2. Navegue até IAM & Admin > Service Accounts")
        logger.info("3. Crie ou selecione uma conta de serviço")
        logger.info("4. Crie uma chave (formato JSON) para esta conta de serviço")
        logger.info("5. Baixe o arquivo JSON e salve-o como 'credentials.json' no diretório do projeto")
        return False
    
    # Verificar nome do bucket
    bucket_name = os.getenv('GCS_BUCKET_NAME')
    if not bucket_name:
        logger.warning("Variável GCS_BUCKET_NAME não definida no arquivo .env")
        return False
    
    logger.info(f"Configuração OK. Bucket: {bucket_name}, Credenciais: {credentials_path}")
    return True


if __name__ == "__main__":
    # Verificar configuração antes de executar
    if verificar_configuracao():
        main()
    else:
        logger.error("Configuração incompleta. Corrija os problemas acima antes de continuar.")
