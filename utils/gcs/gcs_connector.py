#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Conector Windsurf para Google Cloud Storage (GCS)

Este módulo fornece funcionalidades para conectar o Windsurf diretamente com buckets do Google Cloud Storage,
permitindo operações como upload, download, listagem e exclusão de arquivos.

Classes:
    WindsurfGCSConnector: Classe principal para conexão com o Google Cloud Storage.

Exemplo de uso:
    ```python
    # Inicializar o conector
    connector = WindsurfGCSConnector(bucket_name='meu-bucket')
    
    # Listar arquivos em um bucket
    files = connector.list_files(prefix='pasta/')
    
    # Fazer upload de um arquivo
    connector.upload_file('arquivo_local.txt', 'destino.txt')
    ```

Autor: Equipe Windsurf
Versão: 1.0.0
Data: 27/05/2025
"""

import os
import logging
from typing import List, Optional, BinaryIO, Dict, Any
from pathlib import Path
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud.storage.blob import Blob
from google.cloud.storage.bucket import Bucket

# Importar o gerenciador de configurações
try:
    from config.config_manager import ConfigManager
    # Inicializar o gerenciador de configurações
    config = ConfigManager()
    USE_CONFIG_FILE = True
except (ImportError, FileNotFoundError):
    # Fallback para variáveis de ambiente se o gerenciador de configurações não estiver disponível
    USE_CONFIG_FILE = False
    # Carrega variáveis de ambiente do arquivo .env
    load_dotenv()

# Configuração de logging
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s') if USE_CONFIG_FILE else '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging_level = config.get('logging.level', 'INFO') if USE_CONFIG_FILE else 'INFO'
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('windsurf-gcs-connector')


class WindsurfGCSConnector:
    """
    Classe para conectar o Windsurf com o Google Cloud Storage.
    
    Esta classe fornece uma interface simplificada para interagir com o Google Cloud Storage,
    permitindo operações comuns como upload, download, listagem e exclusão de arquivos.
    Gerencia automaticamente a autenticação e conexão com o bucket especificado.
    
    Atributos:
        client (storage.Client): Cliente do Google Cloud Storage.
        bucket_name (str): Nome do bucket GCS.
        bucket (Bucket): Objeto bucket do GCS.
        credentials_path (str): Caminho para o arquivo de credenciais.
    """

    def __init__(self, bucket_name: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Inicializa o conector GCS.

        Args:
            bucket_name (Optional[str]): Nome do bucket GCS. Se não fornecido, será lido do arquivo de configuração
                                       ou da variável de ambiente GCS_BUCKET_NAME.
            credentials_path (Optional[str]): Caminho para o arquivo de credenciais. Se não fornecido, será lido do
                                          arquivo de configuração ou da variável de ambiente GOOGLE_APPLICATION_CREDENTIALS.
                             
        Raises:
            ValueError: Se o nome do bucket não for fornecido em nenhuma fonte de configuração.
            FileNotFoundError: Se o arquivo de credenciais não for encontrado.
        """
        # Obter credenciais do arquivo de configuração ou das variáveis de ambiente
        if USE_CONFIG_FILE:
            # Usar configurações do arquivo YAML
            config_credentials_path = config.get('gcs.credentials_path')
            config_bucket_name = config.get('gcs.bucket_name')
        else:
            # Usar variáveis de ambiente
            config_credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            config_bucket_name = os.getenv('GCS_BUCKET_NAME')
        
        # Prioridade: parâmetros > arquivo de configuração > variáveis de ambiente
        self.credentials_path = credentials_path or config_credentials_path
        
        # Verificar se o arquivo de credenciais existe
        if self.credentials_path:
            credentials_file = Path(self.credentials_path)
            if not credentials_file.exists():
                error_msg = f"Arquivo de credenciais não encontrado: {self.credentials_path}"
                logger.error(error_msg)
                raise FileNotFoundError(error_msg)
                
            # Configurar variável de ambiente para o cliente GCS
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = str(credentials_file.absolute())
            logger.info(f"Usando credenciais do arquivo: {self.credentials_path}")

        # Inicializar cliente GCS
        self.client = storage.Client()
        
        # Configurar bucket (prioridade: parâmetros > arquivo de configuração > variáveis de ambiente)
        self.bucket_name = bucket_name or config_bucket_name
        
        if not self.bucket_name:
            error_msg = "Nome do bucket não fornecido. Defina no arquivo de configuração, no arquivo .env ou forneça no construtor."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Criar referência ao bucket
        self.bucket = self.client.bucket(self.bucket_name)
        
        # Configurar timeout e retries se disponíveis no arquivo de configuração
        if USE_CONFIG_FILE:
            self.timeout = config.get('connection.timeout_seconds', 30)
            self.max_retries = config.get('connection.max_retries', 3)
        else:
            self.timeout = 30  # valor padrão
            self.max_retries = 3  # valor padrão
            
        logger.info(f"Conector GCS inicializado para o bucket: {self.bucket_name}")
        logger.debug(f"Configurações de conexão: timeout={self.timeout}s, max_retries={self.max_retries}")

    def list_files(self, prefix: str = "", delimiter: Optional[str] = None) -> List[str]:
        """
        Lista arquivos no bucket com um prefixo opcional.

        Este método permite listar todos os arquivos (blobs) em um bucket que começam
        com o prefixo especificado. Pode ser usado para simular uma estrutura de diretórios
        usando o parâmetro delimiter.

        Args:
            prefix (str): Prefixo para filtrar os arquivos. Por padrão, lista todos os arquivos.
            delimiter (Optional[str]): Delimitador para agrupar resultados. Use '/' para simular
                                      uma estrutura de diretórios.

        Returns:
            List[str]: Lista de nomes de arquivos (caminhos completos dos blobs).
            
        Exemplo:
            ```python
            # Listar todos os arquivos no bucket
            all_files = connector.list_files()
            
            # Listar arquivos em uma pasta específica
            files_in_folder = connector.list_files(prefix='pasta/', delimiter='/')
            ```
        """
        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix, delimiter=delimiter)
        return [blob.name for blob in blobs]

    def upload_file(self, source_file_path: str, destination_blob_name: Optional[str] = None) -> str:
        """
        Faz upload de um arquivo para o bucket GCS.

        Este método carrega um arquivo do sistema de arquivos local para o bucket GCS.
        Se o nome do blob de destino não for especificado, o nome do arquivo local será usado.

        Args:
            source_file_path (str): Caminho local do arquivo a ser enviado.
            destination_blob_name (Optional[str]): Nome do blob de destino no GCS. 
                                                 Se não fornecido, usa o nome do arquivo local.

        Returns:
            str: URL pública do arquivo carregado.
            
        Raises:
            FileNotFoundError: Se o arquivo de origem não existir.
            google.cloud.exceptions.GoogleCloudError: Se ocorrer um erro durante o upload.
            
        Exemplo:
            ```python
            # Upload de um arquivo com o mesmo nome
            url = connector.upload_file('arquivo_local.txt')
            
            # Upload de um arquivo com nome personalizado
            url = connector.upload_file('arquivo_local.txt', 'pasta/novo_nome.txt')
            ```
        """
        if not destination_blob_name:
            destination_blob_name = os.path.basename(source_file_path)

        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        
        logger.info(f"Arquivo {source_file_path} enviado para {destination_blob_name}")
        return blob.public_url

    def upload_from_string(self, data: str, destination_blob_name: str, content_type: str = 'text/plain') -> str:
        """
        Faz upload de uma string para o bucket GCS.

        Args:
            data: Conteúdo a ser enviado.
            destination_blob_name: Nome do blob de destino no GCS.
            content_type: Tipo de conteúdo do arquivo.

        Returns:
            URL pública do arquivo carregado.
        """
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type=content_type)
        
        logger.info(f"Dados enviados para {destination_blob_name}")
        return blob.public_url

    def download_file(self, source_blob_name: str, destination_file_path: str) -> None:
        """
        Faz download de um arquivo do bucket GCS.

        Args:
            source_blob_name: Nome do blob no GCS.
            destination_file_path: Caminho local onde o arquivo será salvo.
        """
        blob = self.bucket.blob(source_blob_name)
        os.makedirs(os.path.dirname(destination_file_path), exist_ok=True)
        blob.download_to_filename(destination_file_path)
        
        logger.info(f"Arquivo {source_blob_name} baixado para {destination_file_path}")

    def download_as_string(self, blob_name: str) -> str:
        """
        Faz download do conteúdo de um arquivo como string.

        Args:
            blob_name: Nome do blob no GCS.

        Returns:
            Conteúdo do arquivo como string.
        """
        blob = self.bucket.blob(blob_name)
        return blob.download_as_text()

    def delete_file(self, blob_name: str) -> None:
        """
        Exclui um arquivo do bucket GCS.

        Args:
            blob_name: Nome do blob a ser excluído.
        """
        blob = self.bucket.blob(blob_name)
        blob.delete()
        logger.info(f"Arquivo {blob_name} excluído")

    def get_signed_url(self, blob_name: str, expiration: int = 3600) -> str:
        """
        Gera uma URL assinada para acesso temporário a um arquivo.

        Args:
            blob_name: Nome do blob no GCS.
            expiration: Tempo de expiração em segundos (padrão: 1 hora).

        Returns:
            URL assinada para acesso ao arquivo.
        """
        blob = self.bucket.blob(blob_name)
        url = blob.generate_signed_url(
            version="v4",
            expiration=expiration,
            method="GET"
        )
        return url

    def file_exists(self, blob_name: str) -> bool:
        """
        Verifica se um arquivo existe no bucket.

        Args:
            blob_name: Nome do blob a verificar.

        Returns:
            True se o arquivo existir, False caso contrário.
        """
        blob = self.bucket.blob(blob_name)
        return blob.exists()

    def get_metadata(self, blob_name: str) -> Dict[str, Any]:
        """
        Obtém metadados de um arquivo no bucket.

        Args:
            blob_name: Nome do blob.

        Returns:
            Dicionário com metadados do arquivo.
        """
        blob = self.bucket.blob(blob_name)
        blob.reload()  # Carrega os metadados atualizados
        return {
            'name': blob.name,
            'size': blob.size,
            'updated': blob.updated,
            'content_type': blob.content_type,
            'md5_hash': blob.md5_hash,
            'storage_class': blob.storage_class,
            'time_created': blob.time_created,
            'custom_metadata': blob.metadata
        }

    def create_folder(self, folder_path: str) -> None:
        """
        Cria uma pasta no bucket (no GCS, pastas são simuladas com um objeto vazio).

        Args:
            folder_path: Caminho da pasta a ser criada (deve terminar com '/').
        """
        if not folder_path.endswith('/'):
            folder_path += '/'
            
        blob = self.bucket.blob(folder_path)
        blob.upload_from_string('')
        logger.info(f"Pasta {folder_path} criada")

    def copy_file(self, source_blob_name: str, destination_blob_name: str) -> None:
        """
        Copia um arquivo dentro do bucket.

        Args:
            source_blob_name: Nome do blob de origem.
            destination_blob_name: Nome do blob de destino.
        """
        source_blob = self.bucket.blob(source_blob_name)
        destination_blob = self.bucket.blob(destination_blob_name)
        
        self.bucket.copy_blob(source_blob, self.bucket, destination_blob_name)
        logger.info(f"Arquivo {source_blob_name} copiado para {destination_blob_name}")
