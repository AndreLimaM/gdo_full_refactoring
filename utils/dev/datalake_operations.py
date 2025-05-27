#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Operações específicas para o datalake no bucket repo-dev-gdo-carga

Este módulo fornece funções especializadas para trabalhar com dados
no caminho datalake dentro do bucket repo-dev-gdo-carga do GCS.

Classes:
    DataLakeOperations: Classe principal para operações específicas no datalake.

Funcionalidades:
    - Criação e gerenciamento de datasets
    - Criação e listagem de partições
    - Leitura e escrita de dados em diversos formatos (CSV, Parquet, JSON)
    - Busca de arquivos por padrão
    - Gerenciamento de metadados

Exemplo de uso:
    ```python
    # Inicializar as operações do datalake
    datalake = DataLakeOperations()
    
    # Criar um novo dataset
    datalake.create_dataset("meu_dataset", "Descrição do dataset")
    
    # Ler um arquivo CSV do datalake
    df = datalake.read_csv("meu_dataset/dados.csv")
    ```

Autor: Equipe Windsurf
Versão: 1.0.0
Data: 27/05/2025
"""

import os
import json
import logging
import pandas as pd
from io import StringIO, BytesIO
from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

# Importar o gerenciador de configurações
try:
    from config.config_manager import ConfigManager
    # Inicializar o gerenciador de configurações
    config = ConfigManager()
    USE_CONFIG_FILE = True
except (ImportError, FileNotFoundError):
    # Fallback para valores padrão se o gerenciador de configurações não estiver disponível
    USE_CONFIG_FILE = False
    config = None

# Importar o conector GCS
from gcs_connector import WindsurfGCSConnector

# Configuração de logging
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s') if USE_CONFIG_FILE else '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging_level = config.get('logging.level', 'INFO') if USE_CONFIG_FILE else 'INFO'
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('datalake-operations')


class DataLakeOperations:
    """
    Classe para operações específicas no datalake.
    
    Esta classe fornece uma interface especializada para trabalhar com dados no caminho datalake
    dentro do bucket GCS. Implementa operações de alto nível para manipulação de datasets,
    partições e arquivos em diferentes formatos.
    
    Atributos:
        connector (WindsurfGCSConnector): Conector para o bucket GCS.
        datalake_prefix (str): Prefixo do caminho datalake no bucket.
    
    Nota:
        Esta classe assume que o bucket já está configurado corretamente no arquivo .env
        ou foi especificado durante a inicialização do WindsurfGCSConnector.
    """

    def __init__(self, bucket_name: Optional[str] = None, credentials_path: Optional[str] = None):
        """
        Inicializa o conector para o datalake.
        
        Cria uma instância do WindsurfGCSConnector e configura o prefixo do caminho datalake.
        As configurações são obtidas do arquivo de configuração centralizado, do arquivo .env
        ou das variáveis de ambiente, nessa ordem de prioridade.
        
        Args:
            bucket_name (Optional[str]): Nome do bucket GCS. Se não fornecido, será lido do arquivo
                                       de configuração ou das variáveis de ambiente.
            credentials_path (Optional[str]): Caminho para o arquivo de credenciais. Se não fornecido,
                                          será lido do arquivo de configuração ou das variáveis de ambiente.
        
        Raises:
            ValueError: Se o nome do bucket não estiver configurado.
            FileNotFoundError: Se o arquivo de credenciais não for encontrado.
        """
        # Inicializar o conector GCS com as configurações fornecidas ou do arquivo de configuração
        self.connector = WindsurfGCSConnector(bucket_name, credentials_path)
        
        # Obter o prefixo do datalake do arquivo de configuração ou usar o valor padrão
        if USE_CONFIG_FILE:
            self.datalake_prefix = config.get('gcs.paths.datalake', "datalake/")
        else:
            self.datalake_prefix = "datalake/"
            
        # Configurar cache se disponível no arquivo de configuração
        if USE_CONFIG_FILE and config.get('cache.enabled', False):
            self.cache_enabled = True
            self.cache_max_size = config.get('cache.max_size_mb', 100)
            self.cache_ttl = config.get('cache.ttl_seconds', 3600)
            logger.info(f"Cache habilitado: max_size={self.cache_max_size}MB, ttl={self.cache_ttl}s")
        else:
            self.cache_enabled = False
            
        # Obter configurações de datasets padrão se disponíveis
        if USE_CONFIG_FILE:
            self.default_format = config.get('datasets.default_format', 'parquet')
            self.default_partition_scheme = config.get('datasets.partitions.default_scheme', "ano={year}/mes={month}/dia={day}")
        else:
            self.default_format = 'parquet'
            self.default_partition_scheme = "ano={year}/mes={month}/dia={day}"
            
        logger.info(f"Operações de datalake inicializadas para o bucket: {self.connector.bucket_name}")
        logger.info(f"Prefixo do datalake: {self.datalake_prefix}")
        logger.debug(f"Formato padrão: {self.default_format}, Esquema de partição padrão: {self.default_partition_scheme}")

    def list_partitions(self, dataset_path: str) -> List[str]:
        """Lista as partições disponíveis em um dataset.

        Args:
            dataset_path: Caminho relativo do dataset dentro do datalake

        Returns:
            Lista de partições disponíveis
        """
        full_path = f"{self.datalake_prefix}{dataset_path}/"
        blobs = self.connector.list_files(prefix=full_path, delimiter="/")
        
        # Extrair apenas os nomes das partições do caminho completo
        partitions = []
        for blob in blobs:
            # Remove o prefixo e o sufixo para obter apenas o nome da partição
            if blob.startswith(full_path) and blob.endswith('/'):
                partition = blob[len(full_path):-1]
                if partition:
                    partitions.append(partition)
        
        return partitions

    def read_csv(self, file_path: str, **pandas_args) -> pd.DataFrame:
        """
        Lê um arquivo CSV do datalake para um DataFrame.

        Este método baixa um arquivo CSV do datalake e o carrega em um DataFrame do pandas.
        Suporta todos os argumentos opcionais do pandas.read_csv para personalizar a leitura
        do arquivo, como delimitadores, tipos de colunas, etc.

        Args:
            file_path (str): Caminho relativo do arquivo CSV dentro do datalake.
                             Não deve incluir o prefixo 'datalake/'.
            **pandas_args: Argumentos adicionais para pandas.read_csv, como:
                          - delimiter: Delimitador de campos (padrão: ',')
                          - header: Número da linha do cabeçalho (padrão: 0)
                          - usecols: Lista de colunas para ler
                          - dtype: Dicionário de tipos de dados por coluna

        Returns:
            pd.DataFrame: DataFrame do pandas contendo os dados do CSV.
            
        Raises:
            FileNotFoundError: Se o arquivo não for encontrado no datalake.
            ValueError: Se o arquivo não puder ser lido como CSV.
            
        Exemplo:
            ```python
            # Leitura básica de um CSV
            df = datalake.read_csv("meu_dataset/dados.csv")
            
            # Leitura com opções personalizadas
            df = datalake.read_csv("meu_dataset/dados.csv", delimiter=";", header=1, usecols=['A', 'B'])
            ```
        """
        full_path = f"{self.datalake_prefix}{file_path}"
        
        if not self.connector.file_exists(full_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {full_path}")
        
        # Baixar o conteúdo do arquivo como string
        content = self.connector.download_as_string(full_path)
        
        # Ler o CSV usando pandas
        return pd.read_csv(StringIO(content), **pandas_args)

    def read_parquet(self, file_path: str, **pandas_args) -> pd.DataFrame:
        """Lê um arquivo Parquet do datalake para um DataFrame.

        Args:
            file_path: Caminho relativo do arquivo Parquet dentro do datalake
            **pandas_args: Argumentos adicionais para pandas.read_parquet

        Returns:
            DataFrame com os dados do Parquet
        """
        full_path = f"{self.datalake_prefix}{file_path}"
        
        if not self.connector.file_exists(full_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {full_path}")
        
        # Para arquivos Parquet, precisamos baixar os bytes brutos
        blob = self.connector.bucket.blob(full_path)
        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        
        # Ler o Parquet usando pandas
        return pd.read_parquet(buffer, **pandas_args)

    def read_json(self, file_path: str) -> Dict[str, Any]:
        """Lê um arquivo JSON do datalake.

        Args:
            file_path: Caminho relativo do arquivo JSON dentro do datalake

        Returns:
            Dicionário com os dados do JSON
        """
        full_path = f"{self.datalake_prefix}{file_path}"
        
        if not self.connector.file_exists(full_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {full_path}")
        
        # Baixar o conteúdo do arquivo como string
        content = self.connector.download_as_string(full_path)
        
        # Converter para dicionário
        return json.loads(content)

    def write_dataframe(self, df: pd.DataFrame, file_path: str, format: str = 'csv', **kwargs) -> str:
        """Escreve um DataFrame para o datalake.

        Args:
            df: DataFrame a ser salvo
            file_path: Caminho relativo do arquivo dentro do datalake
            format: Formato do arquivo ('csv', 'parquet', 'json')
            **kwargs: Argumentos adicionais para o método de escrita do pandas

        Returns:
            URL pública do arquivo salvo
        """
        full_path = f"{self.datalake_prefix}{file_path}"
        buffer = BytesIO()
        
        # Salvar o DataFrame no formato especificado
        if format.lower() == 'csv':
            df.to_csv(buffer, **kwargs)
            content_type = 'text/csv'
        elif format.lower() == 'parquet':
            df.to_parquet(buffer, **kwargs)
            content_type = 'application/octet-stream'
        elif format.lower() == 'json':
            df.to_json(buffer, **kwargs)
            content_type = 'application/json'
        else:
            raise ValueError(f"Formato não suportado: {format}. Use 'csv', 'parquet' ou 'json'.")
        
        # Resetar o buffer para o início
        buffer.seek(0)
        
        # Fazer upload do buffer para o GCS
        blob = self.connector.bucket.blob(full_path)
        blob.upload_from_file(buffer, content_type=content_type)
        
        logger.info(f"DataFrame salvo em {full_path} no formato {format}")
        return blob.public_url

    def create_dataset(self, dataset_name: str, description: str = "") -> None:
        """Cria um novo dataset no datalake.

        Args:
            dataset_name: Nome do dataset
            description: Descrição opcional do dataset
        """
        # Criar a pasta do dataset
        dataset_path = f"{self.datalake_prefix}{dataset_name}/"
        self.connector.create_folder(dataset_path)
        
        # Criar um arquivo de metadados para o dataset
        metadata = {
            "name": dataset_name,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "created_by": "windsurf-gcs-connector"
        }
        
        metadata_path = f"{dataset_path}_metadata.json"
        self.connector.upload_from_string(
            json.dumps(metadata, indent=2),
            metadata_path,
            content_type='application/json'
        )
        
        logger.info(f"Dataset '{dataset_name}' criado com sucesso")

    def create_partition(self, dataset_name: str, partition_name: str) -> None:
        """Cria uma nova partição em um dataset.

        Args:
            dataset_name: Nome do dataset
            partition_name: Nome da partição (geralmente uma data ou categoria)
        """
        partition_path = f"{self.datalake_prefix}{dataset_name}/{partition_name}/"
        self.connector.create_folder(partition_path)
        logger.info(f"Partição '{partition_name}' criada no dataset '{dataset_name}'")

    def get_dataset_metadata(self, dataset_name: str) -> Dict[str, Any]:
        """Obtém os metadados de um dataset.

        Args:
            dataset_name: Nome do dataset

        Returns:
            Dicionário com os metadados do dataset
        """
        metadata_path = f"{self.datalake_prefix}{dataset_name}/_metadata.json"
        
        if not self.connector.file_exists(metadata_path):
            raise FileNotFoundError(f"Metadados não encontrados para o dataset: {dataset_name}")
        
        return self.read_json(f"{dataset_name}/_metadata.json")

    def search_files(self, search_pattern: str) -> List[str]:
        """Busca arquivos no datalake que correspondam a um padrão.

        Args:
            search_pattern: Padrão para busca (parte do caminho/nome do arquivo)

        Returns:
            Lista de caminhos de arquivos que correspondem ao padrão
        """
        all_files = self.connector.list_files(prefix=self.datalake_prefix)
        
        # Filtrar arquivos que correspondem ao padrão
        matching_files = [f for f in all_files if search_pattern in f]
        
        # Remover o prefixo do datalake para retornar caminhos relativos
        return [f[len(self.datalake_prefix):] for f in matching_files]


# Exemplo de uso
def exemplo_uso():
    """Demonstra o uso das operações do datalake."""
    try:
        # Inicializar as operações do datalake
        datalake = DataLakeOperations()
        
        # Verificar se o arquivo de credenciais existe
        credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not os.path.exists(credentials_path):
            logger.error(f"Arquivo de credenciais não encontrado: {credentials_path}")
            logger.error("Por favor, configure suas credenciais do Google Cloud antes de executar este exemplo.")
            return
        
        # Exemplo 1: Criar um novo dataset
        dataset_name = "exemplo_dataset"
        logger.info(f"Criando dataset de exemplo: {dataset_name}")
        datalake.create_dataset(dataset_name, "Dataset de exemplo para demonstração")
        
        # Exemplo 2: Criar partições no dataset
        logger.info("Criando partições de exemplo")
        datalake.create_partition(dataset_name, "ano=2023")
        datalake.create_partition(dataset_name, "ano=2024")
        
        # Exemplo 3: Listar partições
        partitions = datalake.list_partitions(dataset_name)
        logger.info(f"Partições disponíveis: {partitions}")
        
        # Exemplo 4: Criar um DataFrame de exemplo e salvá-lo no datalake
        logger.info("Criando e salvando dados de exemplo")
        df = pd.DataFrame({
            'id': range(1, 11),
            'nome': [f'Item {i}' for i in range(1, 11)],
            'valor': [i * 10.5 for i in range(1, 11)]
        })
        
        # Salvar em diferentes formatos
        datalake.write_dataframe(df, f"{dataset_name}/ano=2023/dados.csv", format='csv', index=False)
        datalake.write_dataframe(df, f"{dataset_name}/ano=2023/dados.parquet", format='parquet')
        datalake.write_dataframe(df, f"{dataset_name}/ano=2023/dados.json", format='json', orient='records')
        
        # Exemplo 5: Ler os dados de volta
        logger.info("Lendo dados de exemplo")
        try:
            df_csv = datalake.read_csv(f"{dataset_name}/ano=2023/dados.csv")
            logger.info(f"Dados CSV lidos com sucesso. Forma: {df_csv.shape}")
            
            df_parquet = datalake.read_parquet(f"{dataset_name}/ano=2023/dados.parquet")
            logger.info(f"Dados Parquet lidos com sucesso. Forma: {df_parquet.shape}")
            
            json_data = datalake.read_json(f"{dataset_name}/ano=2023/dados.json")
            logger.info(f"Dados JSON lidos com sucesso. Número de registros: {len(json_data)}")
        except Exception as e:
            logger.error(f"Erro ao ler dados: {str(e)}")
        
        # Exemplo 6: Buscar arquivos
        matching_files = datalake.search_files("dados")
        logger.info(f"Arquivos encontrados com 'dados' no nome: {matching_files}")
        
    except Exception as e:
        logger.error(f"Erro no exemplo de uso: {str(e)}")


if __name__ == "__main__":
    exemplo_uso()
