#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo base para processamento de arquivos JSON.

Este módulo contém classes e funções genéricas para processamento
de arquivos JSON, independente do tipo de payload.
"""

import os
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

# Configuração de logging
logger = logging.getLogger(__name__)


class BaseJsonProcessor(ABC):
    """
    Classe base abstrata para processadores de JSON.
    
    Esta classe define a interface comum para todos os processadores
    de arquivos JSON, independente do tipo de payload.
    """
    
    def __init__(self, spark_session: SparkSession, db_config: Dict[str, str]):
        """
        Inicializa o processador base.
        
        Args:
            spark_session: Sessão Spark ativa
            db_config: Configuração de conexão com o banco de dados
        """
        self.spark = spark_session
        self.db_config = db_config
        self.schema = self._define_schema()
    
    @abstractmethod
    def _define_schema(self) -> StructType:
        """
        Define o schema para os dados.
        
        Returns:
            Schema Spark para os dados
        """
        pass
    
    @abstractmethod
    def _transform_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transforma os dados JSON no formato esperado pelo banco de dados.
        
        Args:
            data: Dados JSON
            
        Returns:
            Lista de dicionários com dados transformados
        """
        pass
    
    @abstractmethod
    def _save_to_database(self, df: DataFrame):
        """
        Salva o DataFrame no banco de dados.
        
        Args:
            df: DataFrame Spark com dados
        """
        pass
    
    def process_file(self, file_path: str) -> Tuple[bool, Optional[str]]:
        """
        Processa um arquivo JSON.
        
        Args:
            file_path: Caminho para o arquivo JSON
            
        Returns:
            Tupla com status de sucesso e mensagem de erro (se houver)
        """
        try:
            logger.info(f"Iniciando processamento do arquivo: {file_path}")
            
            # Lê o arquivo JSON
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Transforma os dados
            transformed_data = self._transform_data(data)
            
            # Cria DataFrame
            df = self.spark.createDataFrame(transformed_data, self.schema)
            
            # Salva no banco de dados
            self._save_to_database(df)
            
            logger.info(f"Arquivo processado com sucesso: {file_path}")
            return True, None
            
        except Exception as e:
            error_msg = f"Erro ao processar arquivo {file_path}: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """
        Converte string de data para objeto datetime.
        
        Args:
            date_str: String de data no formato ISO
            
        Returns:
            Objeto datetime ou None se a data for inválida
        """
        if not date_str:
            return None
            
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return None


def process_directory(processor: BaseJsonProcessor, input_dir: str, done_dir: str, error_dir: str) -> Dict[str, int]:
    """
    Processa todos os arquivos JSON em um diretório.
    
    Args:
        processor: Instância de processador de JSON
        input_dir: Diretório de entrada com arquivos JSON
        done_dir: Diretório para arquivos processados com sucesso
        error_dir: Diretório para arquivos com erro
        
    Returns:
        Dicionário com estatísticas de processamento
    """
    # Verifica se os diretórios existem
    for directory in [input_dir, done_dir, error_dir]:
        os.makedirs(directory, exist_ok=True)
    
    # Estatísticas de processamento
    stats = {
        "total": 0,
        "success": 0,
        "error": 0
    }
    
    # Lista arquivos JSON no diretório de entrada
    json_files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
    stats["total"] = len(json_files)
    
    for file_name in json_files:
        input_path = os.path.join(input_dir, file_name)
        success, error_msg = processor.process_file(input_path)
        
        if success:
            # Move para diretório de sucesso
            done_path = os.path.join(done_dir, file_name)
            os.rename(input_path, done_path)
            stats["success"] += 1
            logger.info(f"Arquivo movido para {done_path}")
        else:
            # Move para diretório de erro
            error_path = os.path.join(error_dir, file_name)
            os.rename(input_path, error_path)
            
            # Cria arquivo de log de erro
            error_log_path = os.path.join(error_dir, f"{file_name}.error")
            with open(error_log_path, 'w') as f:
                f.write(error_msg)
                
            stats["error"] += 1
            logger.info(f"Arquivo movido para {error_path}")
    
    return stats
