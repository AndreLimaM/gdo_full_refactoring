#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de processamento de dados de animais.

Este módulo contém funções para processar arquivos JSON de animais e
inserir os dados processados no banco de dados PostgreSQL.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Importa a classe base de processamento
from src.processors.common.json_processor import BaseJsonProcessor, process_directory as base_process_directory

# Configuração de logging
logger = logging.getLogger(__name__)


class AnimalProcessor(BaseJsonProcessor):
    """
    Classe responsável pelo processamento de dados de animais.
    
    Esta classe contém métodos para ler arquivos JSON de animais,
    transformar os dados e inserir no banco de dados PostgreSQL.
    """
        
    def _define_schema(self) -> StructType:
        """
        Define o schema para os dados de animais.
        
        Returns:
            Schema Spark para os dados de animais
        """
        return StructType([
            StructField("id", StringType(), False),
            StructField("codigo_fazenda", StringType(), True),
            StructField("codigo_animal", StringType(), True),
            StructField("tipo_animal", IntegerType(), True),
            StructField("raca", IntegerType(), True),
            StructField("codigo_lote", StringType(), True),
            StructField("codigo_piquete", StringType(), True),
            StructField("data_nascimento", TimestampType(), True),
            StructField("data_entrada", TimestampType(), True),
            StructField("categoria", IntegerType(), True),
            StructField("origem", IntegerType(), True),
            StructField("classificacao", StringType(), True),
            StructField("lote_anterior", StringType(), True),
            StructField("brinco", StringType(), True),
            StructField("numero", IntegerType(), True),
            StructField("sexo", StringType(), True),
            StructField("pai", StringType(), True),
            StructField("peso_nascimento", FloatType(), True),
            StructField("peso_entrada", FloatType(), True),
            StructField("data_ultima_pesagem", TimestampType(), True),
            StructField("status", IntegerType(), True),
            StructField("observacoes", StringType(), True),
            StructField("dados_adicionais", StringType(), True),
            StructField("codigo_rastreabilidade", StringType(), True),
            StructField("pelagem", StringType(), True),
            StructField("geracao", StringType(), True),
            StructField("grau_sangue", StringType(), True),
            StructField("peso_atual", FloatType(), True),
            StructField("data_ultima_movimentacao", TimestampType(), True),
            StructField("data_entrada_lote", TimestampType(), True),
            StructField("data_criacao", TimestampType(), True),
            StructField("data_atualizacao", TimestampType(), True),
            StructField("data_exclusao", TimestampType(), True)
        ])
    

    
    def _transform_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transforma os dados JSON no formato esperado pelo banco de dados.
        
        Args:
            data: Dados JSON de animais
            
        Returns:
            Lista de dicionários com dados transformados
        """
        result = []
        
        for animal in data.get("animais", []):
            transformed = {
                "id": animal.get("id"),
                "codigo_fazenda": animal.get("codigo_fazenda"),
                "codigo_animal": animal.get("codigo_animal"),
                "tipo_animal": animal.get("tipo_animal"),
                "raca": animal.get("raca"),
                "codigo_lote": animal.get("codigo_lote"),
                "codigo_piquete": animal.get("codigo_piquete"),
                "data_nascimento": self._parse_date(animal.get("data_nascimento")),
                "data_entrada": self._parse_date(animal.get("data_entrada")),
                "categoria": animal.get("categoria"),
                "origem": animal.get("origem"),
                "classificacao": json.dumps(animal.get("classificacao", {})),
                "lote_anterior": animal.get("lote_anterior"),
                "brinco": animal.get("brinco"),
                "numero": animal.get("numero"),
                "sexo": animal.get("sexo"),
                "pai": animal.get("pai"),
                "peso_nascimento": animal.get("peso_nascimento"),
                "peso_entrada": animal.get("peso_entrada"),
                "data_ultima_pesagem": self._parse_date(animal.get("data_ultima_pesagem")),
                "status": animal.get("status"),
                "observacoes": json.dumps(animal.get("observacoes", {})),
                "dados_adicionais": json.dumps(animal.get("dados_adicionais", {})),
                "codigo_rastreabilidade": animal.get("codigo_rastreabilidade"),
                "pelagem": json.dumps(animal.get("pelagem", {})),
                "geracao": json.dumps(animal.get("geracao", {})),
                "grau_sangue": animal.get("grau_sangue"),
                "peso_atual": animal.get("peso_atual"),
                "data_ultima_movimentacao": self._parse_date(animal.get("data_ultima_movimentacao")),
                "data_entrada_lote": self._parse_date(animal.get("data_entrada_lote")),
                "data_criacao": self._parse_date(animal.get("data_criacao")),
                "data_atualizacao": self._parse_date(animal.get("data_atualizacao")),
                "data_exclusao": self._parse_date(animal.get("data_exclusao"))
            }
            result.append(transformed)
            
        return result
    

    
    def _save_to_database(self, df: DataFrame):
        """
        Salva o DataFrame no banco de dados PostgreSQL.
        
        Args:
            df: DataFrame Spark com dados de animais
        """
        # Configuração da conexão com o banco de dados
        jdbc_url = f"jdbc:postgresql://{self.db_config['host']}/{self.db_config['database']}"
        
        # Salva os dados na tabela
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "bt_animais") \
            .option("user", self.db_config["user"]) \
            .option("password", self.db_config["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Dados salvos na tabela bt_animais")


def process_directory(spark: SparkSession, input_dir: str, done_dir: str, error_dir: str, db_config: Dict[str, str]) -> Dict[str, int]:
    """
    Processa todos os arquivos JSON de animais em um diretório.
    
    Args:
        spark: Sessão Spark ativa
        input_dir: Diretório de entrada com arquivos JSON
        done_dir: Diretório para arquivos processados com sucesso
        error_dir: Diretório para arquivos com erro
        db_config: Configuração de conexão com o banco de dados
        
    Returns:
        Dicionário com estatísticas de processamento
    """
    processor = AnimalProcessor(spark, db_config)
    return base_process_directory(processor, input_dir, done_dir, error_dir)
