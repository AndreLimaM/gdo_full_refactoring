#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo para conexão com banco de dados PostgreSQL.

Este módulo fornece classes e funções para facilitar a conexão
e interação com bancos de dados PostgreSQL.
"""

import sys
import logging
from typing import Dict, Any, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from pyspark.sql import SparkSession

# Importa configurações e logging
sys.path.append('/home/andre/CascadeProjects/gdo_full_refactoring')
from src.config.config import get_config
from src.utils.logging.log_manager import get_logger

# Configuração de logging
logger = get_logger(__name__)


class PostgreSQLConnector:
    """
    Classe para conexão com banco de dados PostgreSQL.
    
    Esta classe fornece métodos para conectar e interagir
    com bancos de dados PostgreSQL usando psycopg2.
    """
    
    def __init__(self, config: Dict[str, str] = None):
        """
        Inicializa o conector PostgreSQL.
        
        Args:
            config: Configuração de conexão com o banco de dados (opcional)
                   Se não fornecido, usa as configurações globais
        """
        if config is None:
            self.config = {
                'host': get_config('database', 'host'),
                'database': get_config('database', 'name'),
                'user': get_config('database', 'user'),
                'password': get_config('database', 'password')
            }
        else:
            self.config = config
        
        self.connection = None
        self.cursor = None
    
    def connect(self) -> bool:
        """
        Estabelece conexão com o banco de dados.
        
        Returns:
            True se a conexão foi estabelecida com sucesso, False caso contrário
        """
        try:
            self.connection = psycopg2.connect(
                host=self.config['host'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            self.cursor = self.connection.cursor(cursor_factory=RealDictCursor)
            logger.info(f"Conexão estabelecida com o banco {self.config['database']} em {self.config['host']}")
            return True
        except Exception as e:
            logger.error(f"Erro ao conectar ao banco de dados: {str(e)}")
            return False
    
    def disconnect(self):
        """Fecha a conexão com o banco de dados."""
        if self.cursor:
            self.cursor.close()
            
        if self.connection:
            self.connection.close()
            logger.info("Conexão com o banco de dados fechada")
    
    def execute_query(self, query: str, params: tuple = None) -> Tuple[bool, Optional[List[Dict[str, Any]]]]:
        """
        Executa uma consulta SQL.
        
        Args:
            query: Consulta SQL a ser executada
            params: Parâmetros para a consulta (opcional)
            
        Returns:
            Tupla com status de sucesso e resultados (se houver)
        """
        if not self.connection or self.connection.closed:
            if not self.connect():
                return False, None
        
        try:
            self.cursor.execute(query, params)
            
            # Verifica se a consulta retorna resultados
            if query.strip().upper().startswith(('SELECT', 'SHOW', 'DESCRIBE')):
                results = self.cursor.fetchall()
                return True, [dict(row) for row in results]
            else:
                self.connection.commit()
                return True, None
                
        except Exception as e:
            logger.error(f"Erro ao executar consulta: {str(e)}")
            self.connection.rollback()
            return False, None
    
    def execute_batch(self, query: str, params_list: List[tuple]) -> Tuple[bool, int]:
        """
        Executa uma consulta SQL em lote.
        
        Args:
            query: Consulta SQL a ser executada
            params_list: Lista de parâmetros para a consulta
            
        Returns:
            Tupla com status de sucesso e número de registros afetados
        """
        if not self.connection or self.connection.closed:
            if not self.connect():
                return False, 0
        
        try:
            from psycopg2.extras import execute_batch
            
            execute_batch(self.cursor, query, params_list)
            self.connection.commit()
            
            return True, len(params_list)
                
        except Exception as e:
            logger.error(f"Erro ao executar consulta em lote: {str(e)}")
            self.connection.rollback()
            return False, 0
    
    def table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """
        Verifica se uma tabela existe no banco de dados.
        
        Args:
            table_name: Nome da tabela
            schema: Schema da tabela (padrão: 'public')
            
        Returns:
            True se a tabela existe, False caso contrário
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            );
        """
        
        success, results = self.execute_query(query, (schema, table_name))
        
        if success and results:
            return results[0]['exists']
        
        return False
    
    def get_table_columns(self, table_name: str, schema: str = 'public') -> List[str]:
        """
        Obtém as colunas de uma tabela.
        
        Args:
            table_name: Nome da tabela
            schema: Schema da tabela (padrão: 'public')
            
        Returns:
            Lista com os nomes das colunas
        """
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """
        
        success, results = self.execute_query(query, (schema, table_name))
        
        if success and results:
            return [row['column_name'] for row in results]
        
        return []


class SparkJDBCConnector:
    """
    Classe para conexão com banco de dados PostgreSQL via Spark JDBC.
    
    Esta classe fornece métodos para conectar e interagir
    com bancos de dados PostgreSQL usando Spark JDBC.
    """
    
    def __init__(self, spark: SparkSession, config: Dict[str, str] = None):
        """
        Inicializa o conector Spark JDBC.
        
        Args:
            spark: Sessão Spark ativa
            config: Configuração de conexão com o banco de dados (opcional)
                   Se não fornecido, usa as configurações globais
        """
        self.spark = spark
        
        if config is None:
            self.config = {
                'host': get_config('database', 'host'),
                'database': get_config('database', 'name'),
                'user': get_config('database', 'user'),
                'password': get_config('database', 'password')
            }
        else:
            self.config = config
        
        self.jdbc_url = f"jdbc:postgresql://{self.config['host']}/{self.config['database']}"
        self.properties = {
            "user": self.config["user"],
            "password": self.config["password"],
            "driver": "org.postgresql.Driver"
        }
    
    def read_table(self, table_name: str, schema: str = 'public'):
        """
        Lê uma tabela do banco de dados.
        
        Args:
            table_name: Nome da tabela
            schema: Schema da tabela (padrão: 'public')
            
        Returns:
            DataFrame Spark com os dados da tabela
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema != 'public' else table_name
            
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", full_table_name) \
                .option("user", self.config["user"]) \
                .option("password", self.config["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load()
                
            logger.info(f"Tabela {full_table_name} lida com sucesso")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao ler tabela {table_name}: {str(e)}")
            raise
    
    def write_dataframe(self, df, table_name: str, mode: str = "append", schema: str = 'public'):
        """
        Escreve um DataFrame em uma tabela do banco de dados.
        
        Args:
            df: DataFrame Spark a ser escrito
            table_name: Nome da tabela
            mode: Modo de escrita (append, overwrite, ignore, error)
            schema: Schema da tabela (padrão: 'public')
        """
        try:
            full_table_name = f"{schema}.{table_name}" if schema != 'public' else table_name
            
            df.write \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("dbtable", full_table_name) \
                .option("user", self.config["user"]) \
                .option("password", self.config["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .mode(mode) \
                .save()
                
            logger.info(f"Dados escritos na tabela {full_table_name} com sucesso (modo: {mode})")
            
        except Exception as e:
            logger.error(f"Erro ao escrever na tabela {table_name}: {str(e)}")
            raise
    
    def execute_query(self, query: str):
        """
        Executa uma consulta SQL e retorna o resultado como DataFrame.
        
        Args:
            query: Consulta SQL a ser executada
            
        Returns:
            DataFrame Spark com os resultados da consulta
        """
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.jdbc_url) \
                .option("query", query) \
                .option("user", self.config["user"]) \
                .option("password", self.config["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load()
                
            logger.info("Consulta executada com sucesso")
            return df
            
        except Exception as e:
            logger.error(f"Erro ao executar consulta: {str(e)}")
            raise


def get_db_connector(config: Dict[str, str] = None) -> PostgreSQLConnector:
    """
    Função de conveniência para obter um conector PostgreSQL.
    
    Args:
        config: Configuração de conexão com o banco de dados (opcional)
               Se não fornecido, usa as configurações globais
        
    Returns:
        Conector PostgreSQL configurado
    """
    return PostgreSQLConnector(config)


def get_spark_jdbc_connector(spark: SparkSession, config: Dict[str, str] = None) -> SparkJDBCConnector:
    """
    Função de conveniência para obter um conector Spark JDBC.
    
    Args:
        spark: Sessão Spark ativa
        config: Configuração de conexão com o banco de dados (opcional)
               Se não fornecido, usa as configurações globais
        
    Returns:
        Conector Spark JDBC configurado
    """
    return SparkJDBCConnector(spark, config)
