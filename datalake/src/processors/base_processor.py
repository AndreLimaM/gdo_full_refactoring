#!/usr/bin/env python3
"""Módulo base para processadores de rastreabilidade."""

from abc import ABC, abstractmethod
from datetime import date
import logging
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession


class BaseProcessor(ABC):
    """Classe base para processadores de rastreabilidade."""

    def __init__(self, spark: SparkSession):
        """Inicializa o processador.

        Args:
            spark: Sessão Spark ativa
        """
        self.spark = spark
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def process(self, *args, **kwargs) -> None:
        """Método principal de processamento. Deve ser implementado pelas subclasses."""
        pass

    def _create_dataframe_from_json(self, json_data: List[Dict]) -> DataFrame:
        """Cria um DataFrame Spark a partir de dados JSON.

        Args:
            json_data: Lista de dicionários com dados JSON

        Returns:
            DataFrame Spark
        """
        return self.spark.createDataFrame(json_data)

    def _write_to_postgres(
        self,
        df: DataFrame,
        table: str,
        mode: str = "append",
        options: Optional[Dict] = None
    ) -> None:
        """Escreve um DataFrame no PostgreSQL.

        Args:
            df: DataFrame a ser escrito
            table: Nome da tabela
            mode: Modo de escrita (append, overwrite, etc)
            options: Opções adicionais para a escrita
        """
        try:
            # Combina as opções fornecidas com as opções padrão
            jdbc_options = {
                "driver": "org.postgresql.Driver",
                "dbtable": table
            }
            
            # Adiciona as opções extras, se fornecidas
            if options:
                jdbc_options.update(options)
                
            # Escreve no banco de dados
            df.write \
                .format("jdbc") \
                .mode(mode) \
                .options(**jdbc_options) \
                .save()
        except Exception as e:
            self.logger.error(f"Erro ao escrever no PostgreSQL: {str(e)}")
            self.logger.error(f"Tabela: {table}, Modo: {mode}")
            raise
            
    def _execute_jdbc_query(self, query: str, params: Optional[list] = None, return_df: bool = False, db_options: Optional[Dict] = None) -> Optional[DataFrame]:
        """Executa uma query via JDBC.

        Args:
            query: Query SQL a ser executada
            params: Lista de parâmetros para a query (opcional)
            return_df: Se True, retorna o DataFrame resultante (para consultas SELECT)
                      Se False, executa a query sem retornar dados (para UPDATE, INSERT, DELETE)
            db_options: Opções de conexão com o banco de dados. Se None, usa self.db_options

        Returns:
            DataFrame se return_df for True, None caso contrário
        """
        try:
            # Verifica se as opções de DB foram fornecidas
            options = db_options if db_options is not None else getattr(self, 'db_options', None)
            if options is None:
                raise ValueError("Opções de conexão com o banco não fornecidas")
                
            # Se for uma consulta SELECT (return_df = True)
            if return_df:
                # Cria um dicionário com todas as opções JDBC
                jdbc_options = dict(options)  # Copia as opções originais
                jdbc_options['dbtable'] = f"({query}) as tmp"  # Adiciona a query como tabela temporária
                
                # Carrega os dados usando as opções JDBC
                result = self.spark.read.format("jdbc").options(**jdbc_options).load()
                return result
            # Se for uma consulta de atualização (UPDATE, INSERT, DELETE)
            else:
                # Usa psycopg2 para executar a query diretamente
                import psycopg2
                import re
                
                # Extrai os parâmetros de conexão da URL JDBC
                jdbc_url = options['url']
                url_pattern = r'jdbc:postgresql://([^:]+):([^/]+)/(.+)'
                match = re.match(url_pattern, jdbc_url)
                
                if match:
                    host = match.group(1)
                    port = match.group(2)
                    database = match.group(3)
                    
                    conn = psycopg2.connect(
                        host=host,
                        database=database,
                        user=options['user'],
                        password=options['password'],
                        port=port
                    )
                conn.autocommit = True  # Ativa autocommit
                cursor = conn.cursor()
                
                # Log da query sem parâmetros para segurança
                self.logger.info(f"Executando query: {query}")
                if params:
                    self.logger.debug(f"Parâmetros: {params}")
                
                # Executa a query com ou sem parâmetros
                if params is not None:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                affected_rows = cursor.rowcount
                cursor.close()
                conn.close()
                self.logger.info(f"Query executada com sucesso. Linhas afetadas: {affected_rows}")
                return None
        except Exception as e:
            self.logger.error(f"Erro ao executar query JDBC: {str(e)}")
            self.logger.error(f"Query: {query}")
            if return_df:
                # Retorna um DataFrame vazio em caso de erro
                return self.spark.createDataFrame([], [])
            raise  # Re-lança a exceção para ser tratada pelo chamador
