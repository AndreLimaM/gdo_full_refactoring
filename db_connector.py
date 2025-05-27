#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Conector para Cloud SQL do Sistema de Processamento de Dados GDO

Este mu00f3dulo fornece funcionalidades para conectar o sistema de processamento
de dados GDO com bancos de dados Cloud SQL no Google Cloud Platform.

Classes:
    CloudSQLConnector: Classe principal para conexu00e3o com o Cloud SQL.

Exemplo de uso:
    ```python
    # Inicializar o conector
    db_connector = CloudSQLConnector()
    
    # Executar uma consulta
    results = db_connector.execute_query("SELECT * FROM gdo_payloads LIMIT 10")
    
    # Inserir dados
    db_connector.insert_data("gdo_payloads", {"payload_id": "123", "data": {"key": "value"}})
    ```

Autor: Equipe Windsurf
Versu00e3o: 1.0.0
Data: 27/05/2025
"""

import os
import json
import logging
import time
from typing import Dict, List, Any, Optional, Union, Tuple
from pathlib import Path
from contextlib import contextmanager

# SQLAlchemy para conexu00e3o com o banco de dados
from sqlalchemy import create_engine, text, MetaData, Table, Column, Integer, String, JSON, DateTime, Boolean, ForeignKey, select
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool
from sqlalchemy.exc import SQLAlchemyError

# Secret Manager para obter credenciais seguras
try:
    from google.cloud import secretmanager
    HAS_SECRET_MANAGER = True
except ImportError:
    HAS_SECRET_MANAGER = False

# Importar o gerenciador de configurau00e7u00f5es
try:
    from config.config_manager import ConfigManager
    # Inicializar o gerenciador de configurau00e7u00f5es
    config = ConfigManager()
    USE_CONFIG_FILE = True
except (ImportError, FileNotFoundError):
    # Fallback para valores padru00e3o se o gerenciador de configurau00e7u00f5es nu00e3o estiver disponu00edvel
    USE_CONFIG_FILE = False
    config = None

# Configurau00e7u00e3o de logging
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s') if USE_CONFIG_FILE else '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging_level = config.get('logging.level', 'INFO') if USE_CONFIG_FILE else 'INFO'
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('cloudsql-connector')

# Base para modelos ORM
Base = declarative_base()


class CloudSQLConnector:
    """
    Classe para conexu00e3o com o Cloud SQL no Google Cloud Platform.
    
    Esta classe fornece uma interface simplificada para interagir com bancos de dados
    Cloud SQL, permitindo operau00e7u00f5es como consultas, inseru00e7u00f5es, atualizau00e7u00f5es e exclusu00f5es.
    Suporta PostgreSQL e MySQL, com configurau00e7u00f5es de pool de conexu00f5es e autenticau00e7u00e3o
    segura atravu00e9s do Secret Manager.
    
    Atributos:
        engine (sqlalchemy.engine.Engine): Engine do SQLAlchemy para conexu00e3o com o banco.
        Session (sqlalchemy.orm.sessionmaker): Factory de sessu00f5es do SQLAlchemy.
        metadata (sqlalchemy.MetaData): Metadados do banco de dados.
        db_type (str): Tipo de banco de dados (postgresql, mysql).
        tables (Dict[str, Table]): Dicionu00e1rio de tabelas do banco de dados.
    """

    def __init__(self, db_type: Optional[str] = None, connection_params: Optional[Dict[str, Any]] = None):
        """
        Inicializa o conector Cloud SQL.
        
        Args:
            db_type (Optional[str]): Tipo de banco de dados (postgresql, mysql).
                                     Se nu00e3o fornecido, seru00e1 lido do arquivo de configurau00e7u00e3o.
            connection_params (Optional[Dict[str, Any]]): Paru00e2metros de conexu00e3o personalizados.
                                                        Se nu00e3o fornecidos, seru00e3o lidos do arquivo de configurau00e7u00e3o.
        
        Raises:
            ValueError: Se o tipo de banco de dados nu00e3o for suportado ou se os paru00e2metros de conexu00e3o forem invu00e1lidos.
            RuntimeError: Se ocorrer um erro ao estabelecer a conexu00e3o com o banco de dados.
        """
        # Obter configurau00e7u00f5es do arquivo ou paru00e2metros fornecidos
        self._load_config(db_type, connection_params)
        
        # Criar engine do SQLAlchemy
        self._create_engine()
        
        # Inicializar metadados e sessu00e3o
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)
        
        # Inicializar dicionu00e1rio de tabelas
        self.tables = {}
        
        # Carregar tabelas existentes
        self._load_tables()
        
        logger.info(f"Conector Cloud SQL inicializado para {self.db_type} em {self.instance_connection_name}")

    def _load_config(self, db_type: Optional[str], connection_params: Optional[Dict[str, Any]]) -> None:
        """
        Carrega as configurações de conexão com o banco de dados.
        
        Args:
            db_type (Optional[str]): Tipo de banco de dados.
            connection_params (Optional[Dict[str, Any]]): Parâmetros de conexão personalizados.
        
        Raises:
            ValueError: Se o tipo de banco de dados não for suportado.
        """
        # Definir tipo de banco de dados
        if db_type:
            self.db_type = db_type
        elif USE_CONFIG_FILE:
            self.db_type = config.get('cloudsql.db_type', 'postgresql')
        else:
            self.db_type = os.getenv('DB_TYPE', 'postgresql')
        
        # Validar tipo de banco de dados
        if self.db_type not in ['postgresql', 'mysql']:
            raise ValueError(f"Tipo de banco de dados não suportado: {self.db_type}. Use 'postgresql' ou 'mysql'.")
        
        # Obter parâmetros de conexão
        if connection_params:
            # Usar parâmetros fornecidos
            self.connection_params = connection_params
        elif USE_CONFIG_FILE:
            # Verificar se estamos usando o novo formato de configuração (host/port)
            if config.get('cloudsql.instance.host'):
                self.host = config.get('cloudsql.instance.host')
                self.port = config.get('cloudsql.instance.port', 5432)
                self.database = config.get('cloudsql.connection.database')
                self.user = config.get('cloudsql.connection.user')
                self.password = config.get('cloudsql.connection.password')
                self.use_proxy = config.get('cloudsql.connection.use_proxy', False)
                self.use_ssl = config.get('cloudsql.connection.use_ssl', True)
                # Configurações de pool
                self.pool_size = config.get('cloudsql.pool.pool_size', 5)
                self.max_overflow = config.get('cloudsql.pool.max_overflow', 10)
                self.pool_timeout = config.get('cloudsql.pool.pool_timeout', 30)
                self.pool_recycle = config.get('cloudsql.pool.pool_recycle', 1800)
                # Não precisamos do instance_connection_name no novo formato
                self.instance_connection_name = None
            else:
                # Formato antigo (project_id/region/instance_name)
                self.project_id = config.get('cloudsql.instance.project_id')
                self.region = config.get('cloudsql.instance.region')
                self.instance_name = config.get('cloudsql.instance.instance_name')
                self.database = config.get('cloudsql.connection.database')
                self.user = config.get('cloudsql.connection.user')
                self.password_secret = config.get('cloudsql.connection.password_secret')
                self.use_proxy = config.get('cloudsql.connection.use_proxy', True)
                self.socket_path = config.get('cloudsql.connection.socket_path')
                # Configurações de pool
                self.pool_size = config.get('cloudsql.pool.pool_size', 5)
                self.max_overflow = config.get('cloudsql.pool.max_overflow', 10)
                self.pool_timeout = config.get('cloudsql.pool.pool_timeout', 30)
                self.pool_recycle = config.get('cloudsql.pool.pool_recycle', 1800)
                # Construir nome de conexão da instância
                self.instance_connection_name = f"{self.project_id}:{self.region}:{self.instance_name}"
                # Obter senha do Secret Manager se necessário
                if self.password_secret and HAS_SECRET_MANAGER:
                    self.password = self._get_secret(self.project_id, self.password_secret)
                else:
                    self.password = os.getenv('DB_PASSWORD', '')
        else:
            # Usar variáveis de ambiente
            # Verificar se estamos usando o novo formato de configuração (host/port)
            if os.getenv('DB_HOST'):
                self.host = os.getenv('DB_HOST')
                self.port = int(os.getenv('DB_PORT', '5432'))
                self.database = os.getenv('DB_NAME')
                self.user = os.getenv('DB_USER')
                self.password = os.getenv('DB_PASSWORD', '')
                self.use_proxy = os.getenv('DB_USE_PROXY', 'false').lower() == 'true'
                self.use_ssl = os.getenv('DB_USE_SSL', 'true').lower() == 'true'
                # Não precisamos do instance_connection_name no novo formato
                self.instance_connection_name = None
            else:
                # Formato antigo (project_id/region/instance_name)
                self.project_id = os.getenv('DB_PROJECT_ID')
                self.region = os.getenv('DB_REGION')
                self.instance_name = os.getenv('DB_INSTANCE_NAME')
                self.database = os.getenv('DB_NAME')
                self.user = os.getenv('DB_USER')
                self.password_secret = os.getenv('DB_PASSWORD_SECRET')
                self.use_proxy = os.getenv('DB_USE_PROXY', 'true').lower() == 'true'
                self.socket_path = os.getenv('DB_SOCKET_PATH')
                # Construir nome de conexão da instância
                self.instance_connection_name = f"{self.project_id}:{self.region}:{self.instance_name}"
                # Obter senha do Secret Manager se necessário
                if self.password_secret and HAS_SECRET_MANAGER:
                    self.password = self._get_secret(self.project_id, self.password_secret)
                else:
                    self.password = os.getenv('DB_PASSWORD', '')
            
            # Configurações de pool
            self.pool_size = int(os.getenv('DB_POOL_SIZE', '5'))
            self.max_overflow = int(os.getenv('DB_MAX_OVERFLOW', '10'))
            self.pool_timeout = int(os.getenv('DB_POOL_TIMEOUT', '30'))
            self.pool_recycle = int(os.getenv('DB_POOL_RECYCLE', '1800'))

    def _get_secret(self, project_id: str, secret_name: str) -> str:
        """
        Obtu00e9m um segredo do Secret Manager.
        
        Args:
            project_id (str): ID do projeto GCP.
            secret_name (str): Nome do segredo no Secret Manager.
            
        Returns:
            str: Valor do segredo.
            
        Raises:
            RuntimeError: Se ocorrer um erro ao acessar o Secret Manager.
        """
        try:
            client = secretmanager.SecretManagerServiceClient()
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")
        except Exception as e:
            logger.error(f"Erro ao obter segredo {secret_name}: {str(e)}")
            raise RuntimeError(f"Erro ao acessar o Secret Manager: {str(e)}")

    def _create_engine(self) -> None:
        """
        Cria o engine do SQLAlchemy para conexão com o banco de dados.
        
        Raises:
            RuntimeError: Se ocorrer um erro ao criar o engine.
        """
        try:
            # Construir URL de conexão
            if self.use_proxy:
                # Conexão via socket Unix (Cloud SQL Proxy)
                if self.db_type == 'postgresql':
                    db_url = f"postgresql+psycopg2://{self.user}:{self.password}@/{self.database}?host={self.socket_path}"
                else:  # MySQL
                    db_url = f"mysql+pymysql://{self.user}:{self.password}@/{self.database}?unix_socket={self.socket_path}"
            else:
                # Conexão TCP/IP direta
                if hasattr(self, 'host') and hasattr(self, 'port'):
                    # Usar host e porta diretamente (novo formato)
                    if self.db_type == 'postgresql':
                        db_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                        if hasattr(self, 'use_ssl') and self.use_ssl:
                            db_url += "?sslmode=require"
                    else:  # MySQL
                        db_url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                        if hasattr(self, 'use_ssl') and self.use_ssl:
                            db_url += "?ssl=true"
                else:
                    # Usar instance_connection_name (formato antigo)
                    if self.db_type == 'postgresql':
                        db_url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.instance_connection_name}/{self.database}"
                    else:  # MySQL
                        db_url = f"mysql+pymysql://{self.user}:{self.password}@{self.instance_connection_name}/{self.database}"
            
            # Criar engine com configurações de pool
            self.engine = create_engine(
                db_url,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle
            )
            
            # Testar conexão
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            logger.info("Conexão com o banco de dados estabelecida com sucesso")
            
        except Exception as e:
            error_msg = f"Erro ao criar engine do SQLAlchemy: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

    def _load_tables(self) -> None:
        """
        Carrega as tabelas existentes no banco de dados.
        """
        try:
            # Refletir tabelas existentes
            self.metadata.reflect(bind=self.engine)
            
            # Armazenar referu00eancias u00e0s tabelas
            for table_name in self.metadata.tables:
                self.tables[table_name] = self.metadata.tables[table_name]
                
            logger.info(f"Carregadas {len(self.tables)} tabelas do banco de dados")
            
        except SQLAlchemyError as e:
            logger.warning(f"Erro ao carregar tabelas existentes: {str(e)}")

    @contextmanager
    def session_scope(self):
        """
        Fornece um escopo de sessu00e3o que gerencia o commit/rollback automaticamente.
        
        Yields:
            Session: Sessu00e3o do SQLAlchemy.
            
        Example:
            ```python
            with db_connector.session_scope() as session:
                result = session.execute(text("SELECT * FROM my_table"))
            ```
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Erro na sessu00e3o do banco de dados: {str(e)}")
            raise
        finally:
            session.close()

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Executa uma consulta SQL e retorna os resultados.
        
        Args:
            query (str): Consulta SQL a ser executada.
            params (Optional[Dict[str, Any]]): Parâmetros para a consulta.
            
        Returns:
            List[Dict[str, Any]]: Lista de resultados como dicionários.
            
        Raises:
            SQLAlchemyError: Se ocorrer um erro na execução da consulta.
        """
        with self.session_scope() as session:
            try:
                result = session.execute(text(query), params or {})
                # Obter nomes das colunas
                columns = result.keys()
                # Converter resultados em dicionários
                return [dict(zip(columns, row)) for row in result.fetchall()]
            except Exception as e:
                logger.error(f"Erro ao executar consulta: {str(e)}")
                raise

    def insert_data(self, table_name: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        """
        Insere dados em uma tabela.
        
        Args:
            table_name (str): Nome da tabela.
            data (Union[Dict[str, Any], List[Dict[str, Any]]]): Dados a serem inseridos.
                Pode ser um u00fanico dicionu00e1rio ou uma lista de dicionu00e1rios.
                
        Returns:
            int: Nu00famero de registros inseridos.
            
        Raises:
            ValueError: Se a tabela nu00e3o existir.
            SQLAlchemyError: Se ocorrer um erro na inseru00e7u00e3o.
        """
        # Verificar se a tabela existe
        if table_name not in self.tables:
            raise ValueError(f"Tabela nu00e3o encontrada: {table_name}")
        
        # Converter para lista se for um u00fanico dicionu00e1rio
        if isinstance(data, dict):
            data = [data]
        
        # Inserir dados
        with self.session_scope() as session:
            result = session.execute(self.tables[table_name].insert().values(data))
            return len(data)

    def update_data(self, table_name: str, condition: Dict[str, Any], data: Dict[str, Any]) -> int:
        """
        Atualiza dados em uma tabela.
        
        Args:
            table_name (str): Nome da tabela.
            condition (Dict[str, Any]): Condiu00e7u00e3o para atualizau00e7u00e3o (WHERE).
            data (Dict[str, Any]): Dados a serem atualizados.
            
        Returns:
            int: Nu00famero de registros atualizados.
            
        Raises:
            ValueError: Se a tabela nu00e3o existir.
            SQLAlchemyError: Se ocorrer um erro na atualizau00e7u00e3o.
        """
        # Verificar se a tabela existe
        if table_name not in self.tables:
            raise ValueError(f"Tabela nu00e3o encontrada: {table_name}")
        
        # Construir condiu00e7u00e3o WHERE
        where_clause = []
        for key, value in condition.items():
            where_clause.append(getattr(self.tables[table_name].c, key) == value)
        
        # Atualizar dados
        with self.session_scope() as session:
            result = session.execute(
                self.tables[table_name].update()
                .where(*where_clause)
                .values(**data)
            )
            return result.rowcount

    def delete_data(self, table_name: str, condition: Dict[str, Any]) -> int:
        """
        Exclui dados de uma tabela.
        
        Args:
            table_name (str): Nome da tabela.
            condition (Dict[str, Any]): Condiu00e7u00e3o para exclusu00e3o (WHERE).
            
        Returns:
            int: Nu00famero de registros excluu00eddos.
            
        Raises:
            ValueError: Se a tabela nu00e3o existir.
            SQLAlchemyError: Se ocorrer um erro na exclusu00e3o.
        """
        # Verificar se a tabela existe
        if table_name not in self.tables:
            raise ValueError(f"Tabela nu00e3o encontrada: {table_name}")
        
        # Construir condiu00e7u00e3o WHERE
        where_clause = []
        for key, value in condition.items():
            where_clause.append(getattr(self.tables[table_name].c, key) == value)
        
        # Excluir dados
        with self.session_scope() as session:
            result = session.execute(
                self.tables[table_name].delete()
                .where(*where_clause)
            )
            return result.rowcount

    def create_table(self, table_name: str, columns: List[Column], if_not_exists: bool = True) -> Table:
        """
        Cria uma nova tabela no banco de dados.
        
        Args:
            table_name (str): Nome da tabela.
            columns (List[Column]): Lista de colunas da tabela.
            if_not_exists (bool): Se True, nu00e3o gera erro se a tabela ju00e1 existir.
            
        Returns:
            Table: Objeto Table do SQLAlchemy.
            
        Raises:
            SQLAlchemyError: Se ocorrer um erro na criau00e7u00e3o da tabela.
        """
        # Verificar se a tabela ju00e1 existe
        if table_name in self.tables and if_not_exists:
            return self.tables[table_name]
        
        # Criar nova tabela
        table = Table(table_name, self.metadata, *columns)
        
        # Criar tabela no banco de dados
        with self.engine.begin() as conn:
            table.create(conn, checkfirst=if_not_exists)
        
        # Adicionar ao dicionu00e1rio de tabelas
        self.tables[table_name] = table
        logger.info(f"Tabela {table_name} criada com sucesso")
        
        return table

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtu00e9m o schema de uma tabela.
        
        Args:
            table_name (str): Nome da tabela.
            
        Returns:
            List[Dict[str, Any]]: Lista de colunas e seus tipos.
            
        Raises:
            ValueError: Se a tabela nu00e3o existir.
        """
        # Verificar se a tabela existe
        if table_name not in self.tables:
            raise ValueError(f"Tabela nu00e3o encontrada: {table_name}")
        
        # Obter informau00e7u00f5es das colunas
        columns = []
        for column in self.tables[table_name].columns:
            columns.append({
                "name": column.name,
                "type": str(column.type),
                "nullable": column.nullable,
                "primary_key": column.primary_key,
                "default": str(column.default) if column.default else None
            })
        
        return columns

    def execute_transaction(self, operations: List[Tuple[str, Dict[str, Any]]]) -> bool:
        """
        Executa mu00faltiplas operau00e7u00f5es em uma u00fanica transau00e7u00e3o.
        
        Args:
            operations (List[Tuple[str, Dict[str, Any]]]): Lista de tuplas (query, params).
            
        Returns:
            bool: True se a transau00e7u00e3o for bem-sucedida, False caso contru00e1rio.
            
        Example:
            ```python
            operations = [
                ("INSERT INTO table1 (col1, col2) VALUES (:val1, :val2)", {"val1": 1, "val2": "test"}),
                ("UPDATE table2 SET col1 = :val WHERE id = :id", {"val": "new_value", "id": 123})
            ]
            success = db_connector.execute_transaction(operations)
            ```
        """
        try:
            with self.session_scope() as session:
                for query, params in operations:
                    session.execute(text(query), params or {})
                return True
        except Exception as e:
            logger.error(f"Erro na transau00e7u00e3o: {str(e)}")
            return False

    def check_connection(self) -> bool:
        """
        Verifica se a conexu00e3o com o banco de dados estu00e1 funcionando.
        
        Returns:
            bool: True se a conexu00e3o estiver funcionando, False caso contru00e1rio.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Erro ao verificar conexu00e3o: {str(e)}")
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """
        Retorna informau00e7u00f5es sobre a conexu00e3o com o banco de dados.
        
        Returns:
            Dict[str, Any]: Informau00e7u00f5es sobre a conexu00e3o.
        """
        return {
            "db_type": self.db_type,
            "instance": self.instance_connection_name,
            "database": self.database,
            "user": self.user,
            "use_proxy": self.use_proxy,
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "pool_recycle": self.pool_recycle,
            "tables": list(self.tables.keys())
        }


# Exemplo de uso
if __name__ == "__main__":
    # Inicializar o conector
    try:
        db_connector = CloudSQLConnector()
        
        # Verificar conexu00e3o
        if db_connector.check_connection():
            print("Conexu00e3o com o banco de dados estabelecida com sucesso!")
            
            # Exibir informau00e7u00f5es de conexu00e3o
            connection_info = db_connector.get_connection_info()
            print(f"Tipo de banco: {connection_info['db_type']}")
            print(f"Instu00e2ncia: {connection_info['instance']}")
            print(f"Banco de dados: {connection_info['database']}")
            print(f"Tabelas disponu00edveis: {connection_info['tables']}")
        else:
            print("Falha ao conectar com o banco de dados.")
    except Exception as e:
        print(f"Erro ao inicializar o conector: {str(e)}")
