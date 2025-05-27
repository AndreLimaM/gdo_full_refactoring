#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para listar as tabelas existentes no banco de dados PostgreSQL.

Este script se conecta ao banco de dados usando as credenciais configuradas
e lista todas as tabelas disponíveis, junto com informações sobre suas colunas.
"""

import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import tabulate

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('listar-tabelas-db')

# String de conexão direta
import os
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Construir string de conexão a partir de variáveis de ambiente
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME')
DB_DRIVER = os.getenv('DB_DRIVER', 'postgresql')

CONNECTION_STRING = f"{DB_DRIVER}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Para debug (remover em produção)
# Não exibe a senha real, apenas um placeholder
debug_conn_string = f"{DB_DRIVER}://{DB_USER}:***@{DB_HOST}:{DB_PORT}/{DB_NAME}"
print(f"Conectando ao banco de dados: {debug_conn_string}")



def listar_tabelas():
    """
    Lista todas as tabelas disponíveis no banco de dados.
    
    Returns:
        bool: True se a operação for bem-sucedida, False caso contrário.
    """
    try:
        logger.info("Conectando ao banco de dados...")
        
        # Conectar ao banco de dados
        conn = psycopg2.connect(CONNECTION_STRING, cursor_factory=RealDictCursor)
        logger.info("Conexão estabelecida com sucesso!")
        
        # Criar cursor
        with conn.cursor() as cursor:
            # Consulta para obter informações do banco de dados
            logger.info("Obtendo informações do banco de dados...")
            cursor.execute("SELECT current_database(), current_user, version()")
            result = cursor.fetchone()
            print("\n===== INFORMAÇÕES DO BANCO DE DADOS =====")
            print(f"Banco de dados: {result['current_database']}")
            print(f"Usuário: {result['current_user']}")
            print(f"Versão: {result['version']}\n")
            
            # Listar schemas
            logger.info("Listando schemas disponíveis...")
            cursor.execute(
                "SELECT schema_name, COUNT(table_name) as num_tables " 
                "FROM information_schema.schemata s " 
                "LEFT JOIN information_schema.tables t ON s.schema_name = t.table_schema " 
                "WHERE s.schema_name NOT IN ('pg_catalog', 'information_schema') " 
                "GROUP BY schema_name " 
                "ORDER BY schema_name"
            )
            schemas = cursor.fetchall()
            
            print("===== SCHEMAS DISPONÍVEIS =====")
            schema_data = [[s['schema_name'], s['num_tables']] for s in schemas]
            print(tabulate.tabulate(schema_data, headers=["Schema", "Número de Tabelas"], tablefmt="grid"))
            print()
            
            # Listar tabelas com contagem de colunas
            logger.info("Listando tabelas disponíveis com contagem de colunas...")
            cursor.execute(
                "SELECT t.table_schema, t.table_name, COUNT(c.column_name) as num_columns " 
                "FROM information_schema.tables t " 
                "JOIN information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name " 
                "WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') " 
                "GROUP BY t.table_schema, t.table_name " 
                "ORDER BY t.table_schema, t.table_name"
            )
            tables = cursor.fetchall()
            
            print("===== TABELAS DISPONÍVEIS =====")
            table_data = [[t['table_schema'], t['table_name'], t['num_columns']] for t in tables]
            print(tabulate.tabulate(table_data, headers=["Schema", "Tabela", "Número de Colunas"], tablefmt="grid"))
            print()
            
            # Contar total de tabelas
            total_tables = len(tables)
            print(f"Total de tabelas: {total_tables}\n")
            
            # Listar as 5 tabelas com mais colunas
            logger.info("Identificando as 5 tabelas com mais colunas...")
            cursor.execute(
                "SELECT t.table_schema, t.table_name, COUNT(c.column_name) as num_columns " 
                "FROM information_schema.tables t " 
                "JOIN information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name " 
                "WHERE t.table_schema NOT IN ('pg_catalog', 'information_schema') " 
                "GROUP BY t.table_schema, t.table_name " 
                "ORDER BY num_columns DESC " 
                "LIMIT 5"
            )
            complex_tables = cursor.fetchall()
            
            print("===== TOP 5 TABELAS COM MAIS COLUNAS =====")
            complex_data = [[t['table_schema'], t['table_name'], t['num_columns']] for t in complex_tables]
            print(tabulate.tabulate(complex_data, headers=["Schema", "Tabela", "Número de Colunas"], tablefmt="grid"))
            print()
            
            # Verificar se existem tabelas particionadas
            logger.info("Verificando tabelas particionadas...")
            cursor.execute(
                "SELECT COUNT(*) as count FROM pg_inherits"
            )
            partitioned = cursor.fetchone()
            
            if partitioned['count'] > 0:
                print(f"O banco de dados contém {partitioned['count']} relações de herança/particionamento.\n")
            else:
                print("O banco de dados não contém tabelas particionadas.\n")
            
            # Mostrar detalhes de uma tabela específica (exemplo com a primeira tabela)
            if tables:
                first_table = tables[0]
                logger.info(f"Mostrando detalhes da tabela {first_table['table_schema']}.{first_table['table_name']}...")
                cursor.execute(
                    "SELECT column_name, data_type, character_maximum_length, is_nullable " 
                    "FROM information_schema.columns " 
                    f"WHERE table_schema = '{first_table['table_schema']}' AND table_name = '{first_table['table_name']}' " 
                    "ORDER BY ordinal_position"
                )
                columns = cursor.fetchall()
                
                print(f"===== DETALHES DA TABELA {first_table['table_schema']}.{first_table['table_name']} =====")
                column_data = [
                    [c['column_name'], c['data_type'], 
                     c['character_maximum_length'] if c['character_maximum_length'] else 'N/A', 
                     c['is_nullable']]
                    for c in columns
                ]
                print(tabulate.tabulate(column_data, headers=["Coluna", "Tipo", "Tamanho Máx.", "Nullable"], tablefmt="grid"))
                print()
        
        # Fechar conexão
        conn.close()
        logger.info("Conexão fechada.")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao listar tabelas: {str(e)}")
        return False


if __name__ == "__main__":
    # Verificar se o pacote tabulate está instalado
    try:
        import tabulate
    except ImportError:
        logger.error("O pacote 'tabulate' não está instalado. Instalando...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "tabulate"])
        logger.info("Pacote 'tabulate' instalado com sucesso.")
        import tabulate
    
    # Listar tabelas
    sucesso = listar_tabelas()
    
    if sucesso:
        logger.info("Listagem de tabelas concluída com sucesso!")
        sys.exit(0)
    else:
        logger.error("Falha ao listar tabelas.")
        sys.exit(1)
