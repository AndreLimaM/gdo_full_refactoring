#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para analisar a estrutura do banco de dados PostgreSQL.

Este script realiza uma análise detalhada da estrutura do banco de dados,
incluindo informações sobre particionamento, clusterização, índices e
chaves estrangeiras, para auxiliar no desenvolvimento dos scripts de processamento.
"""

import sys
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
import tabulate
import json
from collections import defaultdict

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('analisar-estrutura-db')

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


# Estrutura para armazenar a análise
db_structure = {
    'info': {},
    'schemas': {},
    'table_groups': defaultdict(list),
    'partitioned_tables': {},
    'indexes': {},
    'foreign_keys': {},
    'clustering': {},
    'size_info': {}
}


def analisar_estrutura_db():
    """
    Analisa a estrutura do banco de dados PostgreSQL.
    
    Returns:
        bool: True se a análise for bem-sucedida, False caso contrário.
    """
    try:
        logger.info("Conectando ao banco de dados...")
        
        # Conectar ao banco de dados
        conn = psycopg2.connect(CONNECTION_STRING, cursor_factory=RealDictCursor)
        logger.info("Conexão estabelecida com sucesso!")
        
        # Criar cursor
        with conn.cursor() as cursor:
            # Obter informações do banco de dados
            analisar_info_db(cursor)
            
            # Analisar schemas
            analisar_schemas(cursor)
            
            # Analisar tabelas e agrupá-las por prefixo
            analisar_tabelas(cursor)
            
            # Analisar particionamento
            analisar_particionamento(cursor)
            
            # Analisar índices
            analisar_indices(cursor)
            
            # Analisar chaves estrangeiras
            analisar_chaves_estrangeiras(cursor)
            
            # Analisar clusterização
            analisar_clusterizacao(cursor)
            
            # Analisar tamanho das tabelas
            analisar_tamanho_tabelas(cursor)
            
            # Gerar relatório
            gerar_relatorio()
        
        # Fechar conexão
        conn.close()
        logger.info("Conexão fechada.")
        
        return True
    except Exception as e:
        logger.error(f"Erro ao analisar estrutura do banco de dados: {str(e)}")
        return False


def analisar_info_db(cursor):
    """
    Obtém informações gerais do banco de dados.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Obtendo informações gerais do banco de dados...")
    
    # Obter versão e nome do banco
    cursor.execute("SELECT current_database(), current_user, version()")
    info = cursor.fetchone()
    db_structure['info']['database'] = info['current_database']
    db_structure['info']['user'] = info['current_user']
    db_structure['info']['version'] = info['version']
    
    # Obter configurações de particionamento
    cursor.execute("SHOW max_parallel_workers_per_gather")
    db_structure['info']['max_parallel_workers_per_gather'] = cursor.fetchone()['max_parallel_workers_per_gather']
    
    # Obter estatísticas do banco
    cursor.execute("""
        SELECT 
            pg_database_size(current_database()) as db_size,
            (SELECT count(*) FROM pg_stat_activity) as active_connections
    """)
    stats = cursor.fetchone()
    db_structure['info']['size_bytes'] = stats['db_size']
    db_structure['info']['size_mb'] = round(stats['db_size'] / (1024 * 1024), 2)
    db_structure['info']['active_connections'] = stats['active_connections']
    
    print("\n===== INFORMAÇÕES DO BANCO DE DADOS =====")
    print(f"Banco de dados: {db_structure['info']['database']}")
    print(f"Versão: {db_structure['info']['version']}")
    print(f"Tamanho: {db_structure['info']['size_mb']} MB")
    print(f"Conexões ativas: {db_structure['info']['active_connections']}")
    print(f"Max workers por gather: {db_structure['info']['max_parallel_workers_per_gather']}")


def analisar_schemas(cursor):
    """
    Analisa os schemas do banco de dados.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Analisando schemas...")
    
    cursor.execute("""
        SELECT 
            schema_name, 
            COUNT(table_name) as num_tables 
        FROM 
            information_schema.schemata s 
        LEFT JOIN 
            information_schema.tables t ON s.schema_name = t.table_schema 
        WHERE 
            s.schema_name NOT IN ('pg_catalog', 'information_schema') 
        GROUP BY 
            schema_name 
        ORDER BY 
            schema_name
    """)
    
    schemas = cursor.fetchall()
    
    for schema in schemas:
        db_structure['schemas'][schema['schema_name']] = {
            'num_tables': schema['num_tables']
        }
    
    print("\n===== SCHEMAS =====")
    schema_data = [[s, db_structure['schemas'][s]['num_tables']] for s in db_structure['schemas']]
    print(tabulate.tabulate(schema_data, headers=["Schema", "Número de Tabelas"], tablefmt="grid"))


def analisar_tabelas(cursor):
    """
    Analisa as tabelas do banco de dados e as agrupa por prefixo.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Analisando tabelas...")
    
    cursor.execute("""
        SELECT 
            t.table_schema, 
            t.table_name, 
            COUNT(c.column_name) as num_columns,
            (SELECT COUNT(*) FROM pg_indexes WHERE schemaname = t.table_schema AND tablename = t.table_name) as num_indexes
        FROM 
            information_schema.tables t 
        JOIN 
            information_schema.columns c ON t.table_schema = c.table_schema AND t.table_name = c.table_name 
        WHERE 
            t.table_schema NOT IN ('pg_catalog', 'information_schema')
            AND t.table_type = 'BASE TABLE' 
        GROUP BY 
            t.table_schema, t.table_name 
        ORDER BY 
            t.table_schema, t.table_name
    """)
    
    tables = cursor.fetchall()
    
    # Agrupar tabelas por prefixo
    for table in tables:
        schema = table['table_schema']
        name = table['table_name']
        
        # Extrair prefixo (até o primeiro _)
        if '_' in name:
            prefix = name.split('_')[0]
        else:
            prefix = 'outros'
        
        db_structure['table_groups'][prefix].append({
            'schema': schema,
            'name': name,
            'num_columns': table['num_columns'],
            'num_indexes': table['num_indexes']
        })
    
    print("\n===== GRUPOS DE TABELAS =====")
    for prefix, tables in db_structure['table_groups'].items():
        print(f"\nGrupo: {prefix} ({len(tables)} tabelas)")
        table_data = [[t['schema'], t['name'], t['num_columns'], t['num_indexes']] for t in tables[:5]]
        if len(tables) > 5:
            table_data.append(['...', f"+ {len(tables) - 5} tabelas", '', ''])
        print(tabulate.tabulate(table_data, headers=["Schema", "Tabela", "Colunas", "Índices"], tablefmt="grid"))


def analisar_particionamento(cursor):
    """
    Analisa o particionamento das tabelas.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Analisando particionamento...")
    
    # Verificar tabelas particionadas
    cursor.execute("""
        SELECT 
            nmsp_parent.nspname AS parent_schema,
            parent.relname AS parent_table,
            nmsp_child.nspname AS child_schema,
            child.relname AS child_table
        FROM 
            pg_inherits
        JOIN 
            pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN 
            pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN 
            pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
        JOIN 
            pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace
        WHERE 
            nmsp_parent.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY 
            parent_schema, parent_table, child_schema, child_table
    """)
    
    partitions = cursor.fetchall()
    
    # Agrupar partições por tabela pai
    partition_groups = defaultdict(list)
    for partition in partitions:
        parent_key = f"{partition['parent_schema']}.{partition['parent_table']}"
        child_key = f"{partition['child_schema']}.{partition['child_table']}"
        partition_groups[parent_key].append(child_key)
    
    # Armazenar informações de particionamento
    for parent, children in partition_groups.items():
        db_structure['partitioned_tables'][parent] = {
            'num_partitions': len(children),
            'partitions': children
        }
    
    print("\n===== TABELAS PARTICIONADAS =====")
    if db_structure['partitioned_tables']:
        partition_data = []
        for parent, info in db_structure['partitioned_tables'].items():
            partition_data.append([parent, info['num_partitions']])
        print(tabulate.tabulate(partition_data, headers=["Tabela Pai", "Número de Partições"], tablefmt="grid"))
        
        # Mostrar exemplo de particionamento para a primeira tabela
        if partition_data:
            first_parent = partition_data[0][0]
            print(f"\nExemplo de partições para {first_parent}:")
            partitions = db_structure['partitioned_tables'][first_parent]['partitions'][:5]
            if len(db_structure['partitioned_tables'][first_parent]['partitions']) > 5:
                partitions.append(f"... + {len(db_structure['partitioned_tables'][first_parent]['partitions']) - 5} partições")
            for p in partitions:
                print(f"  - {p}")
    else:
        print("Nenhuma tabela particionada encontrada.")


def analisar_indices(cursor):
    """
    Analisa os índices do banco de dados.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Analisando índices...")
    
    cursor.execute("""
        SELECT 
            schemaname, 
            tablename, 
            indexname, 
            indexdef
        FROM 
            pg_indexes 
        WHERE 
            schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY 
            schemaname, tablename, indexname
    """)
    
    indexes = cursor.fetchall()
    
    # Agrupar índices por tabela
    for idx in indexes:
        table_key = f"{idx['schemaname']}.{idx['tablename']}"
        if table_key not in db_structure['indexes']:
            db_structure['indexes'][table_key] = []
        
        db_structure['indexes'][table_key].append({
            'name': idx['indexname'],
            'definition': idx['indexdef']
        })
    
    print("\n===== ESTATÍSTICAS DE ÍNDICES =====")
    print(f"Total de índices: {len(indexes)}")
    
    # Tabelas com mais índices
    top_indexed = sorted(db_structure['indexes'].items(), key=lambda x: len(x[1]), reverse=True)[:5]
    if top_indexed:
        print("\nTabelas com mais índices:")
        index_data = [[table, len(indices)] for table, indices in top_indexed]
        print(tabulate.tabulate(index_data, headers=["Tabela", "Número de Índices"], tablefmt="grid"))
        
        # Mostrar exemplo de índices para a primeira tabela
        first_table = top_indexed[0][0]
        print(f"\nExemplo de índices para {first_table}:")
        for idx in db_structure['indexes'][first_table][:3]:
            print(f"  - {idx['name']}: {idx['definition']}")
        if len(db_structure['indexes'][first_table]) > 3:
            print(f"  ... + {len(db_structure['indexes'][first_table]) - 3} índices")


def analisar_chaves_estrangeiras(cursor):
    """
    Analisa as chaves estrangeiras do banco de dados.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Analisando chaves estrangeiras...")
    
    cursor.execute("""
        SELECT
            tc.table_schema, 
            tc.table_name, 
            kcu.column_name, 
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name 
        FROM 
            information_schema.table_constraints AS tc 
        JOIN 
            information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN 
            information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE 
            tc.constraint_type = 'FOREIGN KEY' AND
            tc.table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY 
            tc.table_schema, tc.table_name
    """)
    
    foreign_keys = cursor.fetchall()
    
    # Agrupar chaves estrangeiras por tabela
    for fk in foreign_keys:
        table_key = f"{fk['table_schema']}.{fk['table_name']}"
        if table_key not in db_structure['foreign_keys']:
            db_structure['foreign_keys'][table_key] = []
        
        db_structure['foreign_keys'][table_key].append({
            'column': fk['column_name'],
            'references': f"{fk['foreign_table_schema']}.{fk['foreign_table_name']}.{fk['foreign_column_name']}"
        })
    
    print("\n===== CHAVES ESTRANGEIRAS =====")
    print(f"Total de chaves estrangeiras: {len(foreign_keys)}")
    
    # Tabelas com mais chaves estrangeiras
    top_fk = sorted(db_structure['foreign_keys'].items(), key=lambda x: len(x[1]), reverse=True)[:5]
    if top_fk:
        print("\nTabelas com mais chaves estrangeiras:")
        fk_data = [[table, len(fks)] for table, fks in top_fk]
        print(tabulate.tabulate(fk_data, headers=["Tabela", "Número de FKs"], tablefmt="grid"))
        
        # Mostrar exemplo de chaves estrangeiras para a primeira tabela
        if top_fk:
            first_table = top_fk[0][0]
            print(f"\nExemplo de chaves estrangeiras para {first_table}:")
            for fk in db_structure['foreign_keys'][first_table][:5]:
                print(f"  - {fk['column']} -> {fk['references']}")
            if len(db_structure['foreign_keys'][first_table]) > 5:
                print(f"  ... + {len(db_structure['foreign_keys'][first_table]) - 5} chaves estrangeiras")


def analisar_clusterizacao(cursor):
    """
    Analisa a clusterização das tabelas.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Analisando clusterização...")
    
    cursor.execute("""
        SELECT
            c.relname AS tablename,
            n.nspname AS schemaname,
            i.relname AS indexname
        FROM
            pg_index x
        JOIN
            pg_class c ON c.oid = x.indrelid
        JOIN
            pg_class i ON i.oid = x.indexrelid
        JOIN
            pg_namespace n ON n.oid = c.relnamespace
        WHERE
            x.indisclustered AND
            n.nspname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY
            schemaname, tablename
    """)
    
    clustered = cursor.fetchall()
    
    for cluster in clustered:
        table_key = f"{cluster['schemaname']}.{cluster['tablename']}"
        db_structure['clustering'][table_key] = cluster['indexname']
    
    print("\n===== TABELAS CLUSTERIZADAS =====")
    if db_structure['clustering']:
        cluster_data = [[table, index] for table, index in db_structure['clustering'].items()]
        print(tabulate.tabulate(cluster_data, headers=["Tabela", "Índice de Clusterização"], tablefmt="grid"))
    else:
        print("Nenhuma tabela clusterizada encontrada.")


def analisar_tamanho_tabelas(cursor):
    """
    Analisa o tamanho das tabelas.
    
    Args:
        cursor: Cursor do banco de dados.
    """
    logger.info("Analisando tamanho das tabelas...")
    
    cursor.execute("""
        SELECT
            schemaname,
            relname AS tablename,
            pg_size_pretty(pg_relation_size(schemaname || '.' || relname)) AS size,
            pg_relation_size(schemaname || '.' || relname) AS size_bytes
        FROM
            pg_stat_user_tables
        ORDER BY
            size_bytes DESC
        LIMIT 20
    """)
    
    sizes = cursor.fetchall()
    
    for size in sizes:
        table_key = f"{size['schemaname']}.{size['tablename']}"
        db_structure['size_info'][table_key] = {
            'size': size['size'],
            'size_bytes': size['size_bytes']
        }
    
    print("\n===== MAIORES TABELAS =====")
    size_data = [[table, info['size']] for table, info in db_structure['size_info'].items()]
    print(tabulate.tabulate(size_data, headers=["Tabela", "Tamanho"], tablefmt="grid"))


def gerar_relatorio():
    """
    Gera um relatório com a análise da estrutura do banco de dados.
    """
    logger.info("Gerando relatório de análise...")
    
    # Converter defaultdict para dict para serialização JSON
    db_structure_copy = db_structure.copy()
    db_structure_copy['table_groups'] = dict(db_structure_copy['table_groups'])
    
    # Salvar análise em arquivo JSON
    with open('analise_db_estrutura.json', 'w', encoding='utf-8') as f:
        json.dump(db_structure_copy, f, indent=2, ensure_ascii=False)
    
    # Gerar recomendações
    print("\n===== RECOMENDAÇÕES PARA PROCESSAMENTO =====")
    
    # Recomendações baseadas em particionamento
    if db_structure['partitioned_tables']:
        print("\n1. Particionamento:")
        print("   - Aproveite o particionamento existente para processar dados em paralelo")
        print("   - Utilize filtros por partição para reduzir a quantidade de dados processados")
        print("   - Considere o particionamento ao definir a estratégia de processamento incremental")
    
    # Recomendações baseadas em índices
    print("\n2. Índices:")
    print("   - Utilize os índices existentes para otimizar consultas de junção e filtragem")
    print("   - Evite operações que invalidem os índices durante o processamento em massa")
    
    # Recomendações baseadas em tamanho das tabelas
    print("\n3. Tamanho das tabelas:")
    print("   - Considere processar as tabelas maiores em lotes menores")
    print("   - Priorize a otimização de consultas em tabelas grandes")
    
    # Recomendações baseadas em relacionamentos
    if db_structure['foreign_keys']:
        print("\n4. Relacionamentos:")
        print("   - Respeite as dependências entre tabelas ao definir a ordem de processamento")
        print("   - Utilize junções eficientes baseadas nas chaves estrangeiras existentes")
    
    # Recomendações gerais
    print("\n5. Recomendações gerais:")
    print("   - Utilize consultas parametrizadas para evitar injeção de SQL")
    print("   - Implemente controle de transações para garantir consistência dos dados")
    print("   - Considere o uso de processamento paralelo para tabelas grandes")
    print("   - Monitore o desempenho das consultas durante o processamento")
    
    print("\nRelatório completo salvo em: analise_db_estrutura.json")


if __name__ == "__main__":
    # Analisar estrutura do banco de dados
    sucesso = analisar_estrutura_db()
    
    if sucesso:
        logger.info("Análise da estrutura do banco de dados concluída com sucesso!")
        sys.exit(0)
    else:
        logger.error("Falha ao analisar estrutura do banco de dados.")
        sys.exit(1)
