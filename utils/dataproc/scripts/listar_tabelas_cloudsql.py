#!/usr/bin/env python3

"""
Script para listar todas as tabelas do banco de dados Cloud SQL

Este script conecta ao Cloud SQL e lista todas as tabelas disponu00edveis,
mostrando detalhes como nu00famero de registros, estrutura e exemplos de dados.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import sys
import time

# Iniciar sessu00e3o Spark
print("=== Iniciando Sessu00e3o Spark ===")
spark = SparkSession.builder \
    .appName("ListarTabelasCloudSQL") \
    .config("spark.jars", "/usr/lib/spark/jars/postgresql-42.2.23.jar") \
    .getOrCreate()

# Configurar o nu00edvel de log para reduzir a verbosidade
spark.sparkContext.setLogLevel("WARN")

# Configurau00e7u00f5es de conexu00e3o com o banco de dados
db_host = "34.48.11.43"  # IP pu00fablico do Cloud SQL
db_name = "db_eco_tcbf_25"
db_user = "db_eco_tcbf_25_user"
db_password = "5HN33PHKjXcLTz3tBC"
db_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"

# Propriedades da conexu00e3o JDBC
db_properties = {
    "url": db_url,
    "user": db_user,
    "password": db_password,
    "driver": "org.postgresql.Driver"
}

# Testar conexu00e3o com o banco de dados
print("\n=== Testando conexu00e3o com o Cloud SQL ===")
print(f"Conectando a: {db_url}")

try:
    # Executar uma consulta simples para testar a conexu00e3o
    inicio = time.time()
    test_df = spark.read \
        .format("jdbc") \
        .option("driver", db_properties["driver"]) \
        .option("url", db_properties["url"]) \
        .option("dbtable", "(SELECT 1 as teste) AS test") \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .load()
    fim = time.time()
    
    test_df.show()
    print(f"Conexu00e3o estabelecida com sucesso! Tempo: {fim - inicio:.2f} segundos")
    
    # Listar todas as tabelas do esquema pu00fablico
    print("\n=== Listando tabelas do banco de dados ===")
    tables_query = """
    SELECT 
        table_name, 
        table_type,
        (pg_stat_all_tables.n_live_tup) AS row_count,
        pg_size_pretty(pg_total_relation_size(table_name::text)) AS table_size
    FROM 
        information_schema.tables
    LEFT JOIN 
        pg_stat_all_tables ON information_schema.tables.table_name = pg_stat_all_tables.relname
    WHERE 
        table_schema = 'public'
    ORDER BY 
        row_count DESC
    """
    
    tables_df = spark.read \
        .format("jdbc") \
        .option("driver", db_properties["driver"]) \
        .option("url", db_properties["url"]) \
        .option("dbtable", f"({tables_query}) AS tables") \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .load()
    
    # Mostrar informau00e7u00f5es sobre as tabelas
    print(f"Total de tabelas encontradas: {tables_df.count()}")
    print("\nInformau00e7u00f5es das tabelas:")
    tables_df.show(100, truncate=False)
    
    # Obter detalhes sobre a tabela bt_animais (se existir)
    if "bt_animais" in [row["table_name"] for row in tables_df.collect()]:
        print("\n=== Detalhes da tabela bt_animais ===")
        
        # Obter a estrutura da tabela
        print("\nEstrutura da tabela:")
        animais_df = spark.read \
            .format("jdbc") \
            .option("driver", db_properties["driver"]) \
            .option("url", db_properties["url"]) \
            .option("dbtable", "bt_animais") \
            .option("user", db_properties["user"]) \
            .option("password", db_properties["password"]) \
            .load()
        
        animais_df.printSchema()
        
        # Contar registros
        count = animais_df.count()
        print(f"\nNu00famero total de registros: {count}")
        
        # Mostrar alguns exemplos de registros
        if count > 0:
            print("\nExemplos de registros:")
            animais_df.show(5, truncate=False)
            
            # Estatu00edsticas bu00e1sicas
            print("\nEstatu00edsticas bu00e1sicas:")
            # Verificar se a coluna 'sexo' existe
            if "sexo" in animais_df.columns:
                print("\nDistribuiu00e7u00e3o por sexo:")
                animais_df.groupBy("sexo").count().orderBy(col("count").desc()).show()
            
            # Verificar se a coluna 'raca' existe
            if "raca" in animais_df.columns:
                print("\nDistribuiu00e7u00e3o por rau00e7a:")
                animais_df.groupBy("raca").count().orderBy(col("count").desc()).show(10)
    
    # Listar outras tabelas importantes (primeiras 5 tabelas com mais registros)
    top_tables = [row["table_name"] for row in tables_df.orderBy(col("row_count").desc()).take(5)]
    
    for table in top_tables:
        if table != "bt_animais":  # Evitar mostrar novamente a tabela bt_animais
            print(f"\n=== Amostra da tabela {table} ===")
            
            table_df = spark.read \
                .format("jdbc") \
                .option("driver", db_properties["driver"]) \
                .option("url", db_properties["url"]) \
                .option("dbtable", table) \
                .option("user", db_properties["user"]) \
                .option("password", db_properties["password"]) \
                .load()
            
            print("Estrutura:")
            table_df.printSchema()
            
            print("Amostra de dados:")
            table_df.show(3, truncate=False)
    
    print("\n=== Informau00e7u00f5es sobre o banco de dados ===")
    
    # Obter informau00e7u00f5es sobre o banco de dados
    db_info_query = """
    SELECT 
        current_database() as database_name,
        current_user as current_user,
        version() as postgresql_version,
        pg_size_pretty(pg_database_size(current_database())) as database_size
    """
    
    db_info_df = spark.read \
        .format("jdbc") \
        .option("driver", db_properties["driver"]) \
        .option("url", db_properties["url"]) \
        .option("dbtable", f"({db_info_query}) AS db_info") \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .load()
    
    db_info_df.show(truncate=False)
    
except Exception as e:
    print(f"Erro ao conectar ou consultar o banco de dados: {e}")
    sys.exit(1)

# Encerrar sessu00e3o Spark
spark.stop()
print("\n=== Teste concluu00eddo com sucesso ===")
sys.exit(0)
