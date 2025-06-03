#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import RealDictCursor
import json
from tabulate import tabulate

def check_flag_rastreavel():
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        host="34.48.11.43",
        port="5432",
        database="db_eco_tcbf_25",
        user="db_eco_tcbf_25_user",
        password="5HN33PHKjXcLTz3tBC"
    )
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Verificar se a coluna flag_rastreavel existe na tabela bt_caixas
    print("\n=== Verificando se a coluna flag_rastreavel existe na tabela bt_caixas ===")
    cursor.execute("""
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'bt_caixas' 
    AND column_name = 'flag_rastreavel'
    """)
    
    rows = cursor.fetchall()
    if rows:
        print("A coluna flag_rastreavel existe na tabela bt_caixas")
    else:
        print("A coluna flag_rastreavel NÃO existe na tabela bt_caixas")
        return
    
    # Verificar status da flag_rastreavel
    print("\n=== Status da flag_rastreavel na tabela bt_caixas ===")
    cursor.execute("""
    SELECT 
        token_cliente,
        COUNT(*) as total_caixas,
        SUM(CASE WHEN flag_rastreavel = true THEN 1 ELSE 0 END) as caixas_rastreaveis,
        SUM(CASE WHEN flag_rastreavel = false THEN 1 ELSE 0 END) as caixas_nao_rastreaveis,
        SUM(CASE WHEN flag_rastreavel IS NULL THEN 1 ELSE 0 END) as caixas_flag_null
    FROM bt_caixas
    WHERE token_cliente = '24ad9d'
    GROUP BY token_cliente
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("Nenhum registro encontrado na tabela bt_caixas")
    
    # Verificar relação entre flag_rastreavel e outras flags
    print("\n=== Relação entre flag_rastreavel e outras flags ===")
    cursor.execute("""
    SELECT 
        CASE 
            WHEN flag_desossa = true AND flag_propriedades = true AND flag_animais = true THEN 'Todas flags true'
            WHEN flag_desossa = true AND (flag_propriedades = false OR flag_animais = false) THEN 'Desossa true, outras false'
            WHEN flag_desossa = false THEN 'Desossa false'
            ELSE 'Outros casos'
        END as status_flags,
        COUNT(*) as total_caixas,
        SUM(CASE WHEN flag_rastreavel = true THEN 1 ELSE 0 END) as caixas_rastreaveis,
        SUM(CASE WHEN flag_rastreavel = false THEN 1 ELSE 0 END) as caixas_nao_rastreaveis,
        SUM(CASE WHEN flag_rastreavel IS NULL THEN 1 ELSE 0 END) as caixas_flag_null
    FROM bt_caixas
    WHERE token_cliente = '24ad9d'
    GROUP BY status_flags
    ORDER BY status_flags
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("Nenhum registro encontrado na tabela bt_caixas")
    
    # Fechar conexão
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_flag_rastreavel()
