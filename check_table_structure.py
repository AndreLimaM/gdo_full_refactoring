#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import RealDictCursor
from tabulate import tabulate

def check_table_structure():
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        host="34.48.11.43",
        port="5432",
        database="db_eco_tcbf_25",
        user="db_eco_tcbf_25_user",
        password="5HN33PHKjXcLTz3tBC"
    )
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Verificar estrutura da tabela bt_caixas
    print("\n=== Estrutura da tabela bt_caixas ===")
    cursor.execute("""
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'bt_caixas'
    ORDER BY ordinal_position
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("Tabela bt_caixas não encontrada")
    
    # Verificar se há algum trigger ou constraint relacionado à flag_rastreavel
    print("\n=== Triggers na tabela bt_caixas ===")
    cursor.execute("""
    SELECT trigger_name, event_manipulation, action_statement
    FROM information_schema.triggers
    WHERE event_object_table = 'bt_caixas'
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("Nenhum trigger encontrado para a tabela bt_caixas")
    
    # Verificar se há alguma função que possa estar atualizando a flag_rastreavel
    print("\n=== Funções que mencionam flag_rastreavel ===")
    cursor.execute("""
    SELECT routine_name, routine_definition
    FROM information_schema.routines
    WHERE routine_type = 'FUNCTION'
    AND routine_definition LIKE '%flag_rastreavel%'
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    else:
        print("Nenhuma função encontrada que mencione flag_rastreavel")
    
    # Fechar conexão
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_table_structure()
