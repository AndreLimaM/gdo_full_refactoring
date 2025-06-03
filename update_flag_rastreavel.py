#!/usr/bin/env python3

import psycopg2
from psycopg2.extras import RealDictCursor
from tabulate import tabulate

def update_flag_rastreavel():
    # Conectar ao banco de dados
    conn = psycopg2.connect(
        host="34.48.11.43",
        port="5432",
        database="db_eco_tcbf_25",
        user="db_eco_tcbf_25_user",
        password="5HN33PHKjXcLTz3tBC"
    )
    
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Verificar status antes da atualizau00e7u00e3o
    print("\n=== Status das Flags Antes da Atualizau00e7u00e3o ===")
    cursor.execute("""
    SELECT 
        token_cliente,
        COUNT(*) as total_caixas,
        SUM(CASE WHEN flag_desossa = true AND flag_propriedades = true AND flag_animais = true THEN 1 ELSE 0 END) as caixas_completas,
        SUM(CASE WHEN flag_rastreavel = true THEN 1 ELSE 0 END) as caixas_rastreaveis
    FROM bt_caixas
    WHERE token_cliente = '24ad9d'
    GROUP BY token_cliente
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    
    # Atualizar flag_rastreavel para true onde todas as outras flags su00e3o true
    print("\n=== Atualizando flag_rastreavel para caixas com rastreabilidade completa ===")
    cursor.execute("""
    UPDATE bt_caixas
    SET flag_rastreavel = true
    WHERE token_cliente = '24ad9d'
      AND flag_desossa = true
      AND flag_propriedades = true
      AND flag_animais = true
      AND (flag_rastreavel = false OR flag_rastreavel IS NULL)
    RETURNING COUNT(*) as caixas_atualizadas
    """)
    
    conn.commit()
    result = cursor.fetchone()
    if result:
        print(f"Caixas atualizadas: {result['caixas_atualizadas']}")
    
    # Verificar status apu00f3s a atualizau00e7u00e3o
    print("\n=== Status das Flags Apu00f3s a Atualizau00e7u00e3o ===")
    cursor.execute("""
    SELECT 
        token_cliente,
        COUNT(*) as total_caixas,
        SUM(CASE WHEN flag_desossa = true AND flag_propriedades = true AND flag_animais = true THEN 1 ELSE 0 END) as caixas_completas,
        SUM(CASE WHEN flag_rastreavel = true THEN 1 ELSE 0 END) as caixas_rastreaveis
    FROM bt_caixas
    WHERE token_cliente = '24ad9d'
    GROUP BY token_cliente
    """)
    
    rows = cursor.fetchall()
    if rows:
        print(tabulate(rows, headers="keys", tablefmt="psql"))
    
    # Fechar conexu00e3o
    cursor.close()
    conn.close()

if __name__ == "__main__":
    update_flag_rastreavel()
