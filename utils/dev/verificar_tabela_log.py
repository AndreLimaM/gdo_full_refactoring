#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para verificar a estrutura da tabela log_processamento.
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Adiciona o diretu00f3rio raiz ao path para importau00e7u00f5es relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Carrega as variu00e1veis de ambiente
load_dotenv()

# Conecta ao banco de dados
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

try:
    # Cria um cursor
    cursor = conn.cursor()
    
    # Verifica a estrutura da tabela
    cursor.execute("""
    SELECT column_name, data_type, character_maximum_length 
    FROM information_schema.columns 
    WHERE table_name = 'log_processamento' 
    ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    
    print('\n===== ESTRUTURA DA TABELA log_processamento =====\n')
    for col in columns:
        max_length = f" (tamanho: {col[2]})" if col[2] else ""
        print(f"{col[0]}: {col[1]}{max_length}")
    
    # Verifica os u00edndices da tabela
    cursor.execute("""
    SELECT indexname, indexdef
    FROM pg_indexes
    WHERE tablename = 'log_processamento'
    """)
    
    indices = cursor.fetchall()
    
    print('\n===== u00cdNDICES DA TABELA log_processamento =====\n')
    if indices:
        for idx in indices:
            print(f"{idx[0]}: {idx[1]}")
    else:
        print("Nenhum u00edndice encontrado.")
    
except Exception as e:
    print(f'Erro ao verificar tabela log_processamento: {str(e)}')
finally:
    # Fecha a conexu00e3o
    cursor.close()
    conn.close()
