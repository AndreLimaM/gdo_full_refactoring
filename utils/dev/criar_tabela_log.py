#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para criar a tabela log_processamento no banco de dados.
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Adiciona o diretório raiz ao path para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Carrega as variáveis de ambiente
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
    
    # Lê o arquivo SQL
    with open('src/sql/create_log_table.sql', 'r') as f:
        sql = f.read()
    
    # Executa o SQL
    cursor.execute(sql)
    
    # Commit das alterações
    conn.commit()
    
    print('Tabela log_processamento criada com sucesso!')
    
    # Verifica se a tabela foi criada
    cursor.execute("SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'log_processamento')")
    if cursor.fetchone()[0]:
        print('Verificação: Tabela log_processamento existe no banco de dados.')
    else:
        print('Verificação: Tabela log_processamento NÃO foi encontrada no banco de dados!')
    
except Exception as e:
    print(f'Erro ao criar tabela log_processamento: {str(e)}')
finally:
    # Fecha a conexão
    cursor.close()
    conn.close()
