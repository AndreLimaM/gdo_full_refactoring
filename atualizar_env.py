#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para atualizar o arquivo .env com o caminho correto das credenciais
"""

import os

# Caminho para o arquivo .env
env_file = '.env'

# Conteúdo atualizado
env_content = '''
# Credenciais do Google Cloud Storage
GOOGLE_APPLICATION_CREDENTIALS=./credentials.json

# Configurações do bucket
GCS_BUCKET_NAME=repo-dev-gdo-carga
'''

# Escrever no arquivo .env
with open(env_file, 'w') as f:
    f.write(env_content)

print(f"Arquivo {env_file} atualizado com sucesso!")
