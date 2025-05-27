#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para limpar a raiz do projeto, movendo todos os arquivos utilitu00e1rios
para pastas apropriadas e mantendo apenas os arquivos essenciais para o
processamento diu00e1rio.
"""

import os
import shutil
import logging

# Configurau00e7u00e3o de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('limpar-raiz-projeto')

# Diretu00f3rio base do projeto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Arquivos que devem permanecer na raiz (essenciais para o processamento)
ARQUIVOS_ESSENCIAIS = [
    # Arquivos de configurau00e7u00e3o e inicializau00e7u00e3o
    '__init__.py',
    'setup.py',
    'requirements.txt',
    'README.md',
    '.gitignore',
    '.env',
    '.env.example',
    'credentials.json',
    'credentials.json.example',
    # Scripts de processamento principal
    'process_raw.py',
    'process_trusted.py',
    'process_service.py',
    'main_processor.py',
    # Script atual
    'limpar_raiz_projeto.py'
]

# Pasta para scripts utilitu00e1rios diversos
PASTA_UTILS_DEV = os.path.join(BASE_DIR, 'utils', 'dev')


def limpar_raiz():
    """
    Move todos os arquivos utilitu00e1rios da raiz para pastas apropriadas.
    """
    logger.info("Iniciando limpeza da raiz do projeto...")
    
    # Criar pasta utils/dev se nu00e3o existir
    os.makedirs(PASTA_UTILS_DEV, exist_ok=True)
    
    # Criar arquivo README.md na pasta utils/dev se nu00e3o existir
    readme_path = os.path.join(PASTA_UTILS_DEV, 'README.md')
    if not os.path.exists(readme_path):
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write("# Scripts de Desenvolvimento\n\n")
            f.write("Esta pasta contu00e9m scripts utilitu00e1rios criados durante o desenvolvimento ")
            f.write("do projeto, que nu00e3o su00e3o essenciais para o processamento diu00e1rio dos payloads, ")
            f.write("mas podem ser u00fateis para tarefas de configurau00e7u00e3o, organizau00e7u00e3o e testes.\n")
    
    # Listar todos os arquivos Python na raiz
    arquivos_py = [f for f in os.listdir(BASE_DIR) 
                  if os.path.isfile(os.path.join(BASE_DIR, f)) and f.endswith('.py')]
    
    # Mover arquivos nu00e3o essenciais para a pasta utils/dev
    for arquivo in arquivos_py:
        if arquivo not in ARQUIVOS_ESSENCIAIS:
            origem = os.path.join(BASE_DIR, arquivo)
            destino = os.path.join(PASTA_UTILS_DEV, arquivo)
            
            # Verificar se o arquivo ju00e1 existe no destino
            if os.path.exists(destino):
                logger.info(f"Arquivo {arquivo} ju00e1 existe em utils/dev")
            else:
                shutil.copy2(origem, destino)
                logger.info(f"Arquivo {arquivo} copiado para utils/dev")
                # Remover o arquivo original da raiz
                os.remove(origem)
                logger.info(f"Arquivo {arquivo} removido da raiz")
    
    # Mover arquivos MD que nu00e3o su00e3o README.md para utils/dev
    arquivos_md = [f for f in os.listdir(BASE_DIR) 
                  if os.path.isfile(os.path.join(BASE_DIR, f)) and f.endswith('.md') 
                  and f != 'README.md']
    
    for arquivo in arquivos_md:
        origem = os.path.join(BASE_DIR, arquivo)
        destino = os.path.join(PASTA_UTILS_DEV, arquivo)
        
        # Verificar se o arquivo ju00e1 existe no destino
        if os.path.exists(destino):
            logger.info(f"Arquivo {arquivo} ju00e1 existe em utils/dev")
        else:
            shutil.copy2(origem, destino)
            logger.info(f"Arquivo {arquivo} copiado para utils/dev")
            # Remover o arquivo original da raiz
            os.remove(origem)
            logger.info(f"Arquivo {arquivo} removido da raiz")
    
    # Mover arquivos JSON que nu00e3o su00e3o credentials.json para utils/dev
    arquivos_json = [f for f in os.listdir(BASE_DIR) 
                    if os.path.isfile(os.path.join(BASE_DIR, f)) and f.endswith('.json') 
                    and f != 'credentials.json' and f != 'credentials.json.example']
    
    for arquivo in arquivos_json:
        origem = os.path.join(BASE_DIR, arquivo)
        destino = os.path.join(PASTA_UTILS_DEV, arquivo)
        
        # Verificar se o arquivo ju00e1 existe no destino
        if os.path.exists(destino):
            logger.info(f"Arquivo {arquivo} ju00e1 existe em utils/dev")
        else:
            shutil.copy2(origem, destino)
            logger.info(f"Arquivo {arquivo} copiado para utils/dev")
            # Remover o arquivo original da raiz
            os.remove(origem)
            logger.info(f"Arquivo {arquivo} removido da raiz")
    
    logger.info("Limpeza da raiz do projeto concluu00edda!")
    
    # Listar arquivos que permaneceram na raiz
    arquivos_raiz = [f for f in os.listdir(BASE_DIR) if os.path.isfile(os.path.join(BASE_DIR, f))]
    logger.info("Arquivos que permaneceram na raiz:")
    for arquivo in sorted(arquivos_raiz):
        logger.info(f"  - {arquivo}")


if __name__ == "__main__":
    limpar_raiz()
