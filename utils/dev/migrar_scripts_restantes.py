#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para migrar os arquivos restantes da raiz do projeto para as pastas de utilitu00e1rios.

Este script identifica os arquivos Python que ainda estu00e3o na raiz do projeto e os
migra para as pastas apropriadas de utilitu00e1rios, mantendo apenas os arquivos
essenciais na raiz.
"""

import os
import shutil
import logging

# Configurau00e7u00e3o de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('migrar-scripts-restantes')

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
    # Scripts de processamento principal que seru00e3o desenvolvidos
    'process_raw.py',
    'process_trusted.py',
    'process_service.py',
    'main_processor.py',
    # Scripts de migrau00e7u00e3o e organizau00e7u00e3o
    'migrar_scripts_restantes.py'
]

# Mapeamento de arquivos para pastas de destino
MAPEAMENTO_ARQUIVOS = {
    # Scripts relacionados ao GCS
    'criar_estrutura_codigo_corrigido.py': 'utils/gcs/',
    'criar_estrutura_medalha.py': 'utils/gcs/',
    'criar_estrutura_pastas_corrigida.py': 'utils/gcs/',
    'datalake_operations.py': 'utils/gcs/',
    'exemplo_acesso_datalake.py': 'utils/gcs/',
    'listar_datalake.py': 'utils/gcs/',
    
    # Scripts relacionados ao ambiente
    'atualizar_env.py': 'utils/config/'
}


def migrar_scripts():
    """
    Migra os scripts restantes da raiz para as pastas apropriadas.
    """
    logger.info("Iniciando migrau00e7u00e3o de scripts restantes...")
    
    # Listar todos os arquivos Python na raiz
    arquivos_py = [f for f in os.listdir(BASE_DIR) if f.endswith('.py') and os.path.isfile(os.path.join(BASE_DIR, f))]
    
    # Verificar quais arquivos ju00e1 foram migrados para as pastas de utilitu00e1rios
    arquivos_migrados = []
    for root, _, files in os.walk(os.path.join(BASE_DIR, 'utils')):
        for file in files:
            if file.endswith('.py'):
                arquivos_migrados.append(file)
    
    # Migrar arquivos que nu00e3o su00e3o essenciais e ainda nu00e3o foram migrados
    for arquivo in arquivos_py:
        if arquivo not in ARQUIVOS_ESSENCIAIS and arquivo not in arquivos_migrados:
            # Verificar se o arquivo estu00e1 no mapeamento
            if arquivo in MAPEAMENTO_ARQUIVOS:
                pasta_destino = MAPEAMENTO_ARQUIVOS[arquivo]
            else:
                # Para arquivos nu00e3o mapeados, determinar a pasta com base no nome
                if 'db' in arquivo or 'database' in arquivo or 'sql' in arquivo:
                    pasta_destino = 'utils/database/'
                elif 'gcs' in arquivo or 'bucket' in arquivo or 'storage' in arquivo or 'datalake' in arquivo:
                    pasta_destino = 'utils/gcs/'
                elif 'config' in arquivo or 'env' in arquivo or 'settings' in arquivo:
                    pasta_destino = 'utils/config/'
                else:
                    pasta_destino = 'utils/misc/'
            
            # Criar pasta de destino se nu00e3o existir
            pasta_destino_completa = os.path.join(BASE_DIR, pasta_destino)
            os.makedirs(pasta_destino_completa, exist_ok=True)
            
            # Copiar o arquivo para a pasta de destino
            origem = os.path.join(BASE_DIR, arquivo)
            destino = os.path.join(pasta_destino_completa, arquivo)
            
            # Verificar se o arquivo ju00e1 existe no destino
            if os.path.exists(destino):
                logger.info(f"Arquivo {arquivo} ju00e1 existe em {pasta_destino}")
            else:
                shutil.copy2(origem, destino)
                logger.info(f"Arquivo {arquivo} copiado para {pasta_destino}")
    
    logger.info("Migrau00e7u00e3o de scripts concluu00edda!")
    
    # Listar arquivos que permaneceram na raiz
    arquivos_raiz = [f for f in os.listdir(BASE_DIR) if os.path.isfile(os.path.join(BASE_DIR, f))]
    logger.info("Arquivos que permaneceram na raiz:")
    for arquivo in sorted(arquivos_raiz):
        logger.info(f"  - {arquivo}")


def criar_scripts_processamento():
    """
    Cria scripts vazios para o processamento principal.
    """
    logger.info("Criando scripts de processamento principal...")
    
    # Conteu00fado base para os scripts de processamento
    conteudo_base = '''#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""{}"""  # Descrição será formatada depois

import os
import logging
import json
from datetime import datetime
from utils.database.db_connector import CloudSQLConnector
from utils.gcs.gcs_connector import WindsurfGCSConnector
from config.config_manager import ConfigManager

# Configuração de logging
config = ConfigManager()
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging_level = config.get('logging.level', 'INFO')
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('{}')  # Nome do logger será formatado depois


def main():
    """
    Função principal de processamento.
    """
    logger.info("Iniciando processamento {}...")  # Camada será formatada depois
    
    # Inicializar conectores
    db_connector = CloudSQLConnector()
    gcs_connector = WindsurfGCSConnector()
    
    # TODO: Implementar lógica de processamento {}
    
    logger.info("Processamento {} concluído com sucesso!")


if __name__ == "__main__":
    main()
'''
    
    # Scripts de processamento a serem criados
    scripts_processamento = {
        'process_raw.py': {
            'descricao': 'Script para processamento da camada Raw do GDO.\n\nEste script lu00ea os arquivos JSON do bucket GCS, processa os dados brutos\ne os armazena na camada Raw, mantendo a integridade original dos dados.',
            'nome_logger': 'process-raw',
            'camada': 'Raw'
        },
        'process_trusted.py': {
            'descricao': 'Script para processamento da camada Trusted do GDO.\n\nEste script lu00ea os dados da camada Raw, aplica validau00e7u00f5es, correu00e7u00f5es\ne transformau00e7u00f5es para garantir a qualidade e consistu00eancia dos dados.',
            'nome_logger': 'process-trusted',
            'camada': 'Trusted'
        },
        'process_service.py': {
            'descricao': 'Script para processamento da camada Service do GDO.\n\nEste script lu00ea os dados da camada Trusted, aplica transformau00e7u00f5es\nespecu00edficas para atender aos requisitos de negu00f3cio e disponibiliza\nos dados para consumo pela aplicau00e7u00e3o cliente.',
            'nome_logger': 'process-service',
            'camada': 'Service'
        },
        'main_processor.py': {
            'descricao': 'Script principal para orquestrar o processamento completo do GDO.\n\nEste script coordena a execuu00e7u00e3o sequencial dos processamentos\nRaw, Trusted e Service, garantindo a integridade do fluxo de dados.',
            'nome_logger': 'main-processor',
            'camada': 'completo'
        }
    }
    
    # Criar os scripts de processamento
    for nome_script, info in scripts_processamento.items():
        caminho_script = os.path.join(BASE_DIR, nome_script)
        
        # Verificar se o script ju00e1 existe
        if os.path.exists(caminho_script):
            logger.info(f"Script {nome_script} ju00e1 existe")
        else:
            # Criar o script com o conteúdo base
            with open(caminho_script, 'w', encoding='utf-8') as f:
                conteudo_formatado = conteudo_base.format(
                    info['descricao'],
                    info['nome_logger'],
                    info['camada'],
                    info['camada'],
                    info['camada']
                )
                f.write(conteudo_formatado)
            logger.info(f"Script {nome_script} criado com sucesso")


if __name__ == "__main__":
    # Criar pasta misc se nu00e3o existir
    os.makedirs(os.path.join(BASE_DIR, 'utils/misc'), exist_ok=True)
    
    # Migrar scripts restantes
    migrar_scripts()
    
    # Criar scripts de processamento
    criar_scripts_processamento()
