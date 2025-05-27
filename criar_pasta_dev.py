#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar a pasta de desenvolvimento com scripts de teste e utilitários temporários.

Este script cria a seguinte estrutura no bucket:
- datalake/dev/
  - tests/
  - utils/
  - samples/
  - notebooks/
  - temp/

Esta estrutura contém scripts e arquivos que são úteis durante o desenvolvimento,
mas que podem ser removidos ao entregar o projeto final.
"""

import os
import logging
import tempfile
from gcs_connector import WindsurfGCSConnector
from config.config_manager import ConfigManager

# Configuração de logging
config = ConfigManager()
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging_level = config.get('logging.level', 'INFO')
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('criar-pasta-dev')

# Caminho base no bucket
BASE_PATH = "datalake/dev/"

# Subpastas de desenvolvimento
DEV_FOLDERS = {
    'tests/': "Scripts de testes unitários e de integração",
    'utils/': "Utilitários para desenvolvimento e depuração",
    'samples/': "Arquivos de exemplo e dados de teste",
    'notebooks/': "Jupyter notebooks para análise exploratória",
    'temp/': "Arquivos temporários e experimentos"
}


def upload_string_to_file(connector, file_path, content):
    """
    Faz upload de uma string como conteúdo de um arquivo no bucket GCS.
    
    Args:
        connector: Instância do WindsurfGCSConnector
        file_path: Caminho do arquivo no bucket
        content: Conteúdo do arquivo como string
    """
    try:
        # Criar arquivo temporário
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
            temp.write(content)
            temp_filename = temp.name
        
        # Fazer upload do arquivo
        connector.upload_file(temp_filename, file_path)
        logger.info(f"Arquivo criado: {file_path}")
        
        # Remover arquivo temporário
        os.remove(temp_filename)
    except Exception as e:
        logger.error(f"Erro ao fazer upload do arquivo {file_path}: {str(e)}")
        raise


def criar_pasta_dev():
    """
    Cria a estrutura de pastas para desenvolvimento no bucket GCS.
    """
    try:
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Criar a estrutura de pastas
        pastas_criadas = []
        
        # Criar pasta base datalake/dev/
        connector.create_folder(BASE_PATH)
        pastas_criadas.append(BASE_PATH)
        logger.info(f"Pasta base criada: {BASE_PATH}")
        
        # Adicionar README na pasta base
        readme_base = "# Pasta de Desenvolvimento\n\nEste diretório contém scripts, utilitários e arquivos temporários utilizados durante o desenvolvimento do projeto.\n\n## Estrutura\n\n"
        for folder, desc in DEV_FOLDERS.items():
            readme_base += f"- **{folder}**: {desc}\n"
        
        readme_base += "\n## Observação\n\nEsta pasta e seu conteúdo podem ser removidos ao entregar o projeto final, pois não fazem parte do código de produção."
        
        upload_string_to_file(connector, BASE_PATH + "README.md", readme_base)
        logger.info(f"README adicionado à pasta base: {BASE_PATH}")
        
        # Criar subpastas de desenvolvimento
        for folder, desc in DEV_FOLDERS.items():
            folder_path = f"{BASE_PATH}{folder}"
            connector.create_folder(folder_path)
            pastas_criadas.append(folder_path)
            logger.info(f"Pasta criada: {folder_path}")
            
            # Adicionar README na subpasta
            readme_content = f"# {folder.rstrip('/')}\n\n{desc}\n\n## Propósito\n\nEsta pasta contém {desc.lower()}.\n\n## Observação\n\nEsta pasta e seu conteúdo podem ser removidos ao entregar o projeto final."
            upload_string_to_file(connector, folder_path + "README.md", readme_content)
            logger.info(f"README adicionado à pasta: {folder_path}")
        
        # Adicionar arquivo .gitignore na pasta temp
        gitignore_content = "# Ignorar todos os arquivos na pasta temp\n*\n# Exceto o README.md\n!README.md\n"
        upload_string_to_file(connector, BASE_PATH + "temp/.gitignore", gitignore_content)
        logger.info(f"Arquivo .gitignore adicionado à pasta temp")
        
        logger.info(f"Estrutura de desenvolvimento criada com sucesso! Total de {len(pastas_criadas)} pastas.")
        return pastas_criadas
        
    except Exception as e:
        logger.error(f"Erro ao criar estrutura de desenvolvimento: {str(e)}")
        raise


def verificar_pasta_dev():
    """
    Verifica se a estrutura de desenvolvimento existe no bucket GCS.
    """
    try:
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Listar todas as pastas no bucket
        blobs = connector.list_files(prefix=BASE_PATH)
        
        # Verificar se as pastas existem
        pastas_esperadas = [BASE_PATH]
        
        # Adicionar subpastas de desenvolvimento
        for folder in DEV_FOLDERS.keys():
            folder_path = f"{BASE_PATH}{folder}"
            pastas_esperadas.append(folder_path)
        
        # Verificar cada pasta esperada
        pastas_existentes = []
        pastas_faltantes = []
        
        for pasta in pastas_esperadas:
            if pasta in blobs:
                pastas_existentes.append(pasta)
                logger.info(f"Pasta encontrada: {pasta}")
            else:
                pastas_faltantes.append(pasta)
                logger.warning(f"Pasta não encontrada: {pasta}")
        
        return {
            "existentes": pastas_existentes,
            "faltantes": pastas_faltantes
        }
        
    except Exception as e:
        logger.error(f"Erro ao verificar estrutura de desenvolvimento: {str(e)}")
        raise


def documentar_estrutura():
    """
    Gera documentação da estrutura de desenvolvimento.
    """
    doc = """# Estrutura de Desenvolvimento para o Projeto GDO

Este documento descreve a estrutura de pastas de desenvolvimento criada no bucket GCS.

## Visão Geral

A estrutura de desenvolvimento contém scripts, utilitários e arquivos temporários utilizados durante o desenvolvimento do projeto, mas que não farão parte do código de produção final.

## Estrutura de Pastas

```
datalake/dev/
├── tests/                 # Scripts de testes unitários e de integração
├── utils/                 # Utilitários para desenvolvimento e depuração
├── samples/               # Arquivos de exemplo e dados de teste
├── notebooks/             # Jupyter notebooks para análise exploratória
└── temp/                  # Arquivos temporários e experimentos
```

## Propósito

Esta estrutura foi criada para:

1. **Separar código de desenvolvimento do código de produção**: Manter o código de produção limpo e organizado
2. **Facilitar a remoção de código temporário**: Ao entregar o projeto, esta pasta pode ser facilmente removida
3. **Organizar scripts de teste e utilitários**: Manter os scripts de teste e utilitários em um local centralizado

## Observação

Esta pasta e seu conteúdo podem ser removidos ao entregar o projeto final, pois não fazem parte do código de produção.
"""
    
    # Salvar a documentação em um arquivo
    with open("estrutura_dev_gdo.md", "w") as f:
        f.write(doc)
    
    logger.info("Documentação da estrutura de desenvolvimento gerada: estrutura_dev_gdo.md")
    return doc


if __name__ == "__main__":
    # Verificar se a estrutura já existe
    resultado = verificar_pasta_dev()
    
    # Se existem pastas faltantes, criar a estrutura
    if resultado["faltantes"]:
        logger.info(f"Existem {len(resultado['faltantes'])} pastas faltantes. Criando estrutura...")
        criar_pasta_dev()
        
        # Verificar novamente após a criação
        resultado = verificar_pasta_dev()
        if not resultado["faltantes"]:
            logger.info("Estrutura de desenvolvimento criada com sucesso!")
        else:
            logger.warning(f"Ainda existem {len(resultado['faltantes'])} pastas faltantes após a criação.")
    else:
        logger.info("Estrutura de desenvolvimento já existe completamente no bucket.")
    
    # Gerar documentação
    documentar_estrutura()
