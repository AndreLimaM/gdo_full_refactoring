#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar a estrutura de pastas no bucket GCS para processamento de payloads GDO

Este script cria a seguinte estrutura no bucket:
- datalake/payload_data/
  - pending/
    - movimentacoes/
    - caixas/
    - desossas/
    - animais/
    - escalas/
  - done/
    - movimentacoes/
    - caixas/
    - desossas/
    - animais/
    - escalas/

Esta estrutura permite o controle fu00edsico dos arquivos a processar (pending)
e dos que ju00e1 foram processados (done).
"""

import os
import logging
from gcs_connector import WindsurfGCSConnector
from config.config_manager import ConfigManager

# Configurau00e7u00e3o de logging
config = ConfigManager()
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging_level = config.get('logging.level', 'INFO')
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('criar-estrutura-pastas')

# Caminho base no bucket
BASE_PATH = "datalake/payload_data/"

# Tipos de payloads
TIPOS_PAYLOAD = ['movimentacoes', 'caixas', 'desossas', 'animais', 'escalas']

# Estrutura de pastas
ESTRUTURA = {
    'pending/': TIPOS_PAYLOAD,
    'done/': TIPOS_PAYLOAD
}

def criar_estrutura_pastas():
    """
    Cria a estrutura de pastas no bucket GCS para processamento de payloads GDO.
    """
    try:
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Criar a estrutura de pastas
        pastas_criadas = []
        
        # Criar pasta base datalake/payload_data/
        connector.create_folder(BASE_PATH)
        pastas_criadas.append(BASE_PATH)
        logger.info(f"Pasta base criada: {BASE_PATH}")
        
        # Adicionar README na pasta base
        readme_base = "# Diretório de Dados GDO\n\nEste diretório contém os dados para processamento do GDO, organizados em pastas para arquivos pendentes e processados."
        upload_string_to_file(connector, BASE_PATH + "README.md", readme_base)
        logger.info(f"README adicionado à pasta base: {BASE_PATH}")
        
        # Criar subpastas pending/ e done/
        for subpasta_nivel1 in ESTRUTURA.keys():
            caminho_completo = f"{BASE_PATH}{subpasta_nivel1}"
            connector.create_folder(caminho_completo)
            pastas_criadas.append(caminho_completo)
            logger.info(f"Subpasta criada: {caminho_completo}")
            
            # Adicionar README na subpasta
            if "pending" in subpasta_nivel1:
                readme_subpasta = "# Diretório de Arquivos Pendentes\n\nEste diretório contém os arquivos JSON pendentes de processamento, organizados por tipo de payload."
            else:
                readme_subpasta = "# Diretório de Arquivos Processados\n\nEste diretório contém os arquivos JSON que já foram processados, organizados por tipo de payload."
            
            upload_string_to_file(connector, caminho_completo + "README.md", readme_subpasta)
            logger.info(f"README adicionado à pasta: {caminho_completo}")
            
            # Criar subpastas para cada tipo de payload
            for tipo_payload in ESTRUTURA[subpasta_nivel1]:
                caminho_tipo = f"{caminho_completo}{tipo_payload}/"
                connector.create_folder(caminho_tipo)
                pastas_criadas.append(caminho_tipo)
                logger.info(f"Pasta de tipo de payload criada: {caminho_tipo}")
                
                # Adicionar README na pasta de tipo de payload
                tipo_desc = {
                    "movimentacoes": "movimentações de animais",
                    "caixas": "caixas de produtos",
                    "desossas": "processos de desossa",
                    "animais": "informações sobre animais",
                    "escalas": "escalas de produção"
                }
                
                status = "pendentes" if "pending" in subpasta_nivel1 else "processados"
                readme_tipo = f"# Diretório de {tipo_payload.capitalize()}\n\nEste diretório contém os arquivos JSON de {tipo_desc.get(tipo_payload, tipo_payload)} {status} de processamento."
                
                upload_string_to_file(connector, caminho_tipo + "README.md", readme_tipo)
                logger.info(f"README adicionado à pasta: {caminho_tipo}")
        
        logger.info(f"Estrutura de pastas criada com sucesso! Total de {len(pastas_criadas)} pastas.")
        return pastas_criadas
        
    except Exception as e:
        logger.error(f"Erro ao criar estrutura de pastas: {str(e)}")
        raise

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
        temp_file = "/tmp/temp_readme.md"
        with open(temp_file, "w") as f:
            f.write(content)
        
        # Fazer upload do arquivo
        connector.upload_file(temp_file, file_path)
        logger.info(f"Arquivo criado: {file_path}")
        
        # Remover arquivo temporário
        os.remove(temp_file)
    except Exception as e:
        logger.error(f"Erro ao fazer upload do arquivo {file_path}: {str(e)}")
        raise

def verificar_estrutura_pastas():
    """
    Verifica se a estrutura de pastas existe no bucket GCS.
    """
    try:
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Listar todas as pastas no bucket
        blobs = connector.list_files(prefix=BASE_PATH)
        
        # Verificar se as pastas existem
        pastas_esperadas = [
            BASE_PATH,
            BASE_PATH + "pending/",
            BASE_PATH + "done/"
        ]
        
        # Adicionar pastas de tipos de payload
        for subpasta in ["pending/", "done/"]:
            for tipo in TIPOS_PAYLOAD:
                pastas_esperadas.append(f"{BASE_PATH}{subpasta}{tipo}/")
        
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
        logger.error(f"Erro ao verificar estrutura de pastas: {str(e)}")
        raise

def documentar_estrutura():
    """
    Gera documentação da estrutura de pastas.
    """
    doc = """# Estrutura de Pastas para Processamento de Payloads GDO

Este documento descreve a estrutura de pastas criada no bucket GCS para o processamento de payloads do GDO.

## Visão Geral

A estrutura foi projetada para facilitar o controle físico dos arquivos a processar e dos que já foram processados:

```
datalake/payload_data/
├── pending/               # Arquivos pendentes de processamento
│   ├── movimentacoes/     # Payloads de movimentações
│   ├── caixas/            # Payloads de caixas
│   ├── desossas/          # Payloads de desossas
│   ├── animais/           # Payloads de animais
│   └── escalas/           # Payloads de escalas
└── done/                  # Arquivos já processados
    ├── movimentacoes/     # Payloads processados de movimentações
    ├── caixas/            # Payloads processados de caixas
    ├── desossas/          # Payloads processados de desossas
    ├── animais/           # Payloads processados de animais
    └── escalas/           # Payloads processados de escalas
```

## Fluxo de Processamento

1. Os arquivos de dados são recebidos nas respectivas pastas dentro de `datalake/payload_data/pending/`
2. O sistema processa diariamente os arquivos encontrados nestas pastas
3. Após o processamento, os arquivos são movidos para a pasta correspondente dentro de `datalake/payload_data/done/`

Esta estrutura permite um controle eficiente dos arquivos a processar e dos que já foram processados, facilitando o monitoramento e a recuperação em caso de falhas.

## Tipos de Payloads

- **Movimentações**: Dados de movimentação de animais
- **Caixas**: Informações sobre caixas de produtos
- **Desossas**: Dados do processo de desossa
- **Animais**: Informações sobre os animais
- **Escalas**: Dados de escalas de produção

## Observações Importantes

- Cada pasta contém um arquivo README.md para evitar que o GCP exclua pastas vazias
- Os arquivos README.md também servem como documentação da estrutura e propósito de cada pasta
"""
    
    # Salvar a documentação em um arquivo
    with open("estrutura_pastas_gdo_corrigida.md", "w") as f:
        f.write(doc)
    
    logger.info("Documentação da estrutura de pastas gerada: estrutura_pastas_gdo_corrigida.md")
    return doc


if __name__ == "__main__":
    # Verificar se a estrutura já existe
    resultado = verificar_estrutura_pastas()
    
    # Se existem pastas faltantes, criar a estrutura
    if resultado["faltantes"]:
        logger.info(f"Existem {len(resultado['faltantes'])} pastas faltantes. Criando estrutura...")
        criar_estrutura_pastas()
        
        # Verificar novamente após a criação
        resultado = verificar_estrutura_pastas()
        if not resultado["faltantes"]:
            logger.info("Estrutura de pastas criada com sucesso!")
        else:
            logger.warning(f"Ainda existem {len(resultado['faltantes'])} pastas faltantes após a criação.")
    else:
        logger.info("Estrutura de pastas já existe completamente no bucket.")
    
    # Gerar documentação
    documentar_estrutura()
