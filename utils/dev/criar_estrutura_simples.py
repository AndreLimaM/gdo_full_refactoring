#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar a estrutura de pastas para os cu00f3digos de processamento GDO no bucket GCS.

Este script cria a estrutura de camadas (medalha) no bucket:
- datalake/code/
  - raw/
  - trusted/
  - service/

Cada camada contu00e9m subpastas para cada tipo de payload e mu00f3dulos comuns.
"""

import os
import logging
import tempfile
from gcs_connector import WindsurfGCSConnector
from config.config_manager import ConfigManager

# Configurau00e7u00e3o de logging
config = ConfigManager()
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging_level = config.get('logging.level', 'INFO')
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('criar-estrutura-simples')

# Caminho base no bucket
BASE_PATH = "datalake/code/"

# Tipos de payloads
TIPOS_PAYLOAD = ['movimentacoes', 'caixas', 'desossas', 'animais', 'escalas', 'common']

# Camadas de processamento (abordagem medalha)
CAMADAS = ['raw', 'trusted', 'service']

# Descriu00e7u00f5es das camadas
DESCRICAO_CAMADAS = {
    'raw': "Camada para processamento de dados brutos sem modificau00e7u00f5es importantes",
    'trusted': "Camada para processamento inicial, sanitizau00e7u00e3o, tipagem e tratamento de inconsistu00eancias",
    'service': "Camada para gerau00e7u00e3o de dados para consumo pela aplicau00e7u00e3o"
}

# Descriu00e7u00f5es dos tipos de payload
DESCRICAO_TIPOS = {
    'movimentacoes': "Processamento de dados de movimentau00e7u00f5es de animais",
    'caixas': "Processamento de dados de caixas de produtos",
    'desossas': "Processamento de dados de desossas",
    'animais': "Processamento de dados de animais",
    'escalas': "Processamento de dados de escalas de produu00e7u00e3o",
    'common': "Mu00f3dulos e utilitu00e1rios comuns compartilhados entre os processadores"
}


def upload_string_to_file(connector, file_path, content):
    """
    Faz upload de uma string como conteu00fado de um arquivo no bucket GCS.
    
    Args:
        connector: Instu00e2ncia do WindsurfGCSConnector
        file_path: Caminho do arquivo no bucket
        content: Conteu00fado do arquivo como string
    """
    try:
        # Criar arquivo temporu00e1rio
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp:
            temp.write(content)
            temp_filename = temp.name
        
        # Fazer upload do arquivo
        connector.upload_file(temp_filename, file_path)
        logger.info(f"Arquivo criado: {file_path}")
        
        # Remover arquivo temporu00e1rio
        os.remove(temp_filename)
    except Exception as e:
        logger.error(f"Erro ao fazer upload do arquivo {file_path}: {str(e)}")
        raise


def criar_estrutura_simples():
    """
    Cria a estrutura de pastas para os cu00f3digos de processamento GDO no bucket GCS.
    """
    try:
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Criar a estrutura de pastas
        pastas_criadas = []
        
        # Criar pasta base datalake/code/
        connector.create_folder(BASE_PATH)
        pastas_criadas.append(BASE_PATH)
        logger.info(f"Pasta base criada: {BASE_PATH}")
        
        # Adicionar README na pasta base
        readme_base = "# Cu00f3digo de Processamento GDO\n\nEste diretu00f3rio contu00e9m os scripts para processamento dos dados GDO, organizados em camadas seguindo a abordagem medalha:\n\n## Camadas de Processamento\n\n1. **Raw**: Processamento de dados brutos sem modificau00e7u00f5es importantes\n2. **Trusted**: Processamento inicial, sanitizau00e7u00e3o, tipagem e tratamento de inconsistu00eancias\n3. **Service**: Gerau00e7u00e3o de dados para a camada de consumo pela aplicau00e7u00e3o\n\n## Estrutura de Pastas\n\nCada camada contu00e9m subpastas para cada tipo de payload (movimentau00e7u00f5es, caixas, desossas, animais, escalas) e uma pasta 'common' para mu00f3dulos compartilhados.\n\n## Execuu00e7u00e3o\n\nEstes scripts su00e3o projetados para serem executados pelo Dataproc em ambiente GCP."
        upload_string_to_file(connector, BASE_PATH + "README.md", readme_base)
        logger.info(f"README adicionado u00e0 pasta base: {BASE_PATH}")
        
        # Criar pastas para cada camada
        for camada in CAMADAS:
            caminho_camada = f"{BASE_PATH}{camada}/"
            connector.create_folder(caminho_camada)
            pastas_criadas.append(caminho_camada)
            logger.info(f"Pasta de camada criada: {caminho_camada}")
            
            # Adicionar README na pasta da camada
            readme_camada = f"# Camada {camada.capitalize()}\n\n{DESCRICAO_CAMADAS[camada]}\n\n## Estrutura de Pastas\n\nEsta camada contu00e9m as seguintes subpastas:\n\n{', '.join([f'- **{tipo}**' for tipo in TIPOS_PAYLOAD])}"
            upload_string_to_file(connector, caminho_camada + "README.md", readme_camada)
            logger.info(f"README adicionado u00e0 pasta de camada: {caminho_camada}")
            
            # Criar pastas para cada tipo de payload dentro da camada
            for tipo in TIPOS_PAYLOAD:
                caminho_tipo = f"{caminho_camada}{tipo}/"
                connector.create_folder(caminho_tipo)
                pastas_criadas.append(caminho_tipo)
                logger.info(f"Pasta de tipo criada: {caminho_tipo}")
                
                # Adicionar README na pasta de tipo
                readme_tipo = f"# {tipo.capitalize()} - Camada {camada.capitalize()}\n\n{DESCRICAO_TIPOS[tipo]}\n\n## Propu00f3sito\n\nEsta pasta contu00e9m os scripts para processamento de dados de {tipo} na camada {camada}."
                upload_string_to_file(connector, caminho_tipo + "README.md", readme_tipo)
                logger.info(f"README adicionado u00e0 pasta de tipo: {caminho_tipo}")
                
                # Adicionar arquivo __init__.py para cada pasta de tipo
                init_content = f"#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\n\"\"\"\nMu00f3dulo para processamento de {tipo} na camada {camada}.\n\"\"\"\n\n# Versu00e3o do mu00f3dulo\n__version__ = '0.1.0'"
                upload_string_to_file(connector, caminho_tipo + "__init__.py", init_content)
                logger.info(f"Arquivo __init__.py adicionado u00e0 pasta: {caminho_tipo}")
        
        logger.info(f"Estrutura de cu00f3digo criada com sucesso! Total de {len(pastas_criadas)} pastas.")
        return pastas_criadas
        
    except Exception as e:
        logger.error(f"Erro ao criar estrutura de cu00f3digo: {str(e)}")
        raise


def verificar_estrutura_simples():
    """
    Verifica se a estrutura de cu00f3digo existe no bucket GCS.
    """
    try:
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Listar todas as pastas no bucket
        blobs = connector.list_files(prefix=BASE_PATH)
        
        # Verificar se as pastas existem
        pastas_esperadas = [BASE_PATH]
        
        # Adicionar pastas de camadas
        for camada in CAMADAS:
            caminho_camada = f"{BASE_PATH}{camada}/"
            pastas_esperadas.append(caminho_camada)
            
            # Adicionar pastas de tipos de payload
            for tipo in TIPOS_PAYLOAD:
                caminho_tipo = f"{caminho_camada}{tipo}/"
                pastas_esperadas.append(caminho_tipo)
        
        # Verificar cada pasta esperada
        pastas_existentes = []
        pastas_faltantes = []
        
        for pasta in pastas_esperadas:
            if pasta in blobs:
                pastas_existentes.append(pasta)
                logger.info(f"Pasta encontrada: {pasta}")
            else:
                pastas_faltantes.append(pasta)
                logger.warning(f"Pasta nu00e3o encontrada: {pasta}")
        
        return {
            "existentes": pastas_existentes,
            "faltantes": pastas_faltantes
        }
        
    except Exception as e:
        logger.error(f"Erro ao verificar estrutura de cu00f3digo: {str(e)}")
        raise


def documentar_estrutura():
    """
    Gera documentau00e7u00e3o da estrutura de cu00f3digo.
    """
    doc = """# Estrutura de Cu00f3digo para Processamento de Payloads GDO

Este documento descreve a estrutura de cu00f3digo criada no bucket GCS para o processamento de payloads do GDO.

## Visu00e3o Geral

A estrutura segue a abordagem medalha (camadas) para processamento de dados:

1. **Raw**: Processamento de dados brutos sem modificau00e7u00f5es importantes
2. **Trusted**: Processamento inicial, sanitizau00e7u00e3o, tipagem e tratamento de inconsistu00eancias
3. **Service**: Gerau00e7u00e3o de dados para a camada de consumo pela aplicau00e7u00e3o

## Estrutura de Pastas

```
datalake/code/
u251cu2500u2500 raw/                    # Camada Raw
u2502   u251cu2500u2500 movimentacoes/      # Processamento de movimentau00e7u00f5es
u2502   u251cu2500u2500 caixas/             # Processamento de caixas
u2502   u251cu2500u2500 desossas/           # Processamento de desossas
u2502   u251cu2500u2500 animais/            # Processamento de animais
u2502   u251cu2500u2500 escalas/            # Processamento de escalas
u2502   u2514u2500u2500 common/             # Mu00f3dulos comuns compartilhados
u251cu2500u2500 trusted/                # Camada Trusted
u2502   u251cu2500u2500 movimentacoes/      # Processamento de movimentau00e7u00f5es
u2502   u251cu2500u2500 caixas/             # Processamento de caixas
u2502   u251cu2500u2500 desossas/           # Processamento de desossas
u2502   u251cu2500u2500 animais/            # Processamento de animais
u2502   u251cu2500u2500 escalas/            # Processamento de escalas
u2502   u2514u2500u2500 common/             # Mu00f3dulos comuns compartilhados
u2514u2500u2500 service/                # Camada Service
    u251cu2500u2500 movimentacoes/      # Processamento de movimentau00e7u00f5es
    u251cu2500u2500 caixas/             # Processamento de caixas
    u251cu2500u2500 desossas/           # Processamento de desossas
    u251cu2500u2500 animais/            # Processamento de animais
    u251cu2500u2500 escalas/            # Processamento de escalas
    u2514u2500u2500 common/             # Mu00f3dulos comuns compartilhados
```

## Conteu00fado das Pastas

Cada pasta de tipo de payload contu00e9m:

1. **README.md**: Documentau00e7u00e3o especu00edfica para o processamento daquele tipo de payload
2. **__init__.py**: Arquivo de inicializau00e7u00e3o do mu00f3dulo Python

## Execuu00e7u00e3o dos Scripts

Os scripts su00e3o projetados para serem executados pelo Dataproc no ambiente GCP.

## Fluxo de Processamento

1. Os dados brutos su00e3o processados pelos scripts da camada Raw
2. Os dados da camada Raw su00e3o processados pelos scripts da camada Trusted
3. Os dados da camada Trusted su00e3o processados pelos scripts da camada Service
4. Os dados da camada Service su00e3o disponibilizados para consumo pela aplicau00e7u00e3o
"""
    
    # Salvar a documentau00e7u00e3o em um arquivo
    with open("estrutura_medalha_gdo.md", "w") as f:
        f.write(doc)
    
    logger.info("Documentau00e7u00e3o da estrutura de cu00f3digo gerada: estrutura_medalha_gdo.md")
    return doc


if __name__ == "__main__":
    # Verificar se a estrutura ju00e1 existe
    resultado = verificar_estrutura_simples()
    
    # Se existem pastas faltantes, criar a estrutura
    if resultado["faltantes"]:
        logger.info(f"Existem {len(resultado['faltantes'])} pastas faltantes. Criando estrutura...")
        criar_estrutura_simples()
        
        # Verificar novamente apu00f3s a criau00e7u00e3o
        resultado = verificar_estrutura_simples()
        if not resultado["faltantes"]:
            logger.info("Estrutura de cu00f3digo criada com sucesso!")
        else:
            logger.warning(f"Ainda existem {len(resultado['faltantes'])} pastas faltantes apu00f3s a criau00e7u00e3o.")
    else:
        logger.info("Estrutura de cu00f3digo ju00e1 existe completamente no bucket.")
    
    # Gerar documentau00e7u00e3o
    documentar_estrutura()
