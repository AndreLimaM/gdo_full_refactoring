#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar a estrutura de pastas para os cu00f3digos de processamento GDO no bucket GCS.

Este script cria a estrutura de camadas (medalhu00e3o) no bucket:
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
logger = logging.getLogger('criar-estrutura-codigo')

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


def criar_estrutura_codigo():
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
        readme_base = """# Cu00f3digo de Processamento GDO

Este diretu00f3rio contu00e9m os scripts para processamento dos dados GDO, organizados em camadas seguindo a abordagem medalha:

## Camadas de Processamento

1. **Raw**: Processamento de dados brutos sem modificau00e7u00f5es importantes
2. **Trusted**: Processamento inicial, sanitizau00e7u00e3o, tipagem e tratamento de inconsistu00eancias
3. **Service**: Gerau00e7u00e3o de dados para a camada de consumo pela aplicau00e7u00e3o

## Estrutura de Pastas

Cada camada contu00e9m subpastas para cada tipo de payload (movimentau00e7u00f5es, caixas, desossas, animais, escalas) e uma pasta 'common' para mu00f3dulos compartilhados.

## Execuu00e7u00e3o

Estes scripts su00e3o projetados para serem executados pelo Dataproc em ambiente GCP.
"""
        upload_string_to_file(connector, BASE_PATH + "README.md", readme_base)
        logger.info(f"README adicionado u00e0 pasta base: {BASE_PATH}")
        
        # Criar pastas para cada camada
        for camada in CAMADAS:
            caminho_camada = f"{BASE_PATH}{camada}/"
            connector.create_folder(caminho_camada)
            pastas_criadas.append(caminho_camada)
            logger.info(f"Pasta de camada criada: {caminho_camada}")
            
            # Adicionar README na pasta da camada
            readme_camada = f"""# Camada {camada.capitalize()}

{DESCRICAO_CAMADAS[camada]}

## Estrutura de Pastas

Esta camada contu00e9m as seguintes subpastas:

{', '.join([f'- **{tipo}**' for tipo in TIPOS_PAYLOAD])}
"""
            upload_string_to_file(connector, caminho_camada + "README.md", readme_camada)
            logger.info(f"README adicionado u00e0 pasta de camada: {caminho_camada}")
            
            # Criar pastas para cada tipo de payload dentro da camada
            for tipo in TIPOS_PAYLOAD:
                caminho_tipo = f"{caminho_camada}{tipo}/"
                connector.create_folder(caminho_tipo)
                pastas_criadas.append(caminho_tipo)
                logger.info(f"Pasta de tipo criada: {caminho_tipo}")
                
                # Adicionar README na pasta de tipo
                readme_tipo = f"""# {tipo.capitalize()} - Camada {camada.capitalize()}

{DESCRICAO_TIPOS[tipo]}

## Propu00f3sito

Esta pasta contu00e9m os scripts para processamento de dados de {tipo} na camada {camada}.
"""
                upload_string_to_file(connector, caminho_tipo + "README.md", readme_tipo)
                logger.info(f"README adicionado u00e0 pasta de tipo: {caminho_tipo}")
                
                # Adicionar arquivo __init__.py para cada pasta de tipo
                init_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Mu00f3dulo para processamento de {tipo} na camada {camada}.
"""

# Versu00e3o do mu00f3dulo
__version__ = '0.1.0'
"""
                upload_string_to_file(connector, caminho_tipo + "__init__.py", init_content)
                logger.info(f"Arquivo __init__.py adicionado u00e0 pasta: {caminho_tipo}")
                
                # Adicionar arquivo principal para cada tipo (exceto common)
                if tipo != 'common':
                    main_filename = f"process_{tipo}.py"
                    main_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal para processamento de dados de {tipo} na camada {camada}.

Este script u00e9 executado pelo Dataproc no ambiente GCP e realiza o processamento
dos dados de {tipo} conforme as regras da camada {camada}.

Exemplo de uso:
    $ python process_{tipo}.py --data-date=2025-5-27
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(f'process-{tipo}-{camada}')


def parse_arguments():
    """
    Analisa os argumentos da linha de comando.
    
    Returns:
        argparse.Namespace: Argumentos analisados
    """
    parser = argparse.ArgumentParser(description=f'Processador de {tipo} - Camada {camada}')
    parser.add_argument('--data-date', type=str, help='Data de referu00eancia para processamento (YYYY-MM-DD)')
    return parser.parse_args()


def main():
    """
    Funu00e7u00e3o principal de execuu00e7u00e3o do processamento.
    """
    # Analisar argumentos
    args = parse_arguments()
    
    # Validar data
    try:
        if args.data_date:
            data_ref = datetime.strptime(args.data_date, '%Y-%m-%d')
            logger.info(f"Iniciando processamento para a data: {{data_ref.strftime('%Y-%m-%d')}}")
        else:
            data_ref = datetime.now()
            logger.info(f"Nenhuma data especificada. Usando data atual: {{data_ref.strftime('%Y-%m-%d')}}")
    except ValueError:
        logger.error(f"Formato de data invu00e1lido: {{args.data_date}}. Use o formato YYYY-MM-DD.")
        sys.exit(1)
    
    # TODO: Implementar lu00f3gica de processamento especu00edfica para {tipo} na camada {camada}
    logger.info(f"Processamento de {tipo} na camada {camada} nu00e3o implementado ainda.")
    
    logger.info(f"Processamento de {tipo} na camada {camada} concluu00eddo com sucesso.")


if __name__ == "__main__":
    main()
"""
                    upload_string_to_file(connector, caminho_tipo + main_filename, main_content)
                    logger.info(f"Arquivo principal {main_filename} adicionado u00e0 pasta: {caminho_tipo}")
                
                # Para a pasta common, adicionar utilitu00e1rios especu00edficos para cada camada
                if tipo == 'common':
                    utils_filename = "utils.py"
                    utils_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilitu00e1rios comuns para a camada {camada}.

Este mu00f3dulo contu00e9m funu00e7u00f5es e classes utilitu00e1rias compartilhadas entre
os diferentes processadores da camada {camada}.
"""

import logging
import os
from datetime import datetime

# Configurar logging
logger = logging.getLogger(f'utils-{camada}')


def get_data_path(tipo_payload, is_processed=False):
    """
    Obtu00e9m o caminho para os arquivos de dados no bucket GCS.
    
    Args:
        tipo_payload (str): Tipo de payload (movimentacoes, caixas, etc.)
        is_processed (bool): Se True, retorna o caminho para dados processados (done),
                            caso contru00e1rio, retorna o caminho para dados pendentes (pending)
    
    Returns:
        str: Caminho completo para a pasta de dados no bucket GCS
    """
    base_path = "datalake/payload_data/"
    status_folder = "done/" if is_processed else "pending/"
    return f"{{base_path}}{{status_folder}}{{tipo_payload}}/"


def format_date(date_obj=None, format_str='%Y-%m-%d'):
    """
    Formata um objeto de data para string no formato especificado.
    
    Args:
        date_obj (datetime, opcional): Objeto de data a ser formatado. Se None, usa a data atual.
        format_str (str, opcional): Formato de data desejado. Padru00e3o: '%Y-%m-%d'
    
    Returns:
        str: Data formatada como string
    """
    if date_obj is None:
        date_obj = datetime.now()
    return date_obj.strftime(format_str)


def validate_payload(payload, schema):
    """
    Valida um payload JSON contra um esquema.
    
    Args:
        payload (dict): Payload JSON a ser validado
        schema (dict): Esquema de validau00e7u00e3o
    
    Returns:
        bool: True se o payload u00e9 vu00e1lido, False caso contru00e1rio
    """
    # TODO: Implementar validau00e7u00e3o de esquema
    logger.warning("Validau00e7u00e3o de payload nu00e3o implementada ainda.")
    return True
"""
                    upload_string_to_file(connector, caminho_tipo + utils_filename, utils_content)
                    logger.info(f"Arquivo de utilitu00e1rios {utils_filename} adicionado u00e0 pasta: {caminho_tipo}")
                    
                    # Adicionar arquivo de configurau00e7u00e3o especu00edfico para cada camada
                    config_filename = "config.py"
                    config_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configurau00e7u00e3o para a camada {camada}.

Este mu00f3dulo contu00e9m constantes e configurau00e7u00f5es especu00edficas para a camada {camada}.
"""

# Configurau00e7u00f5es gerais da camada {camada}
CAMADA_CONFIG = {{
    'nome': '{camada}',
    'descricao': '{DESCRICAO_CAMADAS[camada]}',
    'timeout': 3600,  # timeout em segundos
    'max_retries': 3,
    'batch_size': 1000,
}}

# Configurau00e7u00f5es especu00edficas para cada tipo de payload
PAYLOAD_CONFIG = {{
    'movimentacoes': {{
        'schema_file': 'schemas/movimentacoes_schema.json',
        'partition_by': 'data_movimentacao',
    }},
    'caixas': {{
        'schema_file': 'schemas/caixas_schema.json',
        'partition_by': 'data_producao',
    }},
    'desossas': {{
        'schema_file': 'schemas/desossas_schema.json',
        'partition_by': 'data_desossa',
    }},
    'animais': {{
        'schema_file': 'schemas/animais_schema.json',
        'partition_by': 'data_abate',
    }},
    'escalas': {{
        'schema_file': 'schemas/escalas_schema.json',
        'partition_by': 'data_escala',
    }},
}}
"""
                    upload_string_to_file(connector, caminho_tipo + config_filename, config_content)
                    logger.info(f"Arquivo de configurau00e7u00e3o {config_filename} adicionado u00e0 pasta: {caminho_tipo}")
        
        logger.info(f"Estrutura de cu00f3digo criada com sucesso! Total de {len(pastas_criadas)} pastas.")
        return pastas_criadas
        
    except Exception as e:
        logger.error(f"Erro ao criar estrutura de cu00f3digo: {str(e)}")
        raise


def verificar_estrutura_codigo():
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
3. **process_[tipo].py**: Script principal para processamento do tipo de payload

As pastas 'common' contu00eam:

1. **README.md**: Documentau00e7u00e3o dos mu00f3dulos comuns
2. **__init__.py**: Arquivo de inicializau00e7u00e3o do mu00f3dulo Python
3. **utils.py**: Utilitu00e1rios compartilhados entre os processadores
4. **config.py**: Configurau00e7u00f5es especu00edficas para a camada

## Execuu00e7u00e3o dos Scripts

Os scripts su00e3o projetados para serem executados pelo Dataproc no ambiente GCP. Exemplo de execuu00e7u00e3o:

```bash
python process_movimentacoes.py --data-date=2025-5-27
```

## Fluxo de Processamento

1. Os dados brutos su00e3o processados pelos scripts da camada Raw
2. Os dados da camada Raw su00e3o processados pelos scripts da camada Trusted
3. Os dados da camada Trusted su00e3o processados pelos scripts da camada Service
4. Os dados da camada Service su00e3o disponibilizados para consumo pela aplicau00e7u00e3o
"""
    
    # Salvar a documentau00e7u00e3o em um arquivo
    with open("estrutura_codigo_gdo.md", "w") as f:
        f.write(doc)
    
    logger.info("Documentau00e7u00e3o da estrutura de cu00f3digo gerada: estrutura_codigo_gdo.md")
    return doc


if __name__ == "__main__":
    # Verificar se a estrutura ju00e1 existe
    resultado = verificar_estrutura_codigo()
    
    # Se existem pastas faltantes, criar a estrutura
    if resultado["faltantes"]:
        logger.info(f"Existem {len(resultado['faltantes'])} pastas faltantes. Criando estrutura...")
        criar_estrutura_codigo()
        
        # Verificar novamente apu00f3s a criau00e7u00e3o
        resultado = verificar_estrutura_codigo()
        if not resultado["faltantes"]:
            logger.info("Estrutura de cu00f3digo criada com sucesso!")
        else:
            logger.warning(f"Ainda existem {len(resultado['faltantes'])} pastas faltantes apu00f3s a criau00e7u00e3o.")
    else:
        logger.info("Estrutura de cu00f3digo ju00e1 existe completamente no bucket.")
    
    # Gerar documentau00e7u00e3o
    documentar_estrutura()
