#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar a estrutura de pastas para os códigos de processamento GDO no bucket GCS.

Este script cria a estrutura de camadas (medalha) no bucket:
- datalake/code/
  - raw/
  - trusted/
  - service/

Cada camada contém subpastas para cada tipo de payload e módulos comuns.
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
logger = logging.getLogger('criar-estrutura-medalha')

# Caminho base no bucket
BASE_PATH = "datalake/code/"

# Tipos de payloads
TIPOS_PAYLOAD = ['movimentacoes', 'caixas', 'desossas', 'animais', 'escalas', 'common']

# Camadas de processamento (abordagem medalha)
CAMADAS = ['raw', 'trusted', 'service']

# Descrições das camadas
DESCRICAO_CAMADAS = {
    'raw': "Camada para processamento de dados brutos sem modificações importantes",
    'trusted': "Camada para processamento inicial, sanitização, tipagem e tratamento de inconsistências",
    'service': "Camada para geração de dados para consumo pela aplicação"
}

# Descrições dos tipos de payload
DESCRICAO_TIPOS = {
    'movimentacoes': "Processamento de dados de movimentações de animais",
    'caixas': "Processamento de dados de caixas de produtos",
    'desossas': "Processamento de dados de desossas",
    'animais': "Processamento de dados de animais",
    'escalas': "Processamento de dados de escalas de produção",
    'common': "Módulos e utilitários comuns compartilhados entre os processadores"
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


def criar_estrutura_medalha():
    """
    Cria a estrutura de pastas para os códigos de processamento GDO no bucket GCS.
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
        readme_base = "# Código de Processamento GDO\n\nEste diretório contém os scripts para processamento dos dados GDO, organizados em camadas seguindo a abordagem medalha:\n\n## Camadas de Processamento\n\n1. **Raw**: Processamento de dados brutos sem modificações importantes\n2. **Trusted**: Processamento inicial, sanitização, tipagem e tratamento de inconsistências\n3. **Service**: Geração de dados para a camada de consumo pela aplicação\n\n## Estrutura de Pastas\n\nCada camada contém subpastas para cada tipo de payload (movimentações, caixas, desossas, animais, escalas) e uma pasta 'common' para módulos compartilhados.\n\n## Execução\n\nEstes scripts são projetados para serem executados pelo Dataproc em ambiente GCP."
        upload_string_to_file(connector, BASE_PATH + "README.md", readme_base)
        logger.info(f"README adicionado à pasta base: {BASE_PATH}")
        
        # Criar pastas para cada camada
        for camada in CAMADAS:
            caminho_camada = f"{BASE_PATH}{camada}/"
            connector.create_folder(caminho_camada)
            pastas_criadas.append(caminho_camada)
            logger.info(f"Pasta de camada criada: {caminho_camada}")
            
            # Adicionar README na pasta da camada
            readme_camada = f"# Camada {camada.capitalize()}\n\n{DESCRICAO_CAMADAS[camada]}\n\n## Estrutura de Pastas\n\nEsta camada contém as seguintes subpastas:\n\n{', '.join([f'- **{tipo}**' for tipo in TIPOS_PAYLOAD])}"
            upload_string_to_file(connector, caminho_camada + "README.md", readme_camada)
            logger.info(f"README adicionado à pasta de camada: {caminho_camada}")
            
            # Criar pastas para cada tipo de payload dentro da camada
            for tipo in TIPOS_PAYLOAD:
                caminho_tipo = f"{caminho_camada}{tipo}/"
                connector.create_folder(caminho_tipo)
                pastas_criadas.append(caminho_tipo)
                logger.info(f"Pasta de tipo criada: {caminho_tipo}")
                
                # Adicionar README na pasta de tipo
                readme_tipo = f"# {tipo.capitalize()} - Camada {camada.capitalize()}\n\n{DESCRICAO_TIPOS[tipo]}\n\n## Propósito\n\nEsta pasta contém os scripts para processamento de dados de {tipo} na camada {camada}."
                upload_string_to_file(connector, caminho_tipo + "README.md", readme_tipo)
                logger.info(f"README adicionado à pasta de tipo: {caminho_tipo}")
                
                # Adicionar arquivo __init__.py para cada pasta de tipo
                init_content = f"#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\n\"\"\"\nMódulo para processamento de {tipo} na camada {camada}.\n\"\"\"\n\n# Versão do módulo\n__version__ = '0.1.0'"
                upload_string_to_file(connector, caminho_tipo + "__init__.py", init_content)
                logger.info(f"Arquivo __init__.py adicionado à pasta: {caminho_tipo}")
                
                # Adicionar arquivo principal para cada tipo (exceto common)
                if tipo != 'common':
                    main_filename = f"process_{tipo}.py"
                    main_content = f"#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\n\"\"\"\nScript principal para processamento de dados de {tipo} na camada {camada}.\n\nEste script é executado pelo Dataproc no ambiente GCP e realiza o processamento\ndos dados de {tipo} conforme as regras da camada {camada}.\n\nExemplo de uso:\n    $ python process_{tipo}.py --data-date=2025-5-27\n\"\"\"\n\nimport argparse\nimport logging\nimport os\nimport sys\nfrom datetime import datetime\n\n# Configurar logging\nlogging.basicConfig(\n    level=logging.INFO,\n    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'\n)\nlogger = logging.getLogger(f'process-{tipo}-{camada}')\n\n\ndef parse_arguments():\n    \"\"\"\n    Analisa os argumentos da linha de comando.\n    \n    Returns:\n        argparse.Namespace: Argumentos analisados\n    \"\"\"\n    parser = argparse.ArgumentParser(description=f'Processador de {tipo} - Camada {camada}')\n    parser.add_argument('--data-date', type=str, help='Data de referência para processamento (YYYY-MM-DD)')\n    return parser.parse_args()\n\n\ndef main():\n    \"\"\"\n    Função principal de execução do processamento.\n    \"\"\"\n    # Analisar argumentos\n    args = parse_arguments()\n    \n    # Validar data\n    try:\n        if args.data_date:\n            data_ref = datetime.strptime(args.data_date, '%Y-%m-%d')\n            logger.info(f\"Iniciando processamento para a data: {data_ref.strftime('%Y-%m-%d')}\")\n        else:\n            data_ref = datetime.now()\n            logger.info(f\"Nenhuma data especificada. Usando data atual: {data_ref.strftime('%Y-%m-%d')}\")\n    except ValueError:\n        logger.error(f\"Formato de data inválido: {args.data_date}. Use o formato YYYY-MM-DD.\")\n        sys.exit(1)\n    \n    # TODO: Implementar lógica de processamento específica para {tipo} na camada {camada}\n    logger.info(f\"Processamento de {tipo} na camada {camada} não implementado ainda.\")\n    \n    logger.info(f\"Processamento de {tipo} na camada {camada} concluído com sucesso.\")\n\n\nif __name__ == \"__main__\":\n    main()"
                    upload_string_to_file(connector, caminho_tipo + main_filename, main_content)
                    logger.info(f"Arquivo principal {main_filename} adicionado à pasta: {caminho_tipo}")
                
                # Para a pasta common, adicionar utilitários específicos para cada camada
                if tipo == 'common':
                    utils_filename = "utils.py"
                    utils_content = f"#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\n\"\"\"\nUtilitários comuns para a camada {camada}.\n\nEste módulo contém funções e classes utilitárias compartilhadas entre\nos diferentes processadores da camada {camada}.\n\"\"\"\n\nimport logging\nimport os\nfrom datetime import datetime\n\n# Configurar logging\nlogger = logging.getLogger(f'utils-{camada}')\n\n\ndef get_data_path(tipo_payload, is_processed=False):\n    \"\"\"\n    Obtém o caminho para os arquivos de dados no bucket GCS.\n    \n    Args:\n        tipo_payload (str): Tipo de payload (movimentacoes, caixas, etc.)\n        is_processed (bool): Se True, retorna o caminho para dados processados (done),\n                            caso contrário, retorna o caminho para dados pendentes (pending)\n    \n    Returns:\n        str: Caminho completo para a pasta de dados no bucket GCS\n    \"\"\"\n    base_path = \"datalake/payload_data/\"\n    status_folder = \"done/\" if is_processed else \"pending/\"\n    return f\"{base_path}{status_folder}{tipo_payload}/\"\n\n\ndef format_date(date_obj=None, format_str='%Y-%m-%d'):\n    \"\"\"\n    Formata um objeto de data para string no formato especificado.\n    \n    Args:\n        date_obj (datetime, opcional): Objeto de data a ser formatado. Se None, usa a data atual.\n        format_str (str, opcional): Formato de data desejado. Padrão: '%Y-%m-%d'\n    \n    Returns:\n        str: Data formatada como string\n    \"\"\"\n    if date_obj is None:\n        date_obj = datetime.now()\n    return date_obj.strftime(format_str)\n\n\ndef validate_payload(payload, schema):\n    \"\"\"\n    Valida um payload JSON contra um esquema.\n    \n    Args:\n        payload (dict): Payload JSON a ser validado\n        schema (dict): Esquema de validação\n    \n    Returns:\n        bool: True se o payload é válido, False caso contrário\n    \"\"\"\n    # TODO: Implementar validação de esquema\n    logger.warning(\"Validação de payload não implementada ainda.\")\n    return True"
                    upload_string_to_file(connector, caminho_tipo + utils_filename, utils_content)
                    logger.info(f"Arquivo de utilitários {utils_filename} adicionado à pasta: {caminho_tipo}")
                    
                    # Adicionar arquivo de configuração específico para cada camada
                    config_filename = "config.py"
                    config_content = f"#!/usr/bin/env python\n# -*- coding: utf-8 -*-\n\n\"\"\"\nConfiguração para a camada {camada}.\n\nEste módulo contém constantes e configurações específicas para a camada {camada}.\n\"\"\"\n\n# Configurações gerais da camada {camada}\nCAMADA_CONFIG = {{\n    'nome': '{camada}',\n    'descricao': '{DESCRICAO_CAMADAS[camada]}',\n    'timeout': 3600,  # timeout em segundos\n    'max_retries': 3,\n    'batch_size': 1000,\n}}\n\n# Configurações específicas para cada tipo de payload\nPAYLOAD_CONFIG = {{\n    'movimentacoes': {{\n        'schema_file': 'schemas/movimentacoes_schema.json',\n        'partition_by': 'data_movimentacao',\n    }},\n    'caixas': {{\n        'schema_file': 'schemas/caixas_schema.json',\n        'partition_by': 'data_producao',\n    }},\n    'desossas': {{\n        'schema_file': 'schemas/desossas_schema.json',\n        'partition_by': 'data_desossa',\n    }},\n    'animais': {{\n        'schema_file': 'schemas/animais_schema.json',\n        'partition_by': 'data_abate',\n    }},\n    'escalas': {{\n        'schema_file': 'schemas/escalas_schema.json',\n        'partition_by': 'data_escala',\n    }},\n}}"
                    upload_string_to_file(connector, caminho_tipo + config_filename, config_content)
                    logger.info(f"Arquivo de configuração {config_filename} adicionado à pasta: {caminho_tipo}")
        
        logger.info(f"Estrutura de código criada com sucesso! Total de {len(pastas_criadas)} pastas.")
        return pastas_criadas
        
    except Exception as e:
        logger.error(f"Erro ao criar estrutura de código: {str(e)}")
        raise


def verificar_estrutura_medalha():
    """
    Verifica se a estrutura de código existe no bucket GCS.
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
                logger.warning(f"Pasta não encontrada: {pasta}")
        
        return {
            "existentes": pastas_existentes,
            "faltantes": pastas_faltantes
        }
        
    except Exception as e:
        logger.error(f"Erro ao verificar estrutura de código: {str(e)}")
        raise


def documentar_estrutura():
    """
    Gera documentação da estrutura de código.
    """
    doc = """# Estrutura de Código para Processamento de Payloads GDO

Este documento descreve a estrutura de código criada no bucket GCS para o processamento de payloads do GDO.

## Visão Geral

A estrutura segue a abordagem medalha (camadas) para processamento de dados:

1. **Raw**: Processamento de dados brutos sem modificações importantes
2. **Trusted**: Processamento inicial, sanitização, tipagem e tratamento de inconsistências
3. **Service**: Geração de dados para a camada de consumo pela aplicação

## Estrutura de Pastas

```
datalake/code/
├── raw/                    # Camada Raw
│   ├── movimentacoes/      # Processamento de movimentações
│   ├── caixas/             # Processamento de caixas
│   ├── desossas/           # Processamento de desossas
│   ├── animais/            # Processamento de animais
│   ├── escalas/            # Processamento de escalas
│   └── common/             # Módulos comuns compartilhados
├── trusted/                # Camada Trusted
│   ├── movimentacoes/      # Processamento de movimentações
│   ├── caixas/             # Processamento de caixas
│   ├── desossas/           # Processamento de desossas
│   ├── animais/            # Processamento de animais
│   ├── escalas/            # Processamento de escalas
│   └── common/             # Módulos comuns compartilhados
└── service/                # Camada Service
    ├── movimentacoes/      # Processamento de movimentações
    ├── caixas/             # Processamento de caixas
    ├── desossas/           # Processamento de desossas
    ├── animais/            # Processamento de animais
    ├── escalas/            # Processamento de escalas
    └── common/             # Módulos comuns compartilhados
```

## Conteúdo das Pastas

Cada pasta de tipo de payload contém:

1. **README.md**: Documentação específica para o processamento daquele tipo de payload
2. **__init__.py**: Arquivo de inicialização do módulo Python
3. **process_[tipo].py**: Script principal para processamento do tipo de payload

As pastas 'common' contêm:

1. **README.md**: Documentação dos módulos comuns
2. **__init__.py**: Arquivo de inicialização do módulo Python
3. **utils.py**: Utilitários compartilhados entre os processadores
4. **config.py**: Configurações específicas para a camada

## Execução dos Scripts

Os scripts são projetados para serem executados pelo Dataproc no ambiente GCP. Exemplo de execução:

```bash
python process_movimentacoes.py --data-date=2025-5-27
```

## Fluxo de Processamento

1. Os dados brutos são processados pelos scripts da camada Raw
2. Os dados da camada Raw são processados pelos scripts da camada Trusted
3. Os dados da camada Trusted são processados pelos scripts da camada Service
4. Os dados da camada Service são disponibilizados para consumo pela aplicação
"""
    
    # Salvar a documentação em um arquivo
    with open("estrutura_medalha_gdo.md", "w") as f:
        f.write(doc)
    
    logger.info("Documentação da estrutura de código gerada: estrutura_medalha_gdo.md")
    return doc


if __name__ == "__main__":
    # Verificar se a estrutura já existe
    resultado = verificar_estrutura_medalha()
    
    # Se existem pastas faltantes, criar a estrutura
    if resultado["faltantes"]:
        logger.info(f"Existem {len(resultado['faltantes'])} pastas faltantes. Criando estrutura...")
        criar_estrutura_medalha()
        
        # Verificar novamente após a criação
        resultado = verificar_estrutura_medalha()
        if not resultado["faltantes"]:
            logger.info("Estrutura de código criada com sucesso!")
        else:
            logger.warning(f"Ainda existem {len(resultado['faltantes'])} pastas faltantes após a criação.")
    else:
        logger.info("Estrutura de código já existe completamente no bucket.")
    
    # Gerar documentação
    documentar_estrutura()
