#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para criar a estrutura de pastas para os códigos de processamento GDO no bucket GCS.

Este script cria a seguinte estrutura no bucket:
- datalake/code/
  - raw/
    - movimentacoes/
    - caixas/
    - desossas/
    - animais/
    - escalas/
    - common/
  - trusted/
    - movimentacoes/
    - caixas/
    - desossas/
    - animais/
    - escalas/
    - common/
  - service/
    - movimentacoes/
    - caixas/
    - desossas/
    - animais/
    - escalas/
    - common/

Esta estrutura segue a abordagem medalha (camadas) para processamento de dados:
1. Raw: Processamento de dados brutos sem modificações importantes
2. Trusted: Processamento inicial, sanitização, tipagem e tratamento de inconsistências
3. Service: Geração de dados para a camada de consumo pela aplicação
"""

import os
import logging
from gcs_connector import WindsurfGCSConnector
from config.config_manager import ConfigManager

# Configuração de logging
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

def criar_estrutura_codigo():
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
        readme_base = """# Código de Processamento GDO

Este diretório contém os scripts para processamento dos dados GDO, organizados em camadas seguindo a abordagem medalha:

## Camadas de Processamento

1. **Raw**: Processamento de dados brutos sem modificações importantes
2. **Trusted**: Processamento inicial, sanitização, tipagem e tratamento de inconsistências
3. **Service**: Geração de dados para a camada de consumo pela aplicação

## Estrutura de Pastas

Cada camada contém subpastas para cada tipo de payload (movimentações, caixas, desossas, animais, escalas) e uma pasta 'common' para módulos compartilhados.

## Execução

Estes scripts são projetados para serem executados pelo Dataproc em ambiente GCP.
"""
        upload_string_to_file(connector, BASE_PATH + "README.md", readme_base)
        logger.info(f"README adicionado à pasta base: {BASE_PATH}")
        
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

Esta camada contém as seguintes subpastas:

{', '.join([f'- **{tipo}**' for tipo in TIPOS_PAYLOAD])}
"""
            upload_string_to_file(connector, caminho_camada + "README.md", readme_camada)
            logger.info(f"README adicionado à pasta de camada: {caminho_camada}")
            
            # Criar pastas para cada tipo de payload dentro da camada
            for tipo in TIPOS_PAYLOAD:
                caminho_tipo = f"{caminho_camada}{tipo}/"
                connector.create_folder(caminho_tipo)
                pastas_criadas.append(caminho_tipo)
                logger.info(f"Pasta de tipo criada: {caminho_tipo}")
                
                # Adicionar README na pasta de tipo
                readme_tipo = f"""# {tipo.capitalize()} - Camada {camada.capitalize()}

{DESCRICAO_TIPOS[tipo]}

## Propósito

Esta pasta contém os scripts para processamento de dados de {tipo} na camada {camada}.
"""
                upload_string_to_file(connector, caminho_tipo + "README.md", readme_tipo)
                logger.info(f"README adicionado à pasta de tipo: {caminho_tipo}")
                
                # Adicionar arquivo __init__.py para cada pasta de tipo (para importação de módulos Python)
                init_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Módulo para processamento de {tipo} na camada {camada}.
"""

# Versão do módulo
__version__ = '0.1.0'
"""
                upload_string_to_file(connector, caminho_tipo + "__init__.py", init_content)
                logger.info(f"Arquivo __init__.py adicionado à pasta: {caminho_tipo}")
                
                # Adicionar arquivo principal para cada tipo (exceto common)
                if tipo != 'common':
                    main_filename = f"process_{tipo}.py"
                    main_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script principal para processamento de dados de {tipo} na camada {camada}.

Este script é executado pelo Dataproc no ambiente GCP e realiza o processamento
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
    parser.add_argument('--data-date', type=str, help='Data de referência para processamento (YYYY-MM-DD)')
    return parser.parse_args()

def main():
    """
    Função principal de execução do processamento.
    """
    # Analisar argumentos
    args = parse_arguments()
    
    # Validar data
    try:
        if args.data_date:
            data_ref = datetime.strptime(args.data_date, '%Y-%m-%d')
            logger.info(f"Iniciando processamento para a data: {data_ref.strftime('%Y-%m-%d')}")
        else:
            data_ref = datetime.now()
            logger.info(f"Nenhuma data especificada. Usando data atual: {data_ref.strftime('%Y-%m-%d')}")
    except ValueError:
        logger.error(f"Formato de data inválido: {args.data_date}. Use o formato YYYY-MM-DD.")
        sys.exit(1)
    
    # TODO: Implementar lógica de processamento específica para {tipo} na camada {camada}
    logger.info(f"Processamento de {tipo} na camada {camada} não implementado ainda.")
    
    logger.info(f"Processamento de {tipo} na camada {camada} concluído com sucesso.")

if __name__ == "__main__":
    main()
"""
                    upload_string_to_file(connector, caminho_tipo + main_filename, main_content)
                    logger.info(f"Arquivo principal {main_filename} adicionado à pasta: {caminho_tipo}")
                
                # Para a pasta common, adicionar utilitários específicos para cada camada
                if tipo == 'common':
                    utils_filename = "utils.py"
                    utils_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Utilitários comuns para a camada {camada}.

Este módulo contém funções e classes utilitárias compartilhadas entre
os diferentes processadores da camada {camada}.
"""

import logging
import os
from datetime import datetime

# Configurar logging
logger = logging.getLogger(f'utils-{camada}')

def get_data_path(tipo_payload, is_processed=False):
    """
    Obtém o caminho para os arquivos de dados no bucket GCS.
    
    Args:
        tipo_payload (str): Tipo de payload (movimentacoes, caixas, etc.)
        is_processed (bool): Se True, retorna o caminho para dados processados (done),
                            caso contrário, retorna o caminho para dados pendentes (pending)
    
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
        format_str (str, opcional): Formato de data desejado. Padrão: '%Y-%m-%d'
    
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
        schema (dict): Esquema de validação
    
    Returns:
        bool: True se o payload é válido, False caso contrário
    """
    # TODO: Implementar validação de esquema
    logger.warning("Validação de payload não implementada ainda.")
    return True
"""
                    upload_string_to_file(connector, caminho_tipo + utils_filename, utils_content)
                    logger.info(f"Arquivo de utilitários {utils_filename} adicionado à pasta: {caminho_tipo}")
                    
                    # Adicionar arquivo de configuração específico para cada camada
                    config_filename = "config.py"
                    config_content = f"""#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Configuração para a camada {camada}.

Este módulo contém constantes e configurações específicas para a camada {camada}.
"""

# Configurações gerais da camada {camada}
CAMADA_CONFIG = {{
    'nome': '{camada}',
    'descricao': '{DESCRICAO_CAMADAS[camada]}',
    'timeout': 3600,  # timeout em segundos
    'max_retries': 3,
    'batch_size': 1000,
}}

# Configurações específicas para cada tipo de payload
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
                    logger.info(f"Arquivo de configuração {config_filename} adicionado à pasta: {caminho_tipo}")
        
        logger.info(f"Estrutura de código criada com sucesso! Total de {len(pastas_criadas)} pastas.")
        return pastas_criadas
        
    except Exception as e:
        logger.error(f"Erro ao criar estrutura de código: {str(e)}")
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
        temp_file = "/tmp/temp_file.txt"
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

def verificar_estrutura_codigo():
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
python process_movimentacoes.py --data-date=2025-05-27
```

## Fluxo de Processamento

1. Os dados brutos são processados pelos scripts da camada Raw
2. Os dados da camada Raw são processados pelos scripts da camada Trusted
3. Os dados da camada Trusted são processados pelos scripts da camada Service
4. Os dados da camada Service são disponibilizados para consumo pela aplicação
"""
    
    # Salvar a documentação em um arquivo
    with open("estrutura_codigo_gdo.md", "w") as f:
        f.write(doc)
    
    logger.info("Documentação da estrutura de código gerada: estrutura_codigo_gdo.md")
    return doc


if __name__ == "__main__":
    # Verificar se a estrutura já existe
    resultado = verificar_estrutura_codigo()
    
    # Se existem pastas faltantes, criar a estrutura
    if resultado["faltantes"]:
        logger.info(f"Existem {len(resultado['faltantes'])} pastas faltantes. Criando estrutura...")
        criar_estrutura_codigo()
        
        # Verificar novamente após a criação
        resultado = verificar_estrutura_codigo()
        if not resultado["faltantes"]:
            logger.info("Estrutura de código criada com sucesso!")
        else:
            logger.warning(f"Ainda existem {len(resultado['faltantes'])} pastas faltantes após a criação.")
    else:
        logger.info("Estrutura de código já existe completamente no bucket.")
    
    # Gerar documentação
    documentar_estrutura()
