#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para organizar a estrutura de pastas do projeto GDO localmente e no bucket GCS.

Este script cria uma estrutura organizada para os scripts de documentau00e7u00e3o,
utilitu00e1rios e testes, tanto localmente quanto no bucket GCS. Tambm move os
arquivos existentes para as pastas apropriadas.

Estrutura criada:
- docs/: Documentau00e7u00e3o do projeto
- utils/: Scripts utilitu00e1rios
  - database/: Scripts relacionados ao banco de dados
  - gcs/: Scripts relacionados ao Google Cloud Storage
  - config/: Scripts de configurau00e7u00e3o
- tests/: Testes automatizados
"""

import os
import shutil
import logging
import tempfile
from gcs_connector import WindsurfGCSConnector
from config.config_manager import ConfigManager

# Configurau00e7u00e3o de logging
config = ConfigManager()
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging_level = config.get('logging.level', 'INFO')
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('organizar-projeto')

# Diretório base do projeto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Estrutura de pastas local
LOCAL_DIRS = {
    'docs': 'Documentação do projeto',
    'utils/database': 'Scripts utilitários para operações de banco de dados',
    'utils/gcs': 'Scripts utilitários para operações no Google Cloud Storage',
    'utils/config': 'Scripts de configuração',
    'tests': 'Testes automatizados'
}

# Estrutura de pastas no bucket GCS
GCS_DIRS = {
    'datalake/docs': 'Documentação do projeto',
    'datalake/utils/database': 'Scripts utilitários para operações de banco de dados',
    'datalake/utils/gcs': 'Scripts utilitários para operações no Google Cloud Storage',
    'datalake/utils/config': 'Scripts de configuração',
    'datalake/tests': 'Testes automatizados'
}

# Mapeamento de arquivos para pastas locais
FILE_MAPPING = {
    'database': [
        'db_connector.py',
        'listar_tabelas_db.py',
        'analisar_estrutura_db.py',
        'teste_conexao_simples.py',
        'testar_conexao_db.py'
    ],
    'gcs': [
        'gcs_connector.py',
        'criar_estrutura_pastas.py',
        'criar_estrutura_codigo.py',
        'criar_pasta_dev.py',
        'criar_estrutura_simples.py'
    ],
    'config': [
        'upload_db_connector.py',
        'organizar_projeto.py'
    ]
}

# Arquivos README para cada pasta
README_TEMPLATES = {
    'docs': """
# Documentação do Projeto GDO

Esta pasta contém a documentação do projeto de processamento de dados GDO.

## Conteúdo

- Análises de banco de dados
- Documentação da estrutura do projeto
- Guias de implementação
- Relatórios de análise
""",
    'utils/database': """
# Utilitários de Banco de Dados

Esta pasta contém scripts utilitários para operações de banco de dados.

## Scripts Principais

- `db_connector.py`: Conector para o banco de dados Cloud SQL
- `listar_tabelas_db.py`: Script para listar tabelas do banco de dados
- `analisar_estrutura_db.py`: Script para analisar a estrutura do banco de dados
- `teste_conexao_simples.py`: Script simples para testar a conexão com o banco de dados
- `testar_conexao_db.py`: Script completo para testar a conexão com o banco de dados
""",
    'utils/gcs': """
# Utilitários de Google Cloud Storage

Esta pasta contém scripts utilitários para operações no Google Cloud Storage.

## Scripts Principais

- `gcs_connector.py`: Conector para o Google Cloud Storage
- `criar_estrutura_pastas.py`: Script para criar a estrutura de pastas no bucket
- `criar_estrutura_codigo.py`: Script para criar a estrutura de código no bucket
- `criar_pasta_dev.py`: Script para criar a pasta de desenvolvimento no bucket
- `criar_estrutura_simples.py`: Script para criar a estrutura simplificada no bucket
""",
    'utils/config': """
# Utilitários de Configuração

Esta pasta contém scripts utilitários para configuração do projeto.

## Scripts Principais

- `upload_db_connector.py`: Script para fazer upload do conector de banco de dados para o bucket
- `organizar_projeto.py`: Script para organizar a estrutura de pastas do projeto
""",
    'tests': """
# Testes Automatizados

Esta pasta contém testes automatizados para o projeto.

## Estrutura

- Testes unitários
- Testes de integração
- Testes de sistema
"""
}

# Documentação principal do projeto
PROJECT_README = """
# Projeto de Processamento de Dados GDO

## Visão Geral

Este projeto implementa um sistema de processamento para payloads GDO recebidos via arquivos JSON.
O sistema utiliza o Google Cloud Storage (GCS) para armazenamento de dados e o Cloud SQL para
persistência. A arquitetura segue uma abordagem de camadas (medalhão) com clara separação entre
dados brutos, processamento confiável e camada de serviço para consumo pelos clientes.

## Estrutura do Projeto

### Estrutura de Pastas no GCS

```
datalake/payload_data/
├── pending/
└── done/
```

### Estrutura de Código no GCS

```
datalake/code/
├── raw/
├── trusted/
└── service/
```

### Pasta de Desenvolvimento

```
datalake/dev/
├── tests/
├── utils/
├── samples/
├── notebooks/
└── temp/
```

### Estrutura Local

```
./
├── docs/
├── utils/
│   ├── database/
│   ├── gcs/
│   └── config/
└── tests/
```

## Configuração

O projeto utiliza um arquivo de configuração centralizado (`config/config.yaml`) para armazenar
todos os apontamentos de rotas e configurações, facilitando a migração para outros ambientes.

## Conexão com o Banco de Dados

O sistema se conecta ao banco de dados Cloud SQL PostgreSQL com as seguintes credenciais:

- Host: 34.48.11.43
- Banco: db_eco_tcbf_25
- Usuário: db_eco_tcbf_25_user

A conexão é implementada no módulo `db_connector.py`, que fornece funcionalidades para
executar consultas e gerenciar interações com o banco de dados.

## Processamento de Dados

O processamento de dados segue a abordagem medalha com três camadas:

1. **Raw**: Scripts para processar dados brutos sem modificações importantes
2. **Trusted**: Scripts para processamento inicial, sanitização, tipagem e tratamento de inconsistências
3. **Service**: Scripts para gerar dados para consumo pela aplicação cliente

## Documentação

A documentação completa do projeto está disponível na pasta `docs/`.
"""


def criar_estrutura_local():
    """
    Cria a estrutura de pastas local e move os arquivos para as pastas apropriadas.
    """
    logger.info("Criando estrutura de pastas local...")
    
    # Criar pastas locais
    for dir_path, description in LOCAL_DIRS.items():
        full_path = os.path.join(BASE_DIR, dir_path)
        os.makedirs(full_path, exist_ok=True)
        logger.info(f"Pasta criada: {full_path}")
        
        # Criar README para cada pasta
        if dir_path in README_TEMPLATES:
            readme_path = os.path.join(full_path, "README.md")
            with open(readme_path, 'w', encoding='utf-8') as f:
                f.write(README_TEMPLATES[dir_path])
            logger.info(f"README criado: {readme_path}")
    
    # Criar README principal do projeto
    with open(os.path.join(BASE_DIR, "README.md"), 'w', encoding='utf-8') as f:
        f.write(PROJECT_README)
    logger.info("README principal do projeto criado")
    
    # Mover arquivos para as pastas apropriadas
    for category, files in FILE_MAPPING.items():
        dest_dir = os.path.join(BASE_DIR, f"utils/{category}")
        for file_name in files:
            src_path = os.path.join(BASE_DIR, file_name)
            if os.path.exists(src_path):
                dest_path = os.path.join(dest_dir, file_name)
                # Copiar o arquivo em vez de movê-lo para manter o original
                shutil.copy2(src_path, dest_path)
                logger.info(f"Arquivo copiado: {src_path} -> {dest_path}")
            else:
                logger.warning(f"Arquivo não encontrado: {src_path}")
    
    # Criar arquivo de análise do banco de dados na pasta docs
    src_json = os.path.join(BASE_DIR, "analise_db_estrutura.json")
    if os.path.exists(src_json):
        dest_json = os.path.join(BASE_DIR, "docs", "analise_db_estrutura.json")
        shutil.copy2(src_json, dest_json)
        logger.info(f"Arquivo de análise copiado para docs: {dest_json}")
    
    logger.info("Estrutura de pastas local criada com sucesso!")


def criar_estrutura_gcs():
    """
    Cria a estrutura de pastas no bucket GCS e faz upload dos arquivos.
    """
    try:
        logger.info("Criando estrutura de pastas no bucket GCS...")
        
        # Inicializar o conector GCS
        connector = WindsurfGCSConnector()
        logger.info(f"Conector GCS inicializado para o bucket: {connector.bucket_name}")
        
        # Criar pastas no bucket
        for dir_path, description in GCS_DIRS.items():
            # Criar arquivo README para a pasta
            readme_content = README_TEMPLATES.get(dir_path.replace('datalake/', ''), "# Pasta GCS\n\nEsta pasta contém arquivos do projeto GDO.")
            readme_path = f"{dir_path}/README.md"
            
            # Criar arquivo temporário e fazer upload
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8') as temp:
                temp.write(readme_content)
                temp_path = temp.name
            
            connector.upload_file(temp_path, readme_path)
            os.unlink(temp_path)  # Remover arquivo temporário
            
            logger.info(f"Pasta criada no bucket: {dir_path}")
        
        # Fazer upload dos arquivos para as pastas apropriadas
        for category, files in FILE_MAPPING.items():
            dest_dir = f"datalake/utils/{category}"
            for file_name in files:
                src_path = os.path.join(BASE_DIR, file_name)
                if os.path.exists(src_path):
                    dest_path = f"{dest_dir}/{file_name}"
                    connector.upload_file(src_path, dest_path)
                    logger.info(f"Arquivo enviado para o bucket: {src_path} -> {dest_path}")
        
        # Fazer upload do arquivo de análise do banco de dados
        src_json = os.path.join(BASE_DIR, "analise_db_estrutura.json")
        if os.path.exists(src_json):
            dest_json = "datalake/docs/analise_db_estrutura.json"
            connector.upload_file(src_json, dest_json)
            logger.info(f"Arquivo de análise enviado para o bucket: {dest_json}")
        
        # Fazer upload do README principal do projeto
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8') as temp:
            temp.write(PROJECT_README)
            temp_path = temp.name
        
        connector.upload_file(temp_path, "datalake/README.md")
        os.unlink(temp_path)  # Remover arquivo temporário
        logger.info("README principal do projeto enviado para o bucket")
        
        logger.info("Estrutura de pastas no bucket GCS criada com sucesso!")
        return True
    except Exception as e:
        logger.error(f"Erro ao criar estrutura no bucket GCS: {str(e)}")
        return False


def criar_documentacao_estrutura_db():
    """
    Cria um arquivo de documentação sobre a estrutura do banco de dados.
    """
    logger.info("Criando documentação sobre a estrutura do banco de dados...")
    
    # Conteúdo da documentação
    db_doc_content = """
# Análise da Estrutura do Banco de Dados PostgreSQL

## Características Principais do Banco de Dados

### 1. Estrutura de Particionamento
- **Particionamento extensivo**: 426 relações de herança/particionamento
- **Padrão de particionamento**: Principalmente por mês no formato YYYYMM
- **Tabelas particionadas**: Principalmente tabelas de negócio com prefixo `bt_`
- **Benefício**: Permite processamento paralelo e consultas otimizadas por período

### 2. Grupos de Tabelas
- **bt_**: Tabelas de negócio (animais, caixas, desossa, registro_venda, escala)
- **proc_**: Tabelas de processamento/procedimentos
- **aux_**: Tabelas auxiliares/lookup
- **log_**: Tabelas de registro de operações

### 3. Indexação
- **Total de índices**: 345 índices no banco de dados
- **Tipos de índices**: 
  - BTREE para chaves primárias e buscas pontuais
  - BRIN para otimização de consultas por intervalo temporal
- **Tabelas mais indexadas**: Partições de `bt_animais_24ad9d`

### 4. Relacionamentos
- **Chaves estrangeiras**: 87.903 relações de chave estrangeira
- **Alta integridade referencial**: Indica um esquema altamente relacional
- **Tabelas mais referenciadas**: Tabelas auxiliares (como `aux_tipo_unidade`)

### 5. Tamanho e Volume
- **Maiores tabelas**: Partições recentes (2025/02 e 2025/03) de `bt_animais_24ad9d` e `bt_registro_venda_24ad9d`
- **Tamanho máximo**: Até 399MB por partição
- **Distribuição**: Dados concentrados nos meses mais recentes

## Estratégias para Processamento Otimizado

### 1. Aproveitar o Particionamento
- Processar os dados partição por partição para maximizar o desempenho
- Aplicar filtros por data para direcionar consultas às partições corretas
- Implementar processamento paralelo por partição

### 2. Otimizar Consultas
- Utilizar os índices BRIN para consultas por intervalo de data
- Aproveitar os índices BTREE para consultas pontuais e joins
- Evitar operações de varredura completa nas tabelas maiores

### 3. Implementar Processamento Incremental
- Basear o processamento nas datas das partições
- Priorizar o processamento das partições mais recentes
- Utilizar controle de metadados para rastrear o que já foi processado

### 4. Respeitar Relacionamentos
- Seguir a ordem de processamento baseada nas dependências entre tabelas
- Processar tabelas auxiliares primeiro
- Ordem sugerida: aux_ → bt_animais → bt_caixas → bt_desossa → bt_registro_venda

### 5. Paralelizar com Cuidado
- Implementar processamento paralelo controlado
- Utilizar múltiplos workers para diferentes partições
- Limitar o número de conexões simultâneas para evitar sobrecarga
"""
    
    # Criar arquivo de documentação
    doc_path = os.path.join(BASE_DIR, "docs", "estrutura_banco_dados.md")
    with open(doc_path, 'w', encoding='utf-8') as f:
        f.write(db_doc_content)
    logger.info(f"Documentação da estrutura do banco de dados criada: {doc_path}")
    
    # Fazer upload para o bucket
    try:
        connector = WindsurfGCSConnector()
        dest_path = "datalake/docs/estrutura_banco_dados.md"
        connector.upload_file(doc_path, dest_path)
        logger.info(f"Documentação enviada para o bucket: {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Erro ao enviar documentação para o bucket: {str(e)}")
        return False


def criar_documentacao_estrategia_processamento():
    """
    Cria um arquivo de documentação sobre a estratégia de processamento.
    """
    logger.info("Criando documentação sobre a estratégia de processamento...")
    
    # Conteúdo da documentação
    proc_doc_content = """
# Estratégias de Processamento para o Banco de Dados PostgreSQL

Com base na análise detalhada do banco de dados, identificamos as seguintes estratégias para otimizar o processamento dos dados:

## 1. Aproveitamento do Particionamento

- O banco de dados utiliza particionamento por mês (formato YYYYMM) nas principais tabelas de negócio
- Cada tabela principal (bt_animais, bt_registro_venda, bt_caixas, bt_desossa) possui partições mensais
- Devemos processar os dados partição por partição para maximizar o desempenho
- Filtros por data devem ser aplicados para direcionar consultas às partições corretas

## 2. Otimização de Consultas

- Utilizar os índices BRIN existentes para consultas por intervalo de data
- Aproveitar os índices BTREE para consultas pontuais e joins
- Evitar operações de FULL TABLE SCAN nas tabelas maiores
- Considerar o tamanho das tabelas ao definir a estratégia de processamento

## 3. Processamento Incremental

- Implementar processamento incremental baseado nas datas das partições
- Priorizar o processamento das partições mais recentes (2025/02 e 2025/03)
- Utilizar controle de metadados para rastrear o que foi processado

## 4. Respeito aos Relacionamentos

- O banco possui uma estrutura altamente relacional (87.903 chaves estrangeiras)
- Respeitar a ordem de processamento baseada nas dependências entre tabelas
- Tabelas auxiliares (aux_) devem ser processadas primeiro
- Seguir a ordem: aux_ -> bt_animais -> bt_caixas -> bt_desossa -> bt_registro_venda

## 5. Paralelização

- Implementar processamento paralelo por partição
- Utilizar múltiplos workers para processar diferentes partições simultaneamente
- Limitar o número de conexões simultâneas para evitar sobrecarga do banco

## 6. Implementação nas Camadas de Processamento

### Camada Raw
- Extrair dados das tabelas auxiliares e principais
- Preservar os dados originais sem transformações significativas
- Armazenar metadados de extração para controle incremental

### Camada Trusted
- Aplicar validações e correções nos dados extraídos
- Implementar tipagem adequada e tratamento de valores nulos
- Identificar e tratar duplicidades e inconsistências

### Camada Service
- Agregar dados para atender aos requisitos de negócio
- Otimizar formato dos dados para consumo pela aplicação cliente
- Implementar regras de negócio específicas do domínio
"""
    
    # Criar arquivo de documentação
    doc_path = os.path.join(BASE_DIR, "docs", "estrategia_processamento.md")
    with open(doc_path, 'w', encoding='utf-8') as f:
        f.write(proc_doc_content)
    logger.info(f"Documentação da estratégia de processamento criada: {doc_path}")
    
    # Fazer upload para o bucket
    try:
        connector = WindsurfGCSConnector()
        dest_path = "datalake/docs/estrategia_processamento.md"
        connector.upload_file(doc_path, dest_path)
        logger.info(f"Documentação enviada para o bucket: {dest_path}")
        return True
    except Exception as e:
        logger.error(f"Erro ao enviar documentação para o bucket: {str(e)}")
        return False


if __name__ == "__main__":
    # Criar estrutura local
    criar_estrutura_local()
    
    # Criar documentação sobre a estrutura do banco de dados
    criar_documentacao_estrutura_db()
    
    # Criar documentação sobre a estratégia de processamento
    criar_documentacao_estrategia_processamento()
    
    # Criar estrutura no bucket GCS
    criar_estrutura_gcs()
    
    logger.info("Organização do projeto concluída com sucesso!")
