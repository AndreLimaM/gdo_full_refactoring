# Sistema de Processamento de Dados GDO

Este projeto implementa um sistema completo para processamento dos payloads do GDO recebidos via arquivos JSON. O sistema processa esses dados e os persiste em um banco de dados Cloud SQL no Google Cloud Platform.

O projeto utiliza um sistema de configuração centralizado baseado em YAML, facilitando a migração entre diferentes ambientes. Os conectores para GCS e Cloud SQL são componentes auxiliares que facilitam o desenvolvimento, mas o foco principal é o processamento dos dados do GDO.

## Requisitos

- Python 3.10 ou superior
- Credenciais do Google Cloud com acesso ao bucket `repo-dev-gdo-carga`
- Acesso a uma instância Cloud SQL PostgreSQL
- Permissões para acessar o Secret Manager (opcional, para gerenciamento seguro de credenciais)

## Instalação

1. Clone este repositório
2. Instale as dependências:

```bash
pip install -r requirements.txt
```

3. Configure suas credenciais:
   - Obtenha um arquivo de credenciais JSON do Google Cloud
   - Salve-o como `credentials.json` na raiz do projeto ou defina o caminho no arquivo `.env`

## Configuração

O projeto oferece duas formas de configuração:

### 1. Arquivo de Configuração YAML (Recomendado)

O arquivo de configuração centralizado `config/config.yaml` contém todas as configurações do projeto:

```yaml
# Configurações do Google Cloud Storage
gcs:
  # Nome do bucket principal
  bucket_name: "repo-dev-gdo-carga"
  
  # Caminho para o arquivo de credenciais
  credentials_path: "./credentials.json"
  
  # Prefixos de caminhos no bucket
  paths:
    # Caminho base para o datalake
    datalake: "datalake/"
```

### 2. Arquivo .env (Alternativa)

Alternativamente, você pode criar um arquivo `.env` na raiz do projeto:

```
# Credenciais do Google Cloud Storage
GOOGLE_APPLICATION_CREDENTIALS=./credentials.json

# Configurações do bucket
GCS_BUCKET_NAME=repo-dev-gdo-carga
```

## Uso Básico

### Exemplo de Conexão Simples

```python
from gcs_connector import WindsurfGCSConnector

# Inicializar o conector (usa configurações do arquivo config.yaml ou .env)
connector = WindsurfGCSConnector()

# Listar arquivos no caminho datalake/
files = connector.list_files(prefix="datalake/")
for file in files:
    print(file)
```

### Utilizando o Gerenciador de Configurações

```python
from config.config_manager import ConfigManager

# Carregar configurações
config = ConfigManager()

# Obter valores específicos de configuração
bucket_name = config.get('gcs.bucket_name')
datalake_path = config.get('gcs.paths.datalake')

# Modificar configurações em tempo de execução
config.set('gcs.paths.temp', 'novo/caminho/temp/')

# Salvar alterações no arquivo de configuração
config.save()
```

### Operações Específicas do Datalake

Para operações mais específicas com o datalake, use a classe `DataLakeOperations`:

```python
from datalake_operations import DataLakeOperations

# Inicializar as operações do datalake
datalake = DataLakeOperations()

# Criar um novo dataset
datalake.create_dataset("meu_dataset", "Descrição do meu dataset")

# Criar partições
datalake.create_partition("meu_dataset", "ano=2024")

# Listar partições disponíveis
partitions = datalake.list_partitions("meu_dataset")
print(partitions)

# Trabalhar com DataFrames
import pandas as pd

# Criar um DataFrame de exemplo
df = pd.DataFrame({
    'id': range(1, 11),
    'nome': [f'Item {i}' for i in range(1, 11)],
    'valor': [i * 10.5 for i in range(1, 11)]
})

# Salvar DataFrame no datalake
datalake.write_dataframe(df, "meu_dataset/ano=2024/dados.csv", format='csv', index=False)

# Ler DataFrame do datalake
df_lido = datalake.read_csv("meu_dataset/ano=2024/dados.csv")
print(df_lido.head())
```

## Estrutura do Projeto

### Componentes Principais

- `gdo_processor.py`: Módulo principal para processamento dos payloads do GDO (a ser implementado)
- `db_connector.py`: Conector para o Cloud SQL para persistência dos dados processados
- `config/config.yaml`: Arquivo de configuração centralizado
- `config/config_manager.py`: Gerenciador de configurações

### Conectores Auxiliares

- `gcs_connector.py`: Conector para acesso ao Google Cloud Storage
- `datalake_operations.py`: Operações para manipulação de dados no datalake

### Utilitários e Exemplos

- `exemplo_acesso_datalake.py`: Exemplo de acesso ao datalake
- `listar_datalake.py`: Utilitário para listar conteúdo do datalake
- `requirements.txt`: Dependências do projeto
- `.env.example`: Exemplo de configurações de ambiente

## Funcionalidades

### Processamento de Dados GDO

- Leitura de payloads JSON do bucket GCS
- Validação de schema dos payloads
- Processamento e transformação de dados
- Persistência em banco de dados Cloud SQL
- Monitoramento e registro de erros
- Processamento em lote para alta performance

### Operações de Banco de Dados

- Conexão segura com Cloud SQL PostgreSQL
- Gerenciamento de pool de conexões
- Execução de consultas e transações
- Criação e gerenciamento de tabelas
- Integração com Secret Manager para credenciais seguras

### Conectores Auxiliares

- Upload e download de arquivos do GCS
- Listagem e manipulação de dados no datalake
- Leitura e escrita de dados em diversos formatos (CSV, Parquet, JSON)
- Gerenciamento de configurações centralizadas

## Obtendo Credenciais do Google Cloud

Para usar este conector, você precisa de credenciais do Google Cloud com permissões adequadas para o bucket `repo-dev-gdo-carga`. Siga estes passos para obter as credenciais:

1. Acesse o [Console do Google Cloud](https://console.cloud.google.com/)
2. Navegue até IAM & Admin > Service Accounts
3. Crie ou selecione uma conta de serviço
4. Crie uma chave (formato JSON) para esta conta de serviço
5. Baixe o arquivo JSON e salve-o como `credentials.json` no diretório do projeto

## Segurança

- **NUNCA** comite o arquivo de credenciais no controle de versão
- **NUNCA** comite o arquivo `.env` no controle de versão
- Use o `.gitignore` para evitar o versionamento acidental desses arquivos
- Considere o uso de ferramentas de gerenciamento de segredos para ambientes de produção
