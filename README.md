# Projeto de Processamento de Dados GDO

## Visão Geral

Este projeto implementa um sistema de processamento para payloads GDO recebidos via arquivos JSON.
O sistema utiliza o Google Cloud Storage (GCS) para armazenamento de dados e o Cloud SQL para
persistência. A arquitetura segue uma abordagem de camadas (medalhão) com clara separação entre
dados brutos, processamento confiável e camada de serviço para consumo pelos clientes.

## Estrutura do Projeto

### Estrutura de Diretórios

```
./
├── config/                     # Configurações do sistema
│   ├── config.yaml            # Arquivo principal de configuração
│   └── config_manager.py      # Gerenciador de configurações
│
├── src/                       # Código fonte principal
│   ├── raw/                   # Processamento da camada Raw
│   │   ├── __init__.py
│   │   └── process_raw_animais.py  # Processamento de animais
│   │
│   ├── trusted/               # Processamento da camada Trusted
│   │
│   ├── service/               # Processamento da camada Service
│   │
│   ├── utils/                 # Utilitários internos
│   │   ├── __init__.py
│   │   └── log_manager.py     # Gerenciador de logs
│   │
│   └── sql/                   # Scripts SQL
│       └── create_log_table.sql  # Criação da tabela de log
│
├── utils/                     # Utilitários gerais
│   ├── database/              # Utilitários de banco de dados
│   │   ├── db_connector.py    # Conector para o banco de dados
│   │   └── ...
│   │
│   ├── gcs/                   # Utilitários para Google Cloud Storage
│   │   ├── gcs_connector.py   # Conector para o GCS
│   │   └── ...
│   │
│   └── dev/                   # Utilitários de desenvolvimento
│       ├── criar_tabela_log.py
│       ├── verificar_tabela_log.py
│       └── ...
│
├── tests/                     # Testes automatizados
│   ├── data/                  # Dados de teste
│   │   └── animais/           # Dados de teste para animais
│   │       └── animal_teste_001.json
│   └── test_process_raw_animais.py
│
├── .env.example               # Modelo para variáveis de ambiente
├── .gitignore                 # Arquivos ignorados pelo Git
└── README.md                  # Este arquivo
```

### Estrutura de Dados no GCS

```
datalake/payload_data/
├── pending/                   # Arquivos pendentes para processamento
│   └── animais/               # Arquivos de animais pendentes
└── done/                      # Arquivos já processados
    └── animais/               # Arquivos de animais processados
```

## Configuração

### Variáveis de Ambiente

O projeto utiliza variáveis de ambiente para configurações sensíveis. Crie um arquivo `.env` na raiz do projeto com base no modelo `.env.example`:

```
DB_HOST=34.48.11.43
DB_PORT=5432
DB_NAME=db_eco_tcbf_25
DB_USER=db_eco_tcbf_25_user
DB_PASSWORD=sua_senha_aqui
DB_DRIVER=postgresql
DB_SSLMODE=prefer
```

### Arquivo de Configuração

O arquivo `config/config.yaml` contém configurações não-sensíveis do sistema:

```yaml
# Configurações do Google Cloud Storage
gcs:
  bucket_name: "repo-dev-gdo-carga"
  credentials_path: "./credentials.json"
  paths:
    datalake: "datalake/"
    temp: "datalake/temp/"
    logs: "datalake/logs/"

# Configurações de logging
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
```

## Componentes Implementados

### 1. Conectores

#### Conector de Banco de Dados (CloudSQLConnector)

Implementado em `utils/database/db_connector.py`, fornece uma interface para interagir com o banco de dados PostgreSQL:

- Conexão segura usando variáveis de ambiente
- Execução de consultas SQL
- Gerenciamento de transações
- Operações CRUD (Create, Read, Update, Delete)

#### Conector GCS (WindsurfGCSConnector)

Implementado em `utils/gcs/gcs_connector.py`, fornece uma interface para interagir com o Google Cloud Storage:

- Upload e download de arquivos
- Listagem de arquivos em um bucket
- Movimentação e cópia de arquivos
- Verificação de existência de arquivos

### 2. Sistema de Logging

#### Tabela de Log (log_processamento)

Implementada via script SQL em `src/sql/create_log_table.sql`, armazena informações detalhadas sobre o processamento:

- Identificação: tipo de payload, camada, nome do arquivo
- Status: sucesso, erro, alerta, iniciado, finalizado
- Métricas: duração, registros processados, registros inválidos
- Detalhes: campo JSONB para informações adicionais

#### Gerenciador de Logs (LogManager)

Implementado em `src/utils/log_manager.py`, fornece uma interface para registrar logs no banco de dados:

- Registro de eventos de processamento
- Tratamento de exceções
- Métricas de desempenho
- Rastreabilidade completa

### 3. Processamento Raw de Animais

Implementado em `src/raw/process_raw_animais.py`, realiza o processamento de arquivos JSON de animais:

- Leitura de arquivos da pasta `pending/animais`
- Validação do formato JSON e campos obrigatórios
- Inserção dos dados na tabela `raw_animais`
- Movimentação dos arquivos processados para `done/animais`
- Registro de logs de processamento

### 4. Utilitários de Desenvolvimento

Vários scripts para auxiliar no desenvolvimento e testes:

- `utils/dev/criar_tabela_log.py`: Cria a tabela de log no banco de dados
- `utils/dev/verificar_tabela_log.py`: Verifica a estrutura da tabela de log
- `tests/test_process_raw_animais.py`: Testa o processamento de animais

## Uso do Sistema

### Processamento de Dados Raw

Para processar arquivos de animais na camada raw:

```bash
python -m src.raw.process_raw_animais
```

### Verificação de Logs

Para verificar os logs de processamento no banco de dados:

```sql
SELECT * FROM log_processamento ORDER BY data_hora DESC LIMIT 10;
```

### Testes

Para executar os testes de processamento de animais:

```bash
python -m tests.test_process_raw_animais
```

## Segurança

- Credenciais sensíveis são armazenadas em variáveis de ambiente
- O arquivo `.env` é excluído do controle de versão via `.gitignore`
- Conexões com banco de dados utilizam SSL (sslmode=prefer)

## Integração Dataproc e Cloud SQL

Foi implementada uma solução para processamento de dados JSON utilizando o Google Dataproc e armazenamento no Cloud SQL:

### Estrutura da Solução

```
utils/dataproc/
├── docs/                          # Documentação detalhada
│   ├── CONEXAO_DATAPROC_CLOUDSQL.md  # Documentação sobre a conexão entre Dataproc e Cloud SQL
│   ├── PROCESSAMENTO_JSON_PARA_SQL.md # Documentação sobre o processamento de JSONs
│   └── TESTES_UNITARIOS.md          # Documentação detalhada sobre os testes unitários
│
├── scripts/                       # Scripts principais para produção
│   ├── configurar_firewall_cloudsql.sh # Script para configurar o firewall do Cloud SQL
│   ├── executar_listagem_tabelas_gcs.sh # Script para listar tabelas do Cloud SQL
│   ├── executar_processamento.sh  # Script para executar o processamento de JSONs
│   ├── init_script.sh             # Script de inicialização do cluster Dataproc
│   ├── install_postgresql_driver.sh # Script para instalar o driver PostgreSQL
│   ├── listar_tabelas_cloudsql.py # Script para listar tabelas do Cloud SQL
│   └── processar_json_para_sql.py # Script PySpark para processar JSONs e gravar no SQL
│
├── tests/                         # Scripts de teste
│   ├── executar_teste_conectividade.sh # Teste de conectividade entre Dataproc e Cloud SQL
│   ├── executar_teste_conexao_simples.sh # Teste simplificado de conexão
│   ├── executar_teste_simples.sh  # Teste básico de conexão
│   ├── testar_conexao_dataproc_cloudsql.py # Script completo de teste de conexão
│   ├── testar_conexao_simples.py  # Script simplificado de teste de conexão
│   ├── teste_conectividade_basica.py # Teste básico de conectividade TCP
│   └── teste_simples_conexao.py   # Teste simples de conexão JDBC
│
└── README.md                      # Documentação específica do módulo Dataproc
```

### Principais Recursos

1. **Processamento de JSONs com Dataproc**: Scripts para processar arquivos JSON armazenados no GCS utilizando clusters Dataproc.
2. **Conexão com Cloud SQL**: Implementação de conexão segura entre o Dataproc e o Cloud SQL PostgreSQL.
3. **Configuração de Firewall**: Scripts para configurar o firewall do Cloud SQL para permitir conexões do Dataproc.
4. **Testes de Conectividade**: Scripts para testar a conexão entre o Dataproc e o Cloud SQL.

### Documentação Detalhada

Para mais informações sobre a implementação, consulte:

- [Conexão entre Dataproc e Cloud SQL](./utils/dataproc/docs/CONEXAO_DATAPROC_CLOUDSQL.md)
- [Processamento de JSONs e Gravação no SQL](./utils/dataproc/docs/PROCESSAMENTO_JSON_PARA_SQL.md)
- [Testes Unitários](./utils/dataproc/docs/TESTES_UNITARIOS.md)

## Próximos Passos

1. Implementar processamento para outros tipos de payload (caixas, desossa, etc.)
2. Desenvolver a camada Trusted para transformação e validação de dados
3. Implementar a camada Service para disponibilização de dados para consumo
4. Adicionar testes automatizados mais abrangentes
5. Implementar monitoramento e alertas

## Documentação

A documentação detalhada dos componentes está disponível nos docstrings dos módulos e funções.
