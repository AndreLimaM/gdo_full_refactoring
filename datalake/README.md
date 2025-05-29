# Estrutura do Datalake GDO

Este diretu00f3rio contu00e9m a estrutura completa do projeto de processamento de dados GDO, organizada de acordo com os padru00f5es estabelecidos.

## Estrutura de Diretu00f3rios

```
./
u251cu2500u2500 code/                     # Cu00f3digo-fonte do projeto
u2502   u251cu2500u2500 processors/           # Mu00f3dulos de processamento de payloads
u2502   u2502   u251cu2500u2500 animais/          # Processamento especu00edfico para animais
u2502   u2502   u2514u2500u2500 common/           # Cu00f3digo compartilhado entre processadores
u2502   u2514u2500u2500 utils/               # Utilitu00e1rios gerais
u2502       u251cu2500u2500 db/               # Utilitu00e1rios de banco de dados
u2502       u251cu2500u2500 gcs/              # Utilitu00e1rios para Google Cloud Storage
u2502       u2514u2500u2500 logging/          # Utilitu00e1rios de logging
u251cu2500u2500 config/                   # Configurau00e7u00f5es do projeto
u251cu2500u2500 docs/                     # Documentau00e7u00e3o
u2502   u2514u2500u2500 user_guides/         # Guias de usuu00e1rio
u251cu2500u2500 libs/                     # Bibliotecas externas
u251cu2500u2500 payload_data/             # Dados de entrada e sau00edda
u2502   u251cu2500u2500 pending/             # Arquivos pendentes para processamento
u2502   u251cu2500u2500 done/                # Arquivos processados com sucesso
u2502   u2514u2500u2500 error/               # Arquivos com erro de processamento
u251cu2500u2500 scripts/                  # Scripts de execuu00e7u00e3o e automau00e7u00e3o
u2502   u251cu2500u2500 dataproc/            # Scripts especu00edficos para Dataproc
u2502   u2502   u251cu2500u2500 init/            # Scripts de inicializau00e7u00e3o
u2502   u2502   u2514u2500u2500 jobs/            # Definiu00e7u00f5es de jobs
u2502   u2514u2500u2500 deployment/          # Scripts de implantau00e7u00e3o
u251cu2500u2500 tests/                    # Testes automatizados
u2502   u251cu2500u2500 unit/                # Testes unitu00e1rios
u2502   u2514u2500u2500 integration/         # Testes de integrau00e7u00e3o
u2514u2500u2500 utils/                    # Utilitu00e1rios do projeto
```

## Padru00f5es de Nomenclatura

### Mu00f3dulos de Processamento

- **Formato**: `process_[tipo_payload].py`
- **Exemplos**: `process_animais.py`, `process_insumos.py`

### Scripts de Execuu00e7u00e3o

- **Formato**: `run_[operau00e7u00e3o]_[tipo_payload].sh`
- **Exemplos**: `run_process_animais.sh`, `run_process_insumos.sh`

### Scripts de Inicializau00e7u00e3o

- **Formato**: `init_[ambiente]_[componente].sh`
- **Exemplos**: `init_dataproc_cluster.sh`, `init_cloudsql_proxy.sh`

### Testes

- **Formato**: `test_[componente_testado].py`
- **Exemplos**: `test_process_animais.py`, `test_db_connection.py`

### Documentau00e7u00e3o

- **Formato**: `[COMPONENTE]_[TIPO_DOC].md`
- **Exemplos**: `PROCESSAMENTO_ANIMAIS_GUIA.md`, `CONEXAO_DATAPROC_CLOUDSQL_REFERENCIA.md`

## Fluxo de Processamento

1. Arquivos JSON su00e3o depositados em `payload_data/pending/[tipo_payload]/`
2. Os scripts de processamento lu00eaem os arquivos, processam os dados e os inserem no banco de dados
3. Os arquivos processados su00e3o movidos para `payload_data/done/[tipo_payload]/` (sucesso) ou `payload_data/error/[tipo_payload]/` (falha)
4. Os logs de processamento su00e3o registrados no banco de dados e em arquivos de log

## Execuu00e7u00e3o de Jobs

Para executar um job de processamento:

```bash
bash scripts/dataproc/jobs/run_process_[tipo_payload].sh
```

Para mais informau00e7u00f5es, consulte a documentau00e7u00e3o especu00edfica em `docs/user_guides/`.
