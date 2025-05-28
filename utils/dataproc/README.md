# Dataproc - Cloud SQL Integration

## Visão Geral

Este diretório contém scripts e utilitários para processar arquivos JSON armazenados no Google Cloud Storage (GCS) utilizando o Dataproc e gravar os dados processados no Cloud SQL PostgreSQL.

## Estrutura do Diretório

```
./
├── docs/                          # Documentação detalhada
│   ├── CONEXAO_DATAPROC_CLOUDSQL.md  # Documentação sobre a conexão entre Dataproc e Cloud SQL
│   └── PROCESSAMENTO_JSON_PARA_SQL.md # Documentação sobre o processamento de JSONs
│
├── scripts/                       # Scripts principais para produção
│   ├── configurar_firewall_cloudsql.sh # Script para configurar o firewall do Cloud SQL
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
└── README.md                      # Este arquivo
```

## Uso Básico

### Configuração do Firewall do Cloud SQL

Para permitir que o Dataproc se conecte ao Cloud SQL, é necessário configurar o firewall do Cloud SQL:

```bash
./scripts/configurar_firewall_cloudsql.sh \
  --projeto development-439017 \
  --instancia-sql database-ecotrace-lake \
  --regiao-dataproc us-east4 \
  --regiao-sql us-east4
```

### Processamento de JSONs e Gravação no Cloud SQL

Para processar arquivos JSON e gravar os dados no Cloud SQL:

```bash
./scripts/executar_processamento.sh \
  --projeto development-439017 \
  --bucket repo-dev-gdo-carga \
  --input "gs://repo-dev-gdo-carga/datalake/dados/animais/*.json" \
  --tabela bt_animais
```

### Testes de Conexão

Para testar a conexão entre o Dataproc e o Cloud SQL:

```bash
./tests/executar_teste_conectividade.sh \
  --projeto development-439017 \
  --bucket repo-dev-gdo-carga
```

## Documentação Detalhada

Para informações mais detalhadas sobre a implementação, consulte:

- [Conexão entre Dataproc e Cloud SQL](./docs/CONEXAO_DATAPROC_CLOUDSQL.md)
- [Processamento de JSONs e Gravação no SQL](./docs/PROCESSAMENTO_JSON_PARA_SQL.md)

## Requisitos

- Google Cloud SDK instalado e configurado
- Acesso ao projeto GCP com as APIs necessárias ativadas (Dataproc, Cloud SQL, GCS)
- Bucket GCS para armazenamento de scripts e dados
- Instância Cloud SQL PostgreSQL configurada

## Notas de Segurança

- Em ambiente de produção, não use a configuração de firewall `0.0.0.0/0` que permite conexões de qualquer IP
- Considere usar VPC Peering para conexão segura entre Dataproc e Cloud SQL
- Armazene credenciais de forma segura, preferencialmente usando o Secret Manager do Google Cloud
