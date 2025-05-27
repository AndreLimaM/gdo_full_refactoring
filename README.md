
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
