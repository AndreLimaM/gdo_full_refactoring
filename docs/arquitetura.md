# Arquitetura do Sistema de Processamento de Dados GDO

## Visão Geral

O Sistema de Processamento de Dados GDO é uma solução completa para ingestão, processamento e disponibilização de dados relacionados a animais, caixas e outros payloads. A arquitetura foi projetada seguindo o padrão medalhão (Bronze, Prata, Ouro) para garantir a qualidade e rastreabilidade dos dados em cada etapa do processamento.

## Diagrama de Arquitetura

```
+------------------+     +------------------+     +------------------+
|                  |     |                  |     |                  |
|  Arquivos JSON   +---->+  Camada Raw      +---->+  Camada Trusted  +---->+  Camada Service  +---->+  Aplicações  |
|  (GCS)           |     |  (Bronze)        |     |  (Prata)         |     |  (Ouro)          |     |  Cliente     |
|                  |     |                  |     |                  |     |                  |     |              |
+------------------+     +------------------+     +------------------+     +------------------+     +--------------+
                                 |                       |                        |
                                 v                       v                        v
                         +------------------+    +------------------+    +------------------+
                         |                  |    |                  |    |                  |
                         |  Tabelas Raw     |    |  Tabelas Trusted |    |  Tabelas Service |
                         |  (PostgreSQL)    |    |  (PostgreSQL)    |    |  (PostgreSQL)    |
                         |                  |    |                  |    |                  |
                         +------------------+    +------------------+    +------------------+
                                 |                       |                        |
                                 v                       v                        v
                         +-------------------------------------------------------+
                         |                                                       |
                         |                 Log de Processamento                  |
                         |                 (PostgreSQL)                          |
                         |                                                       |
                         +-------------------------------------------------------+
```

## Componentes Principais

### 1. Armazenamento de Dados

#### Google Cloud Storage (GCS)
- **Bucket**: repo-dev-gdo-carga
- **Estrutura**: 
  - `datalake/payload_data/pending/`: Arquivos JSON pendentes de processamento
  - `datalake/payload_data/done/`: Arquivos JSON já processados

#### PostgreSQL (Cloud SQL)
- **Host**: 34.48.11.43
- **Banco**: db_eco_tcbf_25
- **Tabelas**:
  - `raw_animais`: Armazena dados brutos de animais
  - `log_processamento`: Registra eventos de processamento

### 2. Camadas de Processamento

#### Camada Raw (Bronze)
- **Objetivo**: Ingestão dos dados brutos sem transformações significativas
- **Componentes**:
  - `process_raw_animais.py`: Processa arquivos JSON de animais
  - Tabela `raw_animais`: Armazena os dados brutos

#### Camada Trusted (Prata)
- **Objetivo**: Validação, limpeza e transformação dos dados
- **Componentes**: 
  - Em desenvolvimento

#### Camada Service (Ouro)
- **Objetivo**: Disponibilização dos dados para consumo pelos clientes
- **Componentes**: 
  - Em desenvolvimento

### 3. Sistema de Logging e Monitoramento

#### Log de Processamento
- **Tabela**: `log_processamento`
- **Campos**:
  - Identificação: `id`, `tipo_payload`, `camada`, `nome_arquivo`
  - Status: `status`, `mensagem`, `detalhes`
  - Métricas: `duracao_ms`, `registros_processados`, `registros_invalidos`
  - Ambiente: `usuario`, `hostname`, `versao_script`

#### Gerenciador de Logs
- **Classe**: `LogManager`
- **Funcionalidades**:
  - Registro de início e fim de processamento
  - Registro de sucessos, erros e alertas
  - Métricas de desempenho

## Fluxo de Dados

### Processamento de Animais

1. **Ingestão**:
   - Arquivos JSON são depositados em `datalake/payload_data/pending/animais/`

2. **Processamento Raw**:
   - `process_raw_animais.py` lê os arquivos da pasta `pending`
   - Valida o formato JSON e campos obrigatórios
   - Insere os dados na tabela `raw_animais`
   - Move os arquivos processados para `done/animais/`
   - Registra logs de processamento na tabela `log_processamento`

3. **Processamento Trusted** (a ser implementado):
   - Lê os dados da tabela `raw_animais`
   - Realiza validações e transformações
   - Insere os dados na tabela `trusted_animais`
   - Registra logs de processamento

4. **Processamento Service** (a ser implementado):
   - Lê os dados da tabela `trusted_animais`
   - Prepara os dados para consumo pelos clientes
   - Insere os dados na tabela `service_animais`
   - Registra logs de processamento

## Segurança

### Gerenciamento de Credenciais
- Credenciais de banco de dados armazenadas em variáveis de ambiente
- Arquivo `.env` excluído do controle de versão via `.gitignore`
- Conexões com banco de dados utilizam SSL (sslmode=prefer)

### Controle de Acesso
- Acesso ao banco de dados restrito por firewall
- Autenticação por usuário e senha
- Permissões específicas para cada usuário

## Escalabilidade e Desempenho

### Estratégias de Escalabilidade
- Processamento em lotes para grandes volumes de dados
- Índices otimizados nas tabelas de banco de dados
- Estrutura modular para facilitar a distribuição de carga

### Monitoramento de Desempenho
- Métricas de duração de processamento
- Contagem de registros processados e inválidos
- Logs detalhados para análise de gargalos

## Próximos Passos

1. **Implementação das Camadas Trusted e Service**:
   - Desenvolvimento dos scripts de processamento
   - Criação das tabelas correspondentes
   - Implementação de validações e transformações específicas

2. **Ampliação dos Tipos de Payload**:
   - Implementação do processamento para caixas, desossa, etc.
   - Reutilização da estrutura de logging e monitoramento

3. **Melhorias de Desempenho**:
   - Otimização de consultas SQL
   - Implementação de processamento paralelo
   - Estratégias de particionamento de tabelas

4. **Monitoramento Avançado**:
   - Implementação de dashboards de monitoramento
   - Alertas automáticos para falhas de processamento
   - Análise de tendências e padrões
