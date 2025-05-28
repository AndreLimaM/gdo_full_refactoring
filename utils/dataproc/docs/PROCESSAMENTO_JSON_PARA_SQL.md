# Documentação: Processamento de JSONs com Dataproc e Gravação no Cloud SQL

## Visão Geral

Este documento descreve o processo de processamento de arquivos JSON armazenados no Google Cloud Storage (GCS) utilizando o Dataproc e a gravação dos dados processados no Cloud SQL PostgreSQL.

## Arquitetura da Solução

A solução implementada segue a seguinte arquitetura:

1. **Armazenamento de Dados Brutos**: Arquivos JSON armazenados no Google Cloud Storage (GCS).
2. **Processamento de Dados**: Cluster Dataproc com Apache Spark para processamento distribuído.
3. **Armazenamento de Dados Processados**: Banco de dados PostgreSQL no Cloud SQL.
4. **Orquestração**: Scripts shell para automatizar o processo de criação do cluster, processamento e limpeza de recursos.

## Componentes Principais

### 1. Script de Inicialização do Dataproc

O script `init_script.sh` é executado durante a criação do cluster Dataproc e instala todas as dependências necessárias:

- Driver JDBC do PostgreSQL
- Cloud SQL Auth Proxy
- Bibliotecas Python necessárias

### 2. Script de Processamento PySpark

O script `processar_json_para_sql.py` realiza o processamento dos arquivos JSON e a gravação no Cloud SQL:

- Leitura de arquivos JSON do GCS
- Transformação e processamento dos dados
- Gravação dos dados processados no Cloud SQL

### 3. Script de Execução

O script `executar_processamento.sh` orquestra todo o processo:

- Criação do cluster Dataproc
- Configuração do Cloud SQL para aceitar conexões
- Execução do job de processamento
- Limpeza de recursos após a conclusão

## Fluxo de Execução

1. **Preparação**:
   - Upload dos scripts de inicialização e processamento para o GCS
   - Configuração do Cloud SQL para aceitar conexões do Dataproc

2. **Criação do Cluster**:
   - Criação de um cluster Dataproc com as configurações necessárias
   - Execução do script de inicialização para instalar dependências

3. **Processamento de Dados**:
   - Execução do script PySpark para processar os arquivos JSON
   - Transformação e limpeza dos dados conforme necessário

4. **Gravação no Cloud SQL**:
   - Conexão com o Cloud SQL via JDBC
   - Gravação dos dados processados nas tabelas apropriadas

5. **Limpeza**:
   - Exclusão do cluster Dataproc após a conclusão do processamento

## Configuração do Ambiente

### Pré-requisitos

- Projeto Google Cloud com APIs necessárias ativadas (Dataproc, Cloud SQL, GCS)
- Bucket GCS para armazenamento de scripts e dados
- Instância Cloud SQL PostgreSQL configurada
- Permissões IAM adequadas para acessar os serviços

### Estrutura de Diretórios no GCS

```
gs://repo-dev-gdo-carga/
├── datalake/
│   ├── utils/
│   │   ├── listar_tabelas_cloudsql.py
│   │   ├── processar_json_para_sql.py
│   │   └── ...
│   ├── dados/
│   │   ├── animais/
│   │   │   └── *.json
│   │   └── ...
├── scripts/
│   ├── init_script.sh
│   └── install_postgresql_driver.sh
├── jobs/
│   └── ...
```

## Execução do Processamento

Para executar o processamento de JSONs e gravação no Cloud SQL, utilize o seguinte comando:

```bash
./utils/dataproc/scripts/executar_processamento.sh \
  --projeto development-439017 \
  --bucket repo-dev-gdo-carga \
  --input "gs://repo-dev-gdo-carga/datalake/dados/animais/*.json" \
  --tabela bt_animais
```

### Parâmetros

- `--projeto`: ID do projeto Google Cloud
- `--bucket`: Nome do bucket GCS onde os scripts e dados estão armazenados
- `--input`: Caminho para os arquivos JSON de entrada (suporta wildcards)
- `--tabela`: Nome da tabela no Cloud SQL onde os dados serão gravados

## Personalização do Processamento

O script `processar_json_para_sql.py` contém funções que podem ser personalizadas para atender a requisitos específicos:

- `definir_schema_animal()`: Define o schema para os dados de animais nos arquivos JSON
- `processar_dados()`: Implementa transformações específicas para os dados

## Monitoramento e Logs

O script de processamento gera logs detalhados que podem ser usados para monitorar o progresso e diagnosticar problemas:

- Logs do Dataproc: Disponíveis no Console do Google Cloud
- Logs do script: Exibidos no terminal durante a execução

## Considerações de Segurança

- **Credenciais**: As credenciais do banco de dados são passadas como argumentos para o script. Em ambiente de produção, considere usar o Secret Manager.
- **Firewall**: O Cloud SQL deve ser configurado para aceitar conexões do Dataproc. Em produção, limite os IPs autorizados apenas aos do Dataproc.
- **VPC Peering**: Para maior segurança, considere implementar VPC peering entre as redes do Dataproc e do Cloud SQL.

## Otimização de Desempenho

Para otimizar o desempenho do processamento, considere as seguintes configurações:

- **Tamanho do Cluster**: Ajuste o número de nós workers com base no volume de dados
- **Partição de Dados**: Use a partição de dados para processar grandes volumes de dados
- **Configurações do Spark**: Ajuste as configurações de memória e execução do Spark
- **Gravação em Lote**: Use o parâmetro `batchsize` para otimizar a gravação no banco de dados

## Solução de Problemas

### Problemas Comuns e Soluções

1. **Erro de Conexão com o Cloud SQL**:
   - Verifique se o IP do Dataproc está na lista de IPs autorizados do Cloud SQL
   - Verifique se as credenciais do banco de dados estão corretas

2. **Erro de Processamento de JSON**:
   - Verifique se o schema definido corresponde à estrutura dos arquivos JSON
   - Verifique se os arquivos JSON são válidos

3. **Erro de Memória no Spark**:
   - Aumente a memória alocada para o driver e executors do Spark
   - Particione os dados para processamento em lotes menores

## Conclusão

A solução implementada permite o processamento eficiente de arquivos JSON armazenados no GCS utilizando o Dataproc e a gravação dos dados processados no Cloud SQL PostgreSQL. A arquitetura é escalável e pode ser adaptada para diferentes volumes de dados e requisitos de processamento.
