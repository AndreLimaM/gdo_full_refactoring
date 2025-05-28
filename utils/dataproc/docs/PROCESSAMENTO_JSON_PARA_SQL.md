# Documentau00e7u00e3o: Processamento de JSONs com Dataproc e Gravau00e7u00e3o no Cloud SQL

## Visu00e3o Geral

Este documento descreve o processo de processamento de arquivos JSON armazenados no Google Cloud Storage (GCS) utilizando o Dataproc e a gravau00e7u00e3o dos dados processados no Cloud SQL PostgreSQL.

## Arquitetura da Soluu00e7u00e3o

A soluu00e7u00e3o implementada segue a seguinte arquitetura:

1. **Armazenamento de Dados Brutos**: Arquivos JSON armazenados no Google Cloud Storage (GCS).
2. **Processamento de Dados**: Cluster Dataproc com Apache Spark para processamento distribuu00eddo.
3. **Armazenamento de Dados Processados**: Banco de dados PostgreSQL no Cloud SQL.
4. **Orquestrau00e7u00e3o**: Scripts shell para automatizar o processo de criau00e7u00e3o do cluster, processamento e limpeza de recursos.

## Componentes Principais

### 1. Script de Inicializau00e7u00e3o do Dataproc

O script `init_script.sh` u00e9 executado durante a criau00e7u00e3o do cluster Dataproc e instala todas as dependu00eancias necessu00e1rias:

- Driver JDBC do PostgreSQL
- Cloud SQL Auth Proxy
- Bibliotecas Python necessu00e1rias

### 2. Script de Processamento PySpark

O script `processar_json_para_sql.py` realiza o processamento dos arquivos JSON e a gravau00e7u00e3o no Cloud SQL:

- Leitura de arquivos JSON do GCS
- Transformau00e7u00e3o e processamento dos dados
- Gravau00e7u00e3o dos dados processados no Cloud SQL

### 3. Script de Execuu00e7u00e3o

O script `executar_processamento.sh` orquestra todo o processo:

- Criau00e7u00e3o do cluster Dataproc
- Configurau00e7u00e3o do Cloud SQL para aceitar conexu00f5es
- Execuu00e7u00e3o do job de processamento
- Limpeza de recursos apu00f3s a conclusu00e3o

## Fluxo de Execuu00e7u00e3o

1. **Preparau00e7u00e3o**:
   - Upload dos scripts de inicializau00e7u00e3o e processamento para o GCS
   - Configurau00e7u00e3o do Cloud SQL para aceitar conexu00f5es do Dataproc

2. **Criau00e7u00e3o do Cluster**:
   - Criau00e7u00e3o de um cluster Dataproc com as configurau00e7u00f5es necessu00e1rias
   - Execuu00e7u00e3o do script de inicializau00e7u00e3o para instalar dependu00eancias

3. **Processamento de Dados**:
   - Execuu00e7u00e3o do script PySpark para processar os arquivos JSON
   - Transformau00e7u00e3o e limpeza dos dados conforme necessu00e1rio

4. **Gravau00e7u00e3o no Cloud SQL**:
   - Conexu00e3o com o Cloud SQL via JDBC
   - Gravau00e7u00e3o dos dados processados nas tabelas apropriadas

5. **Limpeza**:
   - Exclusu00e3o do cluster Dataproc apu00f3s a conclusu00e3o do processamento

## Configurau00e7u00e3o do Ambiente

### Pru00e9-requisitos

- Projeto Google Cloud com APIs necessu00e1rias ativadas (Dataproc, Cloud SQL, GCS)
- Bucket GCS para armazenamento de scripts e dados
- Instu00e2ncia Cloud SQL PostgreSQL configurada
- Permissu00f5es IAM adequadas para acessar os serviu00e7os

### Estrutura de Diretu00f3rios no GCS

```
gs://repo-dev-gdo-carga/
u251cu2500u2500 datalake/
u2502   u251cu2500u2500 utils/
u2502   u2502   u251cu2500u2500 listar_tabelas_cloudsql.py
u2502   u2502   u251cu2500u2500 processar_json_para_sql.py
u2502   u2502   u2514u2500u2500 ...
u2502   u251cu2500u2500 dados/
u2502   u2502   u251cu2500u2500 animais/
u2502   u2502   u2502   u2514u2500u2500 *.json
u2502   u2502   u2514u2500u2500 ...
u251cu2500u2500 scripts/
u2502   u251cu2500u2500 init_script.sh
u2502   u2514u2500u2500 install_postgresql_driver.sh
u251cu2500u2500 jobs/
u2502   u2514u2500u2500 ...
```

## Execuu00e7u00e3o do Processamento

Para executar o processamento de JSONs e gravau00e7u00e3o no Cloud SQL, utilize o seguinte comando:

```bash
./utils/dataproc/scripts/executar_processamento.sh \
  --projeto development-439017 \
  --bucket repo-dev-gdo-carga \
  --input "gs://repo-dev-gdo-carga/datalake/dados/animais/*.json" \
  --tabela bt_animais
```

### Paru00e2metros

- `--projeto`: ID do projeto Google Cloud
- `--bucket`: Nome do bucket GCS onde os scripts e dados estu00e3o armazenados
- `--input`: Caminho para os arquivos JSON de entrada (suporta wildcards)
- `--tabela`: Nome da tabela no Cloud SQL onde os dados seru00e3o gravados

## Personalizau00e7u00e3o do Processamento

O script `processar_json_para_sql.py` contu00e9m funu00e7u00f5es que podem ser personalizadas para atender a requisitos especu00edficos:

- `definir_schema_animal()`: Define o schema para os dados de animais nos arquivos JSON
- `processar_dados()`: Implementa transformau00e7u00f5es especu00edficas para os dados

## Monitoramento e Logs

O script de processamento gera logs detalhados que podem ser usados para monitorar o progresso e diagnosticar problemas:

- Logs do Dataproc: Disponu00edveis no Console do Google Cloud
- Logs do script: Exibidos no terminal durante a execuu00e7u00e3o

## Considerau00e7u00f5es de Seguranu00e7a

- **Credenciais**: As credenciais do banco de dados su00e3o passadas como argumentos para o script. Em ambiente de produu00e7u00e3o, considere usar o Secret Manager.
- **Firewall**: O Cloud SQL deve ser configurado para aceitar conexu00f5es do Dataproc. Em produu00e7u00e3o, limite os IPs autorizados apenas aos do Dataproc.
- **VPC Peering**: Para maior seguranu00e7a, considere implementar VPC peering entre as redes do Dataproc e do Cloud SQL.

## Otimizau00e7u00e3o de Desempenho

Para otimizar o desempenho do processamento, considere as seguintes configurau00e7u00f5es:

- **Tamanho do Cluster**: Ajuste o nu00famero de nu00f3s workers com base no volume de dados
- **Partiu00e7u00e3o de Dados**: Use a partiu00e7u00e3o de dados para processar grandes volumes de dados
- **Configurau00e7u00f5es do Spark**: Ajuste as configurau00e7u00f5es de memu00f3ria e execuu00e7u00e3o do Spark
- **Gravau00e7u00e3o em Lote**: Use o paru00e2metro `batchsize` para otimizar a gravau00e7u00e3o no banco de dados

## Soluu00e7u00e3o de Problemas

### Problemas Comuns e Soluu00e7u00f5es

1. **Erro de Conexu00e3o com o Cloud SQL**:
   - Verifique se o IP do Dataproc estu00e1 na lista de IPs autorizados do Cloud SQL
   - Verifique se as credenciais do banco de dados estu00e3o corretas

2. **Erro de Processamento de JSON**:
   - Verifique se o schema definido corresponde u00e0 estrutura dos arquivos JSON
   - Verifique se os arquivos JSON su00e3o vu00e1lidos

3. **Erro de Memu00f3ria no Spark**:
   - Aumente a memu00f3ria alocada para o driver e executors do Spark
   - Particione os dados para processamento em lotes menores

## Conclusu00e3o

A soluu00e7u00e3o implementada permite o processamento eficiente de arquivos JSON armazenados no GCS utilizando o Dataproc e a gravau00e7u00e3o dos dados processados no Cloud SQL PostgreSQL. A arquitetura u00e9 escalu00e1vel e pode ser adaptada para diferentes volumes de dados e requisitos de processamento.
