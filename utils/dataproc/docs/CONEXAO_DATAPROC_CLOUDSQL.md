# Documentação: Conexão entre Dataproc e Cloud SQL

## Visão Geral

Este documento descreve o processo de configuração e estabelecimento de conexão entre clusters Dataproc e instâncias Cloud SQL no Google Cloud Platform, implementado para o projeto de processamento de dados de animais.

## Desafio

Um dos principais desafios enfrentados foi estabelecer uma conexão confiável entre o Dataproc (serviço de processamento de dados) e o Cloud SQL (banco de dados PostgreSQL), devido a restrições de rede e firewall.

## Solução Implementada

### 1. Configuração do Firewall do Cloud SQL

Para permitir que o Dataproc se conecte ao Cloud SQL, foi necessário configurar as regras de firewall do Cloud SQL para aceitar conexões dos IPs do cluster Dataproc.

#### Configuração para Ambiente de Testes

Para fins de teste, configuramos o Cloud SQL para aceitar conexões de qualquer IP:

```bash
gcloud sql instances patch database-ecotrace-lake \
  --project=development-439017 \
  --authorized-networks="0.0.0.0/0"
```

> **ATENÇÃO**: Esta configuração NÃO deve ser usada em ambiente de produção, pois permite conexões de qualquer IP, representando um risco de segurança.

#### Configuração Recomendada para Produção

Para ambientes de produção, recomendamos as seguintes abordagens:

1. **Autorizar apenas IPs específicos do Dataproc**:

   ```bash
   # Obter o IP do nó master do Dataproc
   MASTER_IP=$(gcloud compute instances describe NOME_DO_NO_MASTER \
     --zone=ZONA \
     --project=PROJETO_ID \
     --format="get(networkInterfaces[0].networkIP)")
   
   # Adicionar o IP à lista de IPs autorizados do Cloud SQL
   gcloud sql instances patch NOME_INSTANCIA_SQL \
     --project=PROJETO_ID \
     --authorized-networks="${MASTER_IP}/32"
   ```

2. **Implementar VPC Peering**:
   - Configurar VPC Peering entre a rede do Dataproc e a rede do Cloud SQL
   - Usar o IP privado do Cloud SQL para conexão
   - Esta é a abordagem mais segura, pois o tráfego não passa pela internet pública

3. **Usar o Cloud SQL Auth Proxy**:
   - Instalar o Cloud SQL Auth Proxy nos nós do Dataproc
   - Usar o proxy para conexão segura com o Cloud SQL
   - O proxy gerencia a autenticação e criptografia automaticamente

### 2. Scripts de Inicialização do Dataproc

Para garantir que o cluster Dataproc tenha todas as dependências necessárias para se conectar ao Cloud SQL, criamos um script de inicialização que é executado durante a criação do cluster:

```bash
#!/bin/bash

# Instalar o driver JDBC do PostgreSQL
apt-get update
apt-get install -y postgresql-client

# Baixar o driver JDBC do PostgreSQL
wget -q https://jdbc.postgresql.org/download/postgresql-42.2.23.jar -O /usr/lib/spark/jars/postgresql-42.2.23.jar

# Instalar o Cloud SQL Auth Proxy
wget -q https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O /usr/local/bin/cloud_sql_proxy
chmod +x /usr/local/bin/cloud_sql_proxy

# Instalar bibliotecas Python necessárias
pip install pandas psycopg2-binary google-cloud-storage
```

### 3. Configuração da Conexão JDBC

Para conectar o Spark (no Dataproc) ao PostgreSQL (no Cloud SQL), utilizamos a conexão JDBC com as seguintes configurações:

```python
# Configurações de conexão com o banco de dados
db_host = "34.48.11.43"  # IP público do Cloud SQL (para testes)
# db_host = "10.98.169.3"  # IP privado do Cloud SQL (para produção com VPC Peering)
db_name = "db_eco_tcbf_25"
db_user = "db_eco_tcbf_25_user"
db_password = "5HN33PHKjXcLTz3tBC"
db_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"

# Exemplo de uso com Spark
df = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", db_url) \
    .option("dbtable", "nome_da_tabela") \
    .option("user", db_user) \
    .option("password", db_password) \
    .load()
```

## Testes Realizados

Foram realizados os seguintes testes para validar a conexão:

1. **Teste de Conectividade TCP**: Verificação da capacidade de estabelecer uma conexão TCP com o Cloud SQL.
2. **Teste de Conexão JDBC**: Verificação da capacidade de executar consultas SQL no banco de dados.
3. **Listagem de Tabelas**: Verificação da capacidade de listar todas as tabelas disponíveis no banco de dados.
4. **Consulta de Dados**: Verificação da capacidade de consultar dados de tabelas específicas.

Todos os testes foram bem-sucedidos após a configuração correta do firewall do Cloud SQL.

## Problemas Comuns e Soluções

### Timeout de Conexão

**Problema**: Erro "SocketTimeoutException: connect timed out" ao tentar conectar ao Cloud SQL.

**Solução**: 
- Verificar se o IP do Dataproc está na lista de IPs autorizados do Cloud SQL.
- Verificar se as regras de firewall da VPC permitem conexões na porta 5432 (PostgreSQL).

### Erro de Autenticação

**Problema**: Erro "PSQLException: FATAL: password authentication failed for user" ao tentar conectar ao Cloud SQL.

**Solução**:
- Verificar se as credenciais (usuário e senha) estão corretas.
- Verificar se o usuário tem permissão para acessar o banco de dados especificado.

## Recomendações de Segurança

1. **Nunca expor o Cloud SQL à internet pública** em ambiente de produção.
2. **Usar VPC Peering** para conexão segura entre Dataproc e Cloud SQL.
3. **Implementar o Cloud SQL Auth Proxy** para conexões seguras.
4. **Armazenar credenciais de forma segura**, preferencialmente usando o Secret Manager do Google Cloud.
5. **Limitar as permissões do usuário do banco de dados** ao mínimo necessário para a aplicação.

## Conclusão

A conexão entre Dataproc e Cloud SQL foi estabelecida com sucesso, permitindo o processamento de dados JSON e a gravação dos resultados no banco de dados PostgreSQL. A solução implementada é robusta e pode ser adaptada para diferentes cenários de uso, desde ambientes de desenvolvimento até produção.
