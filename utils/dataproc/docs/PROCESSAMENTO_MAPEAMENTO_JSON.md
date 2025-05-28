# Processamento de JSON com Mapeamento de Campos

## Visão Geral

Este documento descreve a implementação do processamento de arquivos JSON utilizando um mapeamento específico de campos entre o formato JSON de entrada e a estrutura da tabela no banco de dados. O sistema processa arquivos JSON da pasta `pending`, grava os dados em uma tabela PostgreSQL respeitando o mapeamento definido e move os arquivos processados para as pastas `done` ou `error`, dependendo do resultado do processamento.

## Mapeamento de Campos

O processamento utiliza o seguinte mapeamento entre os campos do JSON e as colunas da tabela:

| Nome do campo na tabela | Tipo | Equivalência no JSON do payload |
|------------------------|------|--------------------------------|
| id | bigint | gerado automaticamente pelo banco de dados |
| json_certificado | jsonb | {} um json vazio |
| json_classificacao_ia | jsonb | {} um json vazio |
| json_caracteristicas | jsonb | {} um json vazio |
| json_habilitacoes | jsonb | habilitacoes_etiqueta quando existir, caso contrário {} vazio |
| dt_nascimento | date | dt_nascimento se existir, caso contrário vazio |
| valor_ph | numeric | valor_ph |
| dt_fechamento_camera_abate | timestamp with time zone | dt_fechamento_camera_abate |
| dt_abertura_camera_abate | timestamp with time zone | dt_abertura_camera_abate |
| created_at | timestamp with time zone | Data da criação do registro |
| updated_at | timestamp with time zone | Data da última atualização do registro |
| dt_compra | date | dt_compra |
| dt_abate | date | dt_abate |
| nr_lote_abate | integer | lote_abate |
| id_destino_abate | integer | id_destino_abate |
| json_motivos_dif | jsonb | motivos_dif |
| nr_sequencial | integer | nr_sequencial |
| nr_banda | smallint | nr_banda |
| peso_vivo | numeric | peso_vivo |
| peso_carcaca | numeric | peso_carcaca |
| hr_ultima_pesagem | timestamp with time zone | hr_ultima_pesagem |
| flag_contusao | smallint | flag_contusao |
| token_cliente | character varying | sempre com valor 24ad9d |
| cnpj_industria_abate | character varying | cnpj_industria_abate |
| tipo_unidade_abate | character varying | tipo_unidade_abate |
| nr_unidade_abate | character varying | nr_unidade_abate |
| prod_cpf_cnpj | character varying | prod_cpf_cnpj |
| prod_nr_ie | character varying | prod_nr_ie |
| sexo | character | sexo |
| nr_identificacao | text | nr_identificacao se existir, caso contrário vazio |
| nr_chip | character varying | nr_chip |
| codigo_barra_etiqueta | character varying | cod_barra_abate |

## Arquitetura de Processamento

O processamento de dados segue a seguinte arquitetura:

1. **Armazenamento de Dados Brutos**: Os arquivos JSON são armazenados no Google Cloud Storage (GCS) na pasta `pending`
2. **Processamento com Dataproc**: Um cluster Dataproc executa o script PySpark para processar os dados
3. **Transformação de Dados**: Os dados são validados, transformados e preparados para armazenamento conforme o mapeamento
4. **Armazenamento em Cloud SQL**: Os dados processados são gravados em uma tabela PostgreSQL no Cloud SQL
5. **Movimentação de Arquivos**: Os arquivos processados são movidos para a pasta `done` (sucesso) ou `error` (falha)

### Diagrama de Fluxo

```
[Arquivos JSON] → [GCS (pending)] → [Dataproc (PySpark)] → [Cloud SQL PostgreSQL]
                                   ↓
                   [GCS (done)] ← [Sucesso] / [Falha] → [GCS (error)]
```

## Implementação

### Scripts Principais

1. **`processar_mapeamento_json.py`**: Script PySpark para processar os dados JSON conforme o mapeamento
2. **`executar_processamento_mapeamento.sh`**: Script shell para facilitar a execução do processamento

### Funcionalidades Implementadas

#### 1. Definição de Schema

O schema dos dados JSON é definido para garantir a correta interpretação dos arquivos:

```python
def definir_schema_json():
    """
    Define o schema para os dados nos arquivos JSON conforme o mapeamento.
    """
    return StructType([
        # Campos do JSON conforme mapeamento
        StructField("json_certificado", StringType(), True),
        StructField("json_classificacao_ia", StringType(), True),
        StructField("json_caracteristicas", StringType(), True),
        StructField("habilitacoes_etiqueta", StringType(), True),
        StructField("dt_nascimento", StringType(), True),
        StructField("valor_ph", DoubleType(), True),
        # ... outros campos
    ])
```

#### 2. Processamento de Dados

O processamento inclui as seguintes etapas:

1. **Adição de Campos Gerados**: Adição de campos como `created_at`, `updated_at` e `token_cliente`
2. **Tratamento de Campos Condicionais**: Tratamento especial para campos que podem estar vazios ou ter valores padrão
3. **Mapeamento de Campos**: Mapeamento entre os campos do JSON e as colunas da tabela

```python
def processar_dados(df):
    """
    Processa e transforma os dados conforme o mapeamento.
    """
    # Adicionar campos gerados automaticamente
    df_processado = df.withColumn("created_at", current_timestamp()) \
                      .withColumn("updated_at", current_timestamp()) \
                      .withColumn("token_cliente", lit("24ad9d"))
    
    # Tratamento para campos condicionais
    df_processado = df_processado.withColumn(
        "json_habilitacoes",
        when(col("habilitacoes_etiqueta").isNotNull(), col("habilitacoes_etiqueta")).otherwise(lit("{}"))
    )
    
    return df_processado
```

#### 3. Criação da Tabela no Cloud SQL

A tabela é criada automaticamente no Cloud SQL se não existir, com a estrutura conforme o mapeamento:

```sql
CREATE TABLE IF NOT EXISTS {table_name} (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    json_certificado jsonb,
    json_classificacao_ia jsonb,
    json_caracteristicas jsonb,
    json_habilitacoes jsonb,
    dt_nascimento date,
    valor_ph numeric,
    dt_fechamento_camera_abate timestamp with time zone,
    dt_abertura_camera_abate timestamp with time zone,
    created_at timestamp with time zone,
    updated_at timestamp with time zone,
    -- ... outros campos
)
```

#### 4. Gravação no Cloud SQL

Os dados processados são gravados no Cloud SQL usando o conector JDBC:

```python
def gravar_no_cloud_sql(df, db_properties, table_name, mode):
    """
    Grava os dados processados no Cloud SQL.
    """
    # Mapear colunas do DataFrame para colunas da tabela
    df_final = df.select(
        col("json_certificado"),
        col("json_classificacao_ia"),
        # ... outros campos
        col("cod_barra_abate").alias("codigo_barra_etiqueta")
    )
    
    # Gravar no PostgreSQL usando JDBC
    df_final.write \
        .format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", db_properties["url"]) \
        .option("dbtable", table_name) \
        .option("user", db_properties["user"]) \
        .option("password", db_properties["password"]) \
        .option("truncate", "false") \
        .option("batchsize", 1000) \
        .mode(mode) \
        .save()
```

#### 5. Movimentação de Arquivos

Após o processamento, os arquivos são movidos para as pastas `done` ou `error`:

```python
def mover_arquivos(spark, input_path, output_done_path, output_error_path, arquivos_processados, arquivos_com_erro):
    """
    Move os arquivos processados para as pastas done ou error.
    """
    # Usar o Hadoop FileSystem para operações de arquivo
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    
    for arquivo in arquivos_processados:
        origem = spark._jvm.org.apache.hadoop.fs.Path(f"{input_path}/{arquivo}")
        destino = spark._jvm.org.apache.hadoop.fs.Path(f"{output_done_path}/{arquivo}")
        
        # Mover o arquivo para a pasta done
        fs.rename(origem, destino)
    
    for arquivo in arquivos_com_erro:
        origem = spark._jvm.org.apache.hadoop.fs.Path(f"{input_path}/{arquivo}")
        destino = spark._jvm.org.apache.hadoop.fs.Path(f"{output_error_path}/{arquivo}")
        
        # Mover o arquivo para a pasta error
        fs.rename(origem, destino)
```

## Execução do Processamento

### Pré-requisitos

1. Cluster Dataproc configurado ou permissão para criar um
2. Instância Cloud SQL PostgreSQL configurada e acessível
3. Arquivos JSON armazenados no Google Cloud Storage na pasta `pending`
4. Permissões adequadas para acessar os recursos GCP

### Parâmetros de Execução

O script `executar_processamento_mapeamento.sh` aceita os seguintes parâmetros:

| Parâmetro | Descrição | Obrigatório | Padrão |
|-----------|-----------|------------|--------|
| `--projeto` | ID do projeto GCP | Sim | - |
| `--regiao` | Região do Dataproc | Não | us-central1 |
| `--cluster` | Nome do cluster Dataproc | Não | cluster-dataproc |
| `--bucket` | Nome do bucket GCS | Sim | - |
| `--input-path` | Caminho GCS para os arquivos JSON de entrada | Sim | - |
| `--output-done-path` | Caminho GCS para os arquivos processados com sucesso | Sim | - |
| `--output-error-path` | Caminho GCS para os arquivos com erro | Sim | - |
| `--db-host` | Host do Cloud SQL | Sim | - |
| `--db-name` | Nome do banco de dados | Não | gdo_database |
| `--db-user` | Usuário do banco de dados | Não | postgres |
| `--db-password` | Senha do banco de dados | Sim | - |
| `--table-name` | Nome da tabela para gravar os dados | Não | dados_mapeados |
| `--mode` | Modo de gravação (append/overwrite) | Não | append |

### Exemplo de Uso

```bash
./executar_processamento_mapeamento.sh \
  --projeto meu-projeto-gcp \
  --bucket meu-bucket-gcs \
  --input-path gs://meu-bucket-gcs/datalake/payload_data/pending \
  --output-done-path gs://meu-bucket-gcs/datalake/payload_data/done \
  --output-error-path gs://meu-bucket-gcs/datalake/payload_data/error \
  --db-host 10.0.0.1 \
  --db-password minhasenha \
  --table-name tabela_mapeada
```

## Testes

### Testes Unitários

Os testes unitários para o processamento com mapeamento de campos estão implementados no arquivo `test_processar_mapeamento_json.py` e cobrem as seguintes funcionalidades:

1. Definição de schema
2. Processamento de dados conforme o mapeamento
3. Leitura de arquivos JSON
4. Criação da tabela mapeada
5. Gravação no Cloud SQL
6. Teste de conexão com o banco de dados
7. Listagem de arquivos JSON
8. Processamento de arquivo individual

Para executar os testes unitários:

```bash
cd /home/andre/CascadeProjects/gdo_full_refactoring
python -m utils.dataproc.tests.unit.test_processar_mapeamento_json
```

### Testes de Integração

Para testar a integração completa entre o Dataproc e o Cloud SQL no processamento com mapeamento de campos:

```bash
./utils/dataproc/scripts/executar_processamento_mapeamento.sh \
  --projeto meu-projeto-gcp \
  --bucket meu-bucket-gcs \
  --input-path gs://meu-bucket-gcs/datalake/payload_data/pending \
  --output-done-path gs://meu-bucket-gcs/datalake/payload_data/done \
  --output-error-path gs://meu-bucket-gcs/datalake/payload_data/error \
  --db-host 10.0.0.1 \
  --db-password minhasenha \
  --table-name tabela_teste \
  --mode append
```

## Monitoramento e Logs

O processamento gera logs detalhados que podem ser visualizados de duas formas:

1. **Logs do Dataproc**: Acessíveis através do Console GCP ou usando o comando:
   ```bash
   gcloud dataproc jobs describe JOB_ID --region=REGION --project=PROJECT_ID
   ```

2. **Logs do Script**: O script utiliza o módulo `logging` do Python para registrar informações importantes:
   - Início e fim do processamento
   - Número de arquivos processados com sucesso e com erro
   - Erros encontrados durante o processamento

## Considerações de Segurança

1. **Credenciais**: As senhas do banco de dados são passadas como parâmetros de linha de comando. Em ambientes de produção, considere usar o Secret Manager do GCP.

2. **Conexão com o Cloud SQL**: A conexão com o Cloud SQL é feita usando o IP público. Em ambientes de produção, considere usar o Cloud SQL Auth Proxy ou VPC Peering.

3. **Permissões**: O script requer permissões para acessar o GCS, criar/executar jobs no Dataproc e acessar o Cloud SQL. Certifique-se de que as contas de serviço tenham as permissões adequadas.

## Próximos Passos

1. **Implementar Validações Adicionais**: Adicionar validações mais rigorosas para os dados de entrada, como verificação de formatos de data e valores permitidos para campos específicos.

2. **Otimizar Desempenho**: Ajustar os parâmetros do Spark e do JDBC para melhorar o desempenho do processamento para grandes volumes de dados.

3. **Implementar Particionamento**: Particionar os dados no Cloud SQL para melhorar o desempenho de consultas.

4. **Adicionar Métricas**: Implementar métricas de processamento para monitorar o desempenho e a qualidade dos dados.

5. **Implementar Reprocessamento**: Adicionar funcionalidade para reprocessar arquivos da pasta `error` após correção de problemas.
