# Processamento de Dados de Animais com Dataproc e Cloud SQL

## Visão Geral

Este documento descreve a implementação do processamento de dados de animais utilizando o Google Dataproc e armazenamento no Cloud SQL PostgreSQL. O sistema processa arquivos JSON contendo informações de animais, realiza transformações nos dados e os armazena em um banco de dados PostgreSQL para análise posterior.

## Estrutura dos Dados de Animais

Os arquivos JSON de animais seguem a estrutura conforme o exemplo localizado em `/json_examples/animais/animais_2025_02_01_05_42_01_406Z.json`. Abaixo está um exemplo simplificado:

```json
{
    "cod_empresa": 180,
    "cnpj_industria_abate": "02916265008306",
    "tipo_unidade_abate": "1",
    "nr_unidade_abate": "4507",
    "nr_op": "1975",
    "prod_cpf_cnpj": "33333333333",
    "dt_compra": "2025-01-09",
    "dt_abate": "2025-01-27",
    "flag_contusao": "False",
    "id_destino_abate": -1,
    "motivos_dif": [
        {
            "id": -1,
            "descricao": "N/A"
        }
    ],
    "nr_sequencial": 1357,
    "nr_banda": 1,
    "sexo": "M",
    "nr_chip": "",
    "peso_vivo": 0,
    "peso_carcaca": 224.4,
    "hr_ultima_pesagem": "2025-01-27T14:27:00",
    "dt_fechamento_camera_abate": "2025-01-28",
    "dt_abertura_camera_abate": "2025-01-27",
    "valor_ph": 5.66,
    "cod_barra_abate": "045072701202513571",
    "lote_abate": 8
}
```

### Campos Obrigatórios

- `id_animal`: Identificador único do animal
- `id_propriedade`: Identificador da propriedade onde o animal está localizado

### Campos Opcionais

- `data_nascimento`: Data de nascimento do animal (formato YYYY-MM-DD)
- `sexo`: Sexo do animal (M/F)
- `raca`: Raça do animal
- `peso_nascimento`: Peso do animal ao nascer (em kg)
- `data_entrada`: Data de entrada na propriedade (formato YYYY-MM-DD)
- `data_saida`: Data de saída da propriedade (formato YYYY-MM-DD)
- `status`: Status atual do animal (ativo/inativo)
- `dados_adicionais`: Objeto contendo informações adicionais:
  - `vacinado`: Indica se o animal foi vacinado (true/false)
  - `vacinas`: Lista de vacinas aplicadas
    - `tipo`: Tipo da vacina
    - `data`: Data da aplicação (formato YYYY-MM-DD)
  - `observacoes`: Observações gerais sobre o animal

## Arquitetura de Processamento

O processamento de dados de animais segue a seguinte arquitetura:

1. **Armazenamento de Dados Brutos**: Os arquivos JSON são armazenados no Google Cloud Storage (GCS)
2. **Processamento com Dataproc**: Um cluster Dataproc executa o script PySpark para processar os dados
3. **Transformação de Dados**: Os dados são validados, transformados e preparados para armazenamento
4. **Armazenamento em Cloud SQL**: Os dados processados são gravados em uma tabela PostgreSQL no Cloud SQL

### Diagrama de Fluxo

```
[Arquivos JSON] → [Google Cloud Storage] → [Dataproc (PySpark)] → [Cloud SQL PostgreSQL]
```

## Implementação

### Scripts Principais

1. **`processar_animais.py`**: Script PySpark para processar os dados de animais
2. **`executar_processamento_animais.sh`**: Script shell para facilitar a execução do processamento

### Funcionalidades Implementadas

#### 1. Definição de Schema

O schema dos dados de animais é definido para garantir a correta interpretação dos arquivos JSON:

```python
def definir_schema_animal():
    """
    Define o schema para os dados de animais nos arquivos JSON.
    """
    return StructType([
        StructField("id_animal", StringType(), True),
        StructField("data_nascimento", StringType(), True),
        StructField("id_propriedade", StringType(), True),
        StructField("sexo", StringType(), True),
        StructField("raca", StringType(), True),
        StructField("peso_nascimento", DoubleType(), True),
        StructField("data_entrada", StringType(), True),
        StructField("data_saida", StringType(), True),
        StructField("status", StringType(), True),
        StructField("dados_adicionais", definir_schema_dados_adicionais(), True)
    ])
```

#### 2. Processamento de Dados

O processamento inclui as seguintes etapas:

1. **Validação**: Remoção de registros com campos obrigatórios ausentes
2. **Conversão de Tipos**: Conversão de campos de data para o formato timestamp
3. **Extração de Informações**: Extração de informações de vacinas para uma coluna separada
4. **Adição de Metadados**: Adição de informações como data de processamento

```python
def processar_dados(df):
    """
    Processa e transforma os dados conforme necessário.
    """
    # 1. Remover registros com campos obrigatórios nulos
    df_processado = df.filter(
        col("id_animal").isNotNull() & 
        col("id_propriedade").isNotNull()
    )
    
    # 2. Converter campos de data para timestamp
    df_processado = df_processado \
        .withColumn("data_nascimento", to_timestamp(col("data_nascimento"), "yyyy-MM-dd")) \
        .withColumn("data_entrada", to_timestamp(col("data_entrada"), "yyyy-MM-dd")) \
        .withColumn("data_saida", to_timestamp(col("data_saida"), "yyyy-MM-dd"))
    
    # 3. Adicionar campo de processamento
    df_processado = df_processado.withColumn("data_processamento", current_timestamp())
    
    # 4. Extrair informações de vacinas para uma coluna separada
    df_processado = df_processado.withColumn(
        "vacinas_info", 
        col("dados_adicionais.vacinas").cast("string")
    )
    
    return df_processado
```

#### 3. Criação da Tabela no Cloud SQL

A tabela `bt_animais` é criada automaticamente no Cloud SQL se não existir:

```sql
CREATE TABLE IF NOT EXISTS bt_animais (
    id SERIAL PRIMARY KEY,
    id_animal VARCHAR(50) NOT NULL,
    data_nascimento TIMESTAMP,
    id_propriedade VARCHAR(50) NOT NULL,
    sexo VARCHAR(1),
    raca VARCHAR(100),
    peso_nascimento DOUBLE PRECISION,
    data_entrada TIMESTAMP,
    data_saida TIMESTAMP,
    status VARCHAR(20),
    vacinas_info TEXT,
    dados_adicionais JSONB,
    data_processamento TIMESTAMP
)
```

#### 4. Gravação no Cloud SQL

Os dados processados são gravados no Cloud SQL usando o conector JDBC:

```python
def gravar_no_cloud_sql(df, db_properties, table_name, mode):
    """
    Grava os dados processados no Cloud SQL.
    """
    df.write \
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

## Execução do Processamento

### Pré-requisitos

1. Cluster Dataproc configurado ou permissão para criar um
2. Instância Cloud SQL PostgreSQL configurada e acessível
3. Arquivos JSON de animais armazenados no Google Cloud Storage
4. Permissões adequadas para acessar os recursos GCP

### Parâmetros de Execução

O script `executar_processamento_animais.sh` aceita os seguintes parâmetros:

| Parâmetro | Descrição | Obrigatório | Padrão |
|-----------|-----------|------------|--------|
| `--projeto` | ID do projeto GCP | Sim | - |
| `--regiao` | Região do Dataproc | Não | us-central1 |
| `--cluster` | Nome do cluster Dataproc | Não | cluster-dataproc |
| `--bucket` | Nome do bucket GCS | Sim | - |
| `--input-path` | Caminho GCS para os arquivos JSON | Sim | - |
| `--db-host` | Host do Cloud SQL | Sim | - |
| `--db-name` | Nome do banco de dados | Não | gdo_database |
| `--db-user` | Usuário do banco de dados | Não | postgres |
| `--db-password` | Senha do banco de dados | Sim | - |
| `--table-name` | Nome da tabela para gravar os dados | Não | animais |
| `--mode` | Modo de gravação (append/overwrite) | Não | append |

### Exemplo de Uso

```bash
./executar_processamento_animais.sh \
  --projeto meu-projeto-gcp \
  --bucket meu-bucket-gcs \
  --input-path gs://meu-bucket-gcs/datalake/payload_data/pending/animais/*.json \
  --db-host 10.0.0.1 \
  --db-password minhasenha
```

## Testes

### Testes Unitários

Os testes unitários para o processamento de animais estão implementados no arquivo `test_processar_animais.py` e cobrem as seguintes funcionalidades:

1. Definição de schema
2. Processamento de dados
3. Leitura de arquivos JSON
4. Criação da tabela de animais
5. Gravação no Cloud SQL
6. Teste de conexão com o banco de dados

Para executar os testes unitários:

```bash
cd /home/andre/CascadeProjects/gdo_full_refactoring
python -m utils.dataproc.tests.unit.test_processar_animais
```

### Testes de Integração

Para testar a integração completa entre o Dataproc e o Cloud SQL no processamento de animais:

```bash
./utils/dataproc/scripts/executar_processamento_animais.sh \
  --projeto meu-projeto-gcp \
  --bucket meu-bucket-gcs \
  --input-path gs://meu-bucket-gcs/datalake/payload_data/pending/animais/animal_teste_001.json \
  --db-host 10.0.0.1 \
  --db-password minhasenha \
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
   - Número de registros processados
   - Erros encontrados durante o processamento

## Considerações de Segurança

1. **Credenciais**: As senhas do banco de dados são passadas como parâmetros de linha de comando. Em ambientes de produção, considere usar o Secret Manager do GCP.

2. **Conexão com o Cloud SQL**: A conexão com o Cloud SQL é feita usando o IP público. Em ambientes de produção, considere usar o Cloud SQL Auth Proxy ou VPC Peering.

3. **Permissões**: O script requer permissões para acessar o GCS, criar/executar jobs no Dataproc e acessar o Cloud SQL. Certifique-se de que as contas de serviço tenham as permissões adequadas.

## Próximos Passos

1. **Implementar Processamento Incremental**: Atualmente, o script processa todos os arquivos no caminho especificado. Uma melhoria seria implementar o processamento incremental, processando apenas os novos arquivos.

2. **Adicionar Validações Adicionais**: Implementar validações mais rigorosas para os dados de entrada, como verificação de formatos de data e valores permitidos para campos específicos.

3. **Otimizar Desempenho**: Ajustar os parâmetros do Spark e do JDBC para melhorar o desempenho do processamento para grandes volumes de dados.

4. **Implementar Particionamento**: Particionar os dados no Cloud SQL para melhorar o desempenho de consultas.

5. **Adicionar Métricas**: Implementar métricas de processamento para monitorar o desempenho e a qualidade dos dados.
