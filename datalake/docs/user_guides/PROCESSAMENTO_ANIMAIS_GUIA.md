# Guia de Processamento de Dados de Animais

## Visão Geral

Este documento descreve o processo de processamento de dados de animais no sistema GDO, desde a recepção dos arquivos JSON até a persistência no banco de dados PostgreSQL.

## Arquitetura

O processamento de dados de animais segue a arquitetura padrão do projeto GDO, utilizando Google Cloud Dataproc para processamento distribuído e Cloud SQL PostgreSQL para armazenamento persistente.

### Fluxo de Processamento

1. Arquivos JSON são depositados no bucket GCS na pasta `data/raw/animais/pending`
2. O job Dataproc é acionado para processar os arquivos
3. Os dados são transformados e validados
4. Os dados processados são inseridos na tabela `bt_animais` no PostgreSQL
5. Os arquivos processados são movidos para `data/raw/animais/done` (sucesso) ou `data/raw/animais/error` (falha)

## Estrutura de Dados

### Formato do JSON de Entrada

Os arquivos JSON de animais seguem o seguinte formato:

```json
{
  "animais": [
    {
      "id": "123456789",
      "codigo_fazenda": "24ad9d",
      "codigo_animal": "02916265006877",
      "tipo_animal": 1,
      "raca": 200,
      "codigo_lote": "45400547915",
      "codigo_piquete": "",
      "data_nascimento": "2024-08-29",
      "data_entrada": "2024-09-20",
      "categoria": 5,
      "origem": -1,
      "classificacao": {"id": -1, "descricao": "N/A"},
      "lote_anterior": "",
      "brinco": "175",
      "numero": 1,
      "sexo": "M",
      "pai": "",
      "peso_nascimento": 0.0,
      "peso_entrada": 161.5,
      "data_ultima_pesagem": "2024-09-20T11:37:00",
      "status": 0,
      "observacoes": {},
      "dados_adicionais": {},
      "codigo_rastreabilidade": "002002009202401751",
      "pelagem": {},
      "geracao": {},
      "grau_sangue": null,
      "peso_atual": 5.83,
      "data_ultima_movimentacao": "2024-09-23T03:00:00",
      "data_entrada_lote": "2024-09-20T03:00:00",
      "data_criacao": "2025-05-21T14:49:39",
      "data_atualizacao": "2025-05-21T14:49:39",
      "data_exclusao": null
    }
  ]
}
```

### Estrutura da Tabela no Banco de Dados

A tabela `bt_animais` no PostgreSQL possui a seguinte estrutura:

| Coluna                  | Tipo          | Descrição                                |
|-------------------------|---------------|-----------------------------------------|
| id                      | VARCHAR       | Identificador único do animal            |
| codigo_fazenda          | VARCHAR       | Código da fazenda                        |
| codigo_animal           | VARCHAR       | Código do animal                         |
| tipo_animal             | INTEGER       | Tipo do animal                           |
| raca                    | INTEGER       | Código da raça                           |
| codigo_lote             | VARCHAR       | Código do lote                           |
| codigo_piquete          | VARCHAR       | Código do piquete                        |
| data_nascimento         | TIMESTAMP     | Data de nascimento                       |
| data_entrada            | TIMESTAMP     | Data de entrada                          |
| categoria               | INTEGER       | Categoria do animal                      |
| origem                  | INTEGER       | Origem do animal                         |
| classificacao           | JSONB         | Classificação do animal (formato JSON)   |
| lote_anterior           | VARCHAR       | Código do lote anterior                  |
| brinco                  | VARCHAR       | Número do brinco                         |
| numero                  | INTEGER       | Número do animal                         |
| sexo                    | VARCHAR       | Sexo do animal (M/F)                     |
| pai                     | VARCHAR       | Identificador do pai                     |
| peso_nascimento         | NUMERIC(18,18)| Peso ao nascer                           |
| peso_entrada            | NUMERIC(18,18)| Peso na entrada                          |
| data_ultima_pesagem     | TIMESTAMP     | Data da última pesagem                   |
| status                  | INTEGER       | Status do animal                         |
| observacoes             | JSONB         | Observações (formato JSON)               |
| dados_adicionais        | JSONB         | Dados adicionais (formato JSON)          |
| codigo_rastreabilidade  | VARCHAR       | Código de rastreabilidade                |
| pelagem                 | JSONB         | Informações sobre pelagem (formato JSON) |
| geracao                 | JSONB         | Informações sobre geração (formato JSON) |
| grau_sangue             | VARCHAR       | Grau de sangue                           |
| peso_atual              | NUMERIC(18,18)| Peso atual                               |
| data_ultima_movimentacao| TIMESTAMP     | Data da última movimentação              |
| data_entrada_lote       | TIMESTAMP     | Data de entrada no lote                  |
| data_criacao            | TIMESTAMP     | Data de criação do registro              |
| data_atualizacao        | TIMESTAMP     | Data da última atualização               |
| data_exclusao           | TIMESTAMP     | Data de exclusão (soft delete)           |

## Componentes do Sistema

### Processador de Animais

O processador de animais é implementado na classe `AnimalProcessor` no arquivo `src/processors/animais/process_animais.py`. Esta classe é responsável por:

1. Ler os arquivos JSON de animais
2. Transformar os dados para o formato esperado pelo banco de dados
3. Validar os dados
4. Inserir os dados no banco de dados PostgreSQL

### Script de Execução

O script `scripts/dataproc/jobs/run_process_animais.py` é o ponto de entrada para o job Dataproc. Ele recebe os parâmetros necessários para o processamento e aciona o processador de animais.

### Script Shell

O script `scripts/dataproc/jobs/run_process_animais.sh` facilita a execução do job Dataproc, criando o cluster, submetendo o job e excluindo o cluster após a conclusão.

## Como Executar o Processamento

### Pré-requisitos

1. Acesso ao Google Cloud Platform com permissões para:
   - Google Cloud Storage
   - Google Dataproc
   - Google Cloud SQL

2. Arquivos JSON de animais no formato correto

### Execução Local (Desenvolvimento)

Para executar o processamento localmente (para desenvolvimento e testes):

```bash
# Configurar variáveis de ambiente
export DB_HOST="10.98.169.3"
export DB_NAME="db_eco_tcbf_25"
export DB_USER="db_eco_tcbf_25_user"
export DB_PASSWORD="5HN33PHKjXcLTz3tBC"

# Executar o processador
python -m src.processors.animais.process_animais \
  --input-dir=/path/to/input \
  --done-dir=/path/to/done \
  --error-dir=/path/to/error
```

### Execução no Dataproc (Produção)

Para executar o processamento no Dataproc:

```bash
# Executar o script shell
bash scripts/dataproc/jobs/run_process_animais.sh
```

## Monitoramento e Logs

### Logs do Processamento

Os logs do processamento são armazenados em:

1. Console do Dataproc (durante a execução)
2. Cloud Logging (após a execução)
3. Arquivos de log no bucket GCS

### Estatísticas de Processamento

Após a conclusão do processamento, são geradas estatísticas com:

- Total de arquivos processados
- Arquivos processados com sucesso
- Arquivos com erro

## Tratamento de Erros

### Tipos de Erros

1. **Erros de Formato**: Quando o arquivo JSON não está no formato esperado
2. **Erros de Validação**: Quando os dados não atendem às regras de validação
3. **Erros de Conexão**: Quando não é possível conectar ao banco de dados
4. **Erros de Inserção**: Quando ocorre um erro ao inserir os dados no banco de dados

### Arquivos de Erro

Para cada arquivo com erro, é gerado um arquivo de log com detalhes do erro no diretório de erro.

## Testes

### Testes Unitários

Os testes unitários para o processador de animais estão implementados em `tests/unit/processors/test_process_animais.py`.

Para executar os testes:

```bash
python -m unittest tests.unit.processors.test_process_animais
```

### Testes de Integração

Os testes de integração verificam a conexão entre o Dataproc e o Cloud SQL e estão implementados em `tests/integration/test_dataproc_cloudsql_connection.py`.

## Referências

- [Documentação do Google Dataproc](https://cloud.google.com/dataproc/docs)
- [Documentação do Cloud SQL PostgreSQL](https://cloud.google.com/sql/docs/postgres)
- [Documentação do PySpark](https://spark.apache.org/docs/latest/api/python/)
