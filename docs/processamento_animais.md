# Processamento de Dados de Animais

## Visão Geral

Este documento descreve o processo de ingestão e processamento de dados de animais no Sistema de Processamento de Dados GDO. O processamento segue a abordagem medalhão (Raw, Trusted, Service) para garantir a qualidade e rastreabilidade dos dados.

## Estrutura dos Dados

### Formato do Arquivo JSON

Os dados de animais são recebidos em formato JSON com a seguinte estrutura:

```json
{
    "id_animal": "ANI001",
    "data_nascimento": "2023-05-15",
    "id_propriedade": "PROP001",
    "sexo": "M",
    "raca": "Nelore",
    "peso_nascimento": 35.5,
    "data_entrada": "2023-05-15",
    "data_saida": null,
    "status": "ativo",
    "dados_adicionais": {
        "vacinado": true,
        "vacinas": [
            {
                "tipo": "Aftosa",
                "data": "2023-06-15"
            },
            {
                "tipo": "Brucelose",
                "data": "2023-06-15"
            }
        ],
        "observacoes": "Animal saudável"
    }
}
```

### Campos Obrigatórios

Os seguintes campos são obrigatórios para o processamento:

| Campo | Tipo | Descrição |
|-------|------|-----------|
| id_animal | string | Identificador único do animal |
| data_nascimento | date | Data de nascimento do animal (formato YYYY-MM-DD) |
| id_propriedade | string | Identificador da propriedade onde o animal está |

### Campos Opcionais

| Campo | Tipo | Descrição |
|-------|------|-----------|
| sexo | string | Sexo do animal (M/F) |
| raca | string | Raça do animal |
| peso_nascimento | decimal | Peso do animal ao nascer (kg) |
| data_entrada | date | Data de entrada na propriedade (formato YYYY-MM-DD) |
| data_saida | date | Data de saída da propriedade (formato YYYY-MM-DD) |
| status | string | Status atual do animal (ativo, inativo, abatido, etc.) |
| dados_adicionais | object | Informações adicionais em formato livre |

## Fluxo de Processamento

### 1. Camada Raw (Bronze)

#### Objetivo
Ingerir os dados brutos sem transformações significativas, preservando o formato original.

#### Processo
1. Os arquivos JSON são depositados na pasta `datalake/payload_data/pending/animais/`
2. O script `process_raw_animais.py` é executado
3. Cada arquivo é lido e validado
4. Os dados são inseridos na tabela `raw_animais`
5. O arquivo processado é movido para `datalake/payload_data/done/animais/`
6. Logs de processamento são registrados na tabela `log_processamento`

#### Tabela Raw_Animais

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| id | SERIAL | Identificador único (PK) |
| id_animal | VARCHAR(50) | Identificador do animal |
| data_nascimento | DATE | Data de nascimento |
| id_propriedade | VARCHAR(50) | Identificador da propriedade |
| sexo | VARCHAR(10) | Sexo do animal |
| raca | VARCHAR(50) | Raça do animal |
| peso_nascimento | DECIMAL(10,2) | Peso ao nascer |
| data_entrada | DATE | Data de entrada na propriedade |
| data_saida | DATE | Data de saída da propriedade |
| status | VARCHAR(20) | Status do animal |
| dados_completos | JSONB | Dados completos em formato JSON |
| nome_arquivo | VARCHAR(255) | Nome do arquivo de origem |
| data_processamento | TIMESTAMP WITH TIME ZONE | Data e hora do processamento |

#### Índices
- `idx_raw_animais_id_animal`: Índice no campo `id_animal`
- `idx_raw_animais_data_processamento`: Índice no campo `data_processamento`

### 2. Camada Trusted (Prata)

#### Objetivo
Validar, limpar e transformar os dados para garantir consistência e qualidade.

#### Processo (a ser implementado)
1. Leitura dos dados da tabela `raw_animais`
2. Validação e limpeza dos dados
   - Verificação de datas válidas
   - Normalização de valores (ex: sexo sempre como "M" ou "F")
   - Verificação de consistência (ex: data_saida > data_entrada)
3. Inserção na tabela `trusted_animais`
4. Registro de logs de processamento

### 3. Camada Service (Ouro)

#### Objetivo
Disponibilizar os dados em formato otimizado para consumo pelos clientes.

#### Processo (a ser implementado)
1. Leitura dos dados da tabela `trusted_animais`
2. Transformação para formato de consumo
   - Cálculo de métricas derivadas (ex: idade do animal)
   - Agregações e sumarizações
3. Inserção na tabela `service_animais`
4. Registro de logs de processamento

## Tratamento de Erros

### Tipos de Erros

1. **Erro de Formato**: O arquivo não está em formato JSON válido
2. **Campos Obrigatórios Ausentes**: Faltam campos obrigatórios no JSON
3. **Erro de Inserção**: Falha ao inserir os dados no banco de dados
4. **Erro de Movimentação**: Falha ao mover o arquivo processado

### Estratégia de Tratamento

- Todos os erros são capturados e registrados na tabela `log_processamento`
- Arquivos com erros não são movidos para a pasta `done`
- Detalhes do erro são armazenados no campo `detalhes` em formato JSON
- O processamento continua para os próximos arquivos, mesmo após um erro

## Monitoramento e Observabilidade

### Logs de Processamento

Os logs são registrados na tabela `log_processamento` com os seguintes detalhes:

| Campo | Descrição |
|-------|-----------|
| tipo_payload | "animais" |
| camada | "raw" |
| status | "sucesso", "erro", "alerta", "iniciado" ou "finalizado" |
| mensagem | Descrição do evento |
| detalhes | Informações adicionais em formato JSON |
| duracao_ms | Duração do processamento em milissegundos |
| registros_processados | Número de registros processados com sucesso |
| registros_invalidos | Número de registros inválidos |

### Consultas Úteis

#### Verificar os últimos processamentos
```sql
SELECT tipo_payload, camada, status, mensagem, data_hora, duracao_ms, registros_processados
FROM log_processamento
ORDER BY data_hora DESC
LIMIT 10;
```

#### Verificar erros recentes
```sql
SELECT tipo_payload, camada, mensagem, detalhes, data_hora
FROM log_processamento
WHERE status = 'erro'
ORDER BY data_hora DESC
LIMIT 10;
```

#### Métricas de desempenho
```sql
SELECT 
    DATE_TRUNC('day', data_hora) AS dia,
    COUNT(*) AS total_processamentos,
    SUM(registros_processados) AS total_registros,
    AVG(duracao_ms) AS duracao_media_ms
FROM log_processamento
WHERE tipo_payload = 'animais' AND camada = 'raw' AND status = 'finalizado'
GROUP BY dia
ORDER BY dia DESC;
```

## Execução Manual

Para executar o processamento manualmente:

```bash
# Navegar até a raiz do projeto
cd /caminho/para/gdo_full_refactoring

# Executar o script de processamento
python3 -m src.raw.process_raw_animais
```

## Solução de Problemas

### Arquivo não foi processado

Verificar:
1. O arquivo está na pasta correta (`pending/animais/`)?
2. O arquivo está em formato JSON válido?
3. O arquivo contém todos os campos obrigatórios?
4. Há logs de erro na tabela `log_processamento`?

### Dados não aparecem na tabela raw_animais

Verificar:
1. A tabela `raw_animais` existe no banco de dados?
2. O script de processamento foi executado com sucesso?
3. Há logs de erro relacionados à inserção de dados?
4. As credenciais de banco de dados estão corretas no arquivo `.env`?

## Próximos Passos

1. Implementar validações adicionais para os dados de animais
2. Desenvolver o processamento da camada Trusted
3. Implementar regras de negócio específicas para animais
4. Adicionar suporte para processamento em lote de grandes volumes de dados
