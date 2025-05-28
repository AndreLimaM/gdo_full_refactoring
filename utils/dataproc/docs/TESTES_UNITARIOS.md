# Documentação de Testes Unitários

## Visão Geral

Este documento descreve a implementação de testes unitários para os scripts de processamento de dados JSON do projeto. Os testes unitários são essenciais para garantir a qualidade e confiabilidade do código, permitindo verificar o funcionamento correto de funções individuais de forma isolada.

## Estrutura de Testes

Os testes unitários estão organizados no diretório `utils/dataproc/tests/unit/` e seguem as convenções do framework `unittest` do Python:

```
tests/
├── unit/
│   ├── test_processar_json_para_sql.py  # Testes para o script de processamento JSON
│   └── (outros arquivos de teste)      # Testes futuros para outros scripts
├── executar_testes_unitarios.sh      # Script para executar todos os testes unitários
└── README.md                        # Documentação geral sobre testes
```

## Testes Implementados

### 1. Testes para `processar_json_para_sql.py`

O arquivo `test_processar_json_para_sql.py` contém testes unitários para as principais funções do script de processamento de JSON:

#### 1.1. `test_definir_schema_animal`

**Objetivo**: Verificar se o schema para dados de animais é definido corretamente.

**O que é testado**:
- Se o retorno é uma instância de `StructType`
- Se todos os campos esperados estão presentes no schema

**Implementação**:
```python
def test_definir_schema_animal(self):
    """Testa se o schema do animal é definido corretamente"""
    schema = pjs.definir_schema_animal()
    
    # Verificar se é uma instância de StructType
    self.assertIsInstance(schema, StructType)
    
    # Verificar se todos os campos esperados estão presentes
    campos_esperados = [
        "id", "identificacao", "data_nascimento", "sexo", "raca", 
        "peso", "propriedade_id", "status", "data_cadastro", "data_atualizacao"
    ]
    
    campos_schema = [field.name for field in schema.fields]
    for campo in campos_esperados:
        self.assertIn(campo, campos_schema)
```

#### 1.2. `test_processar_dados`

**Objetivo**: Verificar se o processamento de dados funciona corretamente.

**O que é testado**:
- Se registros com campos obrigatórios nulos são removidos
- Se a coluna `processado_em` é adicionada aos dados

**Implementação**:
```python
def test_processar_dados(self):
    """Testa o processamento de dados"""
    # Criar um DataFrame de teste
    dados_teste = [
        ("1", "BRINCO123", "2020-01-01", "M", "Nelore", 450.5, "PROP1", "ATIVO", "2022-01-01", "2022-01-01"),
        (None, "BRINCO456", "2019-05-10", "F", "Angus", 380.0, "PROP2", "ATIVO", "2022-01-02", "2022-01-02"),
        ("3", None, "2021-02-15", "M", "Brahman", 520.3, "PROP1", "ATIVO", "2022-01-03", "2022-01-03"),
        ("4", "BRINCO789", "2018-11-20", "F", "Gir", 410.8, "PROP3", "INATIVO", "2022-01-04", "2022-01-04")
    ]
    
    schema = pjs.definir_schema_animal()
    df = self.spark.createDataFrame(dados_teste, schema)
    
    # Processar os dados
    df_processado = pjs.processar_dados(df)
    
    # Verificar se registros com id ou identificacao nulos foram removidos
    self.assertEqual(df_processado.count(), 2)  # Apenas 2 registros válidos
    
    # Verificar se a coluna processado_em foi adicionada
    self.assertIn("processado_em", df_processado.columns)
```

#### 1.3. `test_ler_arquivos_json`

**Objetivo**: Verificar se a leitura de arquivos JSON funciona corretamente.

**O que é testado**:
- Se o método correto do Spark é chamado com os parâmetros esperados
- Se o logger é utilizado para registrar informações

**Implementação**:
```python
@patch('processar_json_para_sql.logger')
def test_ler_arquivos_json(self, mock_logger):
    """Testa a leitura de arquivos JSON"""
    # Criar um mock para o método read.schema().json() do Spark
    mock_df = MagicMock()
    self.spark.read.schema = MagicMock(return_value=MagicMock(json=MagicMock(return_value=mock_df)))
    
    schema = pjs.definir_schema_animal()
    resultado = pjs.ler_arquivos_json(self.spark, "gs://bucket/path/*.json", schema)
    
    # Verificar se o método json foi chamado com o caminho correto
    self.spark.read.schema().json.assert_called_once_with("gs://bucket/path/*.json")
    
    # Verificar se o logger foi chamado
    mock_logger.info.assert_called()
    
    # Verificar se o DataFrame retornado é o esperado
    self.assertEqual(resultado, mock_df)
```

#### 1.4. `test_gravar_no_cloud_sql`

**Objetivo**: Verificar se a gravação no Cloud SQL funciona corretamente.

**O que é testado**:
- Se os métodos corretos do DataFrame são chamados com os parâmetros esperados
- Se o logger é utilizado para registrar informações

**Implementação**:
```python
@patch('processar_json_para_sql.logger')
def test_gravar_no_cloud_sql(self, mock_logger):
    """Testa a gravação no Cloud SQL"""
    # Criar um DataFrame de teste
    dados_teste = [("1", "BRINCO123")]
    df_mock = self.spark.createDataFrame(dados_teste, ["id", "identificacao"])
    
    # Configurar o mock para o método write do DataFrame
    df_mock.write = MagicMock()
    df_mock.write.format = MagicMock(return_value=df_mock.write)
    df_mock.write.option = MagicMock(return_value=df_mock.write)
    df_mock.write.mode = MagicMock(return_value=df_mock.write)
    df_mock.write.save = MagicMock()
    
    # Propriedades de conexão com o banco de dados
    db_properties = {
        "url": "jdbc:postgresql://localhost:5432/testdb",
        "user": "testuser",
        "password": "testpass"
    }
    
    # Chamar a função a ser testada
    pjs.gravar_no_cloud_sql(df_mock, db_properties, "tabela_teste")
    
    # Verificar se os métodos foram chamados com os parâmetros corretos
    df_mock.write.format.assert_called_once_with("jdbc")
    df_mock.write.mode.assert_called_once_with("append")
    df_mock.write.save.assert_called_once()
    
    # Verificar se o logger foi chamado
    mock_logger.info.assert_called()
```

#### 1.5. `test_testar_conexao_sql_sucesso` e `test_testar_conexao_sql_falha`

**Objetivo**: Verificar se o teste de conexão com o Cloud SQL funciona corretamente em cenários de sucesso e falha.

**O que é testado**:
- Se o resultado é `True` quando a conexão é bem-sucedida
- Se o resultado é `False` quando a conexão falha
- Se o logger é utilizado para registrar informações ou erros

**Implementação**:
```python
@patch('processar_json_para_sql.logger')
def test_testar_conexao_sql_sucesso(self, mock_logger):
    """Testa a conexão bem-sucedida com o Cloud SQL"""
    # Criar um mock para o método read.format().option().load() do Spark
    mock_df = MagicMock()
    self.spark.read = MagicMock()
    self.spark.read.format = MagicMock(return_value=self.spark.read)
    self.spark.read.option = MagicMock(return_value=self.spark.read)
    self.spark.read.load = MagicMock(return_value=mock_df)
    
    # Propriedades de conexão com o banco de dados
    db_properties = {
        "url": "jdbc:postgresql://localhost:5432/testdb",
        "user": "testuser",
        "password": "testpass"
    }
    
    # Chamar a função a ser testada
    resultado = pjs.testar_conexao_sql(self.spark, db_properties)
    
    # Verificar se o resultado é True (conexão bem-sucedida)
    self.assertTrue(resultado)
    
    # Verificar se o logger foi chamado
    mock_logger.info.assert_called()

@patch('processar_json_para_sql.logger')
def test_testar_conexao_sql_falha(self, mock_logger):
    """Testa a falha na conexão com o Cloud SQL"""
    # Configurar o mock para lançar uma exceção
    self.spark.read = MagicMock()
    self.spark.read.format = MagicMock(return_value=self.spark.read)
    self.spark.read.option = MagicMock(return_value=self.spark.read)
    self.spark.read.load = MagicMock(side_effect=Exception("Erro de conexão"))
    
    # Propriedades de conexão com o banco de dados
    db_properties = {
        "url": "jdbc:postgresql://localhost:5432/testdb",
        "user": "testuser",
        "password": "testpass"
    }
    
    # Chamar a função a ser testada
    resultado = pjs.testar_conexao_sql(self.spark, db_properties)
    
    # Verificar se o resultado é False (falha na conexão)
    self.assertFalse(resultado)
    
    # Verificar se o logger de erro foi chamado
    mock_logger.error.assert_called()
```

## Técnicas de Teste Utilizadas

### 1. Mocking

Utilizamos a biblioteca `unittest.mock` para substituir componentes reais por objetos simulados (mocks) durante os testes. Isso permite testar funções isoladamente, sem depender de serviços externos como o Spark ou o Cloud SQL.

Exemplos de uso:
- `@patch('processar_json_para_sql.logger')`: Substitui o logger real por um mock
- `MagicMock()`: Cria objetos simulados para substituir componentes do Spark

### 2. Fixtures

Utilizamos os métodos `setUpClass` e `tearDownClass` para configurar e limpar o ambiente de teste:

```python
@classmethod
def setUpClass(cls):
    """Configuração inicial para todos os testes"""
    # Criar uma sessão Spark para testes
    cls.spark = SparkSession.builder \
        .appName("TestProcessarJSONParaSQL") \
        .master("local[1]") \
        .getOrCreate()

@classmethod
def tearDownClass(cls):
    """Limpeza após todos os testes"""
    # Encerrar a sessão Spark
    if cls.spark:
        cls.spark.stop()
```

### 3. Asserções

Utilizamos diversos métodos de asserção para verificar o comportamento esperado:

- `assertIsInstance`: Verifica se um objeto é instância de uma classe
- `assertIn`: Verifica se um elemento está contido em uma coleção
- `assertEqual`: Verifica se dois valores são iguais
- `assertTrue`/`assertFalse`: Verifica se uma expressão é verdadeira ou falsa

## Execução dos Testes

Para executar os testes unitários, utilize o script `executar_testes_unitarios.sh`:

```bash
./utils/dataproc/tests/executar_testes_unitarios.sh
```

Este script:
1. Configura o `PYTHONPATH` para incluir o diretório de scripts
2. Instala as dependências necessárias (pytest, pytest-mock, pyspark, pandas)
3. Executa os testes unitários com o pytest
4. Verifica o resultado dos testes

## Expansão dos Testes Unitários

Para expandir a cobertura de testes unitários, considere:

1. **Adicionar testes para mais funções**: Crie testes para outras funções do script ou para outros scripts do projeto.

2. **Aumentar a cobertura de cenários**: Adicione testes para cenários de borda, como entradas inválidas ou exceções esperadas.

3. **Utilizar parâmetros parametrizados**: Use o recurso de parametrização do pytest para testar a mesma função com diferentes conjuntos de dados.

4. **Implementar testes de regressão**: Crie testes para verificar se bugs corrigidos não voltam a ocorrer.

## Boas Práticas

1. **Mantenha os testes independentes**: Cada teste deve ser independente dos outros e não deve depender da ordem de execução.

2. **Nomeie os testes de forma descritiva**: O nome do teste deve descrever claramente o que está sendo testado.

3. **Mantenha os testes simples**: Cada teste deve verificar apenas um aspecto específico do código.

4. **Atualize os testes quando o código mudar**: Sempre atualize os testes quando modificar o código para garantir que eles continuem válidos.

5. **Use mocks com moderação**: Mocks são úteis para isolar componentes, mas podem tornar os testes frágeis se usados em excesso.
