# Documentau00e7u00e3o de Testes Unitu00e1rios

## Visu00e3o Geral

Este documento descreve a implementau00e7u00e3o de testes unitu00e1rios para os scripts de processamento de dados JSON do projeto. Os testes unitu00e1rios su00e3o essenciais para garantir a qualidade e confiabilidade do cu00f3digo, permitindo verificar o funcionamento correto de funu00e7u00f5es individuais de forma isolada.

## Estrutura de Testes

Os testes unitu00e1rios estu00e3o organizados no diretu00f3rio `utils/dataproc/tests/unit/` e seguem as convenu00e7u00f5es do framework `unittest` do Python:

```
tests/
u251cu2500u2500 unit/
u2502   u251cu2500u2500 test_processar_json_para_sql.py  # Testes para o script de processamento JSON
u2502   u2514u2500u2500 (outros arquivos de teste)      # Testes futuros para outros scripts
u251cu2500u2500 executar_testes_unitarios.sh      # Script para executar todos os testes unitu00e1rios
u2514u2500u2500 README.md                        # Documentau00e7u00e3o geral sobre testes
```

## Testes Implementados

### 1. Testes para `processar_json_para_sql.py`

O arquivo `test_processar_json_para_sql.py` contu00e9m testes unitu00e1rios para as principais funu00e7u00f5es do script de processamento de JSON:

#### 1.1. `test_definir_schema_animal`

**Objetivo**: Verificar se o schema para dados de animais u00e9 definido corretamente.

**O que u00e9 testado**:
- Se o retorno u00e9 uma instu00e2ncia de `StructType`
- Se todos os campos esperados estu00e3o presentes no schema

**Implementau00e7u00e3o**:
```python
def test_definir_schema_animal(self):
    """Testa se o schema do animal u00e9 definido corretamente"""
    schema = pjs.definir_schema_animal()
    
    # Verificar se u00e9 uma instu00e2ncia de StructType
    self.assertIsInstance(schema, StructType)
    
    # Verificar se todos os campos esperados estu00e3o presentes
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

**O que u00e9 testado**:
- Se registros com campos obrigatu00f3rios nulos su00e3o removidos
- Se a coluna `processado_em` u00e9 adicionada aos dados

**Implementau00e7u00e3o**:
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
    self.assertEqual(df_processado.count(), 2)  # Apenas 2 registros vu00e1lidos
    
    # Verificar se a coluna processado_em foi adicionada
    self.assertIn("processado_em", df_processado.columns)
```

#### 1.3. `test_ler_arquivos_json`

**Objetivo**: Verificar se a leitura de arquivos JSON funciona corretamente.

**O que u00e9 testado**:
- Se o mu00e9todo correto do Spark u00e9 chamado com os paru00e2metros esperados
- Se o logger u00e9 utilizado para registrar informau00e7u00f5es

**Implementau00e7u00e3o**:
```python
@patch('processar_json_para_sql.logger')
def test_ler_arquivos_json(self, mock_logger):
    """Testa a leitura de arquivos JSON"""
    # Criar um mock para o mu00e9todo read.schema().json() do Spark
    mock_df = MagicMock()
    self.spark.read.schema = MagicMock(return_value=MagicMock(json=MagicMock(return_value=mock_df)))
    
    schema = pjs.definir_schema_animal()
    resultado = pjs.ler_arquivos_json(self.spark, "gs://bucket/path/*.json", schema)
    
    # Verificar se o mu00e9todo json foi chamado com o caminho correto
    self.spark.read.schema().json.assert_called_once_with("gs://bucket/path/*.json")
    
    # Verificar se o logger foi chamado
    mock_logger.info.assert_called()
    
    # Verificar se o DataFrame retornado u00e9 o esperado
    self.assertEqual(resultado, mock_df)
```

#### 1.4. `test_gravar_no_cloud_sql`

**Objetivo**: Verificar se a gravau00e7u00e3o no Cloud SQL funciona corretamente.

**O que u00e9 testado**:
- Se os mu00e9todos corretos do DataFrame su00e3o chamados com os paru00e2metros esperados
- Se o logger u00e9 utilizado para registrar informau00e7u00f5es

**Implementau00e7u00e3o**:
```python
@patch('processar_json_para_sql.logger')
def test_gravar_no_cloud_sql(self, mock_logger):
    """Testa a gravau00e7u00e3o no Cloud SQL"""
    # Criar um DataFrame de teste
    dados_teste = [("1", "BRINCO123")]
    df_mock = self.spark.createDataFrame(dados_teste, ["id", "identificacao"])
    
    # Configurar o mock para o mu00e9todo write do DataFrame
    df_mock.write = MagicMock()
    df_mock.write.format = MagicMock(return_value=df_mock.write)
    df_mock.write.option = MagicMock(return_value=df_mock.write)
    df_mock.write.mode = MagicMock(return_value=df_mock.write)
    df_mock.write.save = MagicMock()
    
    # Propriedades de conexu00e3o com o banco de dados
    db_properties = {
        "url": "jdbc:postgresql://localhost:5432/testdb",
        "user": "testuser",
        "password": "testpass"
    }
    
    # Chamar a funu00e7u00e3o a ser testada
    pjs.gravar_no_cloud_sql(df_mock, db_properties, "tabela_teste")
    
    # Verificar se os mu00e9todos foram chamados com os paru00e2metros corretos
    df_mock.write.format.assert_called_once_with("jdbc")
    df_mock.write.mode.assert_called_once_with("append")
    df_mock.write.save.assert_called_once()
    
    # Verificar se o logger foi chamado
    mock_logger.info.assert_called()
```

#### 1.5. `test_testar_conexao_sql_sucesso` e `test_testar_conexao_sql_falha`

**Objetivo**: Verificar se o teste de conexu00e3o com o Cloud SQL funciona corretamente em cenu00e1rios de sucesso e falha.

**O que u00e9 testado**:
- Se o resultado u00e9 `True` quando a conexu00e3o u00e9 bem-sucedida
- Se o resultado u00e9 `False` quando a conexu00e3o falha
- Se o logger u00e9 utilizado para registrar informau00e7u00f5es ou erros

**Implementau00e7u00e3o**:
```python
@patch('processar_json_para_sql.logger')
def test_testar_conexao_sql_sucesso(self, mock_logger):
    """Testa a conexu00e3o bem-sucedida com o Cloud SQL"""
    # Criar um mock para o mu00e9todo read.format().option().load() do Spark
    mock_df = MagicMock()
    self.spark.read = MagicMock()
    self.spark.read.format = MagicMock(return_value=self.spark.read)
    self.spark.read.option = MagicMock(return_value=self.spark.read)
    self.spark.read.load = MagicMock(return_value=mock_df)
    
    # Propriedades de conexu00e3o com o banco de dados
    db_properties = {
        "url": "jdbc:postgresql://localhost:5432/testdb",
        "user": "testuser",
        "password": "testpass"
    }
    
    # Chamar a funu00e7u00e3o a ser testada
    resultado = pjs.testar_conexao_sql(self.spark, db_properties)
    
    # Verificar se o resultado u00e9 True (conexu00e3o bem-sucedida)
    self.assertTrue(resultado)
    
    # Verificar se o logger foi chamado
    mock_logger.info.assert_called()

@patch('processar_json_para_sql.logger')
def test_testar_conexao_sql_falha(self, mock_logger):
    """Testa a falha na conexu00e3o com o Cloud SQL"""
    # Configurar o mock para lanu00e7ar uma exceu00e7u00e3o
    self.spark.read = MagicMock()
    self.spark.read.format = MagicMock(return_value=self.spark.read)
    self.spark.read.option = MagicMock(return_value=self.spark.read)
    self.spark.read.load = MagicMock(side_effect=Exception("Erro de conexu00e3o"))
    
    # Propriedades de conexu00e3o com o banco de dados
    db_properties = {
        "url": "jdbc:postgresql://localhost:5432/testdb",
        "user": "testuser",
        "password": "testpass"
    }
    
    # Chamar a funu00e7u00e3o a ser testada
    resultado = pjs.testar_conexao_sql(self.spark, db_properties)
    
    # Verificar se o resultado u00e9 False (falha na conexu00e3o)
    self.assertFalse(resultado)
    
    # Verificar se o logger de erro foi chamado
    mock_logger.error.assert_called()
```

## Tu00e9cnicas de Teste Utilizadas

### 1. Mocking

Utilizamos a biblioteca `unittest.mock` para substituir componentes reais por objetos simulados (mocks) durante os testes. Isso permite testar funu00e7u00f5es isoladamente, sem depender de serviu00e7os externos como o Spark ou o Cloud SQL.

Exemplos de uso:
- `@patch('processar_json_para_sql.logger')`: Substitui o logger real por um mock
- `MagicMock()`: Cria objetos simulados para substituir componentes do Spark

### 2. Fixtures

Utilizamos os mu00e9todos `setUpClass` e `tearDownClass` para configurar e limpar o ambiente de teste:

```python
@classmethod
def setUpClass(cls):
    """Configurau00e7u00e3o inicial para todos os testes"""
    # Criar uma sessu00e3o Spark para testes
    cls.spark = SparkSession.builder \
        .appName("TestProcessarJSONParaSQL") \
        .master("local[1]") \
        .getOrCreate()

@classmethod
def tearDownClass(cls):
    """Limpeza apu00f3s todos os testes"""
    # Encerrar a sessu00e3o Spark
    if cls.spark:
        cls.spark.stop()
```

### 3. Asseru00e7u00f5es

Utilizamos diversos mu00e9todos de asseru00e7u00e3o para verificar o comportamento esperado:

- `assertIsInstance`: Verifica se um objeto u00e9 instu00e2ncia de uma classe
- `assertIn`: Verifica se um elemento estu00e1 contido em uma coleu00e7u00e3o
- `assertEqual`: Verifica se dois valores su00e3o iguais
- `assertTrue`/`assertFalse`: Verifica se uma expressu00e3o u00e9 verdadeira ou falsa

## Execuu00e7u00e3o dos Testes

Para executar os testes unitu00e1rios, utilize o script `executar_testes_unitarios.sh`:

```bash
./utils/dataproc/tests/executar_testes_unitarios.sh
```

Este script:
1. Configura o `PYTHONPATH` para incluir o diretu00f3rio de scripts
2. Instala as dependu00eancias necessu00e1rias (pytest, pytest-mock, pyspark, pandas)
3. Executa os testes unitu00e1rios com o pytest
4. Verifica o resultado dos testes

## Expansu00e3o dos Testes Unitu00e1rios

Para expandir a cobertura de testes unitu00e1rios, considere:

1. **Adicionar testes para mais funu00e7u00f5es**: Crie testes para outras funu00e7u00f5es do script ou para outros scripts do projeto.

2. **Aumentar a cobertura de cenu00e1rios**: Adicione testes para cenu00e1rios de borda, como entradas invu00e1lidas ou exceu00e7u00f5es esperadas.

3. **Utilizar paru00e2metros parametrizados**: Use o recurso de parametrizau00e7u00e3o do pytest para testar a mesma funu00e7u00e3o com diferentes conjuntos de dados.

4. **Implementar testes de regressu00e3o**: Crie testes para verificar se bugs corrigidos nu00e3o voltam a ocorrer.

## Boas Pru00e1ticas

1. **Mantenha os testes independentes**: Cada teste deve ser independente dos outros e nu00e3o deve depender da ordem de execuu00e7u00e3o.

2. **Nomeie os testes de forma descritiva**: O nome do teste deve descrever claramente o que estu00e1 sendo testado.

3. **Mantenha os testes simples**: Cada teste deve verificar apenas um aspecto especu00edfico do cu00f3digo.

4. **Atualize os testes quando o cu00f3digo mudar**: Sempre atualize os testes quando modificar o cu00f3digo para garantir que eles continuem vu00e1lidos.

5. **Use mocks com moderau00e7u00e3o**: Mocks su00e3o u00fateis para isolar componentes, mas podem tornar os testes fru00e1geis se usados em excesso.
