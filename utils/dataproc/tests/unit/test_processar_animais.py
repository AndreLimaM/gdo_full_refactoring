#!/usr/bin/env python3

"""
Testes unitários para o script processar_animais.py

Este módulo contém testes unitários para as funções do script de processamento
de arquivos JSON de animais e gravação no Cloud SQL.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, BooleanType, ArrayType

# Adicionar o diretório de scripts ao path para importar o módulo
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts')))

# Importar o módulo a ser testado
import processar_animais as pa


class TestProcessarAnimais(unittest.TestCase):
    """Testes unitários para o script processar_animais.py"""

    @classmethod
    def setUpClass(cls):
        """Configuração inicial para todos os testes"""
        # Criar uma sessão Spark para testes
        cls.spark = SparkSession.builder \
            .appName("TestProcessarAnimais") \
            .master("local[1]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Limpeza após todos os testes"""
        # Encerrar a sessão Spark
        if cls.spark:
            cls.spark.stop()

    def test_definir_schema_animal(self):
        """Testa se o schema do animal é definido corretamente"""
        schema = pa.definir_schema_animal()
        
        # Verificar se é uma instância de StructType
        self.assertIsInstance(schema, StructType)
        
        # Verificar se todos os campos esperados estão presentes
        campos_esperados = [
            "id_animal", "data_nascimento", "id_propriedade", "sexo", "raca", 
            "peso_nascimento", "data_entrada", "data_saida", "status", "dados_adicionais"
        ]
        
        campos_schema = [field.name for field in schema.fields]
        for campo in campos_esperados:
            self.assertIn(campo, campos_schema)

    def test_definir_schema_dados_adicionais(self):
        """Testa se o schema dos dados adicionais é definido corretamente"""
        schema = pa.definir_schema_dados_adicionais()
        
        # Verificar se é uma instância de StructType
        self.assertIsInstance(schema, StructType)
        
        # Verificar se todos os campos esperados estão presentes
        campos_esperados = ["vacinado", "vacinas", "observacoes"]
        
        campos_schema = [field.name for field in schema.fields]
        for campo in campos_esperados:
            self.assertIn(campo, campos_schema)
        
        # Verificar se o campo vacinas é um ArrayType
        campo_vacinas = next(field for field in schema.fields if field.name == "vacinas")
        self.assertIsInstance(campo_vacinas.dataType, ArrayType)

    def test_processar_dados(self):
        """Testa o processamento de dados de animais"""
        # Criar um DataFrame de teste
        dados_teste = [
            (
                "ANI001", "2023-05-15", "PROP001", "M", "Nelore", 35.5, 
                "2023-05-15", None, "ativo", 
                {"vacinado": True, "vacinas": [{"tipo": "Aftosa", "data": "2023-06-15"}], "observacoes": "Animal saudável"}
            ),
            (
                None, "2023-04-10", "PROP002", "F", "Angus", 32.0, 
                "2023-04-10", None, "ativo", 
                {"vacinado": True, "vacinas": [{"tipo": "Brucelose", "data": "2023-05-10"}], "observacoes": ""}
            ),
            (
                "ANI003", "2023-03-20", None, "M", "Gir", 33.5, 
                "2023-03-20", "2023-10-15", "inativo", 
                {"vacinado": False, "vacinas": [], "observacoes": "Animal vendido"}
            ),
            (
                "ANI004", "2023-02-05", "PROP001", "F", "Holandês", 30.0, 
                "2023-02-05", None, "ativo", 
                {"vacinado": True, "vacinas": [{"tipo": "Aftosa", "data": "2023-03-05"}, {"tipo": "Brucelose", "data": "2023-03-05"}], "observacoes": ""}
            )
        ]
        
        schema = pa.definir_schema_animal()
        df = self.spark.createDataFrame(dados_teste, schema)
        
        # Processar os dados
        df_processado = pa.processar_dados(df)
        
        # Verificar se registros com id_animal ou id_propriedade nulos foram removidos
        self.assertEqual(df_processado.count(), 2)  # Apenas 2 registros válidos
        
        # Verificar se a coluna data_processamento foi adicionada
        self.assertIn("data_processamento", df_processado.columns)
        
        # Verificar se a coluna vacinas_info foi adicionada
        self.assertIn("vacinas_info", df_processado.columns)

    @patch('processar_animais.logger')
    def test_ler_arquivos_json(self, mock_logger):
        """Testa a leitura de arquivos JSON de animais"""
        # Criar um mock para o método read.schema().json() do Spark
        mock_df = MagicMock()
        self.spark.read.schema = MagicMock(return_value=MagicMock(json=MagicMock(return_value=mock_df)))
        
        schema = pa.definir_schema_animal()
        resultado = pa.ler_arquivos_json(self.spark, "gs://bucket/animais/*.json", schema)
        
        # Verificar se o método json foi chamado com o caminho correto
        self.spark.read.schema().json.assert_called_once_with("gs://bucket/animais/*.json")
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()
        
        # Verificar se o DataFrame retornado é o esperado
        self.assertEqual(resultado, mock_df)

    @patch('processar_animais.logger')
    def test_criar_tabela_animais_sucesso(self, mock_logger):
        """Testa a criação bem-sucedida da tabela de animais"""
        # Configurar o mock para o método read.format().option().load() do Spark
        self.spark.read = MagicMock()
        self.spark.read.format = MagicMock(return_value=self.spark.read)
        self.spark.read.option = MagicMock(return_value=self.spark.read)
        self.spark.read.load = MagicMock()
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        resultado = pa.criar_tabela_animais(self.spark, db_properties)
        
        # Verificar se o resultado é True (tabela criada com sucesso)
        self.assertTrue(resultado)
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()

    @patch('processar_animais.logger')
    def test_criar_tabela_animais_falha(self, mock_logger):
        """Testa a falha na criação da tabela de animais"""
        # Configurar o mock para lançar uma exceção
        self.spark.read = MagicMock()
        self.spark.read.format = MagicMock(return_value=self.spark.read)
        self.spark.read.option = MagicMock(return_value=self.spark.read)
        self.spark.read.load = MagicMock(side_effect=Exception("Erro ao criar tabela"))
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        resultado = pa.criar_tabela_animais(self.spark, db_properties)
        
        # Verificar se o resultado é False (falha na criação da tabela)
        self.assertFalse(resultado)
        
        # Verificar se o logger de erro foi chamado
        mock_logger.error.assert_called()

    @patch('processar_animais.logger')
    def test_gravar_no_cloud_sql(self, mock_logger):
        """Testa a gravação no Cloud SQL"""
        # Criar um DataFrame de teste
        dados_teste = [("ANI001", "PROP001")]
        df_mock = self.spark.createDataFrame(dados_teste, ["id_animal", "id_propriedade"])
        
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
        pa.gravar_no_cloud_sql(df_mock, db_properties, "animais", "append")
        
        # Verificar se os métodos foram chamados com os parâmetros corretos
        df_mock.write.format.assert_called_once_with("jdbc")
        df_mock.write.mode.assert_called_once_with("append")
        df_mock.write.save.assert_called_once()
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()

    @patch('processar_animais.logger')
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
        resultado = pa.testar_conexao_sql(self.spark, db_properties)
        
        # Verificar se o resultado é True (conexão bem-sucedida)
        self.assertTrue(resultado)
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()

    @patch('processar_animais.logger')
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
        resultado = pa.testar_conexao_sql(self.spark, db_properties)
        
        # Verificar se o resultado é False (falha na conexão)
        self.assertFalse(resultado)
        
        # Verificar se o logger de erro foi chamado
        mock_logger.error.assert_called()


if __name__ == "__main__":
    unittest.main()
