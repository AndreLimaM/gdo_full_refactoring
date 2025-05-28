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
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, BooleanType, ArrayType, IntegerType

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
            "cod_empresa", "cnpj_industria_abate", "tipo_unidade_abate", "nr_unidade_abate", 
            "nr_op", "prod_cpf_cnpj", "dt_compra", "dt_abate", "flag_contusao", "id_destino_abate",
            "motivos_dif", "nr_sequencial", "nr_banda", "sexo", "peso_vivo", "peso_carcaca",
            "hr_ultima_pesagem", "dt_fechamento_camera_abate", "dt_abertura_camera_abate",
            "valor_ph", "cod_barra_abate", "lote_abate"
        ]
        
        campos_schema = [field.name for field in schema.fields]
        for campo in campos_esperados:
            self.assertIn(campo, campos_schema)

    def test_definir_schema_motivos_dif(self):
        """Testa se o schema dos motivos diferenciados é definido corretamente"""
        schema = pa.definir_schema_motivos_dif()
        
        # Verificar se é uma instância de ArrayType
        self.assertIsInstance(schema, ArrayType)
        
        # Verificar se o tipo de elemento é StructType
        self.assertIsInstance(schema.elementType, StructType)
        
        # Verificar se todos os campos esperados estão presentes no elemento
        campos_esperados = ["id", "descricao"]
        
        campos_schema = [field.name for field in schema.elementType.fields]
        for campo in campos_esperados:
            self.assertIn(campo, campos_schema)

    @patch('processar_animais.logger')
    def test_ler_arquivos_json(self, mock_logger):
        """Testa a leitura de arquivos JSON de animais"""
        # Criar um mock para o SparkSession
        mock_spark = MagicMock()
        mock_read = MagicMock()
        mock_schema_reader = MagicMock()
        mock_df = MagicMock()
        
        # Configurar o comportamento do mock
        mock_spark.read = mock_read
        mock_read.schema.return_value = mock_schema_reader
        mock_schema_reader.json.return_value = mock_df
        
        schema = pa.definir_schema_animal()
        resultado = pa.ler_arquivos_json(mock_spark, "file:///tmp/animais/*.json", schema)
        
        # Verificar se o método json foi chamado com o caminho correto
        mock_schema_reader.json.assert_called_once_with("file:///tmp/animais/*.json")
        
        # Verificar se o DataFrame retornado é o esperado
        self.assertEqual(resultado, mock_df)

    @patch('processar_animais.logger')
    def test_criar_tabela_animais(self, mock_logger):
        """Testa a criação da tabela bt_animais"""
        # Criar um mock para o SparkSession
        mock_spark = MagicMock()
        mock_read = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        
        # Configurar o comportamento do mock
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.load.return_value = None
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        resultado = pa.criar_tabela_animais(mock_spark, db_properties)
        
        # Verificar se o resultado é True (tabela criada com sucesso)
        self.assertTrue(resultado)
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called_with("Tabela bt_animais verificada/criada com sucesso!")

    @patch('processar_animais.logger')
    def test_criar_tabela_animais_falha(self, mock_logger):
        """Testa a falha na criação da tabela bt_animais"""
        # Criar um mock para o SparkSession
        mock_spark = MagicMock()
        mock_read = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        
        # Configurar o comportamento do mock para lançar uma exceção
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.load.side_effect = Exception("Erro ao criar tabela")
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        resultado = pa.criar_tabela_animais(mock_spark, db_properties)
        
        # Verificar se o resultado é False (falha na criação da tabela)
        self.assertFalse(resultado)
        
        # Verificar se o logger de erro foi chamado
        mock_logger.error.assert_called_with("Erro ao criar tabela de animais: Erro ao criar tabela")

    @patch('processar_animais.logger')
    def test_gravar_no_cloud_sql(self, mock_logger):
        """Testa a gravação no Cloud SQL"""
        # Criar um mock para o DataFrame
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        mock_mode = MagicMock()
        
        # Configurar o comportamento do mock
        mock_df.write = mock_write
        mock_write.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.mode.return_value = mock_mode
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        pa.gravar_no_cloud_sql(mock_df, db_properties, "bt_animais", "append")
        
        # Verificar se os métodos foram chamados com os parâmetros corretos
        mock_write.format.assert_called_once_with("jdbc")
        mock_mode.save.assert_called_once()
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called_with("Dados gravados com sucesso na tabela bt_animais")

    @patch('processar_animais.logger')
    def test_testar_conexao_sql(self, mock_logger):
        """Testa a conexão bem-sucedida com o Cloud SQL"""
        # Criar um mock para o SparkSession
        mock_spark = MagicMock()
        mock_read = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        mock_df = MagicMock()
        
        # Configurar o comportamento do mock
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.load.return_value = mock_df
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        resultado = pa.testar_conexao_sql(mock_spark, db_properties)
        
        # Verificar se o resultado é True (conexão bem-sucedida)
        self.assertTrue(resultado)
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called_with("Conexão com o Cloud SQL estabelecida com sucesso!")

    @patch('processar_animais.logger')
    def test_testar_conexao_sql_falha(self, mock_logger):
        """Testa a falha na conexão com o Cloud SQL"""
        # Criar um mock para o SparkSession
        mock_spark = MagicMock()
        mock_read = MagicMock()
        mock_format = MagicMock()
        mock_option = MagicMock()
        
        # Configurar o comportamento do mock para lançar uma exceção
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.option.return_value = mock_option
        mock_option.option.return_value = mock_option
        mock_option.load.side_effect = Exception("Erro de conexão")
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        resultado = pa.testar_conexao_sql(mock_spark, db_properties)
        
        # Verificar se o resultado é False (falha na conexão)
        self.assertFalse(resultado)
        
        # Verificar se o logger de erro foi chamado
        mock_logger.error.assert_called_with("Erro ao conectar ao Cloud SQL: Erro de conexão")


if __name__ == "__main__":
    unittest.main()
