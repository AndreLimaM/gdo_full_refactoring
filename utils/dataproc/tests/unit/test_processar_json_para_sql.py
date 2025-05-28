#!/usr/bin/env python3

"""
Testes unitu00e1rios para o script processar_json_para_sql.py

Este mu00f3dulo contu00e9m testes unitu00e1rios para as funu00e7u00f5es do script de processamento
de arquivos JSON e gravau00e7u00e3o no Cloud SQL.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Adicionar o diretu00f3rio de scripts ao path para importar o mu00f3dulo
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts')))

# Importar o mu00f3dulo a ser testado
import processar_json_para_sql as pjs


class TestProcessarJsonParaSql(unittest.TestCase):
    """Testes unitu00e1rios para o script processar_json_para_sql.py"""

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


if __name__ == "__main__":
    unittest.main()
