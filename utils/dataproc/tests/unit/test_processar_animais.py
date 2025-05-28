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

    def test_processar_dados(self):
        """Testa o processamento de dados de animais"""
        # Criar um DataFrame de teste
        dados_teste = [
            (
                180, "02916265008306", "1", "4507", "1975", "33333333333",
                "2025-01-09", "2025-01-27", "False", -1,
                [{"id": -1, "descricao": "N/A"}], "", 370362, "MEIA CARCAÇA RESF BOI INTEIRO (AVULSO)",
                1357, 1, "M", "", 0.0, 224.4, "2025-01-27T14:27:00", 332, 0, 0, 0, 0, 0, 0,
                "2025-01-28", "2025-01-27", "2025-01-28T19:50:12.247", 5.66, "045072701202513571", 8
            ),
            (
                180, "02916265008306", "1", "4507", "466070", "45701849104",
                "2025-01-09", "2025-01-27", "False", -1,
                [{"id": -1, "descricao": "N/A"}], "", 370362, "MEIA CARCAÇA RESF BOI INTEIRO (AVULSO)",
                1357, 2, "M", "", 0.0, 232.9, "2025-01-27T14:27:00", 332, 0, 0, 0, 0, 0, 0,
                "2025-01-28", "2025-01-27", "2025-01-28T19:50:12.480", 5.61, "045072701202513572", 8
            ),
            (
                None, "02916265008306", "1", "4507", "466070", "45701849104",
                "2025-01-09", "2025-01-27", "False", -1,
                [{"id": -1, "descricao": "N/A"}], "", 382440, "MEIA CARCAÇA RESF NOVILHA (JOVEM)",
                None, 1, "F", "", 0.0, 99.9, "2025-01-27T14:28:00", 331, 0, 0, 0, 0, 0, 0,
                "2025-01-28", "2025-01-27", "2025-01-28T15:24:49.647", 5.67, "045072701202513601", 9
            ),
            (
                180, None, "1", "4507", "466070", "45701849104",
                "2025-01-09", "2025-01-27", "False", -1,
                [{"id": -1, "descricao": "N/A"}], "", 382440, "MEIA CARCAÇA RESF NOVILHA (JOVEM)",
                1361, 1, "F", "", 0.0, 101.9, "2025-01-27T14:28:00", 21, 0, 0, 0, 0, 0, 0,
                "2025-01-28", "2025-01-27", "2025-01-31T20:54:57.203", 5.57, "045072701202513611", 9
            )
        ]
        
        schema = pa.definir_schema_animal()
        df = self.spark.createDataFrame(dados_teste, schema)
        
        # Processar os dados
        df_processado = pa.processar_dados(df)
        
        # Verificar se registros com nr_sequencial ou cnpj_industria_abate nulos foram removidos
        self.assertEqual(df_processado.count(), 2)
        
        # Verificar se os campos de data foram convertidos para timestamp
        self.assertEqual(df_processado.schema["dt_compra"].dataType.typeName(), "timestamp")
        self.assertEqual(df_processado.schema["dt_abate"].dataType.typeName(), "timestamp")
        self.assertEqual(df_processado.schema["dt_fechamento_camera_abate"].dataType.typeName(), "timestamp")
        self.assertEqual(df_processado.schema["dt_abertura_camera_abate"].dataType.typeName(), "timestamp")
        self.assertEqual(df_processado.schema["hr_ultima_pesagem"].dataType.typeName(), "timestamp")
        
        # Verificar se o campo de processamento foi adicionado
        self.assertIn("data_processamento", df_processado.columns)
        
        # Verificar se as informações de motivos_dif foram extraídas
        self.assertIn("motivos_dif_info", df_processado.columns)

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
    def test_criar_tabela_animais(self, mock_logger):
        """Testa a criação da tabela bt_animais"""
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
        mock_logger.info.assert_called_with("Tabela bt_animais verificada/criada com sucesso!")

    @patch('processar_animais.logger')
    def test_criar_tabela_animais_falha(self, mock_logger):
        """Testa a falha na criação da tabela bt_animais"""
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
        mock_logger.error.assert_called_with("Erro ao criar tabela de animais: Erro ao criar tabela")

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
