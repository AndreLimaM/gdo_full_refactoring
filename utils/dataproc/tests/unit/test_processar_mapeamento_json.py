#!/usr/bin/env python3

"""
Testes unitários para o script processar_mapeamento_json.py

Este módulo contém testes unitários para as funções do script de processamento
de arquivos JSON conforme mapeamento específico e gravação no Cloud SQL.
"""

import unittest
from unittest.mock import patch, MagicMock
import sys
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, BooleanType, IntegerType, DateType

# Adicionar o diretório de scripts ao path para importar o módulo
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../scripts')))

# Importar o módulo a ser testado
import processar_mapeamento_json as pmj


class TestProcessarMapeamentoJson(unittest.TestCase):
    """Testes unitários para o script processar_mapeamento_json.py"""

    @classmethod
    def setUpClass(cls):
        """Configuração inicial para todos os testes"""
        # Criar uma sessão Spark para testes
        cls.spark = SparkSession.builder \
            .appName("TestProcessarMapeamentoJSON") \
            .master("local[1]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Limpeza após todos os testes"""
        # Encerrar a sessão Spark
        if cls.spark:
            cls.spark.stop()

    def test_definir_schema_json(self):
        """Testa se o schema JSON é definido corretamente"""
        schema = pmj.definir_schema_json()
        
        # Verificar se é uma instância de StructType
        self.assertIsInstance(schema, StructType)
        
        # Verificar se todos os campos esperados estão presentes
        campos_esperados = [
            "json_certificado", "json_classificacao_ia", "json_caracteristicas", 
            "habilitacoes_etiqueta", "dt_nascimento", "valor_ph", 
            "dt_fechamento_camera_abate", "dt_abertura_camera_abate"
        ]
        
        campos_schema = [field.name for field in schema.fields]
        for campo in campos_esperados:
            self.assertIn(campo, campos_schema)

    def test_processar_dados(self):
        """Testa o processamento de dados conforme o mapeamento"""
        # Criar um DataFrame de teste
        dados_teste = [
            ("json1", "json2", "json3", "habilitacao1", "2023-01-01", 5.5, 
             "2023-01-02 10:00:00", "2023-01-02 08:00:00", "2023-01-01", "2023-01-02", 
             101, 1, "motivo1", 1001, 1, 450.5, 380.0, "2023-01-02 12:00:00", 
             True, "12345678901234", "tipo1", "unidade1", "cpf1", "ie1", "M", 
             "identificacao1", "chip1", "barra1")
        ]
        
        colunas = [
            "json_certificado", "json_classificacao_ia", "json_caracteristicas", 
            "habilitacoes_etiqueta", "dt_nascimento", "valor_ph", 
            "dt_fechamento_camera_abate", "dt_abertura_camera_abate", "dt_compra", 
            "dt_abate", "lote_abate", "id_destino_abate", "motivos_dif", 
            "nr_sequencial", "nr_banda", "peso_vivo", "peso_carcaca", 
            "hr_ultima_pesagem", "flag_contusao", "cnpj_industria_abate", 
            "tipo_unidade_abate", "nr_unidade_abate", "prod_cpf_cnpj", 
            "prod_nr_ie", "sexo", "nr_identificacao", "nr_chip", "cod_barra_abate"
        ]
        
        df = self.spark.createDataFrame(dados_teste, colunas)
        
        # Processar os dados
        df_processado = pmj.processar_dados(df)
        
        # Verificar se as colunas geradas automaticamente foram adicionadas
        self.assertIn("created_at", df_processado.columns)
        self.assertIn("updated_at", df_processado.columns)
        self.assertIn("token_cliente", df_processado.columns)
        self.assertIn("json_habilitacoes", df_processado.columns)
        
        # Verificar se o token_cliente tem o valor correto
        token_cliente = df_processado.select("token_cliente").first()[0]
        self.assertEqual(token_cliente, "24ad9d")

    @patch('processar_mapeamento_json.logger')
    def test_ler_arquivos_json(self, mock_logger):
        """Testa a leitura de arquivos JSON"""
        # Criar um mock para o método read.schema().json() do Spark
        mock_df = MagicMock()
        self.spark.read.schema = MagicMock(return_value=MagicMock(json=MagicMock(return_value=mock_df)))
        
        schema = pmj.definir_schema_json()
        resultado = pmj.ler_arquivos_json(self.spark, "gs://bucket/path/*.json", schema)
        
        # Verificar se o método json foi chamado com o caminho correto
        self.spark.read.schema().json.assert_called_once_with("gs://bucket/path/*.json")
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()
        
        # Verificar se o DataFrame retornado é o esperado
        self.assertEqual(resultado, mock_df)

    @patch('processar_mapeamento_json.logger')
    def test_criar_tabela_mapeada_sucesso(self, mock_logger):
        """Testa a criação bem-sucedida da tabela mapeada"""
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
        resultado = pmj.criar_tabela_mapeada(self.spark, db_properties, "tabela_teste")
        
        # Verificar se o resultado é True (tabela criada com sucesso)
        self.assertTrue(resultado)
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()

    @patch('processar_mapeamento_json.logger')
    def test_criar_tabela_mapeada_falha(self, mock_logger):
        """Testa a falha na criação da tabela mapeada"""
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
        resultado = pmj.criar_tabela_mapeada(self.spark, db_properties, "tabela_teste")
        
        # Verificar se o resultado é False (falha na criação da tabela)
        self.assertFalse(resultado)
        
        # Verificar se o logger de erro foi chamado
        mock_logger.error.assert_called()

    @patch('processar_mapeamento_json.logger')
    def test_gravar_no_cloud_sql(self, mock_logger):
        """Testa a gravação no Cloud SQL"""
        # Criar um DataFrame de teste
        dados_teste = [
            ("json1", "json2", "json3", "habilitacao1", "2023-01-01", 5.5, 
             "2023-01-02 10:00:00", "2023-01-02 08:00:00", "2023-01-01", "2023-01-02", 
             101, 1, "motivo1", 1001, 1, 450.5, 380.0, "2023-01-02 12:00:00", 
             True, "12345678901234", "tipo1", "unidade1", "cpf1", "ie1", "M", 
             "identificacao1", "chip1", "barra1", "2023-01-03 10:00:00", "2023-01-03 10:00:00", 
             "24ad9d", "json_hab1")
        ]
        
        colunas = [
            "json_certificado", "json_classificacao_ia", "json_caracteristicas", 
            "habilitacoes_etiqueta", "dt_nascimento", "valor_ph", 
            "dt_fechamento_camera_abate", "dt_abertura_camera_abate", "dt_compra", 
            "dt_abate", "lote_abate", "id_destino_abate", "motivos_dif", 
            "nr_sequencial", "nr_banda", "peso_vivo", "peso_carcaca", 
            "hr_ultima_pesagem", "flag_contusao", "cnpj_industria_abate", 
            "tipo_unidade_abate", "nr_unidade_abate", "prod_cpf_cnpj", 
            "prod_nr_ie", "sexo", "nr_identificacao", "nr_chip", "cod_barra_abate",
            "created_at", "updated_at", "token_cliente", "json_habilitacoes"
        ]
        
        df_mock = self.spark.createDataFrame(dados_teste, colunas)
        
        # Configurar o mock para o método write do DataFrame
        df_mock.select = MagicMock(return_value=df_mock)
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
        resultado = pmj.gravar_no_cloud_sql(df_mock, db_properties, "tabela_teste", "append")
        
        # Verificar se o resultado é True (gravação bem-sucedida)
        self.assertTrue(resultado)
        
        # Verificar se os métodos foram chamados com os parâmetros corretos
        df_mock.select.assert_called()
        df_mock.write.format.assert_called_once_with("jdbc")
        df_mock.write.mode.assert_called_once_with("append")
        df_mock.write.save.assert_called_once()
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()

    @patch('processar_mapeamento_json.logger')
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
        resultado = pmj.testar_conexao_sql(self.spark, db_properties)
        
        # Verificar se o resultado é True (conexão bem-sucedida)
        self.assertTrue(resultado)
        
        # Verificar se o logger foi chamado
        mock_logger.info.assert_called()

    @patch('processar_mapeamento_json.logger')
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
        resultado = pmj.testar_conexao_sql(self.spark, db_properties)
        
        # Verificar se o resultado é False (falha na conexão)
        self.assertFalse(resultado)
        
        # Verificar se o logger de erro foi chamado
        mock_logger.error.assert_called()

    @patch('processar_mapeamento_json.logger')
    def test_listar_arquivos_json(self, mock_logger):
        """Testa a listagem de arquivos JSON"""
        # Criar mocks para o Hadoop FileSystem
        mock_fs = MagicMock()
        mock_path = MagicMock()
        mock_file_status1 = MagicMock()
        mock_file_status1.isDirectory.return_value = False
        mock_file_status1.getPath().getName.return_value = "arquivo1.json"
        mock_file_status2 = MagicMock()
        mock_file_status2.isDirectory.return_value = False
        mock_file_status2.getPath().getName.return_value = "arquivo2.json"
        mock_file_status3 = MagicMock()
        mock_file_status3.isDirectory.return_value = True
        
        # Configurar os mocks
        self.spark._jvm = MagicMock()
        self.spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = mock_fs
        self.spark._jvm.org.apache.hadoop.fs.Path = MagicMock(return_value=mock_path)
        mock_fs.exists.return_value = True
        mock_fs.listStatus.return_value = [mock_file_status1, mock_file_status2, mock_file_status3]
        
        # Chamar a função a ser testada
        resultado = pmj.listar_arquivos_json(self.spark, "gs://bucket/path")
        
        # Verificar se o resultado contém os arquivos JSON esperados
        self.assertEqual(len(resultado), 2)
        self.assertIn("arquivo1.json", resultado)
        self.assertIn("arquivo2.json", resultado)

    @patch('processar_mapeamento_json.processar_dados')
    @patch('processar_mapeamento_json.gravar_no_cloud_sql')
    @patch('processar_mapeamento_json.logger')
    def test_processar_arquivo_sucesso(self, mock_logger, mock_gravar, mock_processar):
        """Testa o processamento bem-sucedido de um arquivo"""
        # Configurar os mocks
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        self.spark.read = MagicMock()
        self.spark.read.schema = MagicMock(return_value=self.spark.read)
        self.spark.read.json = MagicMock(return_value=mock_df)
        
        mock_df_processado = MagicMock()
        mock_processar.return_value = mock_df_processado
        mock_gravar.return_value = True
        
        schema = pmj.definir_schema_json()
        
        # Propriedades de conexão com o banco de dados
        db_properties = {
            "url": "jdbc:postgresql://localhost:5432/testdb",
            "user": "testuser",
            "password": "testpass"
        }
        
        # Chamar a função a ser testada
        resultado = pmj.processar_arquivo(
            self.spark, "gs://bucket/path", "arquivo.json", 
            schema, db_properties, "tabela_teste", "append"
        )
        
        # Verificar se o resultado é True (processamento bem-sucedido)
        self.assertTrue(resultado)
        
        # Verificar se as funções foram chamadas corretamente
        self.spark.read.schema.assert_called_once_with(schema)
        self.spark.read.json.assert_called_once_with("gs://bucket/path/arquivo.json")
        mock_processar.assert_called_once_with(mock_df)
        mock_gravar.assert_called_once_with(mock_df_processado, db_properties, "tabela_teste", "append")


if __name__ == "__main__":
    unittest.main()
