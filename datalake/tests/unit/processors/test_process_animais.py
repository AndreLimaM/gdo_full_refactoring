#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Testes unitários para o processador de animais.

Este módulo contém testes para verificar o funcionamento
correto do processador de dados de animais.
"""

import os
import sys
import json
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime

# Adiciona o diretório raiz ao path para importação de módulos
sys.path.append('/home/andre/CascadeProjects/gdo_full_refactoring')

# Importa o módulo a ser testado
from src.processors.animais.process_animais import AnimalProcessor, process_directory


class TestAnimalProcessor(unittest.TestCase):
    """Classe de teste para o processador de animais."""
    
    def setUp(self):
        """Configura o ambiente de teste."""
        # Mock da sessão Spark
        self.mock_spark = MagicMock()
        self.mock_df = MagicMock()
        self.mock_spark.createDataFrame.return_value = self.mock_df
        
        # Configuração do banco de dados para teste
        self.db_config = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }
        
        # Cria o processador
        self.processor = AnimalProcessor(self.mock_spark, self.db_config)
        
        # Dados de exemplo para teste
        self.sample_data = {
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
                    "grau_sangue": None,
                    "peso_atual": 5.83,
                    "data_ultima_movimentacao": "2024-09-23T03:00:00",
                    "data_entrada_lote": "2024-09-20T03:00:00",
                    "data_criacao": "2025-05-21T14:49:39",
                    "data_atualizacao": "2025-05-21T14:49:39",
                    "data_exclusao": None
                }
            ]
        }
        
        # Cria um arquivo temporário para teste
        self.test_dir = '/tmp/test_animais'
        self.input_dir = os.path.join(self.test_dir, 'pending')
        self.done_dir = os.path.join(self.test_dir, 'done')
        self.error_dir = os.path.join(self.test_dir, 'error')
        
        # Cria os diretórios
        for directory in [self.input_dir, self.done_dir, self.error_dir]:
            os.makedirs(directory, exist_ok=True)
        
        # Cria um arquivo de teste
        self.test_file = os.path.join(self.input_dir, 'test_animal.json')
        with open(self.test_file, 'w') as f:
            json.dump(self.sample_data, f)
    
    def tearDown(self):
        """Limpa o ambiente após os testes."""
        # Remove os arquivos e diretórios de teste
        for directory in [self.input_dir, self.done_dir, self.error_dir]:
            for file in os.listdir(directory):
                os.remove(os.path.join(directory, file))
            os.rmdir(directory)
        os.rmdir(self.test_dir)
    
    def test_define_schema(self):
        """Testa a definição do schema."""
        schema = self.processor._define_schema()
        self.assertEqual(len(schema.fields), 33)  # Verifica o número de campos
        
        # Verifica alguns campos específicos
        field_names = [field.name for field in schema.fields]
        self.assertIn('id', field_names)
        self.assertIn('codigo_fazenda', field_names)
        self.assertIn('data_nascimento', field_names)
        self.assertIn('peso_atual', field_names)
    
    def test_parse_date(self):
        """Testa a função de parsing de datas."""
        # Testa data válida
        date_str = "2024-09-20T11:37:00Z"
        parsed_date = self.processor._parse_date(date_str)
        self.assertIsInstance(parsed_date, datetime)
        self.assertEqual(parsed_date.year, 2024)
        self.assertEqual(parsed_date.month, 9)
        self.assertEqual(parsed_date.day, 20)
        
        # Testa data inválida
        invalid_date = "not-a-date"
        parsed_invalid = self.processor._parse_date(invalid_date)
        self.assertIsNone(parsed_invalid)
        
        # Testa data nula
        null_date = None
        parsed_null = self.processor._parse_date(null_date)
        self.assertIsNone(parsed_null)
    
    def test_transform_data(self):
        """Testa a transformação de dados."""
        transformed = self.processor._transform_data(self.sample_data)
        
        # Verifica se a transformação retornou uma lista
        self.assertIsInstance(transformed, list)
        self.assertEqual(len(transformed), 1)
        
        # Verifica alguns campos específicos
        animal = transformed[0]
        self.assertEqual(animal['id'], "123456789")
        self.assertEqual(animal['codigo_fazenda'], "24ad9d")
        self.assertEqual(animal['tipo_animal'], 1)
        
        # Verifica campos JSON
        self.assertIsInstance(animal['classificacao'], str)
        self.assertIn("N/A", animal['classificacao'])
    
    @patch('src.processors.animais.process_animais.logger')
    def test_process_file_success(self, mock_logger):
        """Testa o processamento de arquivo com sucesso."""
        # Configura o mock
        self.mock_df.write.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.mode.return_value.save.return_value = None
        
        # Executa o processamento
        success, error = self.processor.process_file(self.test_file)
        
        # Verifica o resultado
        self.assertTrue(success)
        self.assertIsNone(error)
        
        # Verifica se os métodos foram chamados
        self.mock_spark.createDataFrame.assert_called_once()
        mock_logger.info.assert_called()
    
    @patch('src.processors.animais.process_animais.logger')
    def test_process_file_error(self, mock_logger):
        """Testa o processamento de arquivo com erro."""
        # Configura o mock para lançar exceção
        self.mock_spark.createDataFrame.side_effect = Exception("Erro de teste")
        
        # Executa o processamento
        success, error = self.processor.process_file(self.test_file)
        
        # Verifica o resultado
        self.assertFalse(success)
        self.assertIsNotNone(error)
        self.assertIn("Erro de teste", error)
        
        # Verifica se o logger foi chamado com erro
        mock_logger.error.assert_called()
    
    @patch('src.processors.animais.process_animais.logger')
    @patch('os.rename')
    def test_process_directory(self, mock_rename, mock_logger):
        """Testa o processamento de diretório."""
        # Configura o mock
        self.mock_df.write.format.return_value.option.return_value.option.return_value.option.return_value.option.return_value.mode.return_value.save.return_value = None
        
        # Executa o processamento
        stats = process_directory(
            self.mock_spark,
            self.input_dir,
            self.done_dir,
            self.error_dir,
            self.db_config
        )
        
        # Verifica o resultado
        self.assertEqual(stats['total'], 1)
        self.assertEqual(stats['success'], 1)
        self.assertEqual(stats['error'], 0)
        
        # Verifica se os métodos foram chamados
        mock_rename.assert_called_once()
        mock_logger.info.assert_called()


if __name__ == '__main__':
    unittest.main()
