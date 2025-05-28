#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para testar o processamento da camada Raw de animais.

Este script simula o fluxo completo de processamento de arquivos JSON de animais,
incluindo a leitura dos arquivos, validação, inserção no banco de dados e
movimentação para a pasta 'done'.
"""

import os
import sys
import json
import shutil
import logging
from pathlib import Path

# Adiciona o diretório raiz ao path para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importa o script de processamento
from src.raw.process_raw_animais import processar_arquivo, criar_tabela_raw_animais
from utils.database.db_connector import CloudSQLConnector

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test-process-raw-animais')

# Diretórios para teste
DIR_TESTE = Path(__file__).parent / 'data' / 'animais'
DIR_PENDING = DIR_TESTE / 'pending'
DIR_DONE = DIR_TESTE / 'done'


def preparar_ambiente():
    """
    Prepara o ambiente de teste, criando as pastas necessárias e
    copiando os arquivos de teste para a pasta 'pending'.
    """
    # Cria as pastas se não existirem
    os.makedirs(DIR_PENDING, exist_ok=True)
    os.makedirs(DIR_DONE, exist_ok=True)
    
    # Copia os arquivos de teste para a pasta 'pending'
    arquivos_teste = [f for f in os.listdir(DIR_TESTE) if f.endswith('.json')]
    
    for arquivo in arquivos_teste:
        origem = DIR_TESTE / arquivo
        destino = DIR_PENDING / arquivo
        shutil.copy2(origem, destino)
        logger.info(f"Arquivo {arquivo} copiado para a pasta 'pending'")
    
    return len(arquivos_teste)


def simular_processamento():
    """
    Simula o processamento dos arquivos na pasta 'pending'.
    """
    # Verifica se a tabela raw_animais existe e cria se necessário
    db_connector = CloudSQLConnector()
    criar_tabela_raw_animais()
    
    # Lista os arquivos na pasta 'pending'
    arquivos = [f for f in os.listdir(DIR_PENDING) if f.endswith('.json')]
    logger.info(f"Encontrados {len(arquivos)} arquivos JSON na pasta 'pending'")
    
    # Processa cada arquivo
    registros_processados = 0
    registros_invalidos = 0
    
    for arquivo in arquivos:
        logger.info(f"Processando arquivo {arquivo}...")
        
        # Lê o conteúdo do arquivo
        with open(DIR_PENDING / arquivo, 'r', encoding='utf-8') as f:
            conteudo = f.read()
        
        # Processa o arquivo (simulando o processamento real)
        try:
            # Valida o JSON
            dados = json.loads(conteudo)
            
            # Verifica campos obrigatórios
            campos_obrigatorios = ['id_animal', 'data_nascimento', 'id_propriedade']
            campos_faltantes = [campo for campo in campos_obrigatorios if campo not in dados]
            
            if campos_faltantes:
                logger.error(f"Arquivo {arquivo} inválido: campos obrigatórios faltando: {', '.join(campos_faltantes)}")
                registros_invalidos += 1
                continue
            
            # Simula a inserção no banco de dados
            logger.info(f"Simulando inserção do animal {dados['id_animal']} no banco de dados...")
            
            # Move o arquivo para a pasta 'done'
            shutil.move(DIR_PENDING / arquivo, DIR_DONE / arquivo)
            logger.info(f"Arquivo {arquivo} movido para a pasta 'done'")
            
            registros_processados += 1
            
        except json.JSONDecodeError as e:
            logger.error(f"Arquivo {arquivo} inválido: {str(e)}")
            registros_invalidos += 1
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {arquivo}: {str(e)}")
            registros_invalidos += 1
    
    return registros_processados, registros_invalidos


def main():
    """
    Função principal de teste.
    """
    logger.info("Iniciando teste de processamento de animais...")
    
    try:
        # Prepara o ambiente de teste
        num_arquivos = preparar_ambiente()
        logger.info(f"Ambiente preparado com {num_arquivos} arquivos de teste")
        
        # Simula o processamento
        registros_processados, registros_invalidos = simular_processamento()
        
        # Exibe resultados
        logger.info("Teste finalizado!")
        logger.info(f"Registros processados: {registros_processados}")
        logger.info(f"Registros inválidos: {registros_invalidos}")
        
    except Exception as e:
        logger.error(f"Erro no teste: {str(e)}")


if __name__ == "__main__":
    main()
