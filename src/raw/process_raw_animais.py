#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script para processamento da camada Raw de animais.

Este script lê os arquivos JSON de animais da pasta pending,
processa os dados brutos e os armazena na camada Raw,
mantendo a integridade original dos dados.
Após o processamento, move os arquivos para a pasta done.
"""

import os
import sys
import json
import glob
import shutil
import logging
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

# Adiciona o diretório raiz ao path para importações relativas
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.database.db_connector import CloudSQLConnector
from utils.gcs.gcs_connector import WindsurfGCSConnector
from src.utils.log_manager import LogManager
from config.config_manager import ConfigManager

# Configuração de logging
config = ConfigManager()
logging_format = config.get('logging.format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging_level = config.get('logging.level', 'INFO')
logging.basicConfig(level=getattr(logging, logging_level), format=logging_format)
logger = logging.getLogger('process-raw-animais')

# Tipo de payload
TIPO_PAYLOAD = 'animais'
CAMADA = 'raw'

# Caminhos no bucket GCS
BASE_PATH = "datalake/payload_data"
PENDING_PATH = f"{BASE_PATH}/pending/{TIPO_PAYLOAD}"
DONE_PATH = f"{BASE_PATH}/done/{TIPO_PAYLOAD}"

# Inicializa o gerenciador de logs
log_manager = LogManager(TIPO_PAYLOAD, CAMADA)

# Inicializa os conectores
db_connector = CloudSQLConnector()
gcs_connector = WindsurfGCSConnector()


def validar_json(conteudo: str) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Valida se o conteúdo é um JSON válido.
    
    Args:
        conteudo: Conteúdo do arquivo JSON
        
    Returns:
        Tupla contendo (valido, dados_json, mensagem_erro)
    """
    try:
        dados = json.loads(conteudo)
        # Verifica se os campos obrigatórios estão presentes
        campos_obrigatorios = ['id_animal', 'data_nascimento', 'id_propriedade']
        campos_faltantes = [campo for campo in campos_obrigatorios if campo not in dados]
        
        if campos_faltantes:
            return False, None, f"Campos obrigatórios faltando: {', '.join(campos_faltantes)}"
        
        return True, dados, None
    except json.JSONDecodeError as e:
        return False, None, f"Erro ao decodificar JSON: {str(e)}"
    except Exception as e:
        return False, None, f"Erro inesperado ao validar JSON: {str(e)}"


def inserir_dados_raw(dados: Dict[str, Any], nome_arquivo: str) -> bool:
    """
    Insere os dados brutos na tabela raw_animais.
    
    Args:
        dados: Dados do animal em formato dict
        nome_arquivo: Nome do arquivo de origem
        
    Returns:
        True se a inserção foi bem-sucedida, False caso contrário
    """
    try:
        # Verifica se a tabela raw_animais existe e cria se necessário
        criar_tabela_raw_animais()
        
        # Prepara os dados para inserção
        dados_json = json.dumps(dados)
        data_processamento = datetime.now()
        
        # SQL para inserir os dados
        sql = """
        INSERT INTO raw_animais (
            id_animal, data_nascimento, id_propriedade, sexo, raca, 
            peso_nascimento, data_entrada, data_saida, status, 
            dados_completos, nome_arquivo, data_processamento
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s
        ) RETURNING id;
        """
        
        # Parâmetros para a query
        params = (
            dados.get('id_animal'),
            dados.get('data_nascimento'),
            dados.get('id_propriedade'),
            dados.get('sexo'),
            dados.get('raca'),
            dados.get('peso_nascimento'),
            dados.get('data_entrada'),
            dados.get('data_saida'),
            dados.get('status'),
            dados_json,
            nome_arquivo,
            data_processamento
        )
        
        # Executa a query
        result = db_connector.execute_query(sql, params)
        
        if result:
            logger.info(f"Dados do animal {dados.get('id_animal')} inseridos com sucesso")
            return True
        else:
            logger.error(f"Falha ao inserir dados do animal {dados.get('id_animal')}")
            return False
            
    except Exception as e:
        logger.error(f"Erro ao inserir dados na tabela raw_animais: {str(e)}")
        log_manager.registrar_erro(
            mensagem=f"Erro ao inserir dados na tabela raw_animais",
            nome_arquivo=nome_arquivo,
            exception=e
        )
        return False


def criar_tabela_raw_animais() -> None:
    """
    Cria a tabela raw_animais se ela não existir.
    """
    try:
        sql = """
        CREATE TABLE IF NOT EXISTS raw_animais (
            id SERIAL PRIMARY KEY,
            id_animal VARCHAR(50),
            data_nascimento DATE,
            id_propriedade VARCHAR(50),
            sexo VARCHAR(10),
            raca VARCHAR(50),
            peso_nascimento DECIMAL(10, 2),
            data_entrada DATE,
            data_saida DATE,
            status VARCHAR(20),
            dados_completos JSONB,
            nome_arquivo VARCHAR(255),
            data_processamento TIMESTAMP WITH TIME ZONE
        );
        
        -- Índices para otimizar consultas
        CREATE INDEX IF NOT EXISTS idx_raw_animais_id_animal ON raw_animais(id_animal);
        CREATE INDEX IF NOT EXISTS idx_raw_animais_data_processamento ON raw_animais(data_processamento);
        """
        
        db_connector.execute_query(sql)
        logger.info("Tabela raw_animais verificada/criada com sucesso")
    except Exception as e:
        logger.error(f"Erro ao criar tabela raw_animais: {str(e)}")
        raise


def processar_arquivo(nome_arquivo: str) -> Tuple[bool, int, int]:
    """
    Processa um arquivo JSON de animais.
    
    Args:
        nome_arquivo: Nome do arquivo a ser processado
        
    Returns:
        Tupla contendo (sucesso, registros_processados, registros_invalidos)
    """
    try:
        # Registra o início do processamento
        log_manager.iniciar_processamento(nome_arquivo)
        
        # Lê o conteúdo do arquivo
        conteudo = gcs_connector.read_file(f"{PENDING_PATH}/{nome_arquivo}")
        if not conteudo:
            log_manager.registrar_erro(
                mensagem=f"Arquivo vazio ou não encontrado",
                nome_arquivo=nome_arquivo
            )
            return False, 0, 0
        
        # Valida o JSON
        valido, dados, mensagem_erro = validar_json(conteudo)
        if not valido:
            log_manager.registrar_erro(
                mensagem=f"JSON inválido: {mensagem_erro}",
                nome_arquivo=nome_arquivo
            )
            return False, 0, 1
        
        # Insere os dados na tabela raw
        sucesso = inserir_dados_raw(dados, nome_arquivo)
        if not sucesso:
            return False, 0, 1
        
        # Move o arquivo para a pasta 'done'
        gcs_connector.move_file(
            f"{PENDING_PATH}/{nome_arquivo}",
            f"{DONE_PATH}/{nome_arquivo}"
        )
        
        # Registra o sucesso
        log_manager.registrar_sucesso(
            mensagem=f"Arquivo processado com sucesso",
            nome_arquivo=nome_arquivo
        )
        
        return True, 1, 0
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {nome_arquivo}: {str(e)}")
        log_manager.registrar_erro(
            mensagem=f"Erro ao processar arquivo",
            nome_arquivo=nome_arquivo,
            exception=e
        )
        return False, 0, 1


def processar_arquivos_pendentes() -> Tuple[int, int]:
    """
    Processa todos os arquivos JSON pendentes na pasta de animais.
    
    Returns:
        Tupla contendo (registros_processados, registros_invalidos)
    """
    # Verifica se as pastas existem
    gcs_connector.create_folder_if_not_exists(PENDING_PATH)
    gcs_connector.create_folder_if_not_exists(DONE_PATH)
    
    # Lista os arquivos pendentes
    arquivos = gcs_connector.list_files(PENDING_PATH)
    arquivos_json = [arquivo for arquivo in arquivos if arquivo.lower().endswith('.json')]
    
    logger.info(f"Encontrados {len(arquivos_json)} arquivos JSON pendentes")
    
    # Processa cada arquivo
    registros_processados = 0
    registros_invalidos = 0
    
    for arquivo in arquivos_json:
        sucesso, proc, inv = processar_arquivo(arquivo)
        registros_processados += proc
        registros_invalidos += inv
    
    return registros_processados, registros_invalidos


def main():
    """
    Função principal de processamento.
    """
    logger.info(f"Iniciando processamento de {TIPO_PAYLOAD} na camada {CAMADA}...")
    
    inicio = time.time()
    
    try:
        # Processa os arquivos pendentes
        registros_processados, registros_invalidos = processar_arquivos_pendentes()
        
        # Calcula a duração
        fim = time.time()
        duracao_ms = int((fim - inicio) * 1000)
        
        # Registra a finalização
        log_manager.finalizar_processamento(
            duracao_ms=duracao_ms,
            registros_processados=registros_processados,
            registros_invalidos=registros_invalidos
        )
        
        logger.info(f"Processamento finalizado em {duracao_ms}ms")
        logger.info(f"Registros processados: {registros_processados}")
        logger.info(f"Registros inválidos: {registros_invalidos}")
        
    except Exception as e:
        logger.error(f"Erro no processamento principal: {str(e)}")
        log_manager.registrar_erro(
            mensagem=f"Erro no processamento principal",
            exception=e
        )


if __name__ == "__main__":
    main()
