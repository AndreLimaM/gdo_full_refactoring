#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerenciador de logs para o processamento de payloads.

Esta classe fornece mu00e9todos para registrar logs de processamento no banco de dados,
facilitando a observabilidade e o monitoramento do sistema.
"""

import os
import json
import socket
import logging
import time
from datetime import datetime
from typing import Dict, Any, Optional, Union

from utils.database.db_connector import CloudSQLConnector

# Configurau00e7u00e3o de logging padru00e3o
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class LogManager:
    """
    Gerenciador de logs para o processamento de payloads.
    """
    
    # Versu00e3o do script de processamento
    VERSION = '1.0.0'
    
    # Status possu00edveis para os logs
    STATUS_SUCESSO = 'sucesso'
    STATUS_ERRO = 'erro'
    STATUS_ALERTA = 'alerta'
    STATUS_INICIADO = 'iniciado'
    STATUS_FINALIZADO = 'finalizado'
    
    def __init__(self, tipo_payload: str, camada: str):
        """
        Inicializa o gerenciador de logs.
        
        Args:
            tipo_payload: Tipo de payload (animais, caixas, etc.)
            camada: Camada de processamento (raw, trusted, service)
        """
        self.logger = logging.getLogger(f'log-manager-{tipo_payload}-{camada}')
        self.db_connector = CloudSQLConnector()
        self.tipo_payload = tipo_payload
        self.camada = camada
        self.hostname = socket.gethostname()
        self.usuario = os.environ.get('USER', 'desconhecido')
        
        # Verifica se a tabela de log existe
        self._verificar_tabela_log()
    
    def _verificar_tabela_log(self) -> None:
        """
        Verifica se a tabela de log existe e a cria se necessu00e1rio.
        """
        try:
            # Carrega o script SQL para criar a tabela
            script_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'sql', 'create_log_table.sql')
            with open(script_path, 'r', encoding='utf-8') as f:
                sql_script = f.read()
            
            # Executa o script SQL
            self.db_connector.execute_query(sql_script)
            self.logger.info("Tabela de log verificada/criada com sucesso")
        except Exception as e:
            self.logger.error(f"Erro ao verificar/criar tabela de log: {str(e)}")
            raise
    
    def registrar_log(self, 
                      status: str, 
                      mensagem: str, 
                      nome_arquivo: Optional[str] = None,
                      detalhes: Optional[Dict[str, Any]] = None,
                      duracao_ms: Optional[int] = None,
                      registros_processados: Optional[int] = None,
                      registros_invalidos: Optional[int] = None) -> int:
        """
        Registra um log no banco de dados.
        
        Args:
            status: Status do processamento (sucesso, erro, alerta, iniciado, finalizado)
            mensagem: Mensagem descritiva do log
            nome_arquivo: Nome do arquivo processado (opcional)
            detalhes: Detalhes adicionais em formato dict (opcional)
            duracao_ms: Durau00e7u00e3o do processamento em milissegundos (opcional)
            registros_processados: Quantidade de registros processados (opcional)
            registros_invalidos: Quantidade de registros invu00e1lidos (opcional)
            
        Returns:
            ID do registro de log criado
        """
        try:
            # Prepara os detalhes em formato JSON
            detalhes_json = json.dumps(detalhes) if detalhes else None
            
            # SQL para inserir o log
            sql = """
            INSERT INTO log_processamento (
                tipo_payload, camada, nome_arquivo, status, mensagem, 
                detalhes, duracao_ms, registros_processados, registros_invalidos,
                usuario, hostname, versao_script
            ) VALUES (
                %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s
            ) RETURNING id;
            """
            
            # Paru00e2metros para a query
            params = (
                self.tipo_payload,
                self.camada,
                nome_arquivo,
                status,
                mensagem,
                detalhes_json,
                duracao_ms,
                registros_processados,
                registros_invalidos,
                self.usuario,
                self.hostname,
                self.VERSION
            )
            
            # Executa a query
            result = self.db_connector.execute_query(sql, params)
            log_id = result[0]['id'] if result else None
            
            # Loga tambu00e9m no console
            log_method = self.logger.error if status == self.STATUS_ERRO else self.logger.info
            log_method(f"{status.upper()}: {mensagem} - ID: {log_id}")
            
            return log_id
        
        except Exception as e:
            self.logger.error(f"Erro ao registrar log no banco de dados: {str(e)}")
            # Em caso de erro ao registrar o log, pelo menos registramos no console
            return -1
    
    def iniciar_processamento(self, nome_arquivo: Optional[str] = None) -> int:
        """
        Registra o inu00edcio de um processamento.
        
        Args:
            nome_arquivo: Nome do arquivo sendo processado (opcional)
            
        Returns:
            ID do registro de log criado
        """
        return self.registrar_log(
            status=self.STATUS_INICIADO,
            mensagem=f"Iniciando processamento de {self.tipo_payload} na camada {self.camada}",
            nome_arquivo=nome_arquivo
        )
    
    def finalizar_processamento(self, 
                               duracao_ms: int, 
                               registros_processados: int,
                               registros_invalidos: int = 0,
                               nome_arquivo: Optional[str] = None) -> int:
        """
        Registra a finalizau00e7u00e3o de um processamento.
        
        Args:
            duracao_ms: Durau00e7u00e3o do processamento em milissegundos
            registros_processados: Quantidade de registros processados
            registros_invalidos: Quantidade de registros invu00e1lidos (opcional)
            nome_arquivo: Nome do arquivo processado (opcional)
            
        Returns:
            ID do registro de log criado
        """
        return self.registrar_log(
            status=self.STATUS_FINALIZADO,
            mensagem=f"Processamento de {self.tipo_payload} na camada {self.camada} finalizado",
            nome_arquivo=nome_arquivo,
            duracao_ms=duracao_ms,
            registros_processados=registros_processados,
            registros_invalidos=registros_invalidos
        )
    
    def registrar_sucesso(self, 
                         mensagem: str, 
                         nome_arquivo: Optional[str] = None,
                         detalhes: Optional[Dict[str, Any]] = None) -> int:
        """
        Registra um sucesso no processamento.
        
        Args:
            mensagem: Mensagem descritiva do sucesso
            nome_arquivo: Nome do arquivo processado (opcional)
            detalhes: Detalhes adicionais em formato dict (opcional)
            
        Returns:
            ID do registro de log criado
        """
        return self.registrar_log(
            status=self.STATUS_SUCESSO,
            mensagem=mensagem,
            nome_arquivo=nome_arquivo,
            detalhes=detalhes
        )
    
    def registrar_erro(self, 
                      mensagem: str, 
                      nome_arquivo: Optional[str] = None,
                      detalhes: Optional[Dict[str, Any]] = None,
                      exception: Optional[Exception] = None) -> int:
        """
        Registra um erro no processamento.
        
        Args:
            mensagem: Mensagem descritiva do erro
            nome_arquivo: Nome do arquivo processado (opcional)
            detalhes: Detalhes adicionais em formato dict (opcional)
            exception: Exceu00e7u00e3o que causou o erro (opcional)
            
        Returns:
            ID do registro de log criado
        """
        if exception and not detalhes:
            detalhes = {
                'exception_type': type(exception).__name__,
                'exception_message': str(exception),
            }
        elif exception and detalhes:
            detalhes.update({
                'exception_type': type(exception).__name__,
                'exception_message': str(exception),
            })
            
        return self.registrar_log(
            status=self.STATUS_ERRO,
            mensagem=mensagem,
            nome_arquivo=nome_arquivo,
            detalhes=detalhes
        )
    
    def registrar_alerta(self, 
                        mensagem: str, 
                        nome_arquivo: Optional[str] = None,
                        detalhes: Optional[Dict[str, Any]] = None) -> int:
        """
        Registra um alerta no processamento.
        
        Args:
            mensagem: Mensagem descritiva do alerta
            nome_arquivo: Nome do arquivo processado (opcional)
            detalhes: Detalhes adicionais em formato dict (opcional)
            
        Returns:
            ID do registro de log criado
        """
        return self.registrar_log(
            status=self.STATUS_ALERTA,
            mensagem=mensagem,
            nome_arquivo=nome_arquivo,
            detalhes=detalhes
        )
    
    def medir_tempo(self, func):
        """
        Decorador para medir o tempo de execuu00e7u00e3o de uma funu00e7u00e3o.
        
        Args:
            func: Funu00e7u00e3o a ser medida
            
        Returns:
            Funu00e7u00e3o decorada
        """
        def wrapper(*args, **kwargs):
            inicio = time.time()
            try:
                return func(*args, **kwargs)
            finally:
                fim = time.time()
                duracao_ms = int((fim - inicio) * 1000)
                self.logger.info(f"Funu00e7u00e3o {func.__name__} executada em {duracao_ms}ms")
        return wrapper
