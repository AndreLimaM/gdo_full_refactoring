#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de gerenciamento de logs para o projeto GDO.

Este módulo fornece funções para configurar e gerenciar logs
de forma padronizada em todo o projeto.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional, Dict, Any

# Importa configurações
sys.path.append('/home/andre/CascadeProjects/gdo_full_refactoring')
from src.config.config import get_config


class LogManager:
    """
    Classe para gerenciar logs do projeto.
    
    Esta classe configura e gerencia logs, permitindo
    configurações personalizadas para diferentes componentes.
    """
    
    def __init__(self):
        """Inicializa o gerenciador de logs."""
        self.default_level = self._get_log_level_from_config()
        self.loggers = {}
        
        # Configura o formato padrão de logs
        self.default_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
    def _get_log_level_from_config(self) -> int:
        """
        Obtém o nível de log da configuração.
        
        Returns:
            Nível de log como constante do módulo logging
        """
        level_str = get_config('logging', 'level', 'INFO')
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        
        return level_map.get(level_str.upper(), logging.INFO)
    
    def get_logger(self, name: str, level: Optional[int] = None, 
                  log_file: Optional[str] = None, 
                  format_str: Optional[str] = None) -> logging.Logger:
        """
        Obtém um logger configurado.
        
        Args:
            name: Nome do logger
            level: Nível de log (opcional)
            log_file: Arquivo de log (opcional)
            format_str: Formato do log (opcional)
            
        Returns:
            Logger configurado
        """
        # Verifica se o logger já existe
        if name in self.loggers:
            return self.loggers[name]
        
        # Cria um novo logger
        logger = logging.getLogger(name)
        
        # Define o nível de log
        logger.setLevel(level if level is not None else self.default_level)
        
        # Remove handlers existentes
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Adiciona handler para console
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(format_str or self.default_format))
        logger.addHandler(console_handler)
        
        # Adiciona handler para arquivo, se especificado
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(logging.Formatter(format_str or self.default_format))
            logger.addHandler(file_handler)
        
        # Armazena o logger para reutilização
        self.loggers[name] = logger
        
        return logger
    
    def configure_job_logger(self, job_name: str, component: str = 'default') -> logging.Logger:
        """
        Configura um logger específico para jobs.
        
        Args:
            job_name: Nome do job
            component: Componente específico dentro do job
            
        Returns:
            Logger configurado para o job
        """
        # Cria nome do logger
        logger_name = f"gdo.job.{job_name}.{component}"
        
        # Define arquivo de log
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_dir = os.path.join('/tmp', 'gdo_logs', job_name)
        log_file = os.path.join(log_dir, f"{component}_{timestamp}.log")
        
        # Obtém o logger configurado
        return self.get_logger(logger_name, log_file=log_file)
    
    def log_job_start(self, logger: logging.Logger, job_name: str, params: Dict[str, Any] = None):
        """
        Registra o início de um job.
        
        Args:
            logger: Logger configurado
            job_name: Nome do job
            params: Parâmetros do job (opcional)
        """
        logger.info(f"Iniciando job: {job_name}")
        if params:
            logger.info(f"Parâmetros: {params}")
    
    def log_job_end(self, logger: logging.Logger, job_name: str, stats: Dict[str, Any] = None):
        """
        Registra o fim de um job.
        
        Args:
            logger: Logger configurado
            job_name: Nome do job
            stats: Estatísticas do job (opcional)
        """
        logger.info(f"Finalizando job: {job_name}")
        if stats:
            logger.info(f"Estatísticas: {stats}")


# Instância global do gerenciador de logs
log_manager = LogManager()


def get_logger(name: str, level: Optional[int] = None, 
              log_file: Optional[str] = None, 
              format_str: Optional[str] = None) -> logging.Logger:
    """
    Função de conveniência para obter um logger configurado.
    
    Args:
        name: Nome do logger
        level: Nível de log (opcional)
        log_file: Arquivo de log (opcional)
        format_str: Formato do log (opcional)
        
    Returns:
        Logger configurado
    """
    return log_manager.get_logger(name, level, log_file, format_str)


def configure_job_logger(job_name: str, component: str = 'default') -> logging.Logger:
    """
    Função de conveniência para configurar um logger de job.
    
    Args:
        job_name: Nome do job
        component: Componente específico dentro do job
        
    Returns:
        Logger configurado para o job
    """
    return log_manager.configure_job_logger(job_name, component)
