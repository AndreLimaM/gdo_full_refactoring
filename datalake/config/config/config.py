#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Módulo de configuração central para o projeto GDO.

Este módulo fornece acesso às configurações do projeto,
carregando-as de variáveis de ambiente ou de arquivos de configuração.
"""

import os
import json
from typing import Dict, Any, Optional


class Config:
    """
    Classe para gerenciar configurações do projeto.
    
    Esta classe carrega configurações de variáveis de ambiente
    e/ou arquivos de configuração JSON.
    """
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Inicializa o gerenciador de configurações.
        
        Args:
            config_file: Caminho opcional para um arquivo de configuração JSON
        """
        self._config = {}
        
        # Carrega configurações de variáveis de ambiente
        self._load_from_env()
        
        # Carrega configurações de arquivo JSON, se fornecido
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
    
    def _load_from_env(self):
        """Carrega configurações de variáveis de ambiente."""
        # Google Cloud
        self._config['gcp'] = {
            'project_id': os.environ.get('GCP_PROJECT_ID', 'development-439017'),
            'region': os.environ.get('GCP_REGION', 'us-central1'),
            'zone': os.environ.get('GCP_ZONE', 'us-central1-a'),
            'bucket_name': os.environ.get('GCS_BUCKET_NAME', 'repo-dev-gdo-carga')
        }
        
        # Dataproc
        self._config['dataproc'] = {
            'image_version': os.environ.get('DATAPROC_IMAGE_VERSION', '2.0-debian10'),
            'master_machine_type': os.environ.get('MASTER_MACHINE_TYPE', 'n1-standard-4'),
            'worker_machine_type': os.environ.get('WORKER_MACHINE_TYPE', 'n1-standard-4'),
            'num_workers': int(os.environ.get('NUM_WORKERS', '2')),
            'master_boot_disk_size': int(os.environ.get('MASTER_BOOT_DISK_SIZE', '500')),
            'worker_boot_disk_size': int(os.environ.get('WORKER_BOOT_DISK_SIZE', '500')),
            'auto_delete_cluster': os.environ.get('AUTO_DELETE_CLUSTER', 'true').lower() == 'true',
            'max_idle_time': os.environ.get('MAX_IDLE_TIME', '30m')
        }
        
        # Cloud SQL
        self._config['database'] = {
            'host': os.environ.get('DB_HOST', '10.98.169.3'),
            'name': os.environ.get('DB_NAME', 'db_eco_tcbf_25'),
            'user': os.environ.get('DB_USER', 'db_eco_tcbf_25_user'),
            'password': os.environ.get('DB_PASSWORD', '5HN33PHKjXcLTz3tBC')
        }
        
        # Diretórios GCS
        bucket = self._config['gcp']['bucket_name']
        base_dir = f"gs://{bucket}"
        
        self._config['directories'] = {
            'base_dir': base_dir,
            'scripts_dir': f"{base_dir}/scripts",
            'init_scripts_dir': f"{base_dir}/scripts/init",
            'jobs_dir': f"{base_dir}/scripts/jobs",
            'data_dir': f"{base_dir}/data",
            'raw_dir': f"{base_dir}/data/raw",
            'processed_dir': f"{base_dir}/data/processed",
            'curated_dir': f"{base_dir}/data/curated"
        }
        
        # Diretórios específicos para animais
        self._config['animais'] = {
            'pending_dir': os.environ.get('ANIMAIS_PENDING_DIR', f"{self._config['directories']['raw_dir']}/animais/pending"),
            'done_dir': os.environ.get('ANIMAIS_DONE_DIR', f"{self._config['directories']['raw_dir']}/animais/done"),
            'error_dir': os.environ.get('ANIMAIS_ERROR_DIR', f"{self._config['directories']['raw_dir']}/animais/error")
        }
        
        # Dependências
        driver_jar = os.environ.get('POSTGRESQL_DRIVER_JAR', 'postgresql-42.2.23.jar')
        self._config['dependencies'] = {
            'postgresql_driver_jar': driver_jar,
            'postgresql_driver_path': os.environ.get('POSTGRESQL_DRIVER_PATH', f"/usr/lib/spark/jars/{driver_jar}"),
            'postgresql_driver_gcs': os.environ.get('POSTGRESQL_DRIVER_GCS', f"{base_dir}/jars/{driver_jar}")
        }
        
        # Logging
        self._config['logging'] = {
            'level': os.environ.get('LOG_LEVEL', 'INFO')
        }
    
    def _load_from_file(self, config_file: str):
        """
        Carrega configurações de um arquivo JSON.
        
        Args:
            config_file: Caminho para o arquivo de configuração JSON
        """
        try:
            with open(config_file, 'r') as f:
                file_config = json.load(f)
                
            # Atualiza as configurações com os valores do arquivo
            for section, values in file_config.items():
                if section not in self._config:
                    self._config[section] = {}
                    
                if isinstance(values, dict):
                    self._config[section].update(values)
                else:
                    self._config[section] = values
                    
        except (json.JSONDecodeError, IOError) as e:
            print(f"Erro ao carregar arquivo de configuração: {str(e)}")
    
    def get(self, section: str, key: Optional[str] = None, default: Any = None) -> Any:
        """
        Obtém um valor de configuração.
        
        Args:
            section: Seção da configuração
            key: Chave específica dentro da seção (opcional)
            default: Valor padrão se a configuração não existir
            
        Returns:
            Valor da configuração ou o valor padrão
        """
        if section not in self._config:
            return default
            
        if key is None:
            return self._config[section]
            
        return self._config[section].get(key, default)
    
    def get_all(self) -> Dict[str, Any]:
        """
        Obtém todas as configurações.
        
        Returns:
            Dicionário com todas as configurações
        """
        return self._config


# Instância global de configuração
config = Config()


def get_config(section: str = None, key: str = None, default: Any = None) -> Any:
    """
    Função de conveniência para acessar configurações.
    
    Args:
        section: Seção da configuração
        key: Chave específica dentro da seção (opcional)
        default: Valor padrão se a configuração não existir
        
    Returns:
        Valor da configuração ou o valor padrão
    """
    if section is None:
        return config.get_all()
        
    return config.get(section, key, default)
