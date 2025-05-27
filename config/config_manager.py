#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gerenciador de Configurau00e7u00f5es do Windsurf GCS Connector

Este mu00f3dulo fornece funcionalidades para carregar e gerenciar as configurau00e7u00f5es
do Windsurf GCS Connector a partir de arquivos YAML.

Classes:
    ConfigManager: Classe principal para gerenciamento de configurau00e7u00f5es.

Exemplo de uso:
    ```python
    # Carregar configurau00e7u00f5es
    config = ConfigManager()
    
    # Obter configurau00e7u00f5es especu00edficas
    bucket_name = config.get('gcs.bucket_name')
    datalake_path = config.get('gcs.paths.datalake')
    ```

Autor: Equipe Windsurf
Versu00e3o: 1.0.0
Data: 27/05/2025
"""

import os
import yaml
import logging
from typing import Any, Dict, List, Optional, Union
from pathlib import Path

# Configurau00e7u00e3o de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('config-manager')


class ConfigManager:
    """
    Classe para gerenciamento de configurau00e7u00f5es do Windsurf GCS Connector.
    
    Esta classe carrega configurau00e7u00f5es de arquivos YAML e fornece mu00e9todos para
    acessar e modificar essas configurau00e7u00f5es de forma segura e estruturada.
    
    Atributos:
        config (Dict): Dicionu00e1rio contendo todas as configurau00e7u00f5es carregadas.
        config_file (Path): Caminho para o arquivo de configurau00e7u00e3o principal.
    """

    def __init__(self, config_file: Optional[str] = None):
        """
        Inicializa o gerenciador de configurau00e7u00f5es.
        
        Args:
            config_file (Optional[str]): Caminho para o arquivo de configurau00e7u00e3o YAML.
                                        Se nu00e3o fornecido, usa o arquivo padru00e3o 'config/config.yaml'.
        
        Raises:
            FileNotFoundError: Se o arquivo de configurau00e7u00e3o nu00e3o for encontrado.
            yaml.YAMLError: Se o arquivo YAML estiver mal formatado.
        """
        # Definir caminho do arquivo de configurau00e7u00e3o
        if config_file is None:
            # Obter o diretu00f3rio do mu00f3dulo atual
            module_dir = Path(__file__).parent
            self.config_file = module_dir / 'config.yaml'
        else:
            self.config_file = Path(config_file)
        
        # Inicializar configurau00e7u00f5es vazias
        self.config = {}
        
        # Carregar configurau00e7u00f5es
        self._load_config()
        
        logger.info(f"Configurau00e7u00f5es carregadas de {self.config_file}")

    def _load_config(self) -> None:
        """
        Carrega as configurau00e7u00f5es do arquivo YAML.
        
        Raises:
            FileNotFoundError: Se o arquivo de configurau00e7u00e3o nu00e3o for encontrado.
            yaml.YAMLError: Se o arquivo YAML estiver mal formatado.
        """
        try:
            with open(self.config_file, 'r', encoding='utf-8') as file:
                self.config = yaml.safe_load(file)
        except FileNotFoundError:
            logger.error(f"Arquivo de configurau00e7u00e3o nu00e3o encontrado: {self.config_file}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Erro ao analisar o arquivo YAML: {e}")
            raise

    def get(self, path: str, default: Any = None) -> Any:
        """
        Obtu00e9m um valor de configurau00e7u00e3o a partir de um caminho de acesso.
        
        Args:
            path (str): Caminho de acesso u00e0 configurau00e7u00e3o, usando notau00e7u00e3o de ponto.
                       Exemplo: 'gcs.bucket_name' para acessar config['gcs']['bucket_name'].
            default (Any): Valor padru00e3o a ser retornado se o caminho nu00e3o existir.
        
        Returns:
            Any: Valor da configurau00e7u00e3o ou o valor padru00e3o se nu00e3o encontrado.
            
        Exemplo:
            ```python
            # Obter o nome do bucket
            bucket_name = config.get('gcs.bucket_name', 'bucket-padrao')
            ```
        """
        if not path:
            return default
        
        # Dividir o caminho em partes
        parts = path.split('.')
        
        # Navegar pela estrutura de configurau00e7u00e3o
        current = self.config
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
        
        return current

    def set(self, path: str, value: Any) -> None:
        """
        Define um valor de configurau00e7u00e3o em um caminho especu00edfico.
        
        Args:
            path (str): Caminho de acesso u00e0 configurau00e7u00e3o, usando notau00e7u00e3o de ponto.
            value (Any): Valor a ser definido.
            
        Raises:
            ValueError: Se o caminho for invu00e1lido ou se uma parte intermediu00e1ria nu00e3o for um dicionu00e1rio.
            
        Exemplo:
            ```python
            # Definir o nome do bucket
            config.set('gcs.bucket_name', 'novo-bucket')
            ```
        """
        if not path:
            raise ValueError("Caminho de configurau00e7u00e3o nu00e3o pode ser vazio")
        
        # Dividir o caminho em partes
        parts = path.split('.')
        
        # Navegar pela estrutura de configurau00e7u00e3o atu00e9 o penu00faltimo nu00edvel
        current = self.config
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                raise ValueError(f"Nu00e3o u00e9 possu00edvel definir '{path}' porque '{'.'.join(parts[:i+1])}' nu00e3o u00e9 um dicionu00e1rio")
            
            current = current[part]
        
        # Definir o valor no u00faltimo nu00edvel
        current[parts[-1]] = value
        logger.debug(f"Configurau00e7u00e3o '{path}' definida como '{value}'")

    def save(self, config_file: Optional[str] = None) -> None:
        """
        Salva as configurau00e7u00f5es atuais em um arquivo YAML.
        
        Args:
            config_file (Optional[str]): Caminho para o arquivo de sau00edda.
                                        Se nu00e3o fornecido, usa o arquivo original.
                                        
        Raises:
            PermissionError: Se nu00e3o tiver permissu00e3o para escrever no arquivo.
            yaml.YAMLError: Se ocorrer um erro ao serializar as configurau00e7u00f5es para YAML.
        """
        output_file = Path(config_file) if config_file else self.config_file
        
        try:
            with open(output_file, 'w', encoding='utf-8') as file:
                yaml.dump(self.config, file, default_flow_style=False, sort_keys=False, allow_unicode=True)
            
            logger.info(f"Configurau00e7u00f5es salvas em {output_file}")
        except PermissionError:
            logger.error(f"Sem permissu00e3o para escrever em {output_file}")
            raise
        except Exception as e:
            logger.error(f"Erro ao salvar configurau00e7u00f5es: {e}")
            raise

    def get_all(self) -> Dict[str, Any]:
        """
        Retorna todas as configurau00e7u00f5es como um dicionu00e1rio.
        
        Returns:
            Dict[str, Any]: Dicionu00e1rio contendo todas as configurau00e7u00f5es.
        """
        return self.config.copy()

    def update(self, new_config: Dict[str, Any], overwrite: bool = True) -> None:
        """
        Atualiza as configurau00e7u00f5es com um novo dicionu00e1rio.
        
        Args:
            new_config (Dict[str, Any]): Dicionu00e1rio com novas configurau00e7u00f5es.
            overwrite (bool): Se True, substitui valores existentes; se False, mantu00e9m os valores originais.
            
        Exemplo:
            ```python
            # Atualizar vu00e1rias configurau00e7u00f5es de uma vez
            config.update({
                'gcs': {
                    'bucket_name': 'novo-bucket',
                    'paths': {
                        'datalake': 'novo-caminho/'
                    }
                }
            })
            ```
        """
        self._update_recursive(self.config, new_config, overwrite)
        logger.info("Configurau00e7u00f5es atualizadas")

    def _update_recursive(self, target: Dict[str, Any], source: Dict[str, Any], overwrite: bool) -> None:
        """
        Atualiza um dicionu00e1rio de forma recursiva.
        
        Args:
            target (Dict[str, Any]): Dicionu00e1rio alvo a ser atualizado.
            source (Dict[str, Any]): Dicionu00e1rio fonte com novos valores.
            overwrite (bool): Se True, substitui valores existentes; se False, mantu00e9m os valores originais.
        """
        for key, value in source.items():
            # Se a chave nu00e3o existe no alvo ou devemos sobrescrever
            if key not in target or not isinstance(value, dict):
                if key not in target or overwrite:
                    target[key] = value
            # Se ambos su00e3o dicionu00e1rios, atualiza recursivamente
            elif isinstance(target[key], dict) and isinstance(value, dict):
                self._update_recursive(target[key], value, overwrite)

    def reset(self) -> None:
        """
        Recarrega as configurau00e7u00f5es do arquivo original, descartando quaisquer alterau00e7u00f5es nu00e3o salvas.
        """
        self._load_config()
        logger.info("Configurau00e7u00f5es recarregadas do arquivo original")


# Exemplo de uso
if __name__ == "__main__":
    # Carregar configurau00e7u00f5es
    config = ConfigManager()
    
    # Exemplo de acesso a configurau00e7u00f5es
    bucket_name = config.get('gcs.bucket_name')
    datalake_path = config.get('gcs.paths.datalake')
    
    print(f"Bucket: {bucket_name}")
    print(f"Caminho do datalake: {datalake_path}")
    
    # Exemplo de modificau00e7u00e3o de configurau00e7u00f5es
    # config.set('gcs.paths.temp', 'datalake/novo-temp/')
    # config.save()
