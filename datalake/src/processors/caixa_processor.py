#!/usr/bin/env python3
"""Módulo para processamento de rastreabilidade de caixas."""

from datetime import date
import logging
from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, to_json, struct

from src.processors.base_processor import BaseProcessor


class CaixaProcessor(BaseProcessor):
    """Processador para dados de caixas."""

    def __init__(self, spark: SparkSession, db_options: Dict[str, str]):
        """Inicializa o processador.

        Args:
            spark: Sessão Spark ativa
            db_options: Opções de conexão com o banco de dados
        """
        super().__init__(spark)
        self.db_options = db_options
        self.table_name = "bt_caixas"
        self.logger = logging.getLogger(__name__)

    def _execute_jdbc_query(self, query: str, return_df: bool = False) -> Optional[DataFrame]:
        """Executa uma query via JDBC.

        Args:
            query: Query SQL a ser executada
            return_df: Se True, retorna o DataFrame resultante (para consultas SELECT)
                      Se False, executa a query sem retornar dados (para UPDATE, INSERT, DELETE)

        Returns:
            DataFrame se return_df for True, None caso contrário
        """
        try:
            # Se for uma consulta SELECT (return_df = True)
            if return_df:
                # Cria um dicionário com todas as opções JDBC
                jdbc_options = dict(self.db_options)  # Copia as opções originais
                jdbc_options['dbtable'] = f"({query}) as tmp"  # Adiciona a query como tabela temporária
                
                # Carrega os dados usando as opções JDBC
                result = self.spark.read.format("jdbc").options(**jdbc_options).load()
                return result
            # Se for uma consulta de atualização (UPDATE, INSERT, DELETE)
            else:
                # Usa psycopg2 para executar a query diretamente
                import psycopg2
                import re
                
                # Extrai os parâmetros de conexão da URL JDBC
                jdbc_url = self.db_options['url']
                url_pattern = r'jdbc:postgresql://([^:]+):([^/]+)/(.+)'
                match = re.match(url_pattern, jdbc_url)
                
                if match:
                    host = match.group(1)
                    port = match.group(2)
                    database = match.group(3)
                    
                    conn = psycopg2.connect(
                        host=host,
                        database=database,
                        user=self.db_options['user'],
                        password=self.db_options['password'],
                        port=port
                    )
                conn.autocommit = True  # Ativa autocommit
                cursor = conn.cursor()
                self.logger.info(f"Executando query: {query}")
                cursor.execute(query)
                affected_rows = cursor.rowcount
                cursor.close()
                conn.close()
                self.logger.info(f"Query executada com sucesso. Linhas afetadas: {affected_rows}")
                return None
        except Exception as e:
            self.logger.error(f"Erro ao executar query JDBC: {str(e)}")
            self.logger.error(f"Query: {query}")
            if return_df:
                # Retorna um DataFrame vazio em caso de erro
                return self.spark.createDataFrame([], [])
            raise  # Re-lança a exceção para ser tratada pelo chamador

    def mark_for_processing(
        self,
        token_cliente: str,
        dt_producao_de: date,
        dt_producao_ate: date
    ) -> None:
        """Marca caixas para processamento alterando a flag_processamento para false.
        Apenas caixas que não tenham sido completamente processadas serão marcadas.
        Uma caixa é considerada não processada completamente se qualquer uma das flags
        (flag_desossa, flag_propriedades, flag_animais) for false ou NULL.

        Args:
            token_cliente: Token do cliente
            dt_producao_de: Data inicial
            dt_producao_ate: Data final
        """
        query = f"""
        UPDATE {self.table_name}
        SET updated_at = now(), flag_processamento = false
        WHERE token_cliente = '{token_cliente}'
          AND dt_producao BETWEEN '{dt_producao_de}' AND '{dt_producao_ate}'
          AND (
              flag_desossa = false OR flag_desossa IS NULL
              OR flag_propriedades = false OR flag_propriedades IS NULL
              OR flag_animais = false OR flag_animais IS NULL
          )
        """

        try:
            self.logger.info(f"Marcando caixas para processamento: {token_cliente}, {dt_producao_de} a {dt_producao_ate}")
            result = self._execute_jdbc_query(query)
            self.logger.info(f"Caixas marcadas para processamento com sucesso. Registros afetados: {result}")
        except Exception as e:
            self.logger.error(f"Erro ao marcar caixas para processamento: {str(e)}")
            raise

    def mark_as_processed(
        self,
        caixa_ids: List[str],
        flags: Dict[str, bool]
    ) -> None:
        """Marca caixas como processadas, definindo flag_processamento como true.

        Args:
            caixa_ids: Lista de IDs das caixas
            flags: Dicionário com flags a serem atualizadas
        """
        if 'flag_processamento' in flags:
            del flags['flag_processamento']

        set_clauses = []
        for flag, value in flags.items():
            set_clauses.append(f"{flag} = {str(value).lower()}")
        
        set_clauses.append("flag_processamento = true")
        set_clauses.append("updated_at = now()")
        
        set_clause = ", ".join(set_clauses)
        ids_str = ", ".join([f"'{id}'" for id in caixa_ids])
        
        query = f"""
        UPDATE {self.table_name}
        SET {set_clause}
        WHERE id IN ({ids_str})
        """
        self._execute_jdbc_query(query)

    def update_flag_rastreavel(self, token_cliente: str) -> None:
        """Atualiza a flag_rastreavel para true onde todas as outras flags são true.
        
        Args:
            token_cliente: Token do cliente
        """
        self.logger.info(f"Atualizando flag_rastreavel para token: {token_cliente}")
        
        query = f"""
        UPDATE {self.table_name}
        SET flag_rastreavel = true,
            updated_at = now()
        WHERE token_cliente = '{token_cliente}'
          AND flag_desossa = true
          AND flag_propriedades = true
          AND flag_animais = true
          AND (flag_rastreavel = false OR flag_rastreavel IS NULL)
        """
        
        self._execute_jdbc_query(query)
        self.logger.info("Atualização de flag_rastreavel concluída")

    def load_unprocessed_caixas(
        self,
        token_cliente: str,
        dt_producao_de: date,
        dt_producao_ate: date,
        flags: Optional[List[str]] = None
    ) -> DataFrame:
        """Carrega caixas não processadas no período.
        Uma caixa é considerada não processada se flag_processamento = false.

        Args:
            token_cliente: Token do cliente
            dt_producao_de: Data inicial
            dt_producao_ate: Data final
            flags: Lista de flags para filtrar (flag_desossa, flag_propriedades, etc)

        Returns:
            DataFrame com caixas não processadas (flag_processamento = false)
        """
        # Marca caixas para processamento
        self.mark_for_processing(token_cliente, dt_producao_de, dt_producao_ate)

        # Utiliza o método _load_caixas para carregar todas as caixas não processadas
        self.logger.info("Carregando todas as caixas com flag_processamento = false...")
        
        # Constrói a query para selecionar todas as caixas não processadas
        query = f"""
        SELECT *
        FROM bt_caixas
        WHERE token_cliente = '{token_cliente}'
          AND dt_producao BETWEEN '{dt_producao_de}' AND '{dt_producao_ate}'
          AND flag_processamento = false
        """
        
        # Adiciona filtros adicionais se especificados
        if flags:
            flag_conditions = []
            for flag in flags:
                flag_conditions.append(f"{flag} = false OR {flag} IS NULL")
            if flag_conditions:
                query += f" AND ({' OR '.join(flag_conditions)})"
        
        df = self._execute_jdbc_query(query, return_df=True)
        self.logger.info(f"Carregadas {df.count()} caixas para processamento")
        return df

    def join_with_desossa(
        self,
        df_caixas: DataFrame,
        desossa_processor: 'DesossaProcessor'
    ) -> DataFrame:
        """Junta caixas com dados de desossa.

        Args:
            df_caixas: DataFrame com caixas
            desossa_processor: Processador de desossa

        Returns:
            DataFrame com caixas e dados de desossa
        """
        # Agrupa caixas por unidade e OP de desossa
        df_grupos = df_caixas.select(
            'token_cliente',
            'nr_unidade_desossa',
            'nr_op_desossa'
        ).distinct()

        # Para cada grupo, busca dados de desossa
        df_desossa = None
        for row in df_grupos.collect():
            df_desossa_grupo = desossa_processor.process(
                token_cliente=row.token_cliente,
                nr_unidade_desossa=row.nr_unidade_desossa,
                nr_op_desossa=row.nr_op_desossa
            )

            if df_desossa is None:
                df_desossa = df_desossa_grupo
            else:
                df_desossa = df_desossa.union(df_desossa_grupo)

        # Converte desossa para JSON
        df_desossa_json = desossa_processor.to_json(df_desossa)

        # Junta com caixas
        return df_caixas.join(
            df_desossa_json,
            [
                df_caixas.nr_unidade_desossa == df_desossa.nr_unidade_desossa,
                df_caixas.nr_op_desossa == df_desossa.nr_op_desossa
            ],
            'left'
        )

    def update_desossa_data(self, df_caixas_desossa: DataFrame) -> None:
        """Atualiza caixas com dados de desossa.

        Args:
            df_caixas_desossa: DataFrame com caixas e dados de desossa
        """
        # Prepara dados para atualização
        df_update = df_caixas_desossa.select(
            'id',
            'json_desossa',
            'dt_abate',
            'nr_unidade_abate'
        )

        # Atualiza no banco
        update_query = """
        UPDATE bt_caixas
        SET json_desossa = ?,
            dt_abate = ?,
            nr_unidade_abate = ?,
            flag_desossa = true,
            updated_at = now()
        WHERE id = ?
        """

        df_update.write \
            .format("jdbc") \
            .options(**self.db_options) \
            .option("query", update_query) \
            .mode("append") \
            .save()

    def gerar_rastreabilidade_caixas_desossa(self, token_cliente: str, dt_producao_de: str = None, dt_producao_ate: str = None) -> None:
        """Atualiza caixas com informações da desossa.
        
        Vincula caixas com dados de desossa através de unidade_desossa + op_desossa.
        Atualiza json_desossa e flag_desossa nas caixas.
        
        Args:
            token_cliente: Token do cliente
            dt_producao_de: Data inicial de produção (opcional)
            dt_producao_ate: Data final de produção (opcional)
        """
        self.logger.info(f"Iniciando vinculação de caixas com dados de desossa para token: {token_cliente}")
        
        # Construímos a condição de filtro por data, se fornecida
        date_condition = ""
        params = [token_cliente]
        
        if dt_producao_de and dt_producao_ate:
            date_condition = "AND dt_producao BETWEEN %s AND %s"
            params.extend([dt_producao_de, dt_producao_ate])
        
        # Carrega caixas que precisam ser vinculadas à desossa
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE token_cliente = %s
          AND (flag_desossa = false OR flag_desossa IS NULL)
          {date_condition}
        """
        
        # Executa a query parametrizada
        self.logger.info("Carregando caixas para vinculação com desossa...")
        # Substituindo os parâmetros na query
        formatted_query = query
        for param in params:
            formatted_query = formatted_query.replace("%s", f"'{param}'", 1)
        df_caixas = self._execute_jdbc_query(formatted_query, return_df=True)
        total_caixas = df_caixas.count()
        self.logger.info(f"Carregadas {total_caixas} caixas para vinculação com desossa")
        
        if total_caixas == 0:
            self.logger.info("Não há caixas para vincular com desossa")
            return
        
        # Processa em lotes para evitar problemas de memória
        batch_size = 1000
        total_batches = (total_caixas + batch_size - 1) // batch_size
        
        self.logger.info(f"Vinculando {total_caixas} caixas com dados de desossa em {total_batches} lotes")
        
        # Processa cada lote
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_caixas)
            
            self.logger.info(f"Processando lote {batch_num + 1}/{total_batches} (registros {start_idx + 1} a {end_idx})")
            
            # Obtém o lote atual
            batch_df = df_caixas.limit(end_idx).subtract(df_caixas.limit(start_idx))
            batch_count = batch_df.count()
            
            if batch_count == 0:
                continue
            
            # Vincula dados de desossa
            self.logger.info(f"Vinculando dados de desossa para o lote {batch_num + 1}")
            batch_df = self._update_desossa_data(batch_df)
            
            # Atualiza as flags no banco de dados
            caixa_ids = [str(row.id) for row in batch_df.select('id').collect()]
            if caixa_ids:
                self.mark_as_processed(
                    caixa_ids,
                    {
                        'flag_desossa': True
                    }
                )
        
        self.logger.info(f"Vinculação de caixas com dados de desossa concluída com sucesso")
    
    def gerar_rastreabilidade_caixas_propriedades(self, token_cliente: str, dt_producao_de: str = None, dt_producao_ate: str = None) -> None:
        """Atualiza caixas com dados de escala e animais.
        
        Vincula caixas com dados de propriedades e animais usando dt_abate e unidade_abate.
        Atualiza json_propriedades, json_animais, flag_propriedades e flag_animais nas caixas.
        
        Args:
            token_cliente: Token do cliente
            dt_producao_de: Data inicial de produção (opcional)
            dt_producao_ate: Data final de produção (opcional)
        """
        self.logger.info(f"Iniciando vinculação de caixas com dados de propriedades e animais para token: {token_cliente}")
        
        # Construímos a condição de filtro por data, se fornecida
        date_condition = ""
        params = [token_cliente]
        
        if dt_producao_de and dt_producao_ate:
            date_condition = "AND dt_producao BETWEEN %s AND %s"
            params.extend([dt_producao_de, dt_producao_ate])
        
        # Carrega caixas que já foram vinculadas à desossa (para obter dt_abate e unidade_abate)
        # mas que ainda não foram vinculadas a propriedades ou animais
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE token_cliente = %s
          AND flag_desossa = true
          AND (flag_propriedades = false OR flag_propriedades IS NULL OR flag_animais = false OR flag_animais IS NULL)
          {date_condition}
        """
        
        # Executa a query parametrizada
        self.logger.info("Carregando caixas para vinculação com propriedades e animais...")
        # Substituindo os parâmetros na query
        formatted_query = query
        for param in params:
            formatted_query = formatted_query.replace("%s", f"'{param}'", 1)
        df_caixas = self._execute_jdbc_query(formatted_query, return_df=True)
        total_caixas = df_caixas.count()
        self.logger.info(f"Carregadas {total_caixas} caixas para vinculação com propriedades e animais")
        
        if total_caixas == 0:
            self.logger.info("Não há caixas para vincular com propriedades e animais")
            return
        
        # Processa em lotes para evitar problemas de memória
        batch_size = 1000
        total_batches = (total_caixas + batch_size - 1) // batch_size
        
        self.logger.info(f"Vinculando {total_caixas} caixas com dados de propriedades e animais em {total_batches} lotes")
        
        # Processa cada lote
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_caixas)
            
            self.logger.info(f"Processando lote {batch_num + 1}/{total_batches} (registros {start_idx + 1} a {end_idx})")
            
            # Obtém o lote atual
            batch_df = df_caixas.limit(end_idx).subtract(df_caixas.limit(start_idx))
            batch_count = batch_df.count()
            
            if batch_count == 0:
                continue
            
            # Vincula dados de propriedades e animais
            self.logger.info(f"Vinculando dados de propriedades e animais para o lote {batch_num + 1}")
            batch_df = self._update_propriedades_animais(batch_df)
            
            # Atualiza as flags no banco de dados
            caixa_ids = [str(row.id) for row in batch_df.select('id').collect()]
            if caixa_ids:
                self.mark_as_processed(
                    caixa_ids,
                    {
                        'flag_propriedades': True,
                        'flag_animais': True
                    }
                )
        
        self.logger.info(f"Vinculação de caixas com dados de propriedades e animais concluída com sucesso")
    
    def process_all(self, token_cliente: str, reset_flags: bool = False, update_rastreavel: bool = True) -> None:
        """Processa a rastreabilidade de todas as caixas de todos os períodos.
        O processamento ocorre em etapas:
        1. Vincula caixas com dados de desossa (atualiza json_desossa e flag_desossa)
        2. Vincula caixas com dados de propriedades e animais (atualiza json_propriedades, json_animais)
        3. Processa caixas vinculadas para a tabela proc_caixas
        4. Atualiza flag_rastreavel para caixas com rastreabilidade completa (opcional)

        Args:
            token_cliente: Token do cliente
            reset_flags: Se True, reseta todas as flags antes do processamento (default: False)
            update_rastreavel: Se True, atualiza flag_rastreavel para caixas com rastreabilidade completa (default: True)
        """
        # Resetar flags apenas se solicitado explicitamente
        if reset_flags:
            self.logger.info(f"Resetando flags de processamento para token: {token_cliente}")
            query_reset_flags = f"""
            UPDATE {self.table_name}
            SET updated_at = now(), flag_processamento = false, 
                flag_desossa = false, flag_propriedades = false, flag_animais = false
            WHERE token_cliente = '{token_cliente}'
            """
            result = self._execute_jdbc_query(query_reset_flags)
            self.logger.info(f"Flags de processamento resetadas. Registros afetados: {result}")
        
        # Etapa 1: Vincula caixas com dados de desossa
        self.logger.info(f"Etapa 1: Vinculando caixas com dados de desossa para token: {token_cliente}")
        self.gerar_rastreabilidade_caixas_desossa(token_cliente)
        
        # Etapa 2: Vincula caixas com dados de propriedades e animais
        self.logger.info(f"Etapa 2: Vinculando caixas com dados de propriedades e animais para token: {token_cliente}")
        self.gerar_rastreabilidade_caixas_propriedades(token_cliente)
        
        # Etapa 3: Processa caixas vinculadas para a tabela proc_caixas
        self.logger.info(f"Etapa 3: Processando caixas vinculadas para a tabela proc_caixas para token: {token_cliente}")
        
        # Carrega caixas que foram vinculadas com sucesso (pelo menos com desossa)
        query = f"""
        SELECT *
        FROM {self.table_name}
        WHERE token_cliente = '{token_cliente}'
          AND flag_desossa = true
        """
        
        df_caixas = self._execute_jdbc_query(query, return_df=True)
        total_caixas = df_caixas.count()
        self.logger.info(f"Carregadas {total_caixas} caixas vinculadas para processamento final")
        
        if total_caixas == 0:
            self.logger.info("Não há caixas vinculadas para processar")
            return
        
        # Processa em lotes para evitar problemas de memória
        batch_size = 1000
        total_batches = (total_caixas + batch_size - 1) // batch_size
        
        self.logger.info(f"Processando {total_caixas} caixas para a tabela proc_caixas em {total_batches} lotes")
        
        # Processa cada lote
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, total_caixas)
            
            self.logger.info(f"Processando lote {batch_num + 1}/{total_batches} (registros {start_idx + 1} a {end_idx})")
            
            # Obtém o lote atual
            batch_df = df_caixas.limit(end_idx).subtract(df_caixas.limit(start_idx))
            batch_count = batch_df.count()
            
            if batch_count == 0:
                continue
            
            # Salva resultados deste lote na tabela proc_caixas
            self.logger.info(f"Salvando resultados do lote {batch_num + 1} na tabela proc_caixas")
            self._save_results(batch_df)
            
            # Marca caixas como processadas
            caixa_ids = [str(row.id) for row in batch_df.select('id').collect()]
            if caixa_ids:
                self.mark_as_processed(
                    caixa_ids,
                    {
                        'flag_processamento': True
                    }
                )
        
        # Etapa 4: Atualiza flag_rastreavel para caixas com rastreabilidade completa
        if update_rastreavel:
            self.logger.info(f"Etapa 4: Atualizando flag_rastreavel para caixas com rastreabilidade completa para token: {token_cliente}")
            self.update_flag_rastreavel(token_cliente)
        
        self.logger.info(f"Processamento de todas as caixas concluído com sucesso")

    def process(
        self,
        token_cliente: str,
        dt_producao_de: date,
        dt_producao_ate: date,
        reset_flags: bool = False
    ) -> None:
        """Processa a rastreabilidade das caixas de um período específico.
        O processamento ocorre em etapas:
        1. Carrega caixas não processadas (flag_processamento = false)
        2. Vincula dados de desossa (atualiza json_desossa e flag_desossa)
        3. Vincula dados de propriedades e animais (atualiza json_propriedades, json_animais)
        4. Marca caixas como processadas (flag_processamento = true)

        Args:
            token_cliente: Token do cliente
            dt_producao_de: Data inicial do período
            dt_producao_ate: Data final do período
            reset_flags: Se True, reseta todas as flags antes do processamento (default: False)
        """
        # Resetar flags apenas se solicitado explicitamente
        if reset_flags:
            self.logger.info(f"Resetando flags de processamento para token: {token_cliente} no período {dt_producao_de} a {dt_producao_ate}")
            query_reset_flags = f"""
            UPDATE {self.table_name}
            SET updated_at = now(), flag_processamento = false, 
                flag_desossa = false, flag_propriedades = false, flag_animais = false
            WHERE token_cliente = '{token_cliente}'
              AND dt_producao BETWEEN '{dt_producao_de}' AND '{dt_producao_ate}'
            """
            result = self._execute_jdbc_query(query_reset_flags)
            self.logger.info(f"Flags de processamento resetadas. Registros afetados: {result}")
        
        # Carrega caixas não processadas
        df_caixas = self.load_unprocessed_caixas(
            token_cliente,
            dt_producao_de,
            dt_producao_ate
        )

        if df_caixas.count() == 0:
            return

        # Vincula dados de desossa
        df_caixas = self._update_desossa_data(df_caixas)
        
        # Vincula dados de propriedades e animais
        df_caixas = self._update_propriedades_animais(df_caixas)
        
        # Marca caixas como processadas
        caixa_ids = [str(row.id) for row in df_caixas.select('id').collect()]
        self.mark_as_processed(
            caixa_ids,
            {
                'flag_desossa': True,
                'flag_propriedades': True,
                'flag_animais': True
            }
        )
        
        # Salva resultados
        self._save_results(df_caixas)

    def _load_caixas(
        self,
        token_cliente: str,
        dt_producao_de: date,
        dt_producao_ate: date,
        reprocess: bool = False
    ) -> DataFrame:
        """Carrega caixas do período especificado.
        Retorna caixas não processadas (flag_processamento = false) ou que precisam ser reprocessadas.

        Args:
            token_cliente: Token do cliente
            dt_producao_de: Data inicial
            dt_producao_ate: Data final
            reprocess: Se True, reprocessa mesmo que já tenha sido processado

        Returns:
            DataFrame com caixas do período que precisam ser processadas
        """
        query = f"""
        SELECT c.*
        FROM {self.table_name} c
        WHERE c.token_cliente = '{token_cliente}'
          AND c.dt_producao BETWEEN '{dt_producao_de}' AND '{dt_producao_ate}'
          AND c.flag_processamento = false
        """

        if not reprocess:
            query += """
            AND (
                c.flag_desossa = false OR c.flag_desossa IS NULL
                OR c.flag_propriedades = false OR c.flag_propriedades IS NULL
                OR c.flag_animais = false OR c.flag_animais IS NULL
            )
            """

        return self._execute_jdbc_query(query, return_df=True)

    def _mark_for_processing(
        self,
        token_cliente: str,
        dt_producao_de: date,
        dt_producao_ate: date
    ) -> None:
        """Marca caixas do período para processamento.

        Args:
            token_cliente: Token do cliente
            dt_producao_de: Data inicial
            dt_producao_ate: Data final
        """
        update_query = f"""
        UPDATE {self.table_name}
        SET updated_at = now()
        WHERE token_cliente = '{token_cliente}'
          AND dt_producao BETWEEN '{dt_producao_de}' AND '{dt_producao_ate}'
        """

        self._execute_jdbc_query(update_query)

    def _update_desossa_data(self, caixas_df: DataFrame) -> DataFrame:
        """Atualiza dados de desossa nas caixas.

        Args:
            caixas_df: DataFrame com as caixas

        Returns:
            DataFrame atualizado
        """
        try:
            self.logger.info("Carregando dados de desossa...")
            
            # Verificando as colunas disponíveis no DataFrame de caixas
            caixas_columns = caixas_df.columns
            self.logger.info(f"Colunas disponíveis no DataFrame de caixas: {caixas_columns}")
            
            # Carregando a tabela bt_desossa
            desossa_df = self.spark.read \
                .format("jdbc") \
                .options(**self.db_options) \
                .option("dbtable", "bt_desossa") \
                .load()
            
            # Verificando as colunas disponíveis no DataFrame de desossa
            desossa_columns = desossa_df.columns
            self.logger.info(f"Colunas disponíveis no DataFrame de desossa: {desossa_columns}")
            
            # Join com bt_desossa usando nr_unidade_desossa e nr_op_desossa
            result_df = caixas_df.join(
                desossa_df,
                (caixas_df.nr_unidade_desossa == desossa_df.nr_unidade_desossa) &
                (caixas_df.nr_op_desossa == desossa_df.nr_op_desossa),
                "left"
            )
            
            # Adicionando a coluna json_desossa com os dados da desossa
            desossa_cols = [col(c).alias(c) for c in desossa_df.columns]
            result_df = result_df.withColumn(
                "json_desossa",
                to_json(struct(*desossa_cols))
            )
            
            # Atualizando a flag_desossa
            result_df = result_df.withColumn(
                "flag_desossa",
                col("json_desossa").isNotNull()
            )
            
            self.logger.info(f"Dados de desossa atualizados com sucesso. Linhas processadas: {result_df.count()}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Erro ao atualizar dados de desossa: {str(e)}")
            # Retorna o DataFrame original em caso de erro
            return caixas_df

    def _get_table_columns(self, table_name: str) -> list:
        """Obtém as colunas de uma tabela do banco de dados.

        Args:
            table_name: Nome da tabela

        Returns:
            Lista com os nomes das colunas da tabela
        """
        try:
            # Importando psycopg2
            import psycopg2
            import re
            
            # Extraindo os parâmetros de conexão
            jdbc_url = self.db_options.get('url', '')
            host = self.db_options.get('host', '')
            port = self.db_options.get('port', '')
            database = self.db_options.get('database', '')
            user = self.db_options.get('user', '')
            password = self.db_options.get('password', '')
            
            # Se a URL JDBC está definida, extrair os parâmetros dela
            if jdbc_url and not host:
                url_pattern = r'jdbc:postgresql://([^:]+):([^/]+)/(.+)'
                match = re.match(url_pattern, jdbc_url)
                if match:
                    host = match.group(1)
                    port = match.group(2)
                    database = match.group(3)
            
            # Conectando ao banco de dados
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port
            )
            
            try:
                # Criando cursor
                cursor = conn.cursor()
                
                # Query para obter as colunas da tabela
                query = f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
                """
                
                # Executando a query
                cursor.execute(query)
                
                # Obtendo os resultados
                columns = [row[0] for row in cursor.fetchall()]
                
                return columns
            
            finally:
                cursor.close()
                conn.close()
        
        except Exception as e:
            self.logger.error(f"Erro ao obter colunas da tabela {table_name}: {str(e)}")
            # Retornando uma lista padrão de colunas para proc_caixas em caso de erro
            if table_name == 'proc_caixas':
                return [
                    'id', 'proc_resgistro_venda_id', 'token_cliente', 'tipo_unidade_desossa', 
                    'nr_unidade_desossa', 'cnpj_industria_desossa', 'nr_op_desossa', 
                    'nr_unidade_abate', 'tipo_unidade_abate', 'cnpj_industria_abate', 
                    'dt_abate', 'dt_producao', 'dt_embalagem', 'dt_validade', 
                    'dt_hora_inicio_desossa', 'qtde_pecas', 'peso_liquido', 'peso_bruto', 
                    'tipo_embalagem', 'tara_primaria', 'tara_total', 'codigo_rastreabilidade_ind', 
                    'cogido_palete', 'tipo_armazenamento', 'dt_hora_pesagem', 'codigo_barras', 
                    'json_habilitacoes', 'created_at', 'updated_at', 'tipo_caixa_id'
                ]
            return []
    
    def _update_propriedades_animais(self, caixas_df: DataFrame) -> DataFrame:
        """Atualiza dados de propriedades e animais nas caixas.

        Args:
            caixas_df: DataFrame com as caixas e dados de desossa

        Returns:
            DataFrame atualizado
        """
        try:
            self.logger.info("Carregando dados de propriedades e animais...")
            
            # Verificando as colunas disponíveis no DataFrame de caixas
            caixas_columns = caixas_df.columns
            self.logger.info(f"Colunas disponíveis no DataFrame de caixas: {caixas_columns}")
            
            # Carregando a tabela bt_escala
            escala_df = self.spark.read \
                .format("jdbc") \
                .options(**self.db_options) \
                .option("dbtable", "bt_escala") \
                .load()
            
            # Verificando as colunas disponíveis no DataFrame de escala
            escala_columns = escala_df.columns
            self.logger.info(f"Colunas disponíveis no DataFrame de escala: {escala_columns}")
            
            # Carregando a tabela bt_animais
            animais_df = self.spark.read \
                .format("jdbc") \
                .options(**self.db_options) \
                .option("dbtable", "bt_animais") \
                .load()
            
            # Verificando as colunas disponíveis no DataFrame de animais
            animais_columns = animais_df.columns
            self.logger.info(f"Colunas disponíveis no DataFrame de animais: {animais_columns}")
            
            # Primeiro join com escala usando nr_unidade_abate em vez de unidade_abate
            self.logger.info("Realizando join com tabela de escala...")
            df_with_escala = caixas_df.join(
                escala_df,
                (caixas_df.dt_abate == escala_df.dt_abate) &
                (caixas_df.nr_unidade_abate == escala_df.nr_unidade_abate),
                "left"
            )
            
            # Criando estrutura para json_propriedades
            escala_cols = [col(c).alias(c) for c in escala_df.columns]
            df_with_escala = df_with_escala.withColumn(
                "json_propriedades",
                to_json(struct(*escala_cols))
            ).withColumn(
                "flag_propriedades",
                col("json_propriedades").isNotNull()
            )
            
            self.logger.info(f"Join com escala realizado. Linhas resultantes: {df_with_escala.count()}")
            
            # Depois join com animais usando nr_unidade_abate em vez de unidade_abate
            self.logger.info("Realizando join com tabela de animais...")
            animais_cols = [col(c).alias(c) for c in animais_df.columns]
            result_df = df_with_escala.join(
                animais_df,
                (df_with_escala.dt_abate == animais_df.dt_abate) &
                (df_with_escala.nr_unidade_abate == animais_df.nr_unidade_abate),
                "left"
            ).withColumn(
                "json_animais",
                to_json(struct(*animais_cols))
            ).withColumn(
                "flag_animais",
                col("json_animais").isNotNull()
            )
            
            self.logger.info(f"Join com animais realizado. Linhas resultantes: {result_df.count()}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Erro ao atualizar dados de propriedades e animais: {str(e)}")
            # Retorna o DataFrame original em caso de erro
            return caixas_df

    def _save_results(self, caixas_df: DataFrame) -> None:
        """Salva os resultados do processamento.

        Args:
            caixas_df: DataFrame com os resultados
        """
        try:
            self.logger.info("Preparando dados para salvar na tabela proc_caixas...")
            
            # Obtendo as colunas da tabela proc_caixas
            proc_caixas_columns = self._get_table_columns('proc_caixas')
            self.logger.info(f"Colunas da tabela proc_caixas: {proc_caixas_columns}")
            
            # Verificando as colunas disponíveis no DataFrame
            caixas_columns = caixas_df.columns
            self.logger.info(f"Colunas disponíveis no DataFrame: {caixas_columns}")
            
            # Renomeando colunas para corresponder à estrutura da tabela proc_caixas
            renamed_df = caixas_df
            if 'tipo_unidade_desossa_id' in caixas_columns:
                renamed_df = renamed_df.withColumnRenamed('tipo_unidade_desossa_id', 'tipo_unidade_desossa')
            
            if 'tipo_unidade_abate_id' in caixas_columns:
                renamed_df = renamed_df.withColumnRenamed('tipo_unidade_abate_id', 'tipo_unidade_abate')
            
            # Filtrando apenas as colunas que existem na tabela proc_caixas
            existing_columns = [col for col in renamed_df.columns if col in proc_caixas_columns]
            self.logger.info(f"Colunas que serão salvas: {existing_columns}")
            
            # Selecionando apenas as colunas existentes
            filtered_df = renamed_df.select(*existing_columns)
            
            # Adicionando colunas obrigatórias com valores padrão se não existirem
            if 'created_at' not in existing_columns:
                from pyspark.sql.functions import current_timestamp
                filtered_df = filtered_df.withColumn('created_at', current_timestamp())
            
            if 'updated_at' not in existing_columns:
                from pyspark.sql.functions import current_timestamp
                filtered_df = filtered_df.withColumn('updated_at', current_timestamp())
            
            # Removendo json_habilitacoes se existir para evitar problemas de tipo
            if 'json_habilitacoes' in filtered_df.columns:
                self.logger.info("Removendo coluna json_habilitacoes para evitar problemas de tipo de dados")
                filtered_df = filtered_df.drop('json_habilitacoes')
            
            # Verificando se há registros duplicados
            from pyspark.sql.functions import col, count
            
            # Verificando quantos registros existem no DataFrame
            total_records = filtered_df.count()
            self.logger.info(f"Total de registros no DataFrame antes da coleta: {total_records}")
            
            # Verificando se há duplicação de IDs
            if 'id' in filtered_df.columns:
                duplicated_ids = filtered_df.groupBy('id').count().filter(col('count') > 1)
                duplicated_count = duplicated_ids.count()
                if duplicated_count > 0:
                    self.logger.warning(f"Encontrados {duplicated_count} IDs duplicados no DataFrame")
                    # Removendo duplicações, mantendo apenas o primeiro registro de cada ID
                    filtered_df = filtered_df.dropDuplicates(['id'])
                    self.logger.info(f"Após remoção de duplicados: {filtered_df.count()} registros")
            
            # Coletando os dados para inserção direta via psycopg2
            rows = filtered_df.collect()
            self.logger.info(f"Preparando para salvar {len(rows)} registros na tabela proc_caixas...")
            
            # Verificando se há valores nulos em colunas obrigatórias
            if len(rows) > 0:
                # Verificando os primeiros registros para diagnóstico
                sample_size = min(5, len(rows))
                self.logger.info(f"Amostra dos primeiros {sample_size} registros:")
                for i in range(sample_size):
                    row_dict = rows[i].asDict()
                    id_value = row_dict.get('id')
                    token_value = row_dict.get('token_cliente')
                    self.logger.info(f"Registro {i+1}: id={id_value}, token_cliente={token_value}")
            
            if len(rows) > 0:
                # Importando psycopg2 para inserção direta
                import psycopg2
                import psycopg2.extras
                import re
                from datetime import datetime
                
                # Extraindo os parâmetros de conexão
                jdbc_url = self.db_options.get('url', '')
                host = self.db_options.get('host', '')
                port = self.db_options.get('port', '')
                database = self.db_options.get('database', '')
                user = self.db_options.get('user', '')
                password = self.db_options.get('password', '')
                
                # Se a URL JDBC está definida, extrair os parâmetros dela
                if jdbc_url and not host:
                    url_pattern = r'jdbc:postgresql://([^:]+):([^/]+)/(.+)'
                    match = re.match(url_pattern, jdbc_url)
                    if match:
                        host = match.group(1)
                        port = match.group(2)
                        database = match.group(3)
                
                # Conectando ao banco de dados
                conn = psycopg2.connect(
                    host=host,
                    database=database,
                    user=user,
                    password=password,
                    port=port
                )
                
                try:
                    # Criando cursor
                    cursor = conn.cursor()
                    
                    # Construindo a query de inserção com UPSERT (INSERT ON CONFLICT UPDATE)
                    columns_str = ", ".join(existing_columns)
                    placeholders = ", ".join(["%s" for _ in existing_columns])
                    
                    # Adicionando json_habilitacoes como NULL se não estiver nas colunas
                    if 'json_habilitacoes' not in existing_columns and 'json_habilitacoes' in proc_caixas_columns:
                        columns_str += ", json_habilitacoes"
                        placeholders += ", NULL"
                    
                    # Construímos a cláusula ON CONFLICT para atualizar todos os campos exceto o ID
                    update_columns = [col for col in existing_columns if col != 'id']
                    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                    
                    # Query UPSERT: INSERT ... ON CONFLICT (id) DO UPDATE SET ...
                    insert_query = f"INSERT INTO proc_caixas ({columns_str}) VALUES ({placeholders}) ON CONFLICT (id) DO UPDATE SET {update_clause}"
                    self.logger.info("Usando estratégia UPSERT para evitar erro de chave duplicada")
                    
                    # Preparando os dados para inserção
                    batch_size = 100  # Inserir em lotes para melhor performance
                    for i in range(0, len(rows), batch_size):
                        batch = rows[i:i+batch_size]
                        batch_data = []
                        
                        for row in batch:
                            # Convertendo Row para dict
                            row_dict = row.asDict()
                            # Extraindo valores na ordem das colunas
                            row_values = [row_dict.get(col) for col in existing_columns]
                            batch_data.append(row_values)
                        
                        # Executando a inserção em lote
                        psycopg2.extras.execute_batch(cursor, insert_query, batch_data)
                    
                    # Commit das alterações
                    conn.commit()
                    self.logger.info(f"Inseridos {len(rows)} registros com sucesso na tabela proc_caixas")
                
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"Erro na inserção direta via psycopg2: {str(e)}")
                    raise
                
                finally:
                    cursor.close()
                    conn.close()
            else:
                self.logger.info("Nenhum registro para salvar")
            
            self.logger.info("Processamento concluído com sucesso!")
        
        except Exception as e:
            self.logger.error(f"Erro ao salvar resultados: {str(e)}")
            raise Exception(f"Erro ao salvar resultados: {str(e)}")
