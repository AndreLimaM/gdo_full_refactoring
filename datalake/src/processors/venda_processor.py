#!/usr/bin/env python3
"""Módulo para processamento de registros de venda e suas propriedades."""

from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, expr, to_json, current_date, from_json, explode, 
    array, struct, create_map, collect_list, to_timestamp, coalesce, 
    when, lower, trim, regexp_replace, concat_ws, size, length, 
    substring, date_format, to_date, year, month, dayofmonth, 
    hour, minute, second, last_day, date_add, date_sub, datediff, 
    from_unixtime, unix_timestamp, udf, array_contains, collect_set,
    max as max_, min as min_, sum as sum_, count as count_
)
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, 
    BooleanType, IntegerType, TimestampType, DateType, MapType
)

from .base_processor import BaseProcessor


class VendaProcessor(BaseProcessor):
    """Processador de registros de venda e suas propriedades."""

    def __init__(self, spark: SparkSession, db_options: Dict[str, str]):
        """Inicializa o processador de vendas.

        Args:
            spark: Sessão Spark ativa
            db_options: Opções de conexão com o banco de dados
        """
        super().__init__(spark)
        self.db_options = db_options

    def process(self, grupo_id: Optional[int] = None, token_cliente: Optional[str] = None) -> None:
        """Processa registros de venda por grupo ou token_cliente.

        Args:
            grupo_id: ID do grupo (opcional)
            token_cliente: Token do cliente (opcional)

        Raises:
            ValueError: Se nenhum parâmetro for fornecido
        """
        try:
            self.logger.info(f"Iniciando processamento de registros de venda")
            self.logger.info(f"Parâmetros: grupo_id={grupo_id}, token_cliente={token_cliente}")
            
            # Validar parâmetros
            if grupo_id is None and token_cliente is None:
                msg = "Pelo menos um dos parâmetros (grupo_id ou token_cliente) deve ser fornecido"
                self.logger.error(msg)
                raise ValueError(msg)
                
            # Carregar registros não processados (limite de 1000 para teste)
            self.logger.info("Carregando registros de venda não processados")
            registros_df = self._load_registros_venda(grupo_id, token_cliente, limite=1000)
            
            # Verificar se há registros para processar
            if registros_df.count() == 0:
                self.logger.info("Não há registros de venda não processados para processar")
                return
                
            self.logger.info(f"Encontrados {registros_df.count()} registros de venda para processar")
            
            # Processar registros
            self.logger.info("Processando registros de venda")
            caixas_processadas = self._process_registros(registros_df)
            
            # Verificar se há caixas processadas
            if caixas_processadas.count() == 0:
                self.logger.info("Não há caixas para processar nos registros de venda")
                return
                
            self.logger.info(f"Processadas {caixas_processadas.count()} caixas")
            
            # Salvar resultados
            self.logger.info("Salvando resultados nas tabelas proc_*")
            self._save_results(caixas_processadas)
            
            # Marcar registros como processados
            registros = caixas_processadas.collect()
            self.logger.info(f"Marcando {len(registros)} registros como processados")
            try:
                # Verificar colunas disponíveis
                if len(registros) > 0:
                    first_row = registros[0]
                    if isinstance(first_row, dict):
                        self.logger.info(f"Colunas disponíveis no registro: {list(first_row.keys())}")
                    else:  # Se for Row do PySpark
                        self.logger.info(f"Colunas disponíveis no registro: {first_row.__fields__}")
                
                # Criar uma lista de IDs de registro válidos
                registro_ids = []
                for row in registros:
                    try:
                        # Verificar todos os possíveis nomes para o campo id
                        id_value = None
                        if isinstance(row, dict):
                            for field_name in ['id', 'registro_id', 'ID', 'Id']:
                                if field_name in row:
                                    id_value = row[field_name]
                                    break
                        else:  # Se for Row do PySpark
                            for field_name in ['id', 'registro_id', 'ID', 'Id']:
                                if hasattr(row, field_name):
                                    id_value = getattr(row, field_name)
                                    break
                        
                        if id_value is not None:
                            registro_ids.append(id_value)
                            
                    except Exception as e:
                        self.logger.warning(f"Erro ao obter id do registro: {str(e)}")
                
                # Atualizar registros em lote se houver IDs válidos
                if registro_ids:
                    placeholders = ", ".join(["'%s'" % str(rid) for rid in registro_ids])
                    update_query = f"""
                        UPDATE bt_registro_venda 
                        SET flag_processada = true, 
                            data_hora_processada = CURRENT_TIMESTAMP 
                        WHERE id IN ({placeholders})
                    """
                    self._execute_jdbc_query(update_query)
                    self.logger.info(f"Atualizados {len(registro_ids)} registros como processados")
                else:
                    self.logger.warning("Nenhum id válido encontrado para atualizar")
                    
                    # Como alternativa, tentar atualizar pelo token_cliente
                    if len(registros) > 0 and hasattr(registros[0], 'token_cliente'):
                        token = registros[0].token_cliente
                        self.logger.info(f"Tentando atualizar pelo token_cliente: {token}")
                        update_query = f"""
                            UPDATE bt_registro_venda 
                            SET flag_processada = true, 
                                data_hora_processada = CURRENT_TIMESTAMP 
                            WHERE token_cliente = '{token}' AND NOT flag_processada
                            LIMIT 1000
                        """
                        self._execute_jdbc_query(update_query)
                        self.logger.info("Registros atualizados pelo token_cliente")

            except Exception as e:
                self.logger.error(f"Erro ao marcar registros como processados: {str(e)}")
                # Não levanta exceção para permitir que o processamento continue
            self.logger.info("Processamento concluído com sucesso")
        except Exception as e:
            self.logger.error(f"Erro no processamento de registros de venda: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise

    def _load_registros_venda(self, grupo_id: Optional[int] = None, token_cliente: Optional[str] = None, limite: Optional[int] = None) -> DataFrame:
        """Carrega registros de venda não processados.

        Args:
            grupo_id: ID do grupo (opcional)
            token_cliente: Token do cliente (opcional)
            limite: Número máximo de registros a retornar (opcional)

        Returns:
            DataFrame com os registros
        """
        try:
            # Constrói a query base
            query = "SELECT * FROM bt_registro_venda WHERE NOT flag_processada"
            
            # Adiciona filtros conforme os parâmetros fornecidos
            if grupo_id is not None:
                # Verificar se a coluna grupo_id existe na tabela antes de filtrar
                if 'grupo_id' in self.spark.read.format("jdbc").option("url", self.db_options['url']) \
                        .option("dbtable", "(SELECT column_name FROM information_schema.columns WHERE table_name='bt_registro_venda' AND column_name='grupo_id') as tmp") \
                        .option("user", self.db_options['user']) \
                        .option("password", self.db_options['password']) \
                        .load().collect():
                    query += f" AND grupo_id = {grupo_id}"
                else:
                    self.logger.warning("Coluna grupo_id não encontrada na tabela bt_registro_venda. Ignorando filtro por grupo_id.")
            
            # Filtro por token_cliente
            if token_cliente is not None:
                query += f" AND token_cliente = '{token_cliente}'"
            
            # Ordena por data de criação para processar os mais antigos primeiro
            query += " ORDER BY created_at ASC"
            
            # Adiciona LIMIT se especificado (deve vir depois do ORDER BY)
            if limite is not None and limite > 0:
                query += f" LIMIT {limite}"
            
            self.logger.info(f"Executando query: {query}")
            df = self._execute_jdbc_query(query, return_df=True)
            
            # Verifica se o DataFrame está vazio
            if df.rdd.isEmpty():
                self.logger.warning("Consulta não retornou registros")
                # Retorna um DataFrame vazio com o schema correto
                schema = self.spark.read.format("jdbc") \
                    .option("url", self.db_options['url']) \
                    .option("dbtable", "(SELECT * FROM bt_registro_venda LIMIT 1) as tmp") \
                    .option("user", self.db_options['user']) \
                    .option("password", self.db_options['password']) \
                    .load().schema
                return self.spark.createDataFrame([], schema)
                
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao carregar registros de venda: {str(e)}")
            # Retorna um DataFrame vazio com o schema correto em caso de erro
            schema = self.spark.read.format("jdbc") \
                .option("url", self.db_options['url']) \
                .option("dbtable", "(SELECT * FROM bt_registro_venda LIMIT 1) as tmp") \
                .option("user", self.db_options['user']) \
                .option("password", self.db_options['password']) \
                .load().schema
            return self.spark.createDataFrame([], schema)

    def _process_registros(self, registros_df: DataFrame) -> DataFrame:
        """Processa registros de venda e suas caixas.

        Args:
            registros_df: DataFrame com os registros

        Returns:
            DataFrame com caixas processadas
        """
        # Define o schema para o campo json_caixas
        caixas_schema = ArrayType(StructType([
            # Campos básicos de identificação da caixa
            StructField("id", StringType(), True),
            StructField("codigo", StringType(), True),
            StructField("peso", StringType(), True)
        ]))

        # Explode json_caixas para processar cada caixa
        try:
            # Log para diagnóstico
            self.logger.info(f"Processando {registros_df.count()} registros de venda")
            self.logger.debug(f"Colunas do DataFrame de registros: {registros_df.columns}")
            
            # Verificar se a coluna json_caixas existe e não é nula
            if "json_caixas" not in registros_df.columns:
                self.logger.error("Coluna json_caixas não encontrada no DataFrame de registros")
                return self.spark.createDataFrame([], [])
                
            # Verificar se há registros com json_caixas nulo
            registros_com_caixas = registros_df.filter(col("json_caixas").isNotNull())
            if registros_com_caixas.count() == 0:
                self.logger.warning("Nenhum registro com caixas válidas encontrado")
                return self.spark.createDataFrame([], [])
                
            # Seleciona apenas as colunas necessárias para o processamento
            columns_to_select = [col("id"), col("token_cliente")]
            if "grupo_id" in registros_df.columns:
                columns_to_select.append(col("grupo_id"))
                self.logger.debug("Coluna grupo_id encontrada e será incluída")
            else:
                # Se não existir, criar uma coluna nula para manter a estrutura esperada
                columns_to_select.append(lit(None).cast("integer").alias("grupo_id"))
                self.logger.debug("Coluna grupo_id não encontrada, será criada como nula")
            
            # Adicionar a coluna caixa (explode)
            columns_to_select.append(explode("caixas").alias("caixa"))
            
            # Converter json_caixas para estrutura e explodir
            self.logger.info("Convertendo json_caixas para estrutura e explodindo")
            caixas_df = registros_com_caixas \
                .withColumn("caixas", from_json(col("json_caixas"), caixas_schema)) \
                .select(*columns_to_select)
                
            # Verificar se a explosão gerou registros
            if caixas_df.count() == 0:
                self.logger.warning("Nenhuma caixa encontrada após explosão do JSON")
                return self.spark.createDataFrame([], [])
                
            # Extrair campos da estrutura caixa
            self.logger.info("Extraindo campos da estrutura caixa")
            caixas_df = caixas_df.select(
                col("id").alias("registro_id"),
                col("token_cliente"),
                col("grupo_id"),
                col("caixa.id").alias("caixa_id")
            )
            
            # Log para diagnóstico
            self.logger.info(f"Extraídas {caixas_df.count()} caixas dos registros de venda")
            self.logger.debug(f"Colunas do DataFrame de caixas: {caixas_df.columns}")
                
            # Verifica se há caixas para processar
            if caixas_df.count() == 0:
                self.logger.warning("Nenhuma caixa encontrada nos registros de venda")
                return self.spark.createDataFrame([], [])
                
            # Join com bt_caixas para obter dados completos
            self.logger.info("Obtendo dados completos das caixas")
            caixas_completas = self._get_caixas_data(caixas_df)
            
            # Verifica se o join retornou registros
            if caixas_completas.count() == 0:
                self.logger.warning("Nenhuma caixa encontrada após join com bt_caixas")
                return self.spark.createDataFrame([], [])
                
            # Log para diagnóstico
            self.logger.info(f"Join resultou em {caixas_completas.count()} caixas completas")
            self.logger.debug(f"Colunas do DataFrame de caixas completas: {caixas_completas.columns}")
            
            # Prepara os dados para as tabelas proc_*
            self.logger.info("Preparando dados para inserção nas tabelas proc_*")
            return self._prepare_proc_data(caixas_completas)
        except Exception as e:
            self.logger.error(f"Erro ao processar registros de venda: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            # Retorna DataFrame vazio em caso de erro
            return self.spark.createDataFrame([], [])

    def _get_caixas_data(self, caixas_df: DataFrame) -> DataFrame:
        """Obtém dados completos das caixas já processadas.

        Args:
            caixas_df: DataFrame com referências das caixas

        Returns:
            DataFrame com dados completos das caixas
        """
        try:
            # Cria uma lista de IDs de caixas para a consulta
            caixa_rows = caixas_df.select('caixa_id').distinct().collect()
            caixa_ids = [str(row.caixa_id) for row in caixa_rows if row.caixa_id is not None]
            
            if not caixa_ids:
                self.logger.warning("Nenhuma caixa válida encontrada para processar")
                return self.spark.createDataFrame([], [])
                
            # Log para diagnóstico
            self.logger.info(f"Processando {len(caixa_ids)} caixas")
            self.logger.debug(f"IDs das caixas: {caixa_ids[:5]}{'...' if len(caixa_ids) > 5 else ''}")
            
            # Remove IDs vazios e garante que são strings
            caixa_ids = [str(cid).strip() for cid in caixa_ids if cid and str(cid).strip()]
            if not caixa_ids:
                self.logger.warning("Nenhum ID de caixa válido após limpeza")
                return self.spark.createDataFrame([], [])
                
            # Formata a lista de IDs para uso na query SQL
            caixa_ids_str = "', '".join(caixa_ids)
            
            # Consulta apenas caixas que já foram processadas (flag_desossa, flag_propriedades, flag_animais)
            # e que tenham flag_rastreavel = true
            query = f"""
            SELECT 
                c.id, 
                c.codigo, 
                c.peso, 
                c.dt_producao,
                c.flag_desossa, 
                c.flag_propriedades, 
                c.flag_animais, 
                c.flag_rastreavel,
                d.id as desossa_id, 
                d.lote as desossa_lote,
                p.id as propriedade_id, 
                p.nome as propriedade_nome,
                a.id as animal_id, 
                a.codigo as animal_codigo
            FROM bt_caixas c
            LEFT JOIN bt_desossa d ON d.id = c.json_desossa->>'id'
            LEFT JOIN bt_propriedades p ON p.id = c.json_propriedades->>'id'
            LEFT JOIN bt_animais a ON a.id = c.json_animais->>'id'
            WHERE c.id IN ('{caixa_ids_str}')
              AND c.flag_desossa = true
              AND c.flag_propriedades = true
              AND c.flag_animais = true
              AND c.flag_rastreavel = true
            """
            
            # Executa a query
            self.logger.info("Executando consulta para obter dados completos das caixas")
            bt_caixas_df = self._execute_jdbc_query(query, return_df=True)
            
            # Log para diagnóstico
            self.logger.info(f"Consulta retornou {bt_caixas_df.count()} caixas")
            self.logger.debug(f"Colunas do DataFrame bt_caixas: {bt_caixas_df.columns}")
            self.logger.debug(f"Colunas do DataFrame caixas_df: {caixas_df.columns}")
            
            # Verificar e renomear colunas comuns para evitar ambiguidade
            bt_caixas_renamed = bt_caixas_df
            common_columns = set(caixas_df.columns).intersection(set(bt_caixas_df.columns))
            
            self.logger.info(f"Colunas comuns entre os DataFrames: {common_columns}")
            
            for column in common_columns:
                if column != "caixa_id" and column != "id":  # Não renomear as colunas de join
                    self.logger.debug(f"Renomeando coluna {column} para caixa_{column}")
                    bt_caixas_renamed = bt_caixas_renamed.withColumnRenamed(column, f"caixa_{column}")
            
            # Join com o DataFrame original para manter o registro_id
            self.logger.info("Realizando join entre caixas_df e bt_caixas_renamed")
            result_df = caixas_df.join(
                bt_caixas_renamed,
                caixas_df.caixa_id == bt_caixas_renamed.id,
                "inner"  # Usa inner join para manter apenas caixas que existem e estão processadas
            )
            
            # Log para diagnóstico
            self.logger.info(f"Join resultou em {result_df.count()} registros")
            self.logger.debug(f"Colunas do DataFrame resultante: {result_df.columns}")
            
            return result_df
            
        except Exception as e:
            self.logger.error(f"Erro ao obter dados completos das caixas: {str(e)}")
            # Retorna o DataFrame original em caso de erro
            return caixas_df

    def _prepare_proc_data(self, caixas_df: DataFrame) -> DataFrame:
        """Prepara os dados para inserção nas tabelas proc_*.

        Args:
            caixas_df: DataFrame com dados completos das caixas

        Returns:
            DataFrame com dados preparados para inserção
        """
        if caixas_df.count() == 0:
            return self.spark.createDataFrame([], [])
        
        # Verificar quais colunas existem no DataFrame para evitar ambiguidade
        available_columns = caixas_df.columns
        self.logger.info(f"Colunas disponíveis no DataFrame: {available_columns}")
        
        # Lista para armazenar as colunas a serem selecionadas
        columns_to_select = []
        
        # Adiciona as colunas obrigatórias
        columns_to_select = [col("registro_id")]
        columns_to_select.append(col("token_cliente"))
        
        # Adiciona grupo_id apenas se existir no DataFrame
        if "grupo_id" in available_columns:
            columns_to_select.append(col("grupo_id"))
            
        # Campos da caixa
        if "id" in available_columns:
            columns_to_select.append(col("id").alias("caixa_id"))
        if "caixa_codigo" in available_columns:
            columns_to_select.append(col("caixa_codigo"))
        if "caixa_peso" in available_columns:
            columns_to_select.append(col("caixa_peso"))
        if "caixa_dt_producao" in available_columns:
            columns_to_select.append(col("caixa_dt_producao"))
        
        # Campos da propriedade - adiciona todos os campos necessários para proc_propriedades
        propriedade_columns = [
            'token_cliente', 'dt_abate', 'nr_unidade_abate', 'nr_lote_abate',
            'prod_nome_razaosocial', 'prod_cpf_cnpj', 'faz_nome', 'faz_cidade',
            'faz_estado', 'faz_nr_car', 'json_certificados', 'json_gtas', 'json_nfs'
        ]
        
        for col_name in propriedade_columns:
            if col_name in available_columns:
                columns_to_select.append(col(col_name))
        
        # Campos da desossa
        if "desossa_id" in available_columns:
            columns_to_select.append(col("desossa_id"))
            columns_to_select.append(col("desossa_lote").alias("lote_desossa"))
            
        # Adiciona colunas de processamento
        from pyspark.sql.functions import current_timestamp
        columns_to_select.append(current_timestamp().alias("dt_processamento"))
        
        # Retorna o DataFrame com as colunas selecionadas
        return caixas_df.select(*columns_to_select)

    def _save_results(self, df: DataFrame) -> None:
        """Salva os resultados do processamento nas tabelas proc_*.

        Args:
            df: DataFrame com os resultados a serem salvos
        """
        if df.count() == 0:
            self.logger.warning("Nenhum dado para salvar")
            return
            
        # Verificar quais colunas existem no DataFrame para evitar ambiguidade
        available_columns = df.columns
        self.logger.info(f"Colunas disponíveis no DataFrame para salvar: {available_columns}")
            
        try:
            # 1. Salva em proc_registro_venda
            registro_columns = []
            
            # Não incluir a coluna id, deixar o banco gerar automaticamente
            # Garantir que as colunas obrigatórias existam
            required_columns = ["token_cliente"]
            for col_name in required_columns:
                if col_name not in available_columns:
                    self.logger.error(f"Coluna obrigatória '{col_name}' não encontrada no DataFrame")
                    return
            
            # Adicionar apenas as colunas que existem na tabela de destino
            registro_columns.append(col("token_cliente"))
            
            # Adicionar campos obrigatórios com valores padrão
            registro_columns.append(lit("00000000000000").alias("nf_destino_cpf_cnpj"))
            registro_columns.append(lit("00000000000000").alias("cnpj_local_estoque"))
            registro_columns.append(current_date().alias("dt_venda"))
            registro_columns.append(current_date().alias("dt_expedicao"))
            registro_columns.append(lit("00000000000000").alias("cnpj_transportadora"))
            registro_columns.append(lit("").alias("nf_numero"))
            registro_columns.append(lit("VENDA").alias("nf_natureza"))
            registro_columns.append(lit("Venda").alias("nf_origem_nome_razaosocial"))
            registro_columns.append(lit("00000000000000").alias("nf_origem_cpf_cnpj"))
            registro_columns.append(current_date().alias("nf_dt_compra"))
            registro_columns.append(current_date().alias("nf_dt_emissao"))
            registro_columns.append(current_date().alias("nf_dt_pedido"))
            
            # Usar a data atual do banco de dados com tipo explícito
            registro_columns.append(expr("CURRENT_TIMESTAMP").cast("timestamp").alias("data_hora_processada"))
            
            # Usar um dicionário vazio como string JSON
            registro_columns.append(lit("{}").alias("json_itens"))
            registro_columns.append(lit("{}").alias("json_caixas"))
            
            # Flag de processamento
            registro_columns.append(lit(True).alias("flag_web"))
            
            # País de destino
            registro_columns.append(lit("Brasil").alias("destino_pais"))
                
            # Coletar os dados para inserção
            proc_registro_df = df.select(*registro_columns).distinct()
            registros = proc_registro_df.collect()
            
            if registros:
                # Construir a consulta SQL para inserção em lote
                columns = ["token_cliente", "nf_destino_cpf_cnpj", "cnpj_local_estoque", 
                          "dt_venda", "dt_expedicao", "cnpj_transportadora", "nf_numero", 
                          "nf_natureza", "nf_origem_nome_razaosocial", "nf_origem_cpf_cnpj", 
                          "nf_dt_compra", "nf_dt_emissao", "nf_dt_pedido", "data_hora_processada", 
                          "json_itens", "json_caixas", "flag_web", "destino_pais"]
                
                # Construir a consulta SQL com CAST explícito para jsonb
                placeholders = ", ".join(["%s"] * len(columns))
                
                # Para cada registro, criar e executar uma consulta individual
                for row in registros:
                    # Construir a lista de valores formatados corretamente
                    values = []
                    params = []
                    
                    for col_name in columns:
                        val = row[col_name]
                        if val is None:
                            values.append("NULL")
                        elif col_name in ["json_itens", "json_caixas"]:
                            # Converter para JSONB
                            values.append("%s::jsonb")
                            params.append(str(val))
                        elif col_name in ["dt_venda", "dt_expedicao", "nf_dt_compra", "nf_dt_emissao", "nf_dt_pedido"]:
                            # Converter para data no formato YYYY-MM-DD
                            if hasattr(val, 'strftime'):
                                date_str = val.strftime('%Y-%m-%d')
                                values.append("%s::date")
                                params.append(date_str)
                            else:
                                values.append("%s::date")
                                params.append(str(val))
                        elif col_name == "data_hora_processada":
                            # Converter para timestamp
                            if hasattr(val, 'strftime'):
                                timestamp_str = val.strftime('%Y-%m-%d %H:%M:%S')
                                values.append("%s::timestamp")
                                params.append(timestamp_str)
                            else:
                                values.append("%s::timestamp")
                                params.append(str(val))
                        elif col_name == "flag_web":
                            # Converter para booleano
                            values.append("%s::boolean")
                            params.append(bool(val))
                        else:
                            # Para strings e números
                            values.append("%s")
                            params.append(val)
                    
                    # Construir a query com placeholders
                    query = """
                        INSERT INTO proc_registro_venda 
                        (token_cliente, nf_destino_cpf_cnpj, cnpj_local_estoque, 
                         dt_venda, dt_expedicao, cnpj_transportadora, nf_numero, 
                         nf_natureza, nf_origem_nome_razaosocial, nf_origem_cpf_cnpj, 
                         nf_dt_compra, nf_dt_emissao, nf_dt_pedido, data_hora_processada, 
                         json_itens, json_caixas, flag_web, destino_pais, created_at, updated_at)
                        VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT DO NOTHING
                    """.format(*values)
                    
                    # Executar a consulta com parâmetros
                    try:
                        self.logger.debug(f"Executando query: {query} com parâmetros: {params}")
                        self._execute_jdbc_query(query, params=params)
                        
                        # Verificar se id está disponível na linha
                        registro_id = None
                        try:
                            if isinstance(row, dict):
                                registro_id = row.get('id')
                            else:  # Se for Row do PySpark
                                registro_id = row['id'] if 'id' in row.__fields__ else None
                        except Exception as e:
                            self.logger.warning(f"Não foi possível obter id: {str(e)}")
                        
                        # Atualizar flag de processamento apenas se tivermos o id
                        if registro_id is not None:
                            update_query = """
                                UPDATE bt_registro_venda 
                                SET flag_processada = true, 
                                    data_hora_processada = CURRENT_TIMESTAMP 
                                WHERE id = %s
                            """
                            self._execute_jdbc_query(update_query, params=[registro_id])
                        else:
                            self.logger.warning("Não foi possível atualizar flag_processada: id não disponível")
                    except Exception as e:
                        self.logger.error(f"Erro ao executar query: {str(e)}")
                        raise
                    
                self.logger.info(f"Inserção de {len(registros)} registros em proc_registro_venda concluída")
            else:
                self.logger.info("Nenhum registro para salvar em proc_registro_venda")
            
            # 2. Salva em proc_propriedades se houver dados de propriedades
            propriedades_columns = [
                'token_cliente', 'dt_abate', 'nr_unidade_abate', 'nr_lote_abate',
                'prod_nome_razaosocial', 'prod_cpf_cnpj', 'faz_nome', 'faz_cidade',
                'faz_estado', 'faz_nr_car', 'json_certificados', 'json_gtas', 'json_nfs'
            ]
            
            # Verifica se temos pelo menos alguns campos obrigatórios
            has_required_columns = all(col in available_columns for col in ['token_cliente', 'dt_abate', 'nr_unidade_abate', 'prod_cpf_cnpj'])
            
            if has_required_columns and df.filter(col("token_cliente").isNotNull()).count() > 0:
                self.logger.info("Preparando dados para proc_propriedades...")
                
                # Seleciona apenas as colunas necessárias que existem no DataFrame
                selected_columns = [col for col in propriedades_columns if col in available_columns]
                
                # Adiciona campos obrigatórios que podem não estar no DataFrame
                if ('dt_abate' not in selected_columns and 
                        'dt_abate' in available_columns):
                    selected_columns.append('dt_abate')
                if ('nr_unidade_abate' not in selected_columns and 
                        'nr_unidade_abate' in available_columns):
                    selected_columns.append('nr_unidade_abate')
                
                # Cria DataFrame com as colunas selecionadas
                proc_propriedades_df = df.select(selected_columns).distinct()
                
                # Log do progresso
                total = proc_propriedades_df.count()
                if total > 0:
                    self.logger.info(
                        f"Salvando {total} registros em proc_propriedades..."
                    )
                    
                    # Salva os dados
                    self._write_to_postgres(
                        df=proc_propriedades_df,
                        table="proc_propriedades",
                        mode="append",
                        options=self.db_options
                    )
                    self.logger.info(
                        f"Dados salvos com sucesso em proc_propriedades. "
                        f"Total: {total} registros"
                    )
                else:
                    self.logger.warning(
                        "Nenhum registro válido encontrado para salvar em "
                        "proc_propriedades"
                    )
            else:
                self.logger.warning(
                    "Dados insuficientes para salvar em proc_propriedades. "
                    "Verifique se os campos obrigatórios estão presentes."
                )
                self.logger.debug(
                    f"Colunas disponíveis: {available_columns}"
                )
                
            # 3. Salva em proc_animais se houver dados de animais
            if ("animal_id" in available_columns and 
                    df.filter(col("animal_id").isNotNull()).count() > 0):
                self.logger.info("Preparando dados para proc_animais...")
                
                # Mapeamento de colunas para a tabela de animais
                animais_columns = [
                    col("animal_id").alias("id"),
                    col("token_cliente")
                ]
                
                # Adiciona colunas opcionais
                if "animal_codigo" in available_columns:
                    animais_columns.append(
                        col("animal_codigo").alias("codigo")
                    )
                else:
                    animais_columns.append(
                        lit(None).cast(StringType()).alias("codigo")
                    )
                
                # Adiciona data de processamento
                if "dt_processamento" in available_columns:
                    animais_columns.append(
                        col("dt_processamento")
                    )
                else:
                    animais_columns.append(
                        lit("now()").cast(TimestampType())
                        .alias("dt_processamento")
                    )
                    
                # Filtra e seleciona os dados dos animais
                proc_animais_df = (
                    df.select(*animais_columns)
                    .filter(col("animal_id").isNotNull())
                    .distinct()
                )
                
                # Salva os dados dos animais
                self._write_to_postgres(
                    df=proc_animais_df,
                    table="proc_animais",
                    mode="append",
                    options=self.db_options
                )
            
            # 4. Salva em proc_cx_venda_propriedade (relacionamento N:N)
            if ("propriedade_id" in available_columns and 
                    "registro_id" in available_columns):
                self.logger.info(
                    "Preparando dados para proc_cx_venda_propriedade..."
                )
                
                # Verifica se há registros para processar
                if (df.filter(
                        col("propriedade_id").isNotNull() & 
                        col("registro_id").isNotNull()
                    ).count() > 0):
                    relacionamento_columns = [
                        col("propriedade_id"),
                        col("registro_id").alias("registro_venda_id")
                    ]
                    
                    # Prepara o DataFrame com os dados do relacionamento
                    proc_relacionamento_df = (
                        df.select(relacionamento_columns)
                        .distinct()
                    )
                    total = proc_relacionamento_df.count()
                    
                    if total > 0:
                        self.logger.info(
                            f"Salvando {total} registros em "
                            "proc_cx_venda_propriedade..."
                        )
                        
                        # Salva os dados do relacionamento
                        self._write_to_postgres(
                            df=proc_relacionamento_df,
                            table="proc_cx_venda_propriedade",
                            mode="append",
                            options=self.db_options
                        )
                        self.logger.info(
                            f"Dados salvos com sucesso em "
                            f"proc_cx_venda_propriedade. Total: {total} registros"
                        )
                    else:
                        self.logger.warning(
                            "Nenhum registro válido encontrado para salvar em "
                            "proc_cx_venda_propriedade"
                        )
                else:
                    self.logger.warning(
                        "Sem registros válidos para processar em "
                        "proc_cx_venda_propriedade"
                    )
                
            self.logger.info("Processo de salvamento concluído com sucesso")
            
        except Exception as e:
            self.logger.error(f"Erro ao salvar resultados: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())
            raise
            
    def _marcar_registros_processados(self, registro_ids: List[str]) -> None:
        """Marca registros de venda como processados.

        Args:
            registro_ids: Lista de IDs dos registros a marcar como processados
        """
        if not registro_ids:
            return
            
        # Formata a lista de IDs para uso na query SQL
        ids_str = "', '".join(registro_ids)
        
        # Atualiza a flag_processada para true
        update_query = f"UPDATE bt_registro_venda SET flag_processada = true WHERE id IN ('{ids_str}')"
        
        # Executa a query
        self._execute_jdbc_query(update_query, return_df=False)
