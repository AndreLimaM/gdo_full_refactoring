#!/usr/bin/env python3
"""Script para processar rastreabilidade entre caixas e desossa."""

import argparse
from datetime import datetime
import logging
import os
import sys

from pyspark.sql import SparkSession

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.db_connection import DatabaseConnection
from src.processors.caixa_processor import CaixaProcessor
from src.processors.desossa_processor import DesossaProcessor

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Processa rastreabilidade entre caixas e desossa'
    )
    
    # Parâmetros de processamento
    parser.add_argument(
        '--token-cliente',
        required=True,
        help='Token do cliente'
    )
    parser.add_argument(
        '--dt-producao-de',
        required=False,
        help='Data inicial de produção (YYYY-MM-DD). Se não especificada, processa todos os períodos.'
    )
    parser.add_argument(
        '--dt-producao-ate',
        required=False,
        help='Data final de produção (YYYY-MM-DD). Se não especificada, processa todos os períodos.'
    )
    parser.add_argument(
        '--todos-periodos',
        action='store_true',
        help='Processa caixas de todos os períodos, ignorando datas'
    )
    
    # Modo de processamento
    parser.add_argument(
        '--modo',
        choices=['completo', 'desossa', 'propriedades', 'proc-caixas'],
        default='completo',
        help='Modo de processamento: completo (todas as etapas), desossa (apenas vinculação com desossa), '
             'propriedades (apenas vinculação com propriedades/animais), proc-caixas (apenas processamento final)'
    )
    
    # Opção para resetar flags
    parser.add_argument(
        '--reset-flags',
        action='store_true',
        help='Reseta todas as flags de processamento antes de iniciar. '
             'ATENÇÃO: Isso fará com que TODOS os registros sejam reprocessados, mesmo os já processados anteriormente.'
    )
    
    # Opção para atualizar flag_rastreavel
    parser.add_argument(
        '--update-rastreavel',
        action='store_true',
        default=True,
        help='Atualiza flag_rastreavel para caixas com rastreabilidade completa.'
    )
    
    parser.add_argument(
        '--no-update-rastreavel',
        action='store_false',
        dest='update_rastreavel',
        help='Não atualiza flag_rastreavel para caixas com rastreabilidade completa.'
    )
    
    # Parâmetros de conexão
    parser.add_argument('--host', default='34.48.11.43', help='Host do banco')
    parser.add_argument('--port', default='5432', help='Porta do banco')
    parser.add_argument('--database', default='db_eco_tcbf_25', help='Nome do banco')
    parser.add_argument('--user', default='db_eco_tcbf_25_user', help='Usuário do banco')
    parser.add_argument('--password', default='5HN33PHKjXcLTz3tBC', help='Senha do banco')
    
    return parser.parse_args()


def main():
    """Função principal do script."""
    args = parse_args()
    
    # Cria sessão Spark
    # Usa caminho absoluto para o driver JDBC
    jar_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dependencies", "jars", "postgresql-42.2.29.jre7.jar"))
    
    spark = SparkSession.builder \
        .appName("ProcessarRastreabilidadeCaixasDesossa") \
        .config("spark.jars", jar_path) \
        .getOrCreate()

    # Configura conexão com o banco usando DatabaseConnection
    db = DatabaseConnection(
        host=args.host,
        database=args.database,
        user=args.user,
        password=args.password,
        port=args.port
    )
    
    # Testa a conexão antes de prosseguir
    if not db.test_connection(spark):
        logger.error("Falha ao conectar com o banco de dados")
        sys.exit(1)
        
    # Obtém opções JDBC para uso no processamento
    db_options = db.get_jdbc_options()

    try:
        # Inicializa processadores
        caixa_processor = CaixaProcessor(spark, db_options)
        desossa_processor = DesossaProcessor(spark, db_options)

        # Prepara as datas se fornecidas
        dt_producao_de = None
        dt_producao_ate = None
        if args.dt_producao_de and args.dt_producao_ate:
            dt_producao_de = datetime.strptime(args.dt_producao_de, "%Y-%m-%d").date()
            dt_producao_ate = datetime.strptime(args.dt_producao_ate, "%Y-%m-%d").date()
        elif args.dt_producao_de or args.dt_producao_ate:
            logger.error("Se uma data for especificada, ambas (dt-producao-de e dt-producao-ate) devem ser fornecidas.")
            sys.exit(1)
        
        # Executa o processamento de acordo com o modo selecionado
        if args.modo == 'completo':
            logger.info(f"Iniciando processamento completo para token: {args.token_cliente}")
            
            if args.todos_periodos or (not dt_producao_de and not dt_producao_ate):
                # Processa todas as caixas não processadas de todos os períodos
                caixa_processor.process_all(token_cliente=args.token_cliente, reset_flags=args.reset_flags, update_rastreavel=args.update_rastreavel)
                print("Processamento completo concluído para todos os períodos.")
            else:
                # Processa caixas do período especificado
                caixa_processor.process(
                    token_cliente=args.token_cliente,
                    dt_producao_de=dt_producao_de,
                    dt_producao_ate=dt_producao_ate,
                    reset_flags=args.reset_flags
                )
                print(f"Processamento completo concluído. Período: {args.dt_producao_de} a {args.dt_producao_ate}")
                
        elif args.modo == 'desossa':
            logger.info(f"Iniciando vinculação com desossa para token: {args.token_cliente}")
            caixa_processor.gerar_rastreabilidade_caixas_desossa(
                token_cliente=args.token_cliente,
                dt_producao_de=args.dt_producao_de,
                dt_producao_ate=args.dt_producao_ate
            )
            print("Vinculação com desossa concluída.")
            
        elif args.modo == 'propriedades':
            logger.info(f"Iniciando vinculação com propriedades/animais para token: {args.token_cliente}")
            caixa_processor.gerar_rastreabilidade_caixas_propriedades(
                token_cliente=args.token_cliente,
                dt_producao_de=args.dt_producao_de,
                dt_producao_ate=args.dt_producao_ate
            )
            print("Vinculação com propriedades/animais concluída.")
            
        elif args.modo == 'proc-caixas':
            logger.info(f"Iniciando processamento final para proc_caixas para token: {args.token_cliente}")
            
            # Carrega caixas que foram vinculadas com sucesso (pelo menos com desossa)
            query = f"""
            SELECT *
            FROM bt_caixas
            WHERE token_cliente = '{args.token_cliente}'
              AND flag_desossa = true
            """
            if dt_producao_de and dt_producao_ate:
                query += f" AND dt_producao BETWEEN '{dt_producao_de}' AND '{dt_producao_ate}'"
            
            df_caixas = caixa_processor._execute_jdbc_query(query, return_df=True)
            total_caixas = df_caixas.count()
            logger.info(f"Carregadas {total_caixas} caixas vinculadas para processamento final")
            
            if total_caixas == 0:
                logger.info("Não há caixas vinculadas para processar")
                return
            
            # Processa em lotes para evitar problemas de memória
            batch_size = 1000
            total_batches = (total_caixas + batch_size - 1) // batch_size
            
            logger.info(f"Processando {total_caixas} caixas para a tabela proc_caixas em {total_batches} lotes")
            
            # Processa cada lote
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, total_caixas)
                
                logger.info(f"Processando lote {batch_num + 1}/{total_batches} (registros {start_idx + 1} a {end_idx})")
                
                # Obtém o lote atual
                batch_df = df_caixas.limit(end_idx).subtract(df_caixas.limit(start_idx))
                batch_count = batch_df.count()
                
                if batch_count == 0:
                    continue
                
                # Salva resultados deste lote na tabela proc_caixas
                logger.info(f"Salvando resultados do lote {batch_num + 1} na tabela proc_caixas")
                caixa_processor._save_results(batch_df)
                
                # Marca caixas como processadas
                caixa_ids = [str(row.id) for row in batch_df.select('id').collect()]
                if caixa_ids:
                    caixa_processor.mark_as_processed(
                        caixa_ids,
                        {
                            'flag_processamento': True
                        }
                    )
            
            print("Processamento final para proc_caixas concluído.")
        
        else:
            logger.error(f"Modo de processamento inválido: {args.modo}")
            sys.exit(1)

    except Exception as e:
        print(f"Erro no processamento: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
