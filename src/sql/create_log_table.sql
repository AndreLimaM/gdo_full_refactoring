-- Script para criar a tabela de log de processamento

CREATE TABLE IF NOT EXISTS log_processamento (
    id SERIAL PRIMARY KEY,
    data_hora TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    tipo_payload VARCHAR(50) NOT NULL,
    camada VARCHAR(20) NOT NULL,
    nome_arquivo VARCHAR(255),
    status VARCHAR(20) NOT NULL,
    mensagem TEXT,
    detalhes JSONB,
    duracao_ms INTEGER,
    registros_processados INTEGER,
    registros_invalidos INTEGER,
    usuario VARCHAR(100),
    hostname VARCHAR(100),
    versao_script VARCHAR(20)
);

-- u00cdndices para otimizar consultas
CREATE INDEX IF NOT EXISTS idx_log_processamento_tipo_payload ON log_processamento(tipo_payload);
CREATE INDEX IF NOT EXISTS idx_log_processamento_data_hora ON log_processamento(data_hora);
CREATE INDEX IF NOT EXISTS idx_log_processamento_status ON log_processamento(status);

-- Comentu00e1rios para documentau00e7u00e3o
COMMENT ON TABLE log_processamento IS 'Tabela para registro de logs de processamento de payloads';
COMMENT ON COLUMN log_processamento.id IS 'Identificador u00fanico do registro de log';
COMMENT ON COLUMN log_processamento.data_hora IS 'Data e hora do registro';
COMMENT ON COLUMN log_processamento.tipo_payload IS 'Tipo de payload (animais, caixas, etc.)';
COMMENT ON COLUMN log_processamento.camada IS 'Camada de processamento (raw, trusted, service)';
COMMENT ON COLUMN log_processamento.nome_arquivo IS 'Nome do arquivo processado';
COMMENT ON COLUMN log_processamento.status IS 'Status do processamento (sucesso, erro, alerta)';
COMMENT ON COLUMN log_processamento.mensagem IS 'Mensagem descritiva do log';
COMMENT ON COLUMN log_processamento.detalhes IS 'Detalhes adicionais em formato JSON';
COMMENT ON COLUMN log_processamento.duracao_ms IS 'Durau00e7u00e3o do processamento em milissegundos';
COMMENT ON COLUMN log_processamento.registros_processados IS 'Quantidade de registros processados';
COMMENT ON COLUMN log_processamento.registros_invalidos IS 'Quantidade de registros invu00e1lidos';
COMMENT ON COLUMN log_processamento.usuario IS 'Usuu00e1rio que executou o processamento';
COMMENT ON COLUMN log_processamento.hostname IS 'Nome da mu00e1quina onde o processamento foi executado';
COMMENT ON COLUMN log_processamento.versao_script IS 'Versu00e3o do script de processamento';
