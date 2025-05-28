#!/bin/bash

# Script para executar testes unitu00e1rios para os scripts de processamento

echo "Executando testes unitu00e1rios para os scripts de processamento..."

# Definir variu00e1veis de ambiente para os testes
export PYTHONPATH="$PYTHONPATH:$(pwd)/utils/dataproc/scripts"

# Instalar dependu00eancias necessu00e1rias para os testes
pip install pytest pytest-mock pyspark pandas

# Executar os testes unitu00e1rios com pytest
python -m pytest utils/dataproc/tests/unit/test_processar_json_para_sql.py -v

# Verificar o resultado dos testes
if [ $? -eq 0 ]; then
    echo "Todos os testes unitu00e1rios passaram com sucesso!"
else
    echo "Alguns testes unitu00e1rios falharam. Verifique os logs acima para mais detalhes."
    exit 1
fi
