# Estrutura de Pastas para Processamento de Payloads GDO

Este documento descreve a estrutura de pastas criada no bucket GCS para o processamento de payloads do GDO.

## Visão Geral

A estrutura foi projetada para facilitar o controle físico dos arquivos a processar e dos que já foram processados:

```
datalake/payload_data/
├── pending/               # Arquivos pendentes de processamento
│   ├── movimentacoes/     # Payloads de movimentações
│   ├── caixas/            # Payloads de caixas
│   ├── desossas/          # Payloads de desossas
│   ├── animais/           # Payloads de animais
│   └── escalas/           # Payloads de escalas
└── done/                  # Arquivos já processados
    ├── movimentacoes/     # Payloads processados de movimentações
    ├── caixas/            # Payloads processados de caixas
    ├── desossas/          # Payloads processados de desossas
    ├── animais/           # Payloads processados de animais
    └── escalas/           # Payloads processados de escalas
```

## Fluxo de Processamento

1. Os arquivos de dados são recebidos nas respectivas pastas dentro de `datalake/payload_data/pending/`
2. O sistema processa diariamente os arquivos encontrados nestas pastas
3. Após o processamento, os arquivos são movidos para a pasta correspondente dentro de `datalake/payload_data/done/`

Esta estrutura permite um controle eficiente dos arquivos a processar e dos que já foram processados, facilitando o monitoramento e a recuperação em caso de falhas.

## Tipos de Payloads

- **Movimentações**: Dados de movimentação de animais
- **Caixas**: Informações sobre caixas de produtos
- **Desossas**: Dados do processo de desossa
- **Animais**: Informações sobre os animais
- **Escalas**: Dados de escalas de produção

## Observações Importantes

- Cada pasta contém um arquivo README.md para evitar que o GCP exclua pastas vazias
- Os arquivos README.md também servem como documentação da estrutura e propósito de cada pasta
