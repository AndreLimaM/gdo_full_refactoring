# Estrutura de Desenvolvimento para o Projeto GDO

Este documento descreve a estrutura de pastas de desenvolvimento criada no bucket GCS.

## Visão Geral

A estrutura de desenvolvimento contém scripts, utilitários e arquivos temporários utilizados durante o desenvolvimento do projeto, mas que não farão parte do código de produção final.

## Estrutura de Pastas

```
datalake/dev/
├── tests/                 # Scripts de testes unitários e de integração
├── utils/                 # Utilitários para desenvolvimento e depuração
├── samples/               # Arquivos de exemplo e dados de teste
├── notebooks/             # Jupyter notebooks para análise exploratória
└── temp/                  # Arquivos temporários e experimentos
```

## Propósito

Esta estrutura foi criada para:

1. **Separar código de desenvolvimento do código de produção**: Manter o código de produção limpo e organizado
2. **Facilitar a remoção de código temporário**: Ao entregar o projeto, esta pasta pode ser facilmente removida
3. **Organizar scripts de teste e utilitários**: Manter os scripts de teste e utilitários em um local centralizado

## Observação

Esta pasta e seu conteúdo podem ser removidos ao entregar o projeto final, pois não fazem parte do código de produção.
