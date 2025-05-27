# Estrutura de Cu00f3digo para Processamento de Payloads GDO

Este documento descreve a estrutura de cu00f3digo criada no bucket GCS para o processamento de payloads do GDO.

## Visu00e3o Geral

A estrutura segue a abordagem medalha (camadas) para processamento de dados:

1. **Raw**: Processamento de dados brutos sem modificau00e7u00f5es importantes
2. **Trusted**: Processamento inicial, sanitizau00e7u00e3o, tipagem e tratamento de inconsistu00eancias
3. **Service**: Gerau00e7u00e3o de dados para a camada de consumo pela aplicau00e7u00e3o

## Estrutura de Pastas

```
datalake/code/
u251cu2500u2500 raw/                    # Camada Raw
u2502   u251cu2500u2500 movimentacoes/      # Processamento de movimentau00e7u00f5es
u2502   u251cu2500u2500 caixas/             # Processamento de caixas
u2502   u251cu2500u2500 desossas/           # Processamento de desossas
u2502   u251cu2500u2500 animais/            # Processamento de animais
u2502   u251cu2500u2500 escalas/            # Processamento de escalas
u2502   u2514u2500u2500 common/             # Mu00f3dulos comuns compartilhados
u251cu2500u2500 trusted/                # Camada Trusted
u2502   u251cu2500u2500 movimentacoes/      # Processamento de movimentau00e7u00f5es
u2502   u251cu2500u2500 caixas/             # Processamento de caixas
u2502   u251cu2500u2500 desossas/           # Processamento de desossas
u2502   u251cu2500u2500 animais/            # Processamento de animais
u2502   u251cu2500u2500 escalas/            # Processamento de escalas
u2502   u2514u2500u2500 common/             # Mu00f3dulos comuns compartilhados
u2514u2500u2500 service/                # Camada Service
    u251cu2500u2500 movimentacoes/      # Processamento de movimentau00e7u00f5es
    u251cu2500u2500 caixas/             # Processamento de caixas
    u251cu2500u2500 desossas/           # Processamento de desossas
    u251cu2500u2500 animais/            # Processamento de animais
    u251cu2500u2500 escalas/            # Processamento de escalas
    u2514u2500u2500 common/             # Mu00f3dulos comuns compartilhados
```

## Conteu00fado das Pastas

Cada pasta de tipo de payload contu00e9m:

1. **README.md**: Documentau00e7u00e3o especu00edfica para o processamento daquele tipo de payload
2. **__init__.py**: Arquivo de inicializau00e7u00e3o do mu00f3dulo Python

## Execuu00e7u00e3o dos Scripts

Os scripts su00e3o projetados para serem executados pelo Dataproc no ambiente GCP.

## Fluxo de Processamento

1. Os dados brutos su00e3o processados pelos scripts da camada Raw
2. Os dados da camada Raw su00e3o processados pelos scripts da camada Trusted
3. Os dados da camada Trusted su00e3o processados pelos scripts da camada Service
4. Os dados da camada Service su00e3o disponibilizados para consumo pela aplicau00e7u00e3o
