# Testes para Integração Dataproc e Cloud SQL

Este diretório contém testes para validar a integração entre o Google Dataproc e o Cloud SQL PostgreSQL, bem como os scripts de processamento de dados JSON.

## Tipos de Testes

Implementamos dois tipos de testes para garantir a qualidade e confiabilidade do código:

### 1. Testes de Integração

Os testes de integração validam a comunicação e o funcionamento conjunto entre diferentes sistemas (Dataproc e Cloud SQL).

**Características:**
- Testam a interação entre componentes reais
- Utilizam infraestrutura real (clusters Dataproc, banco de dados Cloud SQL)
- Verificam a conectividade e o fluxo de dados completo
- São mais lentos e consomem mais recursos

**Arquivos de teste de integração:**
- `executar_teste_conectividade.sh`: Testa a conectividade TCP entre Dataproc e Cloud SQL
- `executar_teste_conexao_simples.sh`: Testa a conexão JDBC simplificada
- `testar_conexao_dataproc_cloudsql.py`: Teste completo de conexão e operações SQL

### 2. Testes Unitários

Os testes unitários validam o funcionamento isolado de funções e métodos específicos, sem dependências externas.

**Características:**
- Testam uma única unidade de código isoladamente
- Utilizam mocks e stubs para simular dependências
- São rápidos e leves
- Facilitam a identificação precisa de problemas

**Arquivos de teste unitário:**
- `unit/test_processar_json_para_sql.py`: Testes unitários para o script de processamento de JSON

## Execução dos Testes

### Executar Testes de Integração

Para executar os testes de integração, use os scripts específicos:

```bash
# Teste de conectividade básica
./executar_teste_conectividade.sh --projeto seu-projeto-id --bucket seu-bucket

# Teste de conexão JDBC simplificada
./executar_teste_conexao_simples.sh --projeto seu-projeto-id --bucket seu-bucket
```

### Executar Testes Unitários

Para executar os testes unitários, use o script:

```bash
./executar_testes_unitarios.sh
```

## Boas Práticas

1. **Mantenha os testes atualizados**: Sempre atualize os testes quando modificar o código.
2. **Execute os testes regularmente**: Inclua a execução dos testes em seu fluxo de trabalho.
3. **Combine ambos os tipos de teste**: Testes unitários e de integração se complementam.
4. **Isole os ambientes de teste**: Use ambientes isolados para evitar interferências.
5. **Documente os resultados**: Mantenha registros dos resultados dos testes para referência futura.

## Expansão dos Testes

Para expandir a cobertura de testes, considere:

1. Adicionar mais testes unitários para outras funções
2. Implementar testes de carga para avaliar o desempenho
3. Criar testes para cenários de falha e recuperação
4. Automatizar a execução dos testes em um pipeline CI/CD
