# Testes para Integrau00e7u00e3o Dataproc e Cloud SQL

Este diretu00f3rio contu00e9m testes para validar a integrau00e7u00e3o entre o Google Dataproc e o Cloud SQL PostgreSQL, bem como os scripts de processamento de dados JSON.

## Tipos de Testes

Implementamos dois tipos de testes para garantir a qualidade e confiabilidade do cu00f3digo:

### 1. Testes de Integrau00e7u00e3o

Os testes de integrau00e7u00e3o validam a comunicau00e7u00e3o e o funcionamento conjunto entre diferentes sistemas (Dataproc e Cloud SQL).

**Caracteru00edsticas:**
- Testam a interau00e7u00e3o entre componentes reais
- Utilizam infraestrutura real (clusters Dataproc, banco de dados Cloud SQL)
- Verificam a conectividade e o fluxo de dados completo
- Su00e3o mais lentos e consomem mais recursos

**Arquivos de teste de integrau00e7u00e3o:**
- `executar_teste_conectividade.sh`: Testa a conectividade TCP entre Dataproc e Cloud SQL
- `executar_teste_conexao_simples.sh`: Testa a conexu00e3o JDBC simplificada
- `testar_conexao_dataproc_cloudsql.py`: Teste completo de conexu00e3o e operau00e7u00f5es SQL

### 2. Testes Unitu00e1rios

Os testes unitu00e1rios validam o funcionamento isolado de funu00e7u00f5es e mu00e9todos especu00edficos, sem dependu00eancias externas.

**Caracteru00edsticas:**
- Testam uma u00fanica unidade de cu00f3digo isoladamente
- Utilizam mocks e stubs para simular dependu00eancias
- Su00e3o ru00e1pidos e leves
- Facilitam a identificau00e7u00e3o precisa de problemas

**Arquivos de teste unitu00e1rio:**
- `unit/test_processar_json_para_sql.py`: Testes unitu00e1rios para o script de processamento de JSON

## Execuu00e7u00e3o dos Testes

### Executar Testes de Integrau00e7u00e3o

Para executar os testes de integrau00e7u00e3o, use os scripts especu00edficos:

```bash
# Teste de conectividade bu00e1sica
./executar_teste_conectividade.sh --projeto seu-projeto-id --bucket seu-bucket

# Teste de conexu00e3o JDBC simplificada
./executar_teste_conexao_simples.sh --projeto seu-projeto-id --bucket seu-bucket
```

### Executar Testes Unitu00e1rios

Para executar os testes unitu00e1rios, use o script:

```bash
./executar_testes_unitarios.sh
```

## Boas Pru00e1ticas

1. **Mantenha os testes atualizados**: Sempre atualize os testes quando modificar o cu00f3digo.
2. **Execute os testes regularmente**: Inclua a execuu00e7u00e3o dos testes em seu fluxo de trabalho.
3. **Combine ambos os tipos de teste**: Testes unitu00e1rios e de integrau00e7u00e3o se complementam.
4. **Isole os ambientes de teste**: Use ambientes isolados para evitar interferu00eancias.
5. **Documente os resultados**: Mantenha registros dos resultados dos testes para referu00eancia futura.

## Expansu00e3o dos Testes

Para expandir a cobertura de testes, considere:

1. Adicionar mais testes unitu00e1rios para outras funu00e7u00f5es
2. Implementar testes de carga para avaliar o desempenho
3. Criar testes para cenu00e1rios de falha e recuperau00e7u00e3o
4. Automatizar a execuu00e7u00e3o dos testes em um pipeline CI/CD
