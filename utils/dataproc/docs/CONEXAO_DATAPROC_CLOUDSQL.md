# Conexão entre Dataproc e Cloud SQL

Este documento descreve como configurar e testar a conexão entre um cluster Dataproc e uma instância Cloud SQL PostgreSQL.

## Pré-requisitos

- Projeto GCP configurado (`development-439017`)
- Instância Cloud SQL PostgreSQL criada (`gdo-cloudsql-instance`)
- Bucket GCS para armazenamento temporário (`repo-dev-gdo-carga`)
- Permissões adequadas para criar clusters Dataproc e acessar o Cloud SQL

## Configuração

A conexão entre o Dataproc e o Cloud SQL pode ser feita de duas maneiras:

1. **Conexão direta usando IP privado**: Recomendada para ambientes de produção
2. **Conexão usando Cloud SQL Auth Proxy**: Mais segura, mas requer configuração adicional

Nossos testes mostraram que a **conexão direta usando IP privado** é a mais confiável e eficiente para o nosso ambiente.

### Conexão usando IP privado (Recomendada)

Para conectar o Dataproc ao Cloud SQL usando IP privado, siga estas etapas:

1. Certifique-se de que o Dataproc e o Cloud SQL estejam na mesma rede VPC
2. Configure as regras de firewall para permitir a comunicação entre eles
3. Use o IP privado do Cloud SQL nas configurações de conexão

**Configurações para o ambiente atual:**

- IP privado do Cloud SQL: `10.98.169.3`
- Porta: `5432`
- Banco de dados: `db_eco_tcbf_25`
- Usuário: `db_eco_tcbf_25_user`
- Senha: `5HN33PHKjXcLTz3tBC`

### Conexão usando Cloud SQL Auth Proxy

Embora tenhamos tentado esta abordagem, encontramos desafios com a instalação do Cloud SQL Auth Proxy em ambientes Dataproc sem acesso à internet. Se for necessário usar esta abordagem, recomendamos:

1. Pré-empacotar o Cloud SQL Auth Proxy em uma imagem personalizada do Dataproc
2. Ou garantir que o cluster Dataproc tenha acesso à internet para baixar e instalar o proxy

## Testes Realizados

Realizamos testes bem-sucedidos de conexão entre o Dataproc e o Cloud SQL usando o script `testar_conexao_dataproc_cloudsql.py` disponível na pasta `tests`.

```bash
python3 testar_conexao_dataproc_cloudsql.py
```

Este script:

1. Cria um cluster Dataproc temporário na região `us-east4`
2. Instala o driver PostgreSQL JDBC no cluster
3. Submete um job PySpark para testar a conexão com o Cloud SQL
4. Verifica a conexão executando uma consulta simples na tabela `bt_animais`
5. Exclui o cluster após o teste

### Resultados dos Testes

Os testes confirmaram que:

1. A conexão entre o Dataproc e o Cloud SQL está funcionando corretamente
2. O método de conexão usando o IP privado (10.98.169.3) é eficaz
3. As credenciais do banco de dados estão corretas
4. A configuração de rede permite a comunicação entre os serviços
5. A tabela `bt_animais` está acessível e contém dados

## Exemplo de Código para Conexão

```python
# Configurar propriedades de conexão com o banco de dados
db_properties = {
    "url": "jdbc:postgresql://10.98.169.3:5432/db_eco_tcbf_25",
    "user": "db_eco_tcbf_25_user",
    "password": "5HN33PHKjXcLTz3tBC"
}

# Ler dados de uma tabela
df = spark.read \
    .format("jdbc") \
    .option("driver", "org.postgresql.Driver") \
    .option("url", db_properties["url"]) \
    .option("dbtable", "bt_animais") \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .load()
```

## Troubleshooting

Se encontrar problemas de conexão, verifique:

1. Regras de firewall: Certifique-se de que a porta 5432 esteja aberta para comunicação entre o Dataproc e o Cloud SQL
2. Configurações de rede VPC: Verifique se ambos os serviços estão na mesma rede VPC
3. Credenciais: Confirme que as credenciais do banco de dados estão corretas
4. Logs: Verifique os logs do Dataproc e do Cloud SQL para identificar possíveis erros
5. Conectividade de rede: Use comandos como `ping` e `telnet` para testar a conectividade básica

## Próximos Passos

Com a conexão estabelecida e testada, podemos prosseguir com a implementação do processamento de dados JSON para o Cloud SQL, utilizando a abordagem de conexão direta com IP privado.

## Referências

- [Documentação do Dataproc](https://cloud.google.com/dataproc)
- [Documentação do Cloud SQL](https://cloud.google.com/sql)
- [Cloud SQL Auth Proxy](https://cloud.google.com/sql/docs/postgres/sql-proxy)
- [Conectando o Dataproc ao Cloud SQL](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example)
