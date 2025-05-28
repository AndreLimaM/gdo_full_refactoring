# Teste de Conexão entre Dataproc e Cloud SQL

## Visão Geral

Este documento descreve como testar a conexão entre um cluster Dataproc e uma instância Cloud SQL PostgreSQL. O teste cria um cluster Dataproc temporário na mesma região do Cloud SQL (us-east4), configura a rede para permitir a comunicação entre eles e executa um job PySpark para verificar a conexão.

## Pré-requisitos

- Google Cloud SDK (gcloud) instalado e configurado
- Python 3.6 ou superior
- Biblioteca google-cloud-dataproc
- Biblioteca google-cloud-storage
- Biblioteca python-dotenv
- Permissões para criar clusters Dataproc e acessar buckets GCS

## Configuração

Os scripts utilizam as seguintes configurações padrão:

- **Região**: us-east4 (mesma região do Cloud SQL)
- **Zona**: us-east4-a
- **Rede**: default
- **Sub-rede**: default
- **Nome do Cluster**: gdo-dataproc-teste
- **Bucket GCS**: repo-dev-gdo-carga

## Credenciais do Cloud SQL

O teste utiliza as seguintes credenciais para conectar ao Cloud SQL:

- **Host**: 10.98.169.3
- **Banco de Dados**: db_eco_tcbf_25
- **Usuário**: db_eco_tcbf_25_user
- **Senha**: 5HN33PHKjXcLTz3tBC

## Como Executar o Teste

### Usando o Script Shell (Recomendado)

O script shell facilita a execução do teste, verificando dependências e configurando o ambiente automaticamente.

```bash
# Navegar até a raiz do projeto
cd /caminho/para/gdo_full_refactoring

# Executar o script de teste
./utils/dataproc/executar_teste_conexao.sh --projeto seu-projeto-gcp
```

### Opções do Script Shell

O script shell aceita as seguintes opções:

- `-p, --projeto ID`: ID do projeto GCP
- `-r, --regiao REGIAO`: Região do cluster (padrão: us-east4)
- `-z, --zona ZONA`: Zona do cluster (padrão: us-east4-a)
- `-c, --cluster NOME`: Nome do cluster (padrão: gdo-dataproc-teste)
- `-b, --bucket NOME`: Nome do bucket GCS (padrão: repo-dev-gdo-carga)
- `-n, --rede NOME`: Nome da rede VPC (padrão: default)
- `-s, --subrede NOME`: Nome da sub-rede (padrão: default)
- `-k, --manter`: Não exclui o cluster após os testes
- `-h, --ajuda`: Exibe a mensagem de ajuda

### Usando o Script Python Diretamente

Você também pode executar o script Python diretamente para mais controle sobre o processo:

```bash
# Navegar até a raiz do projeto
cd /caminho/para/gdo_full_refactoring

# Executar o script Python
python3 utils/dataproc/testar_conexao_dataproc_cloudsql.py \
  --project-id seu-projeto-gcp \
  --region us-east4 \
  --zone us-east4-a \
  --cluster-name gdo-dataproc-teste \
  --bucket-name repo-dev-gdo-carga \
  --network default \
  --subnetwork default
```

## O que o Teste Faz

1. **Preparação**:
   - Faz upload de um script de inicialização para instalar o driver PostgreSQL
   - Faz upload de um job PySpark para testar a conexão

2. **Criação do Cluster**:
   - Cria um cluster Dataproc na região us-east4
   - Configura o cluster com as bibliotecas necessárias
   - Executa o script de inicialização para instalar o driver PostgreSQL

3. **Teste de Conexão**:
   - Executa um job PySpark que tenta conectar ao Cloud SQL
   - Verifica se a conexão foi estabelecida com sucesso
   - Verifica se a tabela bt_animais existe
   - Exibe a estrutura da tabela e o número de registros

4. **Limpeza**:
   - Exclui o cluster após os testes (a menos que a opção --manter seja especificada)

## Interpretando os Resultados

O teste é considerado bem-sucedido se:

1. O cluster Dataproc for criado com sucesso
2. O job PySpark conseguir estabelecer conexão com o Cloud SQL
3. A tabela bt_animais for encontrada no banco de dados

Se o teste falhar, verifique:

- As credenciais do Cloud SQL estão corretas
- A rede está configurada corretamente para permitir a comunicação
- O Cloud SQL está aceitando conexões externas
- O driver PostgreSQL foi instalado corretamente

## Solução de Problemas

### Erro de Conexão com o Cloud SQL

Se o teste falhar com um erro de conexão, verifique:

1. **Configuração de Rede**:
   - O Cloud SQL está na mesma rede VPC ou tem peering configurado
   - As regras de firewall permitem conexões na porta 5432

2. **Credenciais**:
   - As credenciais do banco de dados estão corretas
   - O usuário tem permissão para acessar o banco de dados

3. **IP Autorizado**:
   - O IP do cluster Dataproc está autorizado a acessar o Cloud SQL
   - A opção "internal_ip_only" está configurada corretamente

### Erro na Criação do Cluster

Se o cluster não for criado com sucesso, verifique:

1. **Permissões**:
   - Sua conta tem permissão para criar clusters Dataproc
   - Sua conta tem permissão para acessar o bucket GCS

2. **Cotas**:
   - Você tem cota suficiente para criar VMs na região especificada

3. **Configuração**:
   - A rede e sub-rede especificadas existem
   - A região e zona especificadas são válidas

## Próximos Passos

Após confirmar que a conexão entre o Dataproc e o Cloud SQL funciona corretamente, você pode:

1. Implementar jobs Spark para processar dados de animais
2. Configurar pipelines de processamento de dados
3. Integrar com o sistema de processamento GDO existente
