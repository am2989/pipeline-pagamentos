# Pipeline de Pagamentos — Change Data Capture (CDC)

Uma demonstração local de uma pipeline de **Change Data Capture (CDC)** que captura alterações numa base de dados PostgreSQL e as transmite em tempo real para Apache Kafka, utilizando Debezium.

O projeto demonstra como implementar replicação lógica, capturar operações de inserção, atualização e eliminação de registos e disponibilizar esses eventos para consumo através de tópicos Kafka.

---

## 1. Visão geral do projeto

Esta pipeline permite acompanhar as alterações efetuadas nas tabelas `public.accounts` e `public.payments`, armazenadas no PostgreSQL, e transmitir os respetivos eventos para o Apache Kafka através do Debezium.

O objetivo é demonstrar o funcionamento de uma arquitetura de integração de dados baseada em CDC, permitindo identificar e acompanhar as alterações realizadas numa base de dados sem recorrer a consultas periódicas para detetar diferenças.

A implementação contempla:

- Captura inicial dos registos existentes (snapshot).
- Deteção de novas inserções (INSERT).
- Deteção de atualizações (UPDATE).
- Deteção de eliminações (DELETE).
- Publicação dos eventos no Apache Kafka.
- Monitorização do estado do conector Debezium.

Este projeto foi desenvolvido para fins de aprendizagem, experimentação e demonstração técnica. Não se destina a utilização em ambientes de produção.

## 2. Arquitetura da solução

A arquitetura utiliza PostgreSQL como origem dos dados, Debezium como ferramenta de captura de alterações e Apache Kafka como plataforma de transmissão dos eventos.

### Fluxo de dados

```text
PostgreSQL 16 (payments_db)
  └─ replicação lógica (WAL / pgoutput)
       └─ conector PostgreSQL do Debezium
            └─ Apache Kafka
                 ├─ dbserver1.public.accounts
                 └─ dbserver1.public.payments
```

Os serviços são executados em containers Docker e comunicam entre si através da rede criada pelo Docker Compose.

### Componentes principais

| Componente | Função |
|---|---|
| PostgreSQL 16 | Armazenamento dos dados de pagamentos |
| Logical Replication | Disponibilização das alterações através do WAL |
| Debezium | Captura das alterações na base de dados |
| Kafka Connect | Execução e gestão do conector Debezium |
| Apache Kafka 3.9.1 | Armazenamento e distribuição dos eventos |
| Docker Compose | Orquestração dos serviços |

## 3. Tecnologias utilizadas

- Docker e Docker Compose
- PostgreSQL 16
- Apache Kafka 3.9.1
- Kafka em modo KRaft
- Debezium Connect 3.0
- Kafka Connect REST API
- PostgreSQL Logical Replication
- Plugin `pgoutput`
- JSON Converters

## 4. Estrutura do repositório

```text
pipeline-pagamentos/
│
├── .gitignore
├── .env.example
├── README.md
├── docker-compose.yml
├── consumer/
│   ├── consumer.py
│   ├── requirements.txt
│   └── README.md
├── evidencias/
│   ├── insert_payment_9999.json
│   ├── update_payment_3.json
│   └── delete_payment_9999.json
│
└── postgres/
    └── init.sql
```

A configuração do conector Debezium é atualmente registada através da API REST do Kafka Connect, não estando ainda armazenada num ficheiro independente no repositório.

As futuras extensões poderão incluir diretórios dedicados a configurações Debezium, Kafka, Flink e esquemas de dados.

## 5. Pré-requisitos

Antes de executar o projeto, é necessário ter instalado:

- Docker Desktop ou Docker Engine.
- Docker Compose.
- Git.
- Um terminal com acesso aos comandos Docker.
- `curl` para testar a API REST do Kafka Connect.

## 6. Instalação e configuração

### 6.1. Clonar o repositório

```bash
git clone https://github.com/am2989/pipeline-pagamentos.git

cd pipeline-pagamentos
```

### 6.2. Configurar as variáveis de ambiente

Criar o ficheiro local `.env` a partir do modelo disponibilizado:

```bash
cp .env.example .env
```

Editar o ficheiro `.env` e substituir o valor de exemplo da variável `POSTGRES_PASSWORD` por uma password local de desenvolvimento.

**Importante:** o ficheiro `.env` está excluído pelo `.gitignore` e não deve ser publicado no GitHub.

O ficheiro `.env.example` contém apenas um valor ilustrativo.

### 6.3. Validar a configuração Docker

Antes de iniciar os serviços:

```bash
docker compose config --quiet
```

Se não existirem erros de configuração, iniciar os containers:

```bash
docker compose up -d
```

Verificar o estado dos serviços:

```bash
docker compose ps
```

Aguardar até o PostgreSQL apresentar o estado saudável e o Kafka Connect estar disponível.

### 6.4. Portas utilizadas

| Serviço | Porta local |
|---|---:|
| PostgreSQL | 5432 |
| Apache Kafka | 9092 |
| Debezium Connect REST API | 8083 |

A API REST do Kafka Connect pode ser testada através de:

```bash
curl http://localhost:8083/
```

## 7. Configuração do PostgreSQL

O PostgreSQL está configurado para suportar replicação lógica através do parâmetro:

```text
wal_level=logical
```

O script `postgres/init.sql` é responsável pela inicialização da base de dados e pela criação das tabelas:

- `accounts`
- `payments`

O script também insere registos sintéticos para permitir a realização dos testes.

Na tabela `public.payments` é aplicada a seguinte configuração:

```sql
ALTER TABLE public.payments
REPLICA IDENTITY FULL;
```

Esta configuração permite disponibilizar a imagem completa do registo anterior em eventos de atualização e eliminação.

**Nota:** o script de inicialização é executado apenas quando o PostgreSQL inicializa um diretório de dados vazio. Alterações posteriores ao ficheiro `init.sql` não modificam automaticamente os volumes já existentes.

## 8. Configuração do conector Debezium

O conector PostgreSQL do Debezium é responsável por monitorizar as alterações nas tabelas `public.accounts` e `public.payments`.

A configuração utiliza:

| Parâmetro | Valor |
|---|---|
| Nome do conector | `payments-connector` |
| Classe | `PostgresConnector` |
| Base de dados | `payments_db` |
| Utilizador | `payments` |
| Tabelas monitorizadas | `public.accounts`, `public.payments` |
| Plugin | `pgoutput` |
| Replication Slot | `payments_slot` |
| Publication | `payments_publication` |
| Topic Prefix | `dbserver1` |

### 8.1. Registar o conector

O registo é efetuado através da API REST do Kafka Connect.

O exemplo seguinte utiliza um placeholder para a password. Antes da execução, substituir `<LOCAL_POSTGRES_PASSWORD>` pelo valor local definido no `.env`.

Não guardar a password substituída no repositório.

```bash
curl --fail-with-body -X POST \
  -H "Content-Type: application/json" \
  --data-binary '{
    "name": "payments-connector",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "payments",
      "database.password": "<LOCAL_POSTGRES_PASSWORD>",
      "database.dbname": "payments_db",
      "topic.prefix": "dbserver1",
      "plugin.name": "pgoutput",
      "table.include.list": "public.accounts,public.payments",
      "slot.name": "payments_slot",
      "publication.name": "payments_publication",
      "publication.autocreate.mode": "filtered",
      "schema.include.list": "public"
    }
  }' \
  http://localhost:8083/connectors
```

A configuração é armazenada pelo Kafka Connect nos seus tópicos internos.

### 8.2. Verificar o estado do conector

```bash
curl http://localhost:8083/connectors/payments-connector/status
```

O conector e a respetiva tarefa deverão apresentar o estado:

```text
RUNNING
```

## 9. Tópicos Kafka

O Debezium publica os eventos de alteração nos tópicos:

```text
dbserver1.public.accounts
dbserver1.public.payments
```

Os nomes resultam da combinação do prefixo `dbserver1` com o esquema e cada tabela monitorizada.

O Kafka Connect também utiliza tópicos internos para armazenar configurações, offsets e estados.

| Tópico | Finalidade |
|---|---|
| `dbserver1.public.accounts` | Eventos CDC da tabela de contas |
| `dbserver1.public.payments` | Eventos CDC da tabela de pagamentos |
| `connect_configs` | Configurações dos conectores |
| `connect_offsets` | Posições de leitura e offsets |
| `connect_statuses` | Estados dos conectores |
| `__consumer_offsets` | Offsets dos grupos de consumidores Kafka |

Para listar os tópicos:

```bash
docker exec payments-kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

## 10. Captura e leitura dos eventos CDC

Para consumir os eventos publicados pelo Debezium:

```bash
docker exec payments-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.public.payments \
  --from-beginning
```

O Debezium disponibiliza um campo `op` que identifica o tipo de operação detetada.

### Operações suportadas

| Operação | Código | Descrição |
|---|---|---|
| Snapshot | `r` | Registo lido durante a captura inicial |
| INSERT | `c` | Novo registo inserido |
| UPDATE | `u` | Registo atualizado |
| DELETE | `d` | Registo eliminado |

Os eventos incluem informação sobre os dados anteriores (`before`) e posteriores (`after`), conforme o tipo de operação.

No caso de uma eliminação, o campo `after` apresenta valor `null`.

## Consumidor Python

O consumidor em `consumer/` lê eventos de inserção do tópico de pagamentos e
gera um alerta quando identifica pelo menos três transações do mesmo cartão
numa janela de 60 segundos. Os números de cartão não são apresentados; consulte
[`consumer/README.md`](consumer/README.md) para instalação e configuração.

## 11. REPLICA IDENTITY FULL

Por defeito, o PostgreSQL pode disponibilizar apenas a chave primária no evento de eliminação, dependendo da configuração de identidade da réplica.

A configuração:

```sql
ALTER TABLE public.payments
REPLICA IDENTITY FULL;
```

permite registar no WAL os valores anteriores completos da linha.

Esta configuração é especialmente relevante para o CDC porque permite ao Debezium disponibilizar a informação completa do registo anterior nos eventos de atualização e eliminação.

Nos testes realizados, foi possível confirmar que os eventos UPDATE e DELETE continham as sete colunas da tabela `payments` no campo `before`.

A utilização de `REPLICA IDENTITY FULL` pode aumentar o volume de informação armazenada no WAL.

## 12. Testes e resultados

A validação funcional da pipeline foi realizada num ambiente local com Docker Compose.

### Resultados obtidos

| Teste | Resultado |
|---|---|
| Validação do Docker Compose | Concluído |
| Estado saudável do PostgreSQL | Confirmado |
| Kafka em execução | Confirmado |
| Debezium Connect em execução | Confirmado |
| Registo do conector | Concluído |
| Estado do conector | RUNNING |
| Captura inicial de pagamentos (Snapshot) | Confirmada |
| Evento INSERT em `public.accounts` | Confirmado |
| Evento INSERT em `public.payments` | Confirmado |
| Evento UPDATE | Confirmado |
| Evento DELETE | Confirmado |
| Imagem anterior completa (`before`) | Confirmada |

### Validação dos dados

Foi criado um pagamento sintético para testar as operações de inserção, atualização e eliminação.

Os eventos de pagamentos foram observados no tópico:

```text
dbserver1.public.payments
```

Foi igualmente confirmado que os eventos UPDATE e DELETE disponibilizavam as sete colunas do registo anterior.

Após os testes, o registo utilizado foi eliminado e os cinco pagamentos originais de demonstração permaneceram na base de dados.

As evidências dos eventos de inserção, atualização e eliminação de pagamentos estão guardadas em `evidencias/`.

## 13. Segurança e boas práticas

Este projeto utiliza dados exclusivamente sintéticos e destina-se a um ambiente local de aprendizagem.

Foram consideradas as seguintes práticas:

- Utilização de variáveis de ambiente para configurar a password da base de dados.
- Exclusão do ficheiro `.env` através do `.gitignore`.
- Disponibilização de um `.env.example` sem credenciais reais.
- Utilização de dados fictícios nos registos de demonstração.
- Não utilização de números de cartões de pagamento reais.
- Não publicação de configurações completas do conector que contenham credenciais.
- Não publicação de dumps dos tópicos internos do Kafka.

A configuração do conector pode armazenar a password da base de dados nos tópicos internos do Kafka Connect. Por esse motivo, os dados e configurações internos não devem ser partilhados indiscriminadamente.

Esta implementação não inclui, por si só, todos os mecanismos de segurança necessários para um ambiente de produção.

## 14. Possíveis evoluções do projeto

A arquitetura atual constitui uma base para futuras experiências na área de Engenharia de Dados e processamento de eventos.

Algumas evoluções possíveis incluem:

- Automatizar o registo do conector Debezium através de um ficheiro de configuração.
- Integrar Apache Flink para processamento de dados em fluxo.
- Desenvolver mecanismos de validação e transformação dos eventos.
- Implementar monitorização dos serviços e da pipeline.
- Adicionar testes automatizados.
- Melhorar a gestão de configurações e credenciais.

## 15. Objetivo de aprendizagem

Este projeto foi desenvolvido como exercício prático para explorar conceitos de integração de dados, replicação lógica e arquiteturas orientadas a eventos.

A implementação permitiu experimentar a comunicação entre diferentes tecnologias, compreender o funcionamento do Change Data Capture e observar como as alterações efetuadas numa base de dados podem ser transmitidas para uma plataforma de streaming.

## Limitações e adaptações do projeto

### Ambiente de execução

O projeto foi desenvolvido e testado num ambiente local, utilizando Docker Compose, PostgreSQL, Apache Kafka e Debezium.

A integração com o Confluent Cloud não foi realizada devido à necessidade de disponibilizar um método de pagamento para provisionar recursos na plataforma, algo que não estava disponível no âmbito deste projeto académico.

Por esse motivo, foi adotada uma implementação local com tecnologias open source, permitindo demonstrar e validar o funcionamento do Change Data Capture (CDC), incluindo a captura de operações INSERT, UPDATE e DELETE.

### Âmbito da implementação

A solução local demonstra a captura e transmissão de alterações de dados entre PostgreSQL, Debezium e Kafka.

As componentes previstas no desafio que dependem de serviços adicionais, nomeadamente Confluent Cloud, Schema Registry e Apache Flink SQL, não estão implementadas nesta versão.

A deteção local de padrões de fraude está implementada no consumidor Python:
ele identifica três inserções associadas ao mesmo cartão numa janela de 60
segundos. O estado dessa janela permanece em memória e não constitui um sistema
de prevenção de fraude para produção.

Esta adaptação permitiu desenvolver e validar os fundamentos da arquitetura de streaming de dados sem assumir custos de infraestrutura cloud.

### Trabalho futuro

- Integração com Confluent Cloud.
- Implementação do Schema Registry e respetivo contrato de dados.
- Processamento de eventos com Apache Flink SQL.
- Persistência do estado de deteção entre reinícios do consumidor.
- Configuração de alertas e métricas operacionais.
- Documentação dos controlos de acesso e estimativa de custos.

---

## Autor

**Alda Benta**

Projeto desenvolvido para fins de aprendizagem e demonstração técnica na área de Engenharia de Dados.

**Repositório:** [pipeline-pagamentos](https://github.com/am2989/pipeline-pagamentos)
