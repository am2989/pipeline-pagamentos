# Consumidor Kafka

Este diretório contém o consumidor Python da pipeline CDC de pagamentos.

## Preparação

Na raiz do projeto, crie e ative o ambiente virtual e instale as dependências:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r consumer/requirements.txt
```

## Execução

Com Kafka e Debezium em execução, inicie o consumidor a partir da raiz do
projeto:

```bash
source .venv/bin/activate
python consumer/consumer.py
```

Por padrão, o consumidor lê `dbserver1.public.payments` em
`localhost:9092`. Configurações disponíveis:

| Variável | Padrão | Descrição |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Endereço do broker |
| `KAFKA_TOPIC` | `dbserver1.public.payments` | Tópico CDC |
| `KAFKA_GROUP_ID` | `payments-fraud-detector` | Grupo de consumidores |
| `AUTO_OFFSET_RESET` | `latest` | Posição inicial sem offset confirmado |
| `WINDOW_SECONDS` | `60` | Janela de tempo em segundos |
| `TRANSACTION_THRESHOLD` | `3` | Transações para gerar alerta |
| `CARD_HASH_SECRET` | chave aleatória por execução | Segredo HMAC para agrupar cartões |

O consumidor considera apenas eventos de inserção (`op = c`) e agrupa
transações pela impressão HMAC do número do cartão, sem exibir o número.
Tombstones Kafka (valores `null`, comuns após eliminações) são ignorados.
`created_at` é interpretado como Debezium `MicroTimestamp` em microssegundos;
os valores devem representar horários coerentes para que a janela seja útil.
Os dados de agrupamento ficam em memória e são perdidos ao reiniciar o processo.
