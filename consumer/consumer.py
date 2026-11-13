import base64
import binascii
import hashlib
import hmac
import json
import os
import secrets
from collections import defaultdict
from decimal import Decimal, InvalidOperation

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition


BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.public.payments")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "payments-fraud-detector")
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))
THRESHOLD = int(os.getenv("TRANSACTION_THRESHOLD", "3"))
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")

if WINDOW_SECONDS <= 0:
    raise ValueError("WINDOW_SECONDS deve ser maior que zero.")
if THRESHOLD <= 0:
    raise ValueError("TRANSACTION_THRESHOLD deve ser maior que zero.")

WINDOW_US = WINDOW_SECONDS * 1_000_000

HASH_SECRET = os.getenv("CARD_HASH_SECRET")
if HASH_SECRET:
    HASH_SECRET = HASH_SECRET.encode("utf-8")
else:
    HASH_SECRET = secrets.token_bytes(32)
    print("Nota: chave de hash temporária criada para esta execução.")

transactions = defaultdict(dict)
alert_active = defaultdict(bool)
latest_timestamp = {}


def get_field_schema(node, field_name):
    """Find a field in a Kafka Connect schema."""
    if isinstance(node, dict):
        if node.get("field") == field_name:
            return node

        for child in node.get("fields", []):
            result = get_field_schema(child, field_name)
            if result:
                return result

        for key in ("schema", "items", "values"):
            result = get_field_schema(node.get(key), field_name)
            if result:
                return result

    return None


def decode_amount(value, event_schema):
    """Decode Kafka Connect Decimal (Base64-encoded signed integer)."""
    if value is None:
        return None

    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))

    field_schema = get_field_schema(event_schema, "amount")
    parameters = (field_schema.get("parameters") or {}) if field_schema else {}
    scale = int(parameters.get("scale", 2))
    raw = base64.b64decode(value, validate=True)
    unscaled_value = int.from_bytes(raw, byteorder="big", signed=True)

    return Decimal(unscaled_value).scaleb(-scale)


def normalize_card(card_number):
    """Normalize a card number without logging or retaining its value."""
    if card_number is None:
        return None

    digits = "".join(
        character for character in str(card_number) if character.isdigit()
    )
    return digits if digits else None


def card_fingerprint(card_number):
    """Generate a non-reversible HMAC identifier for in-memory grouping."""
    normalized = normalize_card(card_number)
    if not normalized:
        return None

    return hmac.new(
        HASH_SECRET,
        normalized.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def parse_created_at(value):
    """Return Debezium MicroTimestamp integer microseconds."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError("created_at não está no formato MicroTimestamp inteiro")
    return value


def process_event(event):
    if event is None:
        return
    if not isinstance(event, dict):
        print("Evento ignorado: formato inválido.")
        return

    payload = event.get("payload", event)
    if not isinstance(payload, dict) or payload.get("op") != "c":
        return

    row = payload.get("after")
    if not isinstance(row, dict):
        print("Evento ignorado: campo after inválido.")
        return

    payment_id = row.get("payment_id")
    card_number = row.get("card_number")
    created_at = row.get("created_at")
    if payment_id is None or card_number is None or created_at is None:
        print("Evento ignorado: campos obrigatórios em falta.")
        return

    try:
        timestamp_us = parse_created_at(created_at)
        decode_amount(row.get("amount"), event.get("schema", {}))
    except (ValueError, TypeError, InvalidOperation, binascii.Error) as error:
        print(f"Evento ignorado: formato de dados inválido ({type(error).__name__}).")
        return

    fingerprint = card_fingerprint(card_number)
    if fingerprint is None:
        print("Evento ignorado: identificador de cartão inválido.")
        return

    payment_id = str(payment_id)
    current_transactions = transactions[fingerprint]

    if payment_id in current_transactions:
        return

    watermark = max(latest_timestamp.get(fingerprint, timestamp_us), timestamp_us)
    latest_timestamp[fingerprint] = watermark

    expired = [
        payment
        for payment, payment_timestamp in current_transactions.items()
        if payment_timestamp < watermark - WINDOW_US
    ]
    for payment in expired:
        del current_transactions[payment]

    if len(current_transactions) < THRESHOLD:
        alert_active[fingerprint] = False

    if timestamp_us < watermark - WINDOW_US:
        return

    current_transactions[payment_id] = timestamp_us
    count = len(current_transactions)

    if count >= THRESHOLD and not alert_active[fingerprint]:
        alert_active[fingerprint] = True
        print("\n" + "=" * 55)
        print("ALERTA: PADRÃO DE TRANSAÇÕES SUSPEITO")
        print("=" * 55)
        print(f"Transações detetadas: {count}")
        print(f"Limite configurado: {THRESHOLD}")
        print(f"Janela temporal: {WINDOW_SECONDS} segundos")
        print(f"Último created_at (microssegundos): {watermark}")
        print("Identificadores dos pagamentos:", ", ".join(current_transactions))
        print("Número do cartão: NÃO apresentado")
        print("=" * 55 + "\n")
    elif count < THRESHOLD:
        alert_active[fingerprint] = False


def main():
    print("A iniciar consumidor Kafka...")
    print(f"Broker: {BOOTSTRAP_SERVERS}")
    print(f"Tópico: {TOPIC}")
    print(f"Grupo: {GROUP_ID}")
    print(f"Regra: {THRESHOLD} transações em {WINDOW_SECONDS} segundos")
    print("Os números de cartão não serão apresentados nos logs.")

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=False,
        value_deserializer=lambda value: (
            None if value is None else json.loads(value.decode("utf-8"))
        ),
    )

    try:
        for message in consumer:
            process_event(message.value)
            topic_partition = TopicPartition(message.topic, message.partition)
            leader_epoch = message.leader_epoch
            if leader_epoch is None:
                leader_epoch = -1
            consumer.commit({
                topic_partition: OffsetAndMetadata(
                    message.offset + 1,
                    "",
                    leader_epoch,
                )
            })
    except KeyboardInterrupt:
        print("\nConsumidor interrompido pelo utilizador.")
    finally:
        consumer.close()
        print("Consumidor encerrado.")


if __name__ == "__main__":
    main()
