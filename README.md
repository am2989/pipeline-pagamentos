# Payments CDC Pipeline

A local, containerized change data capture (CDC) demo that streams changes from
PostgreSQL to Kafka with Debezium.

## Project overview

The pipeline captures changes to `public.payments` and publishes Debezium events
to Kafka. It demonstrates an initial snapshot and the CDC operations for
inserting, updating, and deleting a payment.

This project is intended for local development and demonstration, not production
use.

## Architecture

```text
PostgreSQL 16
  └─ logical WAL / pgoutput
       └─ Debezium PostgreSQL connector
            └─ Kafka topic: dbserver1.public.payments
```

Docker Compose runs PostgreSQL, a single-node Kafka broker/controller, and
Debezium Connect. Kafka and Debezium communicate over the Compose network.

## Technologies

- Docker Compose
- PostgreSQL 16
- Apache Kafka 3.9.1 (KRaft, single node)
- Debezium Connect (`quay.io/debezium/connect:3.0`)
- Kafka Connect JSON converters

## Repository layout

```text
.
├── .gitignore
├── .env.example
├── README.md
├── docker-compose.yml
├── postgres/
│   └── init.sql
├── debezium/   # currently empty
├── kafka/      # currently empty
├── flink/      # currently empty
├── schemas/    # currently empty
└── evidence/   # currently empty
```

The connector configuration is currently registered in the running Kafka
Connect instance; it is not stored in a project file.

## Docker setup

From the project directory, start the services:

```bash
[ -f .env ] || cp .env.example .env
```

If you created `.env` from the template, replace its placeholder with a local
development password before continuing. Then validate and start the services:

```bash
docker compose config --quiet
docker compose up -d
docker compose ps
```

The Compose file requires `POSTGRES_PASSWORD` from the local `.env` file and
fails validation if it is missing. Keep `.env` local; `.env.example` contains
only a placeholder.

PostgreSQL is exposed on `localhost:5432`, Kafka on `localhost:9092`, and the
Debezium Connect REST API on `localhost:8083`. Wait for PostgreSQL to report
healthy and for the Connect API to respond before registering the connector.

To check the API:

```bash
curl http://localhost:8083/
```

The Compose file uses a named volume for PostgreSQL data. The initialization
script runs only when PostgreSQL initializes an empty data directory.

## PostgreSQL configuration

The Compose service enables logical replication with `wal_level=logical`.
`postgres/init.sql` creates the `accounts` and `payments` tables and inserts
synthetic demo rows. It also sets `REPLICA IDENTITY FULL` on `public.payments`.

As with the other initialization statements, this setting is applied only when
PostgreSQL initializes an empty data directory. Existing volumes are not
modified by changes to `init.sql`.

## Debezium connector configuration

The `payments-connector` captures `public.payments` from the `payments_db`
database using the `pgoutput` plugin. It uses the `payments` database user,
publication `payments_publication`, replication slot `payments_slot`, and topic
prefix `dbserver1`. The publication is configured for filtered tables.

Register the connector with the Connect REST API. Replace the password
placeholder locally with the `POSTGRES_PASSWORD` value from `.env`; do not
commit the substituted value or include it in published logs:

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
      "table.include.list": "public.payments",
      "slot.name": "payments_slot",
      "publication.name": "payments_publication",
      "publication.autocreate.mode": "filtered",
      "schema.include.list": "public"
    }
  }' \
  http://localhost:8083/connectors
```

Check connector and task status:

```bash
curl http://localhost:8083/connectors/payments-connector/status
```

## Kafka topics

| Topic | Purpose |
| --- | --- |
| `dbserver1.public.payments` | Debezium change events for `public.payments` |
| `connect_configs` | Kafka Connect connector configuration storage |
| `connect_offsets` | Kafka Connect source offsets |
| `connect_statuses` | Kafka Connect status storage |
| `__consumer_offsets` | Kafka consumer group offsets (Kafka internal) |

List topics from the Kafka container:

```bash
docker exec payments-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

## CDC operations

Debezium uses the `op` field to identify the change:

| Operation | `op` | Meaning |
| --- | --- | --- |
| Snapshot | `r` | A row read during the initial snapshot |
| INSERT | `c` | A row was created |
| UPDATE | `u` | A row was updated; `before` and `after` describe the change |
| DELETE | `d` | A row was deleted; `after` is `null` |

Consume events from the beginning with:

```bash
docker exec payments-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.public.payments \
  --from-beginning
```

## `REPLICA IDENTITY FULL`

With PostgreSQL's default replica identity, delete events generally contain only
the primary key in the `before` image. `REPLICA IDENTITY FULL` records the
previous row values in the WAL, allowing Debezium to include the full `before`
image for updates and deletes. This setting can increase WAL volume.

The test database used this setting and confirmed that update and delete events
contained all seven payment columns in `before`. New databases receive this
setting from `postgres/init.sql`.

## Testing results

The local functional test completed successfully:

- `docker compose config --quiet` passed with the local `.env` file.
- PostgreSQL reported healthy; Kafka and Debezium were running.
- The connector and its task reported `RUNNING`.
- Snapshot (`r`), insert (`c`), update (`u`), and delete (`d`) events were
  observed on `dbserver1.public.payments`.
- A synthetic test payment was inserted, updated, and deleted. The update and
  delete events included all seven columns in `before`.
- The five original sample payments remained; the test payment was removed.

## Security and publication notes

- The Compose file reads the database password from local `.env`, which is
  ignored by Git. `.env.example` contains only a placeholder. Do not use a
  development password in production.
- The active connector configuration includes a database password stored by
  Kafka Connect in its internal configuration topic. Do not publish connector
  configuration dumps or Kafka data.
- The SQL seed rows use synthetic demo data. Never put real personal or payment
  card data in this project; `card_number` should not contain real card numbers.
- This directory is not currently a Git repository, so tracked-file status and
  prior Git history could not be audited.
