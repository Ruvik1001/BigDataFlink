# Отчёт по лабораторной работе №3 (BigDataFlink)

Работа выполнена студентом МАИ группы М8О-205СВ-25 Рудневым Виктором Владимировичем.

## Цель

Прочитать поток JSON из Kafka и в режиме streaming собрать из него модель «снежинка» в PostgreSQL.

## Шаги

1. `docker-compose.yml`: PostgreSQL 16, Kafka 3.8 (KRaft, без Zookeeper), Flink 1.19 (jobmanager и taskmanager) и producer.
2. `producer/send_to_kafka.py` читает 10 CSV и шлёт каждую строку отдельным JSON в топик `mock_sales`. В сообщение добавлены `source_file` и номер строки.
3. При старте Postgres создаётся пустая схема `star` (`sql/01_star.sql`). Сами CSV в базу не грузятся: источник этой работы — Kafka.
4. Джоба `flink-job` (`StarStreamJob`, sink `StarJdbcSink`) читает топик с начала и на каждое сообщение в одной транзакции дописывает измерения и факт.
5. Проверил число строк и две выборки.

## Обоснование

Модель та же, что во второй работе. Факт — продажа, измерения нормализованы: страна и город отдельно, у товара категория, бренд, цвет, размер, материал и поставщик.

`id` из CSV снова не брал: в каждом файле они с 1 по 1000. Покупатель, продавец, магазин и поставщик ищутся по email, товар — по `product_nk` (md5 тех же атрибутов, что в Spark-лабе). Сумму чека не пересчитывал.

В потоке ключи нельзя пронумеровать заранее, как в batch. Поэтому id в измерениях выдаёт `SERIAL`, а повторная вставка той же сущности упирается в `ON CONFLICT` и берёт уже существующий id. Повтор той же строки файла не создаёт второй факт: уникальны `source_file` и `source_row`.

CSV читается модулем `csv`, потому что в `product_description` есть переносы строк.

## Конфигурация

Параметры в `.env`. В git его нет, есть `.env_example`.

```bash
copy .env_example .env
```

| Переменная | Зачем |
|------------|--------|
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | доступ к Postgres |
| `POSTGRES_HOST`, `POSTGRES_INTERNAL_PORT` | адрес Postgres из Flink (`postgres` / `5432`) |
| `POSTGRES_PORT` | порт на хосте, в шаблоне `5434` |
| `KAFKA_BOOTSTRAP` | адрес брокера из контейнеров, `kafka:9094` |
| `KAFKA_HOST_PORT` | порт Kafka на хосте, `9092` |
| `KAFKA_TOPIC` | топик, `mock_sales` |
| `KAFKA_NODE_ID`, `KAFKA_PROCESS_ROLES`, `KAFKA_LISTENERS`, `KAFKA_ADVERTISED_LISTENERS`, `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`, `KAFKA_CONTROLLER_LISTENER_NAMES`, `KAFKA_INTER_BROKER_LISTENER_NAME`, `KAFKA_CONTROLLER_QUORUM_VOTERS` | режим KRaft и слушатели брокера |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`, `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR`, `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS`, `KAFKA_NUM_PARTITIONS` | один брокер, одна партиция |
| `KAFKA_CLUSTER_ID` | идентификатор кластера KRaft |
| `FLINK_UI_PORT` | веб-интерфейс Flink, `8081` |
| `DATA_DIR` | каталог CSV внутри producer, `/data` |

В шаблоне Postgres: пользователь `postgres`, пароль `postgres`, база `pet_store`.

Запуск из каталога `BigDataFlink`:

```bash
docker compose up -d --build
docker exec bdfink-jobmanager flink run -d /opt/job/star-stream.jar
```

Первая команда поднимает сервисы и сразу отправляет 10000 JSON в Kafka. Вторая запускает джобу. Она остаётся в статусе RUNNING: поток не заканчивается, новых сообщений она тоже обработает.

Повторная отправка CSV: `docker compose up -d --force-recreate producer`. С нуля: `docker compose down -v && docker compose up -d --build`, затем снова `flink run`.

## Результат

Producer написал в лог: `sent 10000 messages to mock_sales`.

После джобы в `pet_store`:

| Таблица | Строк |
|---------|------:|
| star.fact_sales | 10000 |
| star.dim_customer | 10000 |
| star.dim_product | 10000 |
| star.dim_country | 230 |
| star.dim_date | 364 |

Выручка по категории: Toy 868101.63 (3406), Cage 831117.94 (3327), Food 830632.55 (3267). Это те же суммы, что во второй работе.

Все продажи за 2021 год, 12 месяцев. Больше всего заказов в августе (897), средний чек по месяцам около 246–262.

Джоба `csv-json-to-star` после разбора осталась RUNNING.
