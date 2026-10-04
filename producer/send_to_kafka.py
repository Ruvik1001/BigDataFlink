import csv
import json
import os
from pathlib import Path

from kafka import KafkaProducer


def file_order(path: Path):
    name = path.name
    if name == "MOCK_DATA.csv":
        return 0
    # MOCK_DATA (1).csv ... (9).csv
    number = name.split("(")[1].split(")")[0]
    return int(number)


def main():
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    topic = os.environ["KAFKA_TOPIC"]
    bootstrap = os.environ["KAFKA_BOOTSTRAP"]

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda row: json.dumps(row, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )

    files = sorted(data_dir.glob("MOCK_DATA*.csv"), key=file_order)
    sent = 0
    for path in files:
        # csv.reader нужен из-за переносов внутри product_description
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_no, row in enumerate(reader, start=1):
                payload = dict(row)
                payload["source_file"] = path.name
                payload["source_row"] = row_no
                producer.send(topic, payload)
                sent += 1

    producer.flush()
    producer.close()
    print(f"sent {sent} messages to {topic}")


if __name__ == "__main__":
    main()
