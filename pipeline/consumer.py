#!/usr/bin/env python3
"""
Стадія 2 — потоковий вхід конвеєра.

Підписується на MQTT-топіки вашого вузла, розбирає повідомлення і
накопичує їх у Parquet. Це друга половина конвеєра: пакетне завантаження
вашого варіанта дає історію, а цей модуль — живий потік.

Призначення у проєкті: показати, що конвеєр приймає дані у тому самому
форматі з двох різних джерел, і що пристрій, який перестав публікувати,
виявляється саме тут.

Приклад:
    python consumer.py --variant 47 --device esp32-047 --minutes 10
    python consumer.py --variant 47 --broker localhost --port 8883 --tls
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("Потрібен пакет paho-mqtt:  pip install paho-mqtt")

import pyarrow as pa
import pyarrow.parquet as pq

FLUSH_EVERY = 500          # записів у буфері до скидання на диск


class Collector:
    """Накопичує повідомлення у пам'яті й періодично скидає у Parquet."""

    def __init__(self, out: Path):
        self.out = out
        self.rows: list[dict] = []
        self.total = 0
        self.bad = 0
        self.status_events: list[tuple[str, str, int]] = []
        self.writer: pq.ParquetWriter | None = None
        self.schema = pa.schema([
            ("ts", pa.int64()),
            ("device_id", pa.string()),
            ("metric", pa.string()),
            ("value", pa.float32()),
            ("seq", pa.int64()),
            ("received_at", pa.int64()),
        ])

    def handle(self, topic: str, payload: bytes) -> None:
        # Топік status несе LWT — це не телеметрія, а подія життєвого циклу
        if topic.endswith("/status"):
            try:
                online = json.loads(payload).get("online")
            except Exception:
                online = None
            dev = topic.split("/")[-2] if "/" in topic else "?"
            self.status_events.append((dev, str(online), int(time.time())))
            mark = "онлайн" if online else "ОФЛАЙН (Last Will)"
            print(f"  [status] {dev}: {mark}")
            return

        try:
            msg = json.loads(payload)
            row = {
                "ts": int(msg["ts"]),
                "device_id": str(msg["device_id"]),
                "metric": str(msg["metric"]),
                "value": float(msg["value"]),
                "seq": int(msg.get("seq", -1)),
                "received_at": int(time.time()),
            }
        except Exception as exc:
            # Погане повідомлення не має зупиняти конвеєр: у реальній
            # мережі завжди знайдеться пристрій зі своєю версією прошивки.
            self.bad += 1
            print(f"  !! не вдалося розібрати {topic}: {exc}")
            return

        self.rows.append(row)
        self.total += 1
        if len(self.rows) >= FLUSH_EVERY:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.out, self.schema, compression="zstd")
        self.writer.write_table(table)
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        if self.writer is not None:
            self.writer.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--variant", type=int, required=True)
    ap.add_argument("--device", help="лише цей пристрій; типово — усі")
    ap.add_argument("--broker", default="broker.hivemq.com")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--tls", action="store_true", help="увімкнути TLS (стадія 1)")
    ap.add_argument("--username")
    ap.add_argument("--password")
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--out", default="stream.parquet")
    args = ap.parse_args()

    topic = f"kursova/{args.variant}/{args.device or '+'}/#"
    coll = Collector(Path(args.out))

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if args.username:
        client.username_pw_set(args.username, args.password)
    if args.tls:
        client.tls_set()

    def on_connect(c, userdata, flags, rc, properties=None):
        if rc != 0:
            print(f"!! не вдалося підключитися до брокера, rc={rc}")
            return
        print(f"підключено до {args.broker}:{args.port}")
        print(f"підписка на {topic}")
        c.subscribe(topic, qos=1)

    def on_message(c, userdata, msg):
        coll.handle(msg.topic, msg.payload)

    client.on_connect = on_connect
    client.on_message = on_message

    stop_at = time.time() + args.minutes * 60
    stopping = False

    def on_signal(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, on_signal)

    client.connect(args.broker, args.port, keepalive=30)
    client.loop_start()
    try:
        while time.time() < stop_at and not stopping:
            time.sleep(0.5)
    finally:
        client.loop_stop()
        client.disconnect()
        coll.close()

    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"\nзавершено {started}")
    print(f"  прийнято записів : {coll.total}")
    print(f"  нерозібраних     : {coll.bad}")
    print(f"  подій status     : {len(coll.status_events)}")
    print(f"  файл             : {args.out}")
    if coll.total == 0:
        print("\nЖодного повідомлення. Перевірте: номер варіанта у прошивці,"
              "\nадресу брокера та чи запущено симуляцію Wokwi.")


if __name__ == "__main__":
    main()
