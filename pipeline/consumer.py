#!/usr/bin/env python3
"""
Стадія 2 — потоковий вхід конвеєра.

Підписується на MQTT-топіки вашого вузла, розбирає повідомлення і
накопичує їх у Parquet. Це друга половина конвеєра: пакетне завантаження
вашого варіанта дає історію, а цей модуль — живий потік.

Призначення у проєкті: показати, що конвеєр приймає дані у тому самому
форматі з двох різних джерел, і що пристрій, який перестав публікувати,
виявляється саме тут.

Приклади (з кореня репозиторію):
    python pipeline/consumer.py --variant 47 --device esp32-047 --minutes 10
    python pipeline/consumer.py --variant 47 --cafile broker/certs/ca.crt --username reader

Друга команда — ваш власний брокер зі стадії 1 (TLS, порт 8883, адреса
127.0.0.1): з --cafile ці значення підставляються самі, а пароль буде запитано.
"""

from __future__ import annotations

import argparse
import getpass
import json
import signal
import ssl
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

    def handle(self, topic: str, payload: bytes, retained: bool = False) -> None:
        # Топік status несе LWT — це не телеметрія, а подія життєвого циклу
        if topic.endswith("/status"):
            try:
                online = json.loads(payload).get("online")
            except Exception:
                online = None
            dev = topic.split("/")[-2] if "/" in topic else "?"
            self.status_events.append((dev, str(online), int(time.time())))
            mark = "онлайн" if online else "ОФЛАЙН"
            if retained:
                # retain: брокер зберігає останній статус і віддає його кожному
                # новому підписникові — це стан із минулого, а не подія зараз
                mark += " (збережений брокером статус)"
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
    ap.add_argument("--broker", help="адреса брокера (типово broker.hivemq.com; "
                                     "з --cafile — 127.0.0.1, ваш власний брокер)")
    ap.add_argument("--port", type=int, help="порт брокера (типово 1883, з TLS — 8883)")
    ap.add_argument("--tls", action="store_true", help="увімкнути TLS (стадія 1)")
    ap.add_argument("--cafile", help="сертифікат ЦС власного брокера, напр. "
                                     "broker/certs/ca.crt (вмикає TLS)")
    ap.add_argument("--username")
    ap.add_argument("--password", help="краще не вказувати — тоді його буде запитано")
    ap.add_argument("--minutes", type=float, default=10.0)
    ap.add_argument("--out", default="stream.parquet")
    args = ap.parse_args()
    use_tls = args.tls or bool(args.cafile)
    port = args.port or (8883 if use_tls else 1883)
    args.broker = args.broker or ("127.0.0.1" if args.cafile else "broker.hivemq.com")
    if args.cafile and not Path(args.cafile).is_file():
        sys.exit(f"!! Не знайдено файл {args.cafile}.\n"
                 f"   Створіть сертифікати: python broker/make_certs.py")
    if args.password and not args.username:
        sys.exit("!! Пароль без імені не має сенсу: додайте --username.")
    if args.username and args.password is None:
        try:
            args.password = getpass.getpass(f"Пароль користувача {args.username}: ")
        except (EOFError, KeyboardInterrupt):
            sys.exit("\n!! Пароль не введено.")

    topic = f"kursova/{args.variant}/{args.device or '+'}/#"
    coll = Collector(Path(args.out))

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if args.username:
        client.username_pw_set(args.username, args.password)
    if use_tls:
        client.tls_set(ca_certs=args.cafile)

    fatal: list[str] = []       # причина, з якої далі чекати немає сенсу
    connected = False

    def on_connect(c, userdata, flags, rc, properties=None):
        nonlocal connected
        if rc.is_failure:
            hint = ("Перевірте ім'я й пароль: користувача має бути додано у файл\n"
                    "   паролів брокера (mosquitto_passwd broker/passwd <ім'я>)."
                    if args.username else
                    "Брокер вимагає ім'я та пароль: додайте --username.")
            fatal.append(f"!! Брокер відмовив у підключенні: {rc}.\n   {hint}")
            return
        connected = True
        print(f"підключено до {args.broker}:{port}" + (" (TLS)" if use_tls else ""))
        print(f"підписка на {topic}")
        c.subscribe(topic, qos=1)

    def on_disconnect(c, userdata, flags, rc, properties=None):
        if not rc.is_failure or fatal:
            return                          # штатне від'єднання або причина вже відома
        if connected:
            print("  !! зв'язок із брокером втрачено — пробую підключитися знову...")
        else:
            fatal.append(f"!! Брокер {args.broker}:{port} закрив з'єднання, не відповівши на CONNECT.\n"
                         f"   Найчастіше причина — підключення без TLS до TLS-порту:\n"
                         f"   додайте --cafile broker/certs/ca.crt або використайте порт 1883.")

    def on_message(c, userdata, msg):
        coll.handle(msg.topic, msg.payload, retained=msg.retain)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    stop_at = time.time() + args.minutes * 60
    stopping = False

    def on_signal(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, on_signal)

    try:
        client.connect(args.broker, port, keepalive=30)
    except ssl.SSLCertVerificationError as exc:
        hint = ("--cafile має вказувати на той ca.crt, яким підписано сертифікат\n"
                "   брокера (broker/certs/ca.crt), а --broker — на адресу з сертифіката\n"
                "   (127.0.0.1 або localhost)." if args.cafile else
                "Для власного брокера вкажіть --cafile broker/certs/ca.crt.")
        sys.exit(f"!! Сертифікат брокера не пройшов перевірку: {exc.verify_message}.\n   {hint}")
    except ssl.SSLError as exc:
        sys.exit(f"!! TLS-з'єднання з {args.broker}:{port} не вдалося ({exc.reason}).\n"
                 f"   Схоже, на цьому порту брокер працює без TLS. У broker/mosquitto.conf\n"
                 f"   TLS — на порту 8883, без шифрування — на 1883.")
    except ConnectionRefusedError:
        sys.exit(f"!! Брокер {args.broker}:{port} не відповідає (з'єднання відхилено).\n"
                 f"   Чи запущено mosquitto? Порт 1883 — без TLS, 8883 — з TLS.")
    except OSError as exc:
        sys.exit(f"!! Не вдалося підключитися до {args.broker}:{port}: {exc}\n"
                 f"   Перевірте адресу брокера та підключення до інтернету.")
    client.loop_start()
    try:
        while time.time() < stop_at and not stopping and not fatal:
            time.sleep(0.5)
    finally:
        client.loop_stop()
        client.disconnect()
        coll.close()
    if fatal:
        sys.exit(fatal[0])

    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    print(f"\nзавершено {started}")
    print(f"  прийнято записів : {coll.total}")
    print(f"  нерозібраних     : {coll.bad}")
    print(f"  подій status     : {len(coll.status_events)}")
    print(f"  файл             : {args.out}")
    if coll.total == 0:
        print("\nЖодного повідомлення. Перевірте: номер варіанта у прошивці,"
              "\nадресу брокера та чи запущено вузол (симуляцію Wokwi або node/node.py).")


if __name__ == "__main__":
    main()
