#!/usr/bin/env python3
"""
Стадія 1 — вузол IoT, емульований мовою Python.

Робить те саме, що прошивка firmware/sketch.ino з виконаними TODO: кожні
--interval секунд публікує температуру, вологість і освітленість окремими
повідомленнями за схемою з Додатка А методичних вказівок

    топік:  kursova/<variant>/<device_id>/<metric>
    вміст:  {"device_id":"esp32-047","metric":"temperature","value":22.47,"ts":1757000000,"seq":1041}

і реєструє Last Will: якщо вузол зникне без попередження, брокер сам
опублікує {"online": false} у kursova/<variant>/<device_id>/status.
Біля кожного повідомлення друкується його розмір у JSON і розмір того
самого повідомлення у CBOR — це число потрібне для звіту.

Навіщо: безкоштовний Wokwi не бачить вашого комп'ютера, тому на кроці 3
дослідження захищеності каналу (власний брокер із TLS і паролем) замість
ESP32 працює цей вузол. Оцінку це не знижує.

Приклади (з кореня репозиторію):
    python node/node.py --variant 47 --username node
    python node/node.py --variant 47 --username node --cafile broker/certs/ca.crt

Ctrl+C обриває з'єднання без попередження — брокер має сам опублікувати
Last Will. З --count N вузол після N циклів завершується штатно.
"""

from __future__ import annotations

import argparse
import getpass
import json
import math
import random
import socket
import ssl
import sys
import threading
import time
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("Потрібен пакет paho-mqtt:  pip install paho-mqtt")
try:
    import cbor2
except ImportError:
    sys.exit("Потрібен пакет cbor2:  pip install cbor2")

ONLINE = '{"online": true}'
OFFLINE = '{"online": false}'


def read_sensors() -> dict[str, float]:
    """Імітація DHT22 (температура, вологість) і фоторезистора.

    Добовий хід плюс невеликий шум, щоб ряд був схожий на справжній.
    """
    now = time.localtime()
    hour = now.tm_hour + now.tm_min / 60
    warm = math.sin(2 * math.pi * (hour - 9) / 24)            # найтепліше о 15:00
    light = max(0.0, math.sin(math.pi * (hour - 7) / 12))      # світло 7:00–19:00
    return {
        "temperature": round(22.0 + 1.5 * warm + random.gauss(0, 0.15), 2),
        "humidity": round(min(100.0, max(0.0, 45 - 5 * warm + random.gauss(0, 0.8))), 2),
        "illuminance": round(max(0.0, 450 * light + 3 + random.gauss(0, 2)), 1),
    }


def encode(device: str, metric: str, value: float, seq: int) -> tuple[bytes, bytes]:
    """Те саме повідомлення у JSON (як у прошивці) і у CBOR."""
    msg = {"device_id": device, "metric": metric, "value": value,
           "ts": int(time.time()), "seq": seq}
    as_json = json.dumps(msg, separators=(",", ":")).encode()   # без пробілів, як ArduinoJson
    # cbor2 записує дробове число у 8 байтах (float64). ESP32 оперує
    # 4-байтовим float, тож на пристрої CBOR був би ще на 4 Б коротшим.
    as_cbor = cbor2.dumps(msg)
    return as_json, as_cbor


def num(x: float) -> str:
    """Число з однією цифрою після коми: 88,7."""
    return f"{x:.1f}".replace(".", ",")


def fail(text: str) -> None:
    sys.exit(f"\n!! {text}")


def connect(args) -> mqtt.Client:
    """Підключитися до брокера або пояснити, чому не вдалося."""
    status = f"kursova/{args.variant}/{args.device}/status"
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"{args.device}-{random.randrange(0xFFFF):x}",
                         protocol=mqtt.MQTTv311)            # як PubSubClient у прошивці
    # Last Will передається разом із CONNECT. Брокер тримає його в пам'яті
    # й публікує сам, якщо з'єднання обірветься без DISCONNECT.
    # retain=True: хто підпишеться пізніше, одразу побачить останній статус.
    client.will_set(status, OFFLINE, qos=1, retain=True)
    if args.username:
        client.username_pw_set(args.username, args.password)
    if args.tls:
        client.tls_set(ca_certs=args.cafile)   # без --cafile — системні сертифікати
    client.reconnect_delay_set(1, 10)          # після обриву пробувати щонайменше раз на 10 с

    answer = {}
    answered = threading.Event()

    def on_connect(c, userdata, flags, reason_code, properties):
        if answered.is_set() and not reason_code.is_failure:
            print("   Знову підключено до брокера.")
        answer["rc"] = reason_code
        answered.set()
        if not reason_code.is_failure:          # і після кожного перепідключення
            c.publish(status, ONLINE, qos=1, retain=True)

    def on_disconnect(c, userdata, flags, reason_code, properties):
        was_up = answer.get("rc") is not None and not answer["rc"].is_failure
        if was_up and reason_code.is_failure:
            print("!! Зв'язок із брокером втрачено — пробую підключитися знову...")
        answered.set()                          # брокер закрив з'єднання

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    where = f"{args.host}:{args.port}"
    try:
        client.connect(args.host, args.port, keepalive=15)
    except ssl.SSLCertVerificationError as exc:
        fail(f"Сертифікат брокера не пройшов перевірку: {exc.verify_message}.\n"
             f"   --cafile має вказувати на той ca.crt, яким підписано сертифікат\n"
             f"   брокера (broker/certs/ca.crt), а --host — на адресу з сертифіката\n"
             f"   (127.0.0.1 або localhost).")
    except ssl.SSLError as exc:
        fail(f"TLS-з'єднання з {where} не вдалося ({exc.reason or exc}).\n"
             f"   Схоже, на цьому порту брокер працює без TLS. У broker/mosquitto.conf\n"
             f"   TLS — на порту 8883, без шифрування — на 1883.")
    except ConnectionRefusedError:
        fail(f"Брокер {where} не відповідає (з'єднання відхилено).\n"
             f"   Чи запущено mosquitto? У сусідньому вікні з кореня репозиторію:\n"
             f"   mosquitto -c broker/mosquitto.conf -v")
    except socket.gaierror:
        fail(f"Не вдалося знайти адресу {args.host!r}. Перевірте --host.")
    except OSError as exc:
        fail(f"Помилка мережі під час підключення до {where}: {exc}")

    client.loop_start()
    answered.wait(10)
    rc = answer.get("rc")
    if rc is None:
        client.loop_stop()
        fail(f"Брокер {where} закрив з'єднання, не відповівши на CONNECT.\n"
             f"   Найчастіше причина — підключення без TLS до TLS-порту: додайте\n"
             f"   --cafile broker/certs/ca.crt або використайте порт 1883.")
    if rc.is_failure:
        client.loop_stop()
        hint = ("Перевірте ім'я й пароль: користувача має бути додано у файл паролів\n"
                "   брокера (mosquitto_passwd broker/passwd <ім'я>)." if args.username else
                "Брокер вимагає ім'я та пароль: додайте --username.")
        fail(f"Брокер відмовив у підключенні: {rc}.\n   {hint}")

    sock = client.socket()
    tls = "немає"
    if isinstance(sock, ssl.SSLSocket):
        tls = f"{sock.version()}, шифр {sock.cipher()[0]}"
    print(f"Підключено до {where}. Шифрування: {tls}.")
    print(f"Last Will зареєстровано: {status} = {OFFLINE}")
    print(f"-> {status}  {ONLINE}  (retain)")
    return client


def run(client: mqtt.Client, args) -> None:
    status = f"kursova/{args.variant}/{args.device}/status"
    seq = sent = json_total = cbor_total = cycles = 0
    try:
        while True:
            for metric, value in read_sensors().items():
                as_json, as_cbor = encode(args.device, metric, value, seq)
                seq += 1
                topic = f"kursova/{args.variant}/{args.device}/{metric}"
                ok = client.publish(topic, as_json, qos=0).rc == mqtt.MQTT_ERR_SUCCESS
                print(f"{'->' if ok else '!!'} {topic}  JSON {len(as_json)} Б, "
                      f"CBOR {len(as_cbor)} Б  {as_json.decode()}")
                if ok:
                    sent += 1
                    json_total += len(as_json)
                    cbor_total += len(as_cbor)
            cycles += 1
            if args.count and cycles >= args.count:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        # Обрив без DISCONNECT — для брокера це те саме, що зникле живлення
        client.loop_stop()
        sock = client.socket()
        if sock is not None:
            sock.close()
        print("\nCtrl+C: з'єднання обірвано без DISCONNECT — брокер має сам "
              "опублікувати Last Will.")
    else:
        # Штатне завершення: Last Will не спрацює, тож статус публікуємо самі
        info = client.publish(status, OFFLINE, qos=1, retain=True)
        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            info.wait_for_publish(5)
        client.disconnect()
        client.loop_stop()
        print(f"Штатне завершення: {status} = {OFFLINE}, DISCONNECT надіслано.")

    if sent:
        j, c = json_total / sent, cbor_total / sent
        print(f"\nНадіслано повідомлень: {sent}. Середній розмір: JSON {num(j)} Б, "
              f"CBOR {num(c)} Б — CBOR коротший на {num(j - c)} Б "
              f"({num(100 * (1 - c / j))} %).")


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)   # рядки одразу, навіть у файл
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default="127.0.0.1",
                    help="адреса брокера (типово 127.0.0.1 — цей комп'ютер)")
    ap.add_argument("--port", type=int,
                    help="порт брокера (типово 1883, з TLS — 8883)")
    ap.add_argument("--variant", type=int, default=0, help="номер вашого варіанта")
    ap.add_argument("--device", help="ідентифікатор вузла (типово esp32-<варіант>)")
    ap.add_argument("--interval", type=float, default=10.0,
                    help="секунд між циклами вимірювань (типово 10, як у прошивці)")
    ap.add_argument("--count", type=int, default=0,
                    help="скільки циклів надіслати, по 3 повідомлення; 0 — до Ctrl+C")
    ap.add_argument("--tls", action="store_true", help="шифрувати з'єднання (TLS)")
    ap.add_argument("--cafile",
                    help="сертифікат ЦС для перевірки брокера, напр. "
                         "broker/certs/ca.crt (вмикає TLS)")
    ap.add_argument("--username", help="ім'я користувача на брокері")
    ap.add_argument("--password",
                    help="пароль; краще не вказувати — тоді його буде запитано "
                         "і він не залишиться в історії команд")
    args = ap.parse_args()

    args.tls = args.tls or bool(args.cafile)
    args.port = args.port or (8883 if args.tls else 1883)
    args.device = args.device or f"esp32-{args.variant:03d}"
    if args.cafile and not Path(args.cafile).is_file():
        fail(f"Не знайдено файл {args.cafile}.\n"
             f"   Створіть сертифікати: python broker/make_certs.py")
    if args.password is not None and not args.username:
        fail("Пароль без імені не має сенсу: додайте --username.")
    if args.interval <= 0 or args.count < 0:
        fail("--interval має бути більшим за 0, а --count — не від'ємним.")
    if args.variant == 0:
        print("!! Вкажіть свій номер варіанта: --variant 47")

    print(f"Вузол {args.device}, варіант {args.variant} -> {args.host}:{args.port}, "
          f"TLS: {'так' if args.tls else 'ні'}, "
          f"користувач: {args.username or 'без імені'}")
    try:
        if args.username and args.password is None:
            args.password = getpass.getpass(f"Пароль користувача {args.username}: ")
        client = connect(args)
    except KeyboardInterrupt:
        sys.exit("\nПерервано.")
    except EOFError:
        fail("Пароль не введено: запускайте вузол у терміналі (PowerShell,\n"
             "   Terminal) або вкажіть --password.")
    run(client, args)


if __name__ == "__main__":
    main()
