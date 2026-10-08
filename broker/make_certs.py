#!/usr/bin/env python3
"""
Стадія 1, крок 3 — сертифікати TLS для власного брокера Mosquitto.

Створює у теці broker/certs/ (поряд із цим файлом) чотири файли:

    ca.crt      сертифікат вашого власного центру сертифікації (ЦС).
                Його отримують КЛІЄНТИ (node.py, mosquitto_sub): за ним
                вони перевіряють, що говорять саме з вашим брокером.
    ca.key      закритий ключ ЦС, яким підписано server.crt. Секрет.
    server.crt  сертифікат брокера для адрес 127.0.0.1, ::1 та localhost.
    server.key  закритий ключ брокера. Потрібен лише Mosquitto. Секрет.

Ті самі файли можна зробити командами openssl, але вони поводяться
по-різному у Windows, macOS і Linux. Цей скрипт однаковий усюди.

Запуск (з кореня репозиторію):
    python broker/make_certs.py
    python broker/make_certs.py --force      # створити заново
"""

from __future__ import annotations

import argparse
import datetime as dt
import ipaddress
import sys
from pathlib import Path

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
except ImportError:
    sys.exit("Потрібен пакет cryptography:  pip install cryptography")

CERTS = Path(__file__).resolve().parent / "certs"

# Імена, для яких дійсний сертифікат брокера. Клієнт порівнює адресу,
# до якої підключається (--host), з цим переліком — тому 127.0.0.1.
SERVER_NAMES = [
    x509.DNSName("localhost"),
    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
    x509.IPAddress(ipaddress.ip_address("::1")),
]


def usage(**allowed: bool) -> x509.KeyUsage:
    """Для чого дозволено вживати ключ; усе, що не назване, — заборонено."""
    names = ["digital_signature", "content_commitment", "key_encipherment",
             "data_encipherment", "key_agreement", "key_cert_sign",
             "crl_sign", "encipher_only", "decipher_only"]
    return x509.KeyUsage(**{n: allowed.get(n, False) for n in names})


def certificate(cn: str, key, issuer: x509.Name | None, issuer_key,
                days: int, ca: bool) -> x509.Certificate:
    """Сертифікат на ім'я cn, підписаний issuer_key (для ЦС — власним ключем)."""
    now = dt.datetime.now(dt.timezone.utc)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])
    b = (x509.CertificateBuilder()
         .subject_name(subject)
         .issuer_name(issuer or subject)
         .public_key(key.public_key())
         .serial_number(x509.random_serial_number())
         .not_valid_before(now - dt.timedelta(days=1))   # запас на неточний годинник
         .not_valid_after(now + dt.timedelta(days=days))
         .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()),
                        critical=False))
    if ca:
        # ЦС може підписувати інші сертифікати, але сам не є сервером
        b = (b.add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
              .add_extension(usage(key_cert_sign=True, crl_sign=True), critical=True))
    else:
        # Сертифікат сервера: лише для TLS-сервера і лише для SERVER_NAMES
        b = (b.add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
              .add_extension(usage(digital_signature=True), critical=True)
              .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
                             critical=False)
              .add_extension(x509.SubjectAlternativeName(SERVER_NAMES), critical=False)
              .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(
                  issuer_key.public_key()), critical=False))
    return b.sign(issuer_key, hashes.SHA256())


def save_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def save_key(path: Path, key) -> None:
    path.write_bytes(key.private_bytes(serialization.Encoding.PEM,
                                       serialization.PrivateFormat.PKCS8,
                                       serialization.NoEncryption()))
    path.chmod(0o600)            # читати може лише власник (у Windows не діє)


def shown(path: Path) -> str:
    """Шлях відносно поточної теки, якщо можна, — так його простіше вводити."""
    try:
        return path.relative_to(Path.cwd()).as_posix()
    except ValueError:
        return str(path)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=365,
                    help="скільки днів дійсні сертифікати (типово 365)")
    ap.add_argument("--force", action="store_true",
                    help="перезаписати наявні сертифікати")
    args = ap.parse_args()
    if args.days < 1:
        sys.exit("--days має бути щонайменше 1")

    files = {n: CERTS / n for n in ("ca.crt", "ca.key", "server.crt", "server.key")}
    if any(p.exists() for p in files.values()) and not args.force:
        print(f"Сертифікати вже є у {shown(CERTS)} — нічого не змінено.\n"
              f"Щоб створити нові: python broker/make_certs.py --force\n"
              f"(після цього перезапустіть mosquitto і передайте клієнтам новий ca.crt)")
        return

    CERTS.mkdir(exist_ok=True)
    # Ключі на еліптичній кривій P-256: стійкі, як RSA-3072, і створюються миттєво
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_crt = certificate("Kursova IoT CA", ca_key, None, ca_key, args.days, ca=True)
    srv_key = ec.generate_private_key(ec.SECP256R1())
    srv_crt = certificate("localhost", srv_key, ca_crt.subject, ca_key, args.days, ca=False)

    save_cert(files["ca.crt"], ca_crt)
    save_key(files["ca.key"], ca_key)
    save_cert(files["server.crt"], srv_crt)
    save_key(files["server.key"], srv_key)

    until = srv_crt.not_valid_after_utc.date()
    print(f"Створено у {shown(CERTS)}:")
    print("  ca.crt      сертифікат ЦС — його дають клієнтам (--cafile)")
    print("  ca.key      закритий ключ ЦС — секрет")
    print(f"  server.crt  сертифікат брокера для 127.0.0.1, ::1, localhost; дійсний до {until}")
    print("  server.key  закритий ключ брокера — секрет")
    print("\nДалі, з кореня репозиторію:")
    if sys.platform == "win32":
        print('  0) у кожному новому вікні PowerShell:  $env:Path += ";C:\\Program Files\\Mosquitto"')
    print("  1) користувач node і його пароль:  mosquitto_passwd -c broker/passwd node")
    print("  2) запуск брокера:                 mosquitto -c broker/mosquitto.conf -v")


if __name__ == "__main__":
    main()
