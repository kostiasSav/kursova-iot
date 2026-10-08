# Типові проблеми і як їх виправити

Знайдіть у списку текст помилки, який бачите. Якщо нічого не підходить —
скопіюйте **повний** текст помилки (а не знімок екрана) і надішліть керівнику
разом із командою, яку ви виконували, і назвою вашої операційної системи.

[← До змісту](README.md)

---

## Python і бібліотеки

**«Python was not found; run without arguments to install from the
Microsoft Store…» / «'python' is not recognized…» (Windows).** Python не
встановлено або не додано до PATH (а Windows підставляє замість нього
посилання на Microsoft Store). Перевстановіть Python з позначкою
**Add python.exe to PATH** (крок 0.2), закрийте й знову відкрийте PowerShell.
Тимчасово можна користуватися командою `py` замість `python`.

**«command not found: python» (macOS, Linux).** Пишіть `python3`. Усередині
активованого віртуального середовища працює і `python`.

**«error: externally-managed-environment» під час `pip install`.** Ви
встановлюєте бібліотеки поза віртуальним середовищем. Активуйте його (крок
0.5) і повторіть.

**«ModuleNotFoundError: No module named …».** Або не активоване віртуальне
середовище (немає `(.venv)` на початку рядка), або бібліотеку не встановлено.
Активуйте середовище і виконайте `pip install` з назвою модуля, яку підказує
помилка або посібник.

**«running scripts is disabled on this system» (Windows).** Див. крок 0.5:
`Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`.

**Помилка компіляції під час `pip install` (довгий червоний текст з
`error: Microsoft Visual C++ … is required` або `gcc`).** Для вашої версії
Python немає готової збірки бібліотеки. Найпростіше — встановити Python
версії, рекомендованої в кроці 0.2, і створити віртуальне середовище заново.

---

## Файли і папки

**«FileNotFoundError: … tuning.parquet» / «Файл … не знайдено».** Ви не в
папці проєкту або файл лежить в іншому місці. Виконайте `ls` і переконайтеся,
що бачите і `selfcheck.py`, і файл даних.

**Кирилиця або пробіли в шляху (Windows).** Деякі інструменти, особливо
Docker, плутаються зі шляхами на кшталт `C:\Users\Олена\Мої документи`.
Тримайте проєкт у простій папці, наприклад `C:\kursova-iot`.

**Файл `.parquet` не відкривається в Excel.** Так і має бути: Parquet читають
програми (pandas, DuckDB тощо), а не табличні редактори.

**Завантажений файл має назву на кшталт `variant_047 (1).parquet`.** Браузер
додав номер, бо файл уже був у «Завантаженнях». Перейменуйте його назад на
`variant_047.parquet`.

---

## Wokwi

**Симуляція довго не стартує.** Перша компіляція триває до хвилини. Якщо
довше — оновіть сторінку і натисніть ▶ ще раз.

**Помилка компіляції `DHT.h: No such file or directory` або подібна.** Немає
файла `libraries.txt` або в ньому помилка в назві бібліотеки. Порівняйте з
`firmware/libraries.txt` у заготовці.

**Монітор нескінченно друкує `WiFi: підключення.....`.** У налаштуваннях
мережі має бути `Wokwi-GUEST` без пароля і канал 6, як у заготовці.

**`MQTT: … помилка, rc=-2`.** Вузол не може дістатися брокера. Перевірте назву
`broker.hivemq.com`, порт `1883` і зачекайте: публічний брокер іноді
перевантажений.

**Проєкт зник після закриття вкладки.** Ви працювали без входу в акаунт.
Зареєструйтеся і натискайте **Save**.

---

## MQTT

**Веб-клієнт підключився, але повідомлень немає.** Найчастіше — помилка в
топіку. У прошивці номер варіанта без нулів попереду (`kursova/47/…`), і в
підписці має бути так само: `kursova/47/#`. Також перевірте, що симуляція у
Wokwi працює.

**Повідомлення з'являються і зникають, вузол постійно перепідключається.**
Два клієнти з однаковим ідентифікатором виштовхують один одного. Заготовка
додає до `DEVICE_ID` випадковий суфікс — не прибирайте його.

**`[status] …: ОФЛАЙН (збережений брокером статус)` одразу після підписки.**
Так і має бути: брокер зберігає останній статус вузла (прапорець `retain`) і
віддає його кожному новому підписникові. Свіжий статус прийде, щойно вузол
підключиться.

---

## Власний брокер Mosquitto (стадія 1, крок 3)

**`mosquitto` / `mosquitto_passwd` — «is not recognized…» (Windows).** У
цьому вікні PowerShell не виконано рядок
`$env:Path += ";C:\Program Files\Mosquitto"`. Його треба повторювати в
кожному новому вікні.

**`mosquitto: command not found` (macOS).** На Mac з процесором Intel брокер
лежить у `/usr/local/sbin`, якої немає в PATH. Виконайте
`echo 'export PATH="/usr/local/sbin:$PATH"' >> ~/.zprofile` і відкрийте
Термінал заново.

**`Error: Address already in use` (або «Only one usage of each socket
address…» на Windows).** Порт 1883 уже зайнятий іншим брокером:

- Windows — службою «Mosquitto Broker» (ви не зняли позначку «Service»).
  У PowerShell **від імені адміністратора**: `Stop-Service mosquitto` і
  `Set-Service mosquitto -StartupType Manual`.
- macOS — `brew services`: виконайте `brew services stop mosquitto`.
- Linux — системною службою: `sudo systemctl stop mosquitto` і
  `sudo systemctl disable mosquitto`.
- Або у вас уже відкрите інше вікно з вашим брокером — закрийте його.

**`Error: Unable to open pwfile "broker/passwd".`** Або ви запустили брокер не
з папки проєкту (наприклад, перейшли в `broker`), або ще не створили файл
паролів. Поверніться в папку проєкту (там, де `README.md`) і, якщо треба,
створіть файл: `mosquitto_passwd -c broker/passwd node`.

**`Error: Unable to load CA certificates. Check cafile "broker/certs/ca.crt".`**
Не створено сертифікати або брокер запущено не з тієї папки. Виконайте
`python broker/make_certs.py` у папці проєкту.

**`Error: Unable to open file broker/passwd for writing. File exists.`**
Ви вдруге використали `-c`. Для другого й наступних користувачів пишіть
команду без `-c`: `mosquitto_passwd broker/passwd reader`.

**Після додавання другого користувача ніхто не може підключитися
(`not authorised`) — Linux.** Mosquitto 2.0 з ключем `-c` створює файл
заново і видаляє попередніх користувачів. Додайте їх знову, без `-c`.

**`Unable to decode password salt for user …, removing entry`.** Файл
паролів створено іншою версією Mosquitto. Видаліть `broker/passwd` і
створіть його заново командою `mosquitto_passwd` з тієї самої інсталяції,
що й брокер.

**Брокер не запускається після того, як ви дописали коментар у
`mosquitto.conf`.** Mosquitto вважає текст після значення частиною
значення: коментар має стояти **окремим рядком**, що починається з `#`.

**Запуск із `sudo` — брокер не може прочитати файли.** Запущений від
імені адміністратора, Mosquitto перемикається на службового користувача, який
не бачить файлів у вашій домашній папці. Запускайте **без** `sudo`.

**`Connection error: Connection Refused: not authorised`.** Без імені й пароля
(або з неправильним паролем) брокер нікого не пускає — у кроці 3.8 саме це і
треба отримати. Законний підписник вказує `--username reader`.

**`mosquitto_sub` на порту 8883: `Error: Protocol error` або
`A TLS error occurred.`** Не вказано `--cafile broker/certs/ca.crt` або вказано
`ca.crt` з іншого запуску `make_certs.py`. Якщо ви перестворили сертифікати
(`--force`), перезапустіть брокер.

**`Error: Bad file descriptor` (mosquitto_sub 2.1).** Брокер не запущений
або вказано не той порт. Попри дивний текст, це просто «нема з ким
з'єднатися».

**Вузол чи `consumer.py` пише
`!! Сертифікат брокера не пройшов перевірку: self-signed certificate…`.**
Не вказано `--cafile` або це `ca.crt` з іншого запуску `make_certs.py`.

**`!! Брокер … закрив з'єднання, не відповівши на CONNECT.`** Ви підключаєтеся
без TLS до TLS-порту 8883. Додайте `--cafile broker/certs/ca.crt` або
використайте порт 1883.

**`!! TLS-з'єднання з 127.0.0.1:1883 не вдалося`.** Навпаки: TLS на порт без
шифрування. TLS — порт 8883.

**Запит пароля «зависає» в Git Bash (Windows).** Git Bash не вміє приховано
читати пароль. Запускайте вузол у PowerShell.

---

## Wireshark

**Немає інтерфейсу loopback (Windows).** Не встановлено Npcap. Перевстановіть
Wireshark і погодьтеся встановити Npcap. Потрібний інтерфейс називається
**Adapter for loopback traffic capture**.

**«You don't have permission to capture on that device» або порожній
перелік інтерфейсів (macOS).** Не встановлено ChmodBPF: двічі клацніть
**Install ChmodBPF.pkg** у вікні завантаженого `.dmg`, потім вийдіть із
системи й увійдіть знову.

**Те саме на Linux.** Ви не в групі `wireshark`:
`sudo usermod -a -G wireshark $USER`, потім вийдіть із системи й увійдіть
знову. Якщо під час встановлення ви відповіли «No»:
`sudo dpkg-reconfigure wireshark-common`.

**Фільтр `mqtt` нічого не показує.** Або ви записуєте не той інтерфейс
(Wi-Fi чи Ethernet замість loopback), або трафік іде на порт 8883 — тоді так
і має бути: це TLS, використайте фільтр `tls`.

**У полі Message шістнадцяткові цифри замість JSON.** Клацніть правою кнопкою
**MQ Telemetry Transport Protocol** → **Protocol Preferences** →
**Show Message as text**.

---

## Docker (InfluxDB, TimescaleDB, Grafana)

**`Cannot connect to the Docker daemon … Is the docker daemon running?`
(macOS, Linux) або `error during connect: … dockerDesktopLinuxEngine`
(Windows).** Docker Desktop не запущено. Запустіть його і дочекайтеся,
поки він покаже, що двигун працює (Engine running).

**`no configuration file provided: not found`.** Команду `docker compose`
виконано не з папки проєкту. Поверніться в неї і пишіть
`docker compose -f pipeline/docker-compose.yml up -d`.

**`Ports are not available: … address already in use` / `port is already
allocated`.** Порт 5432 (TimescaleDB) чи 8086 (InfluxDB) уже зайнятий —
найчастіше встановленим раніше PostgreSQL. Зупиніть ту програму (Windows:
застосунок «Служби» → `postgresql-x64-…` → Зупинити, тип запуску «Вручну»).
Якщо зупинити не можна, змініть ліве число порту в
`pipeline/docker-compose.yml`, наприклад `"127.0.0.1:5433:5432"`, і
передавайте нову адресу скриптам `measure.py`, `run_all.py` і `prepare.py`:
`--dsn "host=localhost port=5433 user=student password=kursova2025 dbname=iot"`
(TimescaleDB) або `--url http://localhost:8087` (InfluxDB, якщо змінили
`"127.0.0.1:8087:8086"`).

**`Docker Desktop - Unexpected WSL error` або помилки з `HCS` у тексті
(Windows).** Не встановлено або застарів WSL 2, чи вимкнено віртуалізацію.
У PowerShell від імені адміністратора: `wsl --install` (потім перезавантаження)
або `wsl --update`. Якщо не допомогло — увімкніть віртуалізацію (Intel VT-x
або AMD-V/SVM) у налаштуваннях UEFI/BIOS.

**`permission denied while trying to connect to the Docker daemon socket`
(Linux).** Додайте себе до групи `docker`: `sudo usermod -aG docker $USER`,
потім вийдіть із системи й увійдіть знову.

**`docker-compose: command not found` або помилки про `name:`.** Старий
`docker-compose` (через дефіс) не підходить. Пишіть `docker compose` — через
пробіл.

**Контейнер у стані `(unhealthy)` або `(health: starting)`.** Зачекайте
хвилину й повторіть `docker compose -f pipeline/docker-compose.yml ps`. Перший
запуск довший: Docker завантажує образи.

---

## Детектори (стадія 3)

**Скрипт «завис» на першому запуску.** Перший запуск після встановлення
бібліотек триває до 30–40 секунд: будується кеш шрифтів і компілюються
бібліотеки. Matrix Profile щоразу думає 15–20 секунд — так і має бути.

**`Вкажіть пристрій: --device <назва>.`** Скрипт друкує перелік пристроїв і
їхніх показників — виберіть із нього. Якщо в назві помилка, скрипт
підкаже схожі назви.

**`--step '5': потрібна тривалість з одиницею`.** Тривалості пишуть з
одиницею: `5min`, `30min`, `2h`, `1D`.

**`для --json вкажіть ще й --class`.** Вікна, записані у JSON, мають мати
клас: додайте `--class` з назвою класу з розділу 7 вказівок.

**`tuning_truth.json описує інший набір даних…` або `Відповіді є лише до
навчального набору…`.** `--evaluate` працює тільки на `tuning.parquet` з
`tuning_truth.json`. Для вашого варіанта відповідей немає — так задумано.

**`Не встановлено пакет «…»`.** Встановіть названий пакет у віртуальне
середовище: `pip install statsmodels` (або `scikit-learn`, `stumpy`,
`matplotlib`).

**`pip install stumpy` завершується помилкою про `llvmlite` (Mac з
процесором Intel).** Для Python 3.14 на Mac з Intel немає готової збірки.
Встановіть Python 3.13 (крок 0.2), створіть віртуальне середовище заново і
повторіть.

**Один інцидент розпався на кілька вікон.** Таке вікно рахується як одна
правильна знахідка і кілька хибних. Збільште `--gap` (для рідких даних, як
у шлюзів) або `--contamination` (Isolation Forest, LOF).

**Isolation Forest чи LOF знаходять «аномалії» на здоровому пристрої.**
Вони завжди позначають частку точок `--contamination`. Дивіться на величину
оцінки у стовпці «пік оцінки», а не лише на те, що вікно є.

**Графік перезаписується.** Повторний запуск з тим самим пристроєм і
показником зберігає PNG під тією самою назвою. Щоб зберегти варіанти для
записки, задавайте назву: `--png ewma_L7.png`.

---

## Кирилиця у виводі

**`UnicodeEncodeError` під час запису виводу у файл (Windows).** Команда на
кшталт `python realdata/intel_lab.py > report.txt` на Windows з англійською
мовою системи не може записати українські літери. Перед запуском виконайте у
тому самому вікні PowerShell `$env:PYTHONIOENCODING="utf-8"` або просто
скопіюйте текст із вікна терміналу.

**Замість українських літер «кракозябри» у старому вікні `cmd.exe`.**
Користуйтеся PowerShell або терміналом VS Code.
