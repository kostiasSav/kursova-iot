/*
 * Курсовий проєкт — Стадія 1. Вузол інтернету речей.
 *
 * Шаблон вузла на ESP32, який публікує показання давачів через MQTT.
 * Працює у симуляторі Wokwi — апаратне забезпечення не потрібне.
 *
 * Що вже зроблено:
 *   - підключення до WiFi (віртуальна мережа Wokwi)
 *   - синхронізація часу через NTP
 *   - підключення до публічного MQTT-брокера
 *   - публікація температури у форматі JSON
 *
 * Що треба зробити (позначено TODO):
 *   TODO 1 - публікувати вологість окремим повідомленням
 *   TODO 2 - додати другий давач (фоторезистор) і публікувати освітленість
 *   TODO 3 - зареєструвати Last Will and Testament
 *   TODO 4 - закодувати те саме повідомлення у CBOR і виміряти скорочення обсягу
 *
 * УВАГА. Публічний брокер доступний усім. Будь-хто може підписатися на
 * ваш топік і читати вашу телеметрію. Це навмисно: у стадії 1 ви маєте
 * переконатися в цьому самостійно, а потім усунути проблему.
 */

#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <DHT.h>
#include <time.h>

// ---------------------------------------------------------------------
// Налаштування — замініть на власні
// ---------------------------------------------------------------------

static const char *WIFI_SSID = "Wokwi-GUEST";   // віртуальна мережа Wokwi
static const char *WIFI_PASS = "";
static const int   WIFI_CHANNEL = 6;            // прискорює підключення у Wokwi

static const char *MQTT_HOST = "broker.hivemq.com";
static const int   MQTT_PORT = 1883;

static const int   VARIANT   = 0;               // <- ваш номер варіанта
static const char *DEVICE_ID = "esp32-000";     // <- esp32-<номер варіанта>

static const unsigned long PUBLISH_INTERVAL_MS = 10000;

// ---------------------------------------------------------------------
// Апаратна частина
// ---------------------------------------------------------------------

#define DHT_PIN   15
#define DHT_TYPE  DHT22
#define LDR_PIN   36          // TODO 2: аналоговий вхід фоторезистора

DHT dht(DHT_PIN, DHT_TYPE);
WiFiClient net;
PubSubClient mqtt(net);

static unsigned long lastPublish = 0;
static uint32_t seq = 0;

// ---------------------------------------------------------------------

static void connectWiFi() {
  Serial.print("WiFi: підключення");
  WiFi.begin(WIFI_SSID, WIFI_PASS, WIFI_CHANNEL);
  while (WiFi.status() != WL_CONNECTED) {
    delay(200);
    Serial.print(".");
  }
  Serial.printf("\nWiFi: підключено, IP %s\n", WiFi.localIP().toString().c_str());
}

/*
 * Без справжнього часу телеметрія марна: аналітика будується на мітках
 * часу, а власний годинник ESP32 після ввімкнення показує 1970 рік.
 */
static void syncTime() {
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");
  Serial.print("NTP: синхронізація часу");
  time_t now = time(nullptr);
  while (now < 1700000000) {
    delay(300);
    Serial.print(".");
    now = time(nullptr);
  }
  Serial.printf("\nNTP: час встановлено (%lu)\n", (unsigned long)now);
}

static String topicFor(const char *metric) {
  return String("kursova/") + VARIANT + "/" + DEVICE_ID + "/" + metric;
}

static void connectMQTT() {
  mqtt.setServer(MQTT_HOST, MQTT_PORT);
  while (!mqtt.connected()) {
    Serial.printf("MQTT: підключення до %s ... ", MQTT_HOST);

    String clientId = String(DEVICE_ID) + "-" + String(random(0xffff), HEX);

    // TODO 3 -----------------------------------------------------------
    // Зареєструйте Last Will and Testament. Брокер опублікує це
    // повідомлення САМОСТІЙНО, якщо вузол зникне без попередження —
    // саме так система дізнається про відмову пристрою, який не встиг
    // нічого повідомити.
    //
    // Потрібен варіант connect() з параметрами willTopic, willQos,
    // willRetain, willMessage. Топік: kursova/<variant>/<device_id>/status
    // Вміст: {"online": false}
    //
    // Після під'єднання не забудьте опублікувати {"online": true}
    // з прапорцем retain.
    if (mqtt.connect(clientId.c_str())) {
      Serial.println("успішно");
    } else {
      Serial.printf("помилка, rc=%d; повтор через 2 с\n", mqtt.state());
      delay(2000);
    }
  }
}

/*
 * Схема повідомлення задана у Додатку А методичних вказівок і є
 * обов'язковою: конвеєр обробки даних розраховує саме на ці поля.
 */
static void publishMetric(const char *metric, float value) {
  JsonDocument doc;
  doc["device_id"] = DEVICE_ID;
  doc["metric"]    = metric;
  doc["value"]     = value;
  doc["ts"]        = (uint32_t)time(nullptr);
  doc["seq"]       = seq++;

  char payload[160];
  size_t n = serializeJson(doc, payload, sizeof(payload));

  String topic = topicFor(metric);
  bool ok = mqtt.publish(topic.c_str(), payload, false);

  Serial.printf("%s %s (%u Б) %s\n", ok ? "->" : "!!", topic.c_str(),
                (unsigned)n, payload);

  // TODO 4 -----------------------------------------------------------
  // Закодуйте те саме повідомлення у CBOR і порівняйте розмір із
  // наведеним вище значенням n. Різницю у байтах і у відсотках
  // наведіть у звіті. Помножте економію на кількість пристроїв у
  // вашому варіанті та на 60 діб — і ви отримаєте, скільки трафіку
  // коштує зручність JSON у реальному розгортанні.
}

// ---------------------------------------------------------------------

void setup() {
  Serial.begin(115200);
  delay(100);
  Serial.println("\n=== Вузол IoT: курсовий проєкт ===");

  if (VARIANT == 0) {
    Serial.println("!! Вкажіть свій номер варіанта та DEVICE_ID у налаштуваннях");
  }

  dht.begin();
  pinMode(LDR_PIN, INPUT);

  connectWiFi();
  syncTime();
  connectMQTT();
}

void loop() {
  if (!mqtt.connected()) {
    connectMQTT();
  }
  mqtt.loop();

  if (millis() - lastPublish < PUBLISH_INTERVAL_MS) {
    return;
  }
  lastPublish = millis();

  float t = dht.readTemperature();
  float h = dht.readHumidity();

  // Давач іноді не встигає відповісти. Публікувати NaN не можна —
  // у наборі даних це виглядатиме як несправність, якої насправді немає.
  if (isnan(t) || isnan(h)) {
    Serial.println("!! DHT22: читання не вдалося, пропускаємо цикл");
    return;
  }

  publishMetric("temperature", t);

  // TODO 1 -------------------------------------------------------------
  // Опублікуйте вологість (змінна h) як окреме повідомлення з
  // metric = "humidity".

  // TODO 2 -------------------------------------------------------------
  // Прочитайте фоторезистор через analogRead(LDR_PIN), переведіть
  // показання у люкси (достатньо наближеної залежності — обґрунтуйте
  // її у звіті) і опублікуйте з metric = "illuminance".
}
