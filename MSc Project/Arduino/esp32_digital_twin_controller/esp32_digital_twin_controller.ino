/*
  ESP32 WROOM Industrial Digital Twin Controller (Modular)
  ---------------------------------------------------------
  Hardware modeled:
    - 2x Motors: relay + PWM MOSFET + hall current sensor
    - 2x Lights: relay + PWM MOSFET + hall current sensor
    - 2x Heaters: relay + current sensor (no PWM pin, duty is emulated in software)
    - 2x DS18B20 water temperature sensors (tank1 + tank2 on one OneWire bus)
    - 1x DHT22 ambient sensor
    - 2x analog voltage sensors (load voltage + extra voltage channel)

  MQTT behavior:
    - Publishes telemetry to: dt/<plant_id>/telemetry
    - Subscribes commands from: dt/<plant_id>/cmd
    - Subscribes predictor output: dt/<plant_id>/prediction
    - Publishes command ack to: dt/<plant_id>/cmd_ack

  Control logic:
    - Mirrors simulator style rules:
      * Policy-driven load management with per-load override
      * Mode + season application
      * Overload / peak-risk / restore rules
      * Process/tank-temperature driven heating phases
      * Ambient heater rules

  Required libraries (Arduino Library Manager):
    - ArduinoJson
    - PubSubClient
    - DallasTemperature
    - OneWire
    - DHT sensor library for ESPx (DHTesp)
*/

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <ArduinoOTA.h>
#include <Wire.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <DHTesp.h>
#include <Adafruit_INA219.h>
#include <Preferences.h>
#include <time.h>
#include <esp_wifi.h>
#if __has_include(<esp_eap_client.h>)
#include <esp_eap_client.h>
#define DT_HAS_WIFI_EAP_CLIENT 1
#elif __has_include(<esp_wpa2.h>)
#include <esp_wpa2.h>
#define DT_HAS_WIFI_WPA2_ENTERPRISE 1
#endif
#if __has_include(<esp_arduino_version.h>)
#include <esp_arduino_version.h>
#include "esp_task_wdt.h"
#endif
#if __has_include("secrets.h")
#include "secrets.h"
#endif

#ifndef ESP_ARDUINO_VERSION_MAJOR
#define ESP_ARDUINO_VERSION_MAJOR 2
#endif
#ifndef DT_HAS_WIFI_EAP_CLIENT
#define DT_HAS_WIFI_EAP_CLIENT 0
#endif
#ifndef DT_HAS_WIFI_WPA2_ENTERPRISE
#define DT_HAS_WIFI_WPA2_ENTERPRISE 0
#endif

// Forward declaration for Arduino auto-generated prototypes that use LoadChannel*.
struct LoadChannel;
LoadChannel* findLoadByName(const String& name);

// -----------------------------
// Network / MQTT configuration
// -----------------------------
#ifndef DT_WIFI_SSID
#define DT_WIFI_SSID "YOUR_WIFI_SSID"
#endif
#ifndef DT_WIFI_PASSWORD
#define DT_WIFI_PASSWORD "YOUR_WIFI_PASSWORD"
#endif
#ifndef DT_WIFI_USE_ENTERPRISE
#define DT_WIFI_USE_ENTERPRISE 0
#endif
#ifndef DT_WIFI_EAP_IDENTITY
#define DT_WIFI_EAP_IDENTITY ""
#endif
#ifndef DT_WIFI_EAP_USERNAME
#define DT_WIFI_EAP_USERNAME ""
#endif
#ifndef DT_WIFI_EAP_PASSWORD
#define DT_WIFI_EAP_PASSWORD ""
#endif
#ifndef DT_WIFI_FALLBACK_SSID
#define DT_WIFI_FALLBACK_SSID ""
#endif
#ifndef DT_WIFI_FALLBACK_PASSWORD
#define DT_WIFI_FALLBACK_PASSWORD ""
#endif
#ifndef DT_WIFI_FALLBACK_USE_ENTERPRISE
#define DT_WIFI_FALLBACK_USE_ENTERPRISE 0
#endif
#ifndef DT_WIFI_FALLBACK_EAP_IDENTITY
#define DT_WIFI_FALLBACK_EAP_IDENTITY ""
#endif
#ifndef DT_WIFI_FALLBACK_EAP_USERNAME
#define DT_WIFI_FALLBACK_EAP_USERNAME ""
#endif
#ifndef DT_WIFI_FALLBACK_EAP_PASSWORD
#define DT_WIFI_FALLBACK_EAP_PASSWORD ""
#endif
#ifndef DT_MQTT_BROKER_HOST
#define DT_MQTT_BROKER_HOST "YOUR_MQTT_BROKER_HOST"
#endif
#ifndef DT_MQTT_BROKER_PORT
#define DT_MQTT_BROKER_PORT 8883
#endif
#ifndef DT_MQTT_USERNAME
#define DT_MQTT_USERNAME "YOUR_MQTT_USERNAME"
#endif
#ifndef DT_MQTT_PASSWORD
#define DT_MQTT_PASSWORD "YOUR_MQTT_PASSWORD"
#endif
#ifndef DT_PLANT_IDENTIFIER
#define DT_PLANT_IDENTIFIER "YOUR_PLANT_IDENTIFIER"
#endif
#ifndef DT_OTA_HOSTNAME
#define DT_OTA_HOSTNAME "YOUR_OTA_HOSTNAME"
#endif
#ifndef DT_OTA_PASSWORD
#define DT_OTA_PASSWORD ""
#endif

static const char* DEFAULT_WIFI_SSID = DT_WIFI_SSID;
static const char* DEFAULT_WIFI_PASSWORD = DT_WIFI_PASSWORD;
static const bool WIFI_USE_ENTERPRISE = (DT_WIFI_USE_ENTERPRISE != 0);
static const char* DEFAULT_WIFI_EAP_IDENTITY = DT_WIFI_EAP_IDENTITY;
static const char* DEFAULT_WIFI_EAP_USERNAME = DT_WIFI_EAP_USERNAME;
static const char* DEFAULT_WIFI_EAP_PASSWORD = DT_WIFI_EAP_PASSWORD;
static const char* FALLBACK_WIFI_SSID = DT_WIFI_FALLBACK_SSID;
static const char* FALLBACK_WIFI_PASSWORD = DT_WIFI_FALLBACK_PASSWORD;
static const bool WIFI_FALLBACK_USE_ENTERPRISE = (DT_WIFI_FALLBACK_USE_ENTERPRISE != 0);
static const char* FALLBACK_WIFI_EAP_IDENTITY = DT_WIFI_FALLBACK_EAP_IDENTITY;
static const char* FALLBACK_WIFI_EAP_USERNAME = DT_WIFI_FALLBACK_EAP_USERNAME;
static const char* FALLBACK_WIFI_EAP_PASSWORD = DT_WIFI_FALLBACK_EAP_PASSWORD;
static const char* DEFAULT_MQTT_BROKER_HOST = DT_MQTT_BROKER_HOST;
static const uint16_t DEFAULT_MQTT_BROKER_PORT = DT_MQTT_BROKER_PORT;
static const char* DEFAULT_MQTT_USERNAME = DT_MQTT_USERNAME;
static const char* DEFAULT_MQTT_PASSWORD = DT_MQTT_PASSWORD;

static const unsigned long STARTUP_CONFIG_WINDOW_MS = 8000;
static const unsigned long SERIAL_INPUT_TIMEOUT_MS = 120000;
static const unsigned long WIFI_CONNECT_TIMEOUT_MS = 20000;
static const unsigned long WIFI_RETRY_BACKOFF_MS = 5000;
static const char* PREFERENCES_NAMESPACE = "dtctrl";

static const char* PLANT_IDENTIFIER = DT_PLANT_IDENTIFIER;
static const char* OTA_HOSTNAME = DT_OTA_HOSTNAME;
static const char* OTA_PASSWORD = DT_OTA_PASSWORD;

struct RuntimeConfig {
  String wifiSsid;
  String wifiPassword;
  String mqttHost;
  uint16_t mqttPort = DEFAULT_MQTT_BROKER_PORT;
  String mqttUsername;
  String mqttPassword;
};

RuntimeConfig runtimeConfig;
Preferences preferences;

String topicTelemetry;
String topicCommand;
String topicStatus;
String topicPrediction;
String topicPredictionLong;
String topicPredictionLongLstm;
String topicCommandAck;

// Set true for quick testing. For production, use setCACert with broker root CA.
static const bool USE_INSECURE_TLS = true;

// -----------------------------
// Timing configuration
// -----------------------------
float SAMPLE_INTERVAL_SEC = 1.0f;
unsigned long sampleIntervalMs = 1000;
unsigned long lastSampleMs = 0;

// -----------------------------
// Energy window tracking
// -----------------------------
// Keep per-minute buckets for up to 24h so we can compute rolling windows efficiently.
static const int ENERGY_BUCKETS_PER_DAY = 24 * 60;
float energyMinuteBucketsWh[ENERGY_BUCKETS_PER_DAY] = {0};
int energyBucketHead = 0;
int energyBucketCount = 0;
float currentMinuteBucketWh = 0.0f;
unsigned long currentMinuteBucketStartMs = 0;
float cumulativeEnergyWh = 0.0f;

// -----------------------------
// Sensor conversion constants
// -----------------------------
static const float ADC_REFERENCE_V = 3.3f;
static const float ADC_MAX_COUNTS = 4095.0f;

// INA219 current sensing via TCA9548A I2C multiplexer
static const bool USE_INA219 = true;
static const bool USE_TCA9548A = true; // mux connected
static const uint8_t I2C_SDA_PIN = 32;
static const uint8_t I2C_SCL_PIN = 33;
static const uint8_t TCA9548A_ADDR = 0x70;
static const uint8_t INA219_ADDR = 0x40;
static const uint8_t INA219_CHANNELS[6] = {0, 1, 4, 5, 2, 3}; // load order: motor1, motor2, heater1, heater2, light1, light2
static const float INA219_CURRENT_GAIN[6] = {1.0f, 1.0f, 1.0f, 1.0f, 1.0f, 1.0f};
static const float INA219_CURRENT_DEADBAND_A = 0.015f;
static const float INA219_CURRENT_FILTER_ALPHA = 0.35f;
static const float INA219_OFFSET_TRACK_ALPHA = 0.02f;
static const float INA219_OFFSET_TRACK_MAX_A = 0.12f;
static const uint8_t INA219_ZERO_CAL_SAMPLES = 40;

// Hall current sensor defaults (adjust per sensor model):
// ACS712 30A => ~0.066 V/A at sensor output, zero-current offset near Vcc/2.
// If the sensor is powered at 5V and the signal is divided down to 3.3V ADC,
// set the divider ratio so the ADC sees a safe scaled value.
static const float CURRENT_SENSOR_SUPPLY_V = 5.0f;
static const float CURRENT_SIGNAL_DIVIDER_RATIO = 10.0f / (10.0f + 5.1f); // V_adc / V_sensor_out (10k to GND, 5.1k to sensor)
static const float CURRENT_SENSOR_SENS_V_PER_A = 0.066f;       // sensor output sensitivity
static const float DEFAULT_CURRENT_ZERO_V = (CURRENT_SENSOR_SUPPLY_V * 0.5f) * CURRENT_SIGNAL_DIVIDER_RATIO;
static const float DEFAULT_CURRENT_SENS_V_PER_A = CURRENT_SENSOR_SENS_V_PER_A * CURRENT_SIGNAL_DIVIDER_RATIO;
static const float CURRENT_DEADBAND_A = 0.05f;
static const uint8_t CURRENT_ZERO_CAL_SAMPLES = 32;
static const float CURRENT_ZERO_TRACK_ALPHA = 0.02f;
static const float CURRENT_ZERO_FORCE_A = 0.08f;
static const unsigned long CURRENT_ZERO_SETTLE_MS = 250;
static const bool SIMULATE_TANK_TEMPS_WHEN_SENSOR_MISSING = false;

// Temperature calibration / filtering.
static const float TANK1_TEMP_OFFSET_C = 0.0f;
static const float TANK2_TEMP_OFFSET_C = 0.0f;
static const float DHT_TEMP_OFFSET_C = 0.0f;
static const float DHT_HUM_OFFSET_PCT = 0.0f;
static const float TANK_TEMP_FILTER_ALPHA = 0.30f;
static const float AMBIENT_TEMP_FILTER_ALPHA = 0.30f;
static const float AMBIENT_HUM_FILTER_ALPHA = 0.25f;
static const float TANK_TEMP_MAX_STEP_C = 3.0f;
static const float PROCESS_TEMP_REACHED_TOL_C = 0.25f; // avoid getting stuck due to display-rounding/filter lag
static const float AMBIENT_TEMP_MAX_STEP_C = 4.0f;
static const float AMBIENT_HUM_MAX_STEP_PCT = 10.0f;
static const uint8_t TEMP_INVALID_MAX_HOLD = 4;

// Voltage sensor divider multipliers (adjust to your module calibration)
static const float VOLTAGE_SENSOR_1_RATIO = 12.0f / 2.2f;
static const float VOLTAGE_SENSOR_2_RATIO = 3.3f / 0.5f;

// -----------------------------
// Control settings
// -----------------------------
float MAX_TOTAL_POWER_W = 15.0f;
// Energy goal defaults: 100 Wh cap over a 24h window.
float MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = 100.0f;
float ENERGY_GOAL_DURATION_SEC = 24.0f * 60.0f * 60.0f;
// Undervoltage protection (set threshold <= 0 to disable)
float UNDERVOLTAGE_THRESHOLD_V = 0.0f;
unsigned long UNDERVOLTAGE_TRIP_DELAY_MS = 500;
unsigned long UNDERVOLTAGE_CLEAR_DELAY_MS = 500;
float UNDERVOLTAGE_RESTORE_MARGIN_V = 0.30f;
// Overvoltage protection (set threshold <= 0 to disable)
float OVERVOLTAGE_THRESHOLD_V = 0.0f;
unsigned long OVERVOLTAGE_TRIP_DELAY_MS = 500;
unsigned long OVERVOLTAGE_CLEAR_DELAY_MS = 500;
float OVERVOLTAGE_RESTORE_MARGIN_V = 0.30f;
float RESTORE_POWER_RATIO = 0.80f;
float CONTROL_COOLDOWN_SEC = 5.0f;
float MIN_DEVICE_DUTY = 0.30f;
float DUTY_STEP_SIZE = 0.10f;
float DUTY_RAMP_RATE_PER_SEC = 0.5f;
float CRITICAL_MIN_DEVICE_DUTY = 0.80f;
float HEATER_WARM_TEMP_C = 25.0f;
float HEATER_DUTY_STEP_SIZE = 0.10f;
float HEATER_HOT_OFF_TEMP_C = 30.0f;
bool emergencyStopActive = false;
bool undervoltageActive = false;
unsigned long undervoltageLowSinceMs = 0;
unsigned long undervoltageRecoverSinceMs = 0;
bool overvoltageActive = false;
unsigned long overvoltageHighSinceMs = 0;
unsigned long overvoltageRecoverSinceMs = 0;

enum VoltageInjectionMode { VOLTAGE_INJECTION_NONE, VOLTAGE_INJECTION_UNDERVOLTAGE, VOLTAGE_INJECTION_OVERVOLTAGE };
VoltageInjectionMode voltageInjectionMode = VOLTAGE_INJECTION_NONE;
float voltageInjectionMagnitudeV = 0.0f;
float loadBusVoltageRawV = NAN;
float auxVoltageRawV = NAN;

// Overcurrent protection (per-load)
float LOAD_OVERCURRENT_LIMIT_A = 2.0f;
unsigned long OVERCURRENT_TRIP_DELAY_MS = 100;
unsigned long OVERCURRENT_CLEAR_DELAY_MS = 1000;

// -----------------------------
// Pin map (ESP32 WROOM)
// -----------------------------
// Guide used: ESP32 DevKit/WROOM pinout in your screenshot.
// Strategy:
//   - Current sensing is via INA219 + TCA9548A (I2C), no ADC pins needed.
//   - Use ADC1 pins for voltage sensing to avoid ADC2/WiFi conflicts.

struct DevicePins {
  uint8_t relayPin;
  int8_t pwmPin;      // -1 for no PWM hardware
  uint8_t currentPin; // analog pin
};

// Relay modules are often active-LOW. Set false if your relay turns ON with HIGH.
static const bool RELAY_ACTIVE_LOW = true;
static inline void writeRelayState(uint8_t pin, bool on);

// Motors (relay + PWM + current)
static const DevicePins MOTOR1_PINS = {16, 13, 32};
static const DevicePins MOTOR2_PINS = {17, 26, 33};
// Lights (relay + PWM + current)
static const DevicePins LIGHT1_PINS = {5, 27, 34};
static const DevicePins LIGHT2_PINS = {18, 14, 35};
// Heaters (relay + current only)
static const DevicePins HEATER1_PINS = {19, -1, 36};
static const DevicePins HEATER2_PINS = {21, -1, 39};

// Voltage sensor analog pins (ADC1)
static const uint8_t VOLTAGE_SENSOR_1_PIN = 34; // across loads
static const uint8_t VOLTAGE_SENSOR_2_PIN = 35; // secondary voltage channel

// I2C pins for TCA9548A + INA219
static const uint8_t I2C_SDA_RUNTIME_PIN = I2C_SDA_PIN;
static const uint8_t I2C_SCL_RUNTIME_PIN = I2C_SCL_PIN;

// Temperature sensors
static const uint8_t DS18B20_ONEWIRE_PIN = 23; // OneWire bus for both water DS18B20 sensors
static const uint8_t DHT22_PIN = 22;            // Ambient DHT22 data pin

// Ultrasonic level sensors (echo pins moved to input-only GPIO36/39 to free PWM pins).
static const uint8_t ULTRASONIC_TANK1_TRIG_PIN = 25;  // trigger for tank1
// Use a dedicated trigger line for tank2 to avoid cross-talk between sensors.
static const uint8_t ULTRASONIC_TANK2_TRIG_PIN = 4;   // trigger for tank2
// Tank1 echo moved off GPIO36 to avoid shared-ADC line conflicts in mixed sensor wiring.
static const uint8_t ULTRASONIC_TANK1_ECHO_PIN = 15;  // echo for tank1
static const uint8_t ULTRASONIC_TANK2_ECHO_PIN = 39;  // echo for tank2 (input-only)
// Set true for standard TRIG-high pulse. Set false if TRIG is driven via NPN pull-up (active-low).
static const bool ULTRASONIC_TRIG_ACTIVE_HIGH = true;
static const uint16_t ULTRASONIC_TRIG_PULSE_US = 80; // RCWL-1670 behaves more reliably with a longer trigger pulse
static const uint16_t ULTRASONIC_TRIG_PREPULSE_US = 2;
static const uint8_t ULTRASONIC_RETRY_COUNT = 5;
static const uint16_t ULTRASONIC_RETRY_DELAY_MS = 50;
static const uint16_t ULTRASONIC_INTER_SENSOR_DELAY_MS = 90;
static const unsigned long ULTRASONIC_ECHO_TIMEOUT_US = 35000UL; // sized for <= 4.5m to reduce blocking
static const float ULTRASONIC_MIN_CM = 2.0f;
static const float ULTRASONIC_MAX_CM = 450.0f;
static const float ULTRASONIC_EMA_ALPHA = 0.50f;
static const float ULTRASONIC_FAST_ALPHA = 0.78f;
static const float ULTRASONIC_FAST_DELTA_CM = 1.0f;
static const float ULTRASONIC_JITTER_BAND_CM = 0.20f;
static const float ULTRASONIC_MAX_STEP_CM = 18.0f;
static const uint8_t ULTRASONIC_MAX_CONSECUTIVE_MISSES = 3;
static const uint8_t ULTRASONIC_POST_MEDIAN_WINDOW = 5;
static const float ULTRASONIC_TRANSFER_JITTER_BAND_CM = 0.35f;
// During transfer, allow a small reverse movement band but heavily damp it.
static const float ULTRASONIC_TRANSFER_REVERSE_TOL_CM = 0.6f;
static const float ULTRASONIC_TRANSFER_MAX_REVERSE_STEP_CM = 0.2f;
static const float ULTRASONIC_TRANSFER_ALPHA_FORWARD = 0.45f;
static const float ULTRASONIC_TRANSFER_ALPHA_REVERSE = 0.08f;
static const float ULTRASONIC_HEIGHT_MARGIN_CM = 4.0f;
static const float ULTRASONIC_MAX_RATE_IDLE_CM_PER_SEC = 2.0f;
static const float ULTRASONIC_MAX_RATE_TRANSFER_CM_PER_SEC = 5.0f;
static const unsigned long ULTRASONIC_PUMP_SETTLE_MS = 700;
// SR04M-2 / JSN-SR04T UART mode (set true if your sensor is UART-only).
static const bool ULTRASONIC_UART_MODE = false;
static const uint32_t ULTRASONIC_UART_BAUD = 9600;
static const int8_t ULTRASONIC1_UART_RX_PIN = 36;
static const int8_t ULTRASONIC2_UART_RX_PIN = 39;
static const int8_t ULTRASONIC_UART_TX_PIN = -1; // not required for JSN-SR04T (sensor TX only)

// Tank geometry + production process thresholds
float TANK1_HEIGHT_CM = 19.8f;
float TANK2_HEIGHT_CM = 19.8f;
float TANK1_LOW_LEVEL_PCT = 15.0f;
float TANK2_LOW_LEVEL_PCT = 15.0f;
float TANK1_HIGH_LEVEL_PCT = 90.0f;
float TANK2_HIGH_LEVEL_PCT = 90.0f;
float TANK1_LEVEL_TARGET_PCT = 70.0f;
float TANK2_LEVEL_TARGET_PCT = 70.0f;
float TANK1_TEMP_TARGET_C = 30.0f;
float TANK2_TEMP_TARGET_C = 30.0f;
float PROCESS_TEMP_TARGET_STEP_PER_CYCLE_C = 1.0f;
float TANK_HEAT_RATE_C_PER_SEC = 0.10f;
float TANK_COOL_RATE_PER_SEC = 0.012f;
uint32_t PRODUCTION_GOAL_CYCLES = 4;

// -----------------------------
// Controller state
// -----------------------------
enum ControlPolicy { POLICY_RULE_ONLY, POLICY_HYBRID, POLICY_AI_PREFERRED, POLICY_NO_ENERGY_MANAGEMENT };
enum PlantMode { PLANT_NORMAL, PLANT_HIGH_PROD, PLANT_LOW_PROD, PLANT_ENERGY_SAVING };
enum SeasonMode { SEASON_DEFAULT, SEASON_SUMMER, SEASON_WINTER, SEASON_SPRING, SEASON_AUTUMN };
enum LoadClass { CLASS_CRITICAL, CLASS_ESSENTIAL, CLASS_IMPORTANT, CLASS_SECONDARY, CLASS_NON_ESSENTIAL };

ControlPolicy controlPolicy = POLICY_HYBRID;
PlantMode plantMode = PLANT_NORMAL;
SeasonMode seasonMode = SEASON_DEFAULT;
bool peakEvent = false;
String tariffState = "OFFPEAK";

// Scene automation: drives repeatable production phases for policy comparison.
bool sceneEnabled = false;
String sceneName = "OFF";
float sceneDurationSec = 0.0f;
bool sceneLoop = true;
unsigned long sceneStartMs = 0;
bool sceneLockActive = false;
float sceneTargetTempC = NAN;
float sceneTargetHumidityPct = NAN;
bool sceneOverrideModeEnabled = false;
PlantMode sceneOverrideMode = PLANT_NORMAL;
bool sceneOverrideSeasonEnabled = false;
SeasonMode sceneOverrideSeason = SEASON_DEFAULT;
bool sceneOverridePolicyEnabled = false;
ControlPolicy sceneOverridePolicy = POLICY_HYBRID;
bool sceneOverrideTariffEnabled = false;
String sceneOverrideTariffState = "OFFPEAK";
bool sceneOverridePeakEnabled = false;
bool sceneOverridePeakEvent = false;
bool sceneOverrideTempEnabled = false;
float sceneOverrideTempC = NAN;
bool sceneOverrideHumidityEnabled = false;
float sceneOverrideHumidityPct = NAN;

// Production goal process (two tanks): HEAT1 -> TRANSFER 1->2 -> HEAT2 -> TRANSFER 2->1.
enum ProductionState { PROCESS_IDLE, PROCESS_HEAT_TANK1, PROCESS_TRANSFER_1_TO_2, PROCESS_HEAT_TANK2, PROCESS_TRANSFER_2_TO_1 };
bool productionProcessEnabled = false;
bool processLockActive = false;
ProductionState productionState = PROCESS_IDLE;
unsigned long processStartMs = 0;
uint32_t processCycleCount = 0;
String predictorMode = "OFFLINE";
bool predictorModeSet = true;
bool processConfigGoalCyclesSet = false;
bool processConfigTank1LevelSet = false;
bool processConfigTank2LevelSet = false;
bool processConfigTank1TempSet = false;
bool processConfigTank2TempSet = false;

float tank1LevelPct = 0.0f;
float tank2LevelPct = 0.0f;
float tank1DistanceCm = NAN;
float tank2DistanceCm = NAN;
float tank1RawDistanceCm = NAN;
float tank2RawDistanceCm = NAN;
float lastTank1DistanceCm = NAN;
float lastTank2DistanceCm = NAN;
float tank1DistanceMedianBuf[ULTRASONIC_POST_MEDIAN_WINDOW] = {NAN, NAN, NAN, NAN, NAN};
float tank2DistanceMedianBuf[ULTRASONIC_POST_MEDIAN_WINDOW] = {NAN, NAN, NAN, NAN, NAN};
uint8_t tank1DistanceMedianCount = 0;
uint8_t tank2DistanceMedianCount = 0;
bool tank1LevelValid = false;
bool tank2LevelValid = false;
uint8_t tank1UltrasonicMissCount = 0;
uint8_t tank2UltrasonicMissCount = 0;
unsigned long tank1LastDistanceAcceptedMs = 0;
unsigned long tank2LastDistanceAcceptedMs = 0;
unsigned long ultrasonicPumpSettleUntilMs = 0;
bool ultrasonicPumpStateInitialized = false;
bool ultrasonicLastPump12On = false;
bool ultrasonicLastPump21On = false;
float tank1TempC = 24.0f;
float tank2TempC = 24.0f;

// Predictor signal cache
enum PredictionInputSource { PRED_SRC_SHORT, PRED_SRC_LONG_RF, PRED_SRC_LONG_LSTM };
bool predictorPeakRisk = false;
String predictorRiskLevel = "LOW";
float predictorPowerW = 0.0f;
float predictorHorizonSec = 0.0f;
double predictorSourceTimestampSec = 0.0;
String predictorSourceMode = "FUSED";
bool predictorSourceModelReady = false;
unsigned long predictorLastAppliedMs = 0;
float predictionApplyDelaySec = 0.0f;
static const unsigned long AI_DYNAMIC_PREDICTION_STALE_MS = 15000UL;

// Evaluation counters
unsigned long totalControlActions = 0;
unsigned long sheddingEventCount = 0;
unsigned long curtailEventCount = 0;
unsigned long restoreEventCount = 0;
unsigned long overloadEventCount = 0;
float maxOvershootW = 0.0f;
float overshootEnergyWs = 0.0f;
bool wasOverloadedLastTick = false;
unsigned long loadToggleCount = 0;
bool lastOnState[6] = {true, true, true, true, true, true};

// Peak-risk settling tracker
bool peakRiskActive = false;
unsigned long peakRiskStartMs = 0;
float lastPeakSettlingSec = 0.0f;

struct AISuggestion {
  String deviceName;
  bool hasOn = false;
  bool onValue = true;
  bool hasDuty = false;
  float dutyValue = 1.0f;
  bool hasProcessMode = false;
  ProductionState processMode = PROCESS_IDLE;
};
static const int MAX_AI_SUGGESTIONS = 8;
AISuggestion aiSuggestions[MAX_AI_SUGGESTIONS];
int aiSuggestionCount = 0;

struct PredictionChannelState {
  bool applied = false;
  bool pendingReady = false;
  bool peakRisk = false;
  String riskLevel = "LOW";
  float powerW = 0.0f;
  float horizonSec = 0.0f;
  double sourceTimestampSec = 0.0;
  String sourceMode = "UNSET";
  bool sourceModelReady = false;
  unsigned long lastAppliedMs = 0;
  unsigned long pendingApplyAtMs = 0;
  bool pendingPeakRisk = false;
  String pendingRiskLevel = "LOW";
  float pendingPowerW = 0.0f;
  float pendingHorizonSec = 0.0f;
  double pendingSourceTimestampSec = 0.0;
  String pendingSourceMode = "UNSET";
  bool pendingSourceModelReady = false;
  AISuggestion suggestions[MAX_AI_SUGGESTIONS];
  int suggestionCount = 0;
  AISuggestion pendingSuggestions[MAX_AI_SUGGESTIONS];
  int pendingSuggestionCount = 0;
};

static const int PREDICTION_SOURCE_COUNT = 3;
PredictionChannelState predictionStateBySource[PREDICTION_SOURCE_COUNT];

unsigned long lastControlActionMs = 0;

// -----------------------------
// Hardware clients
// -----------------------------
WiFiClientSecure secureClient;
PubSubClient mqttClient(secureClient);
bool otaStarted = false;
OneWire oneWire(DS18B20_ONEWIRE_PIN);
DallasTemperature ds18b20(&oneWire);
DHTesp dht22;
HardwareSerial UltrasonicSerial1(1);
HardwareSerial UltrasonicSerial2(2);
Adafruit_INA219 ina219(INA219_ADDR);
static const size_t INA219_SENSOR_COUNT = 6;
bool ina219Present[INA219_SENSOR_COUNT] = {false, false, false, false, false, false};

// -----------------------------
// PWM compatibility (ESP32 core v2 vs v3)
// -----------------------------
static const uint32_t PWM_FREQUENCY_HZ = 20000;
static const uint8_t PWM_RESOLUTION_BITS = 8;

static void pwmChannelBegin(uint8_t pin, uint8_t channel) {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  ledcAttachChannel(pin, PWM_FREQUENCY_HZ, PWM_RESOLUTION_BITS, channel);
  ledcWrite(pin, 0);
#else
  ledcSetup(channel, PWM_FREQUENCY_HZ, PWM_RESOLUTION_BITS);
  ledcAttachPin(pin, channel);
  ledcWrite(channel, 0);
#endif
}

static void pwmChannelWrite(uint8_t pin, uint8_t channel, uint8_t duty) {
#if ESP_ARDUINO_VERSION_MAJOR >= 3
  (void)channel;
  ledcWrite(pin, duty);
#else
  (void)pin;
  ledcWrite(channel, duty);
#endif
}

static unsigned long pulseInStable(uint8_t pin, uint8_t state, unsigned long timeoutUs) {
  const uint32_t start = micros();

  // Wait for any previous pulse to end
  while (digitalRead(pin) == state) {
    if ((micros() - start) >= timeoutUs) return 0;
    // keep RTOS/WiFi happy
    if (((micros() - start) & 0x3FFF) == 0) { yield(); }
  }

  // Wait for pulse to start
  while (digitalRead(pin) != state) {
    if ((micros() - start) >= timeoutUs) return 0;
    if (((micros() - start) & 0x3FFF) == 0) { yield(); }
  }

  const uint32_t pulseStart = micros();

  // Measure pulse width
  while (digitalRead(pin) == state) {
    if ((micros() - start) >= timeoutUs) return 0;
    if (((micros() - start) & 0x3FFF) == 0) { yield(); }
  }

  return (unsigned long)(micros() - pulseStart);
}

// -----------------------------
// Device model
// -----------------------------
struct LoadChannel {
  String name;
  DevicePins pins;
  uint8_t pwmChannel;
  bool hasPwm;

  // State
  bool on = true;
  float duty = 1.0f;        // 0..1
  float loadFactor = 1.0f;  // motor-specific usage
  float activityProb = 0.08f; // kept for compatibility (not used for random behavior on hardware)
  LoadClass loadClass = CLASS_IMPORTANT;
  uint8_t priority = 3;

  // Telemetry
  float currentA = 0.0f;
  float powerW = 0.0f;
  float voltageV = 0.0f;
  float tempC = NAN; // used by motors
  String health = "OK";
  String faultCode = "NONE";

  // Calibration
  float currentZeroV = DEFAULT_CURRENT_ZERO_V;
  float currentSensitivityVPerA = DEFAULT_CURRENT_SENS_V_PER_A;
  float currentOffsetA = 0.0f;
  float currentGain = 1.0f;
  float filteredCurrentA = 0.0f;
  bool currentFilterInitialized = false;
  float ratedVoltageV = 12.0f; // overwritten by measured supply at runtime
  float lastDuty = 1.0f;
  float appliedDuty = 0.0f;
  unsigned long lastDutyUpdateMs = 0;
  bool smoothDuty = true;
  float faultInjectA = 0.0f;
  float lastActivePowerW = 0.0f;
  float lastActiveDuty = 1.0f;
  bool policyOverride = false; // exclude this load from policy AI/rule actions
  bool controlReduced = false;
  unsigned long lastAutoCurtailMs = 0;
  unsigned long lastAutoRestoreMs = 0;
  unsigned long restoreBlockedUntilMs = 0;

  // Fault protection
  bool faultActive = false;
  bool faultWasOn = false;
  bool faultLatched = false;
  unsigned long overcurrentStartMs = 0;
  unsigned long faultClearStartMs = 0;
  bool restorePending = false;
  unsigned long restoreAtMs = 0;
  float faultLimitA = LOAD_OVERCURRENT_LIMIT_A;
  unsigned long faultTripDelayMs = OVERCURRENT_TRIP_DELAY_MS;
  unsigned long faultClearDelayMs = OVERCURRENT_CLEAR_DELAY_MS;
  unsigned long faultRestoreDelayMs = OVERCURRENT_CLEAR_DELAY_MS;

  void begin() {
    pinMode(pins.relayPin, OUTPUT);
    writeRelayState(pins.relayPin, false);

    if (hasPwm && pins.pwmPin >= 0) {
      pwmChannelBegin((uint8_t)pins.pwmPin, pwmChannel);
    }

    // When INA219 is used, do not touch the analog current pins (GPIO32/33 are I2C).
    if (!USE_INA219) {
      pinMode(pins.currentPin, INPUT);
    }
  }

  float resolveAppliedDuty(unsigned long nowMs) {
    if (!smoothDuty) {
      appliedDuty = constrain(duty, 0.0f, 1.0f);
      lastDutyUpdateMs = nowMs;
      return appliedDuty;
    }
    if (DUTY_RAMP_RATE_PER_SEC <= 0.0f) {
      appliedDuty = constrain(duty, 0.0f, 1.0f);
      lastDutyUpdateMs = nowMs;
      return appliedDuty;
    }
    if (!on || faultActive) {
      appliedDuty = 0.0f;
      lastDutyUpdateMs = nowMs;
      return 0.0f;
    }
    if (lastDutyUpdateMs == 0) {
      lastDutyUpdateMs = nowMs;
    }
    float dtSec = (nowMs - lastDutyUpdateMs) / 1000.0f;
    if (dtSec <= 0.0f) {
      return appliedDuty;
    }
    lastDutyUpdateMs = nowMs;
    float target = constrain(duty, 0.0f, 1.0f);
    float step = DUTY_RAMP_RATE_PER_SEC * dtSec;
    float diff = target - appliedDuty;
    if (fabs(diff) <= step) appliedDuty = target;
    else appliedDuty += (diff > 0.0f ? step : -step);
    return appliedDuty;
  }

  void applyHardware(unsigned long nowMs) {
    const bool effectiveOn = on && !faultActive;
    float effectiveDuty = constrain(duty, 0.0f, 1.0f);
    if (smoothDuty) {
      effectiveDuty = resolveAppliedDuty(nowMs);
    } else {
      if (!effectiveOn) {
        appliedDuty = 0.0f;
        lastDutyUpdateMs = nowMs;
        effectiveDuty = 0.0f;
      } else {
        appliedDuty = effectiveDuty;
        lastDutyUpdateMs = nowMs;
      }
    }
    // For relay-only channels, emulate duty by time-slicing relay on/off.
    bool relayShouldBeOn = effectiveOn;
    if (!hasPwm && effectiveOn) {
      const unsigned long periodMs = 5000;
      const unsigned long onWindowMs = (unsigned long)(periodMs * constrain(effectiveDuty, 0.0f, 1.0f));
      relayShouldBeOn = ((nowMs % periodMs) < onWindowMs);
    }

    writeRelayState(pins.relayPin, relayShouldBeOn);

    if (hasPwm && pins.pwmPin >= 0) {
      uint8_t pwm = effectiveOn ? (uint8_t)(constrain(effectiveDuty, 0.0f, 1.0f) * 255.0f) : 0;
      pwmChannelWrite((uint8_t)pins.pwmPin, pwmChannel, pwm);
    }
  }

  float readCurrentA() {
    if (USE_INA219) {
      currentA = 0.0f;
      return currentA;
    }
    int raw = analogRead(pins.currentPin);
    float voltage = (raw / ADC_MAX_COUNTS) * ADC_REFERENCE_V;
    if (!on) {
      currentZeroV = (currentZeroV * (1.0f - CURRENT_ZERO_TRACK_ALPHA)) + (voltage * CURRENT_ZERO_TRACK_ALPHA);
    }
    float amps = (voltage - currentZeroV) / currentSensitivityVPerA;
    // Suppress near-zero sensor jitter
    if (fabs(amps) < CURRENT_DEADBAND_A) amps = 0.0f;
    float absA = fabs(amps);
    if (!on && absA < CURRENT_ZERO_FORCE_A) absA = 0.0f;
    currentA = absA;
    return currentA;
  }

  void updateElectricalTelemetry(float measuredSupplyVoltageV) {
    ratedVoltageV = measuredSupplyVoltageV;
    readCurrentA();
    voltageV = measuredSupplyVoltageV;
    powerW = max(0.0f, fabs(currentA) * voltageV);
  }
};

LoadChannel motor1 = {"motor1", MOTOR1_PINS, 0, true};
LoadChannel motor2 = {"motor2", MOTOR2_PINS, 1, true};
LoadChannel light1 = {"lighting1", LIGHT1_PINS, 2, true};
LoadChannel light2 = {"lighting2", LIGHT2_PINS, 3, true};
LoadChannel heater1 = {"heater1", HEATER1_PINS, 255, false};
LoadChannel heater2 = {"heater2", HEATER2_PINS, 255, false};

LoadChannel* loads[] = {&motor1, &motor2, &heater1, &heater2, &light1, &light2};
const size_t LOAD_COUNT = sizeof(loads) / sizeof(loads[0]);

// -----------------------------
// Ambient / sensor state
// -----------------------------
float ambientTempC = 24.0f;
float ambientHumidity = 55.0f;
float seasonTempOffsetC = 0.0f;
float seasonHumidityOffset = 0.0f;
bool ambientTempInitialized = false;
bool ambientHumidityInitialized = false;
bool tank1TempInitialized = false;
bool tank2TempInitialized = false;
uint8_t tank1TempInvalidCount = 0;
uint8_t tank2TempInvalidCount = 0;
static const unsigned long DHT_READ_INTERVAL_MS = 2000;
unsigned long lastDhtReadMs = 0;

float loadBusVoltageV = 11.9f;
float auxVoltageV = 0.0f;

DeviceAddress tankTempAddress1, tankTempAddress2;
bool tankTempSensor1Found = false;
bool tankTempSensor2Found = false;

// -----------------------------
// Utility
// -----------------------------
static inline void writeRelayState(uint8_t pin, bool on) {
  digitalWrite(pin, (on ^ RELAY_ACTIVE_LOW) ? HIGH : LOW);
}

static inline float clampf(float x, float lo, float hi) {
  return max(lo, min(hi, x));
}

const char* voltageInjectionModeToString(VoltageInjectionMode mode) {
  switch (mode) {
    case VOLTAGE_INJECTION_UNDERVOLTAGE: return "UNDERVOLTAGE";
    case VOLTAGE_INJECTION_OVERVOLTAGE: return "OVERVOLTAGE";
    default: return "NONE";
  }
}

bool parseVoltageInjectionMode(const String& input, VoltageInjectionMode& out) {
  String s = input;
  s.trim();
  s.toUpperCase();
  if (s == "UNDERVOLTAGE" || s == "UV" || s == "UNDER") {
    out = VOLTAGE_INJECTION_UNDERVOLTAGE;
    return true;
  }
  if (s == "OVERVOLTAGE" || s == "OV" || s == "OVER") {
    out = VOLTAGE_INJECTION_OVERVOLTAGE;
    return true;
  }
  if (s == "NONE" || s == "CLEAR" || s == "OFF") {
    out = VOLTAGE_INJECTION_NONE;
    return true;
  }
  return false;
}

static void clearVoltageInjection() {
  voltageInjectionMode = VOLTAGE_INJECTION_NONE;
  voltageInjectionMagnitudeV = 0.0f;
}

static float applyInjectedVoltageScenario(float measuredVoltageV) {
  if (isnan(measuredVoltageV)) return measuredVoltageV;
  const float magnitudeV = max(0.0f, voltageInjectionMagnitudeV);
  if (voltageInjectionMode == VOLTAGE_INJECTION_NONE || magnitudeV <= 0.0f) {
    return measuredVoltageV;
  }
  if (voltageInjectionMode == VOLTAGE_INJECTION_UNDERVOLTAGE) {
    return max(0.0f, measuredVoltageV - magnitudeV);
  }
  return measuredVoltageV + magnitudeV;
}

static inline float smoothBounded(float previous, float target, float alpha, float maxStep) {
  if (isnan(target)) return previous;
  if (isnan(previous)) return target;
  float bounded = target;
  if (maxStep > 0.0f) {
    bounded = clampf(target, previous - maxStep, previous + maxStep);
  }
  return previous + (bounded - previous) * clampf(alpha, 0.0f, 1.0f);
}

static inline bool isValidDs18Sample(float t) {
  if (isnan(t)) return false;
  if (t <= -126.0f || t >= 125.0f) return false; // disconnected/invalid markers
  if (fabs(t - 85.0f) < 0.05f) return false;     // DS18B20 power-on default
  return true;
}

static inline bool hasReachedTempTarget(float measuredC, float targetC) {
  if (isnan(measuredC) || isnan(targetC)) return false;
  return measuredC >= max(0.0f, targetC - PROCESS_TEMP_REACHED_TOL_C);
}

static inline float effectiveProcessTempTargetC(float baseTargetC) {
  const float cycleOffsetC = productionProcessEnabled
    ? ((float)processCycleCount * PROCESS_TEMP_TARGET_STEP_PER_CYCLE_C)
    : 0.0f;
  return clampf(baseTargetC + cycleOffsetC, 0.0f, 95.0f);
}

static inline float tank1EffectiveTempTargetC() {
  return effectiveProcessTempTargetC(TANK1_TEMP_TARGET_C);
}

static inline float tank2EffectiveTempTargetC() {
  return effectiveProcessTempTargetC(TANK2_TEMP_TARGET_C);
}

static inline float maxHighPctForHeight(float heightCm) {
  if (heightCm <= 0.0f) return 98.0f;
  const float blindPct = (ULTRASONIC_MIN_CM / heightCm) * 100.0f;
  return clampf(100.0f - blindPct, 20.0f, 98.0f);
}

static inline float clampLowPctForHeight(float lowPct, float heightCm) {
  const float maxBound = min(80.0f, maxHighPctForHeight(heightCm));
  return clampf(lowPct, 0.0f, maxBound);
}

static inline float clampHighPctForHeight(float highPct, float heightCm, float lowPct) {
  const float maxBound = maxHighPctForHeight(heightCm);
  const float minBound = max(20.0f, lowPct);
  return clampf(highPct, minBound, maxBound);
}

static bool processDemandsLoad(const LoadChannel* ld) {
  if (ld == nullptr || !productionProcessEnabled) return false;
  if (productionState == PROCESS_HEAT_TANK1) return ld == &heater1;
  if (productionState == PROCESS_TRANSFER_1_TO_2) return ld == &motor1;
  if (productionState == PROCESS_HEAT_TANK2) return ld == &heater2;
  if (productionState == PROCESS_TRANSFER_2_TO_1) return ld == &motor2;
  return false;
}

static bool isPolicyManagedLoad(const LoadChannel* ld) {
  return ld != nullptr && !ld->policyOverride;
}

void updateUndervoltageProtectionState(unsigned long nowMs) {
  if (UNDERVOLTAGE_THRESHOLD_V <= 0.0f || isnan(loadBusVoltageV)) {
    undervoltageActive = false;
    undervoltageLowSinceMs = 0;
    undervoltageRecoverSinceMs = 0;
    return;
  }

  const bool lowNow = loadBusVoltageV < UNDERVOLTAGE_THRESHOLD_V;
  const float restoreThresholdV = UNDERVOLTAGE_THRESHOLD_V + max(0.05f, UNDERVOLTAGE_RESTORE_MARGIN_V);

  if (lowNow) {
    undervoltageRecoverSinceMs = 0;
    if (undervoltageLowSinceMs == 0) undervoltageLowSinceMs = nowMs;
    if (!undervoltageActive && (nowMs - undervoltageLowSinceMs >= UNDERVOLTAGE_TRIP_DELAY_MS)) {
      undervoltageActive = true;
    }
    return;
  }

  undervoltageLowSinceMs = 0;
  if (!undervoltageActive) return;

  if (loadBusVoltageV >= restoreThresholdV) {
    if (undervoltageRecoverSinceMs == 0) undervoltageRecoverSinceMs = nowMs;
    if (nowMs - undervoltageRecoverSinceMs >= UNDERVOLTAGE_CLEAR_DELAY_MS) {
      undervoltageActive = false;
      undervoltageRecoverSinceMs = 0;
    }
  } else {
    undervoltageRecoverSinceMs = 0;
  }
}

void updateOvervoltageProtectionState(unsigned long nowMs) {
  if (OVERVOLTAGE_THRESHOLD_V <= 0.0f || isnan(loadBusVoltageV)) {
    overvoltageActive = false;
    overvoltageHighSinceMs = 0;
    overvoltageRecoverSinceMs = 0;
    return;
  }

  const bool highNow = loadBusVoltageV > OVERVOLTAGE_THRESHOLD_V;
  const float recoverThresholdV = OVERVOLTAGE_THRESHOLD_V - max(0.05f, OVERVOLTAGE_RESTORE_MARGIN_V);

  if (highNow) {
    overvoltageRecoverSinceMs = 0;
    if (overvoltageHighSinceMs == 0) overvoltageHighSinceMs = nowMs;
    if (!overvoltageActive && (nowMs - overvoltageHighSinceMs >= OVERVOLTAGE_TRIP_DELAY_MS)) {
      overvoltageActive = true;
    }
    return;
  }

  overvoltageHighSinceMs = 0;
  if (!overvoltageActive) return;

  if (loadBusVoltageV <= recoverThresholdV) {
    if (overvoltageRecoverSinceMs == 0) overvoltageRecoverSinceMs = nowMs;
    if (nowMs - overvoltageRecoverSinceMs >= OVERVOLTAGE_CLEAR_DELAY_MS) {
      overvoltageActive = false;
      overvoltageRecoverSinceMs = 0;
    }
  } else {
    overvoltageRecoverSinceMs = 0;
  }
}

void updateOvercurrentFault(LoadChannel* ld, unsigned long nowMs) {
  if (ld == nullptr) return;
  const float currentA = ld->currentA;
  const float limitA = (ld->faultLimitA > 0.0f) ? ld->faultLimitA : LOAD_OVERCURRENT_LIMIT_A;
  const unsigned long tripDelayMs = (ld->faultTripDelayMs > 0) ? ld->faultTripDelayMs : OVERCURRENT_TRIP_DELAY_MS;
  const unsigned long clearDelayMs = (ld->faultClearDelayMs > 0) ? ld->faultClearDelayMs : OVERCURRENT_CLEAR_DELAY_MS;
  const unsigned long restoreDelayMs = (ld->faultRestoreDelayMs > 0) ? ld->faultRestoreDelayMs : OVERCURRENT_CLEAR_DELAY_MS;
  if (currentA >= limitA) {
    if (ld->overcurrentStartMs == 0) {
      ld->overcurrentStartMs = nowMs;
    }
    if (!ld->faultActive && (nowMs - ld->overcurrentStartMs >= tripDelayMs)) {
      ld->faultActive = true;
      ld->faultWasOn = ld->on;
      ld->faultLatched = false;
      ld->restorePending = false;
      ld->faultCode = "OVERCURRENT";
      ld->health = "FAULT";
      if (ld->on) {
        rememberDutyBeforeOff(ld);
      }
      ld->on = false;
      ld->duty = 0.0f;
      ld->appliedDuty = 0.0f;
      ld->lastDutyUpdateMs = nowMs;
      ld->applyHardware(nowMs);
      ld->faultClearStartMs = 0;
    }
  } else {
    ld->overcurrentStartMs = 0;
    if (ld->faultActive) {
      if (ld->faultLatched) {
        ld->faultClearStartMs = 0;
        return;
      }
      if (ld->faultInjectA > 0.0f) {
        ld->faultClearStartMs = 0;
        return;
      }
      if (ld->faultClearStartMs == 0) {
        ld->faultClearStartMs = nowMs;
      }
      if (nowMs - ld->faultClearStartMs >= clearDelayMs) {
        ld->faultActive = false;
        ld->faultCode = "NONE";
        ld->health = "OK";
        ld->faultClearStartMs = 0;
        const bool processDemand = (productionProcessEnabled && processDemandsLoad(ld));
        const bool allowRestore = (ld->faultWasOn || processDemand);
        if ((ld->faultWasOn || processDemand) && allowRestore) {
          ld->restorePending = true;
          ld->restoreAtMs = nowMs + restoreDelayMs;
          ld->on = false;
          ld->duty = 0.0f;
          ld->appliedDuty = 0.0f;
          ld->lastDutyUpdateMs = nowMs;
        } else {
          ld->restorePending = false;
          ld->on = false;
          ld->duty = 0.0f;
          ld->appliedDuty = 0.0f;
          ld->lastDutyUpdateMs = nowMs;
        }
      }
    } else {
      ld->faultClearStartMs = 0;
    }
  }

  if (!ld->faultActive && ld->restorePending && nowMs >= ld->restoreAtMs) {
    const bool processDemand = (productionProcessEnabled && processDemandsLoad(ld));
    const bool allowRestore = (ld->faultWasOn || processDemand);
    const float limitA = (ld->faultLimitA > 0.0f) ? ld->faultLimitA : LOAD_OVERCURRENT_LIMIT_A;
    const float predictedCurrentA = estimateRestoreCurrentA(ld);
    if (ld->faultInjectA > 0.0f) {
      ld->restoreAtMs = nowMs + 500;
      return;
    }
    if (limitA > 0.0f && predictedCurrentA >= limitA) {
      ld->restoreAtMs = nowMs + 1000;
      return;
    }
    if ((ld->faultWasOn || processDemand) && allowRestore) {
      ld->on = true;
      restoreDutyOn(ld);
    }
    ld->restorePending = false;
  }
}

static bool processConfigReady() {
  return processConfigGoalCyclesSet
    && processConfigTank1LevelSet
    && processConfigTank2LevelSet
    && processConfigTank1TempSet
    && processConfigTank2TempSet
    && predictorModeSet;
}

static String processConfigMissing() {
  String missing = "";
  if (!processConfigGoalCyclesSet) missing += "goal_cycles,";
  if (!processConfigTank1LevelSet) missing += "tank1_level_target_pct,";
  if (!processConfigTank2LevelSet) missing += "tank2_level_target_pct,";
  if (!processConfigTank1TempSet) missing += "tank1_temp_target_c,";
  if (!processConfigTank2TempSet) missing += "tank2_temp_target_c,";
  if (!predictorModeSet) missing += "predictor_mode,";
  if (missing.endsWith(",")) missing.remove(missing.length() - 1);
  return missing;
}
const char* productionStateToString(ProductionState s) {
  switch (s) {
    case PROCESS_HEAT_TANK1: return "HEAT_TANK1";
    case PROCESS_TRANSFER_1_TO_2: return "TRANSFER_1_TO_2";
    case PROCESS_HEAT_TANK2: return "HEAT_TANK2";
    case PROCESS_TRANSFER_2_TO_1: return "TRANSFER_2_TO_1";
    default: return "IDLE";
  }
}

bool parseProductionState(const String& input, ProductionState& out) {
  String s = input;
  s.trim();
  s.toUpperCase();
  if (s == "HEAT_TANK1") {
    out = PROCESS_HEAT_TANK1;
    return true;
  }
  if (s == "TRANSFER_1_TO_2") {
    out = PROCESS_TRANSFER_1_TO_2;
    return true;
  }
  if (s == "HEAT_TANK2") {
    out = PROCESS_HEAT_TANK2;
    return true;
  }
  if (s == "TRANSFER_2_TO_1") {
    out = PROCESS_TRANSFER_2_TO_1;
    return true;
  }
  if (s == "IDLE") {
    out = PROCESS_IDLE;
    return true;
  }
  return false;
}

float distanceToLevelPct(float distanceCm, float heightCm) {
  if (heightCm <= 0.1f) return 0.0f;
  float level = ((heightCm - distanceCm) / heightCm) * 100.0f;
  return clampf(level, 0.0f, 100.0f);
}

float levelToDistanceCm(float levelPct, float heightCm) {
  return max(0.0f, heightCm * (1.0f - clampf(levelPct, 0.0f, 100.0f) / 100.0f));
}

static inline float ultrasonicSoundSpeedCmPerUs() {
  // Temperature compensation improves distance accuracy vs fixed 0.0344 cm/us.
  float tempC = ambientTempC;
  if (isnan(tempC)) tempC = 20.0f;
  tempC = clampf(tempC, -10.0f, 60.0f);
  return (331.3f + (0.606f * tempC)) / 10000.0f;
}

bool readUltrasonicDistanceUartCm(HardwareSerial& port, float& distanceCm) {
  const unsigned long timeoutMs = 60;
  unsigned long startMs = millis();
  while ((millis() - startMs) < timeoutMs) {
    while (port.available() >= 4) {
      int header = port.read();
      if (header != 0xFF) continue;
      int high = port.read();
      int low = port.read();
      int checksum = port.read();
      uint8_t sum = (uint8_t)((0xFF + high + low) & 0xFF);
      if (sum != (uint8_t)checksum) {
        continue;
      }
      int distMm = (high << 8) | low;
      if (distMm <= 0) continue;
      distanceCm = distMm / 10.0f;
      return true;
    }
    delay(2);
  }
  return false;
}

static inline void triggerUltrasonicPulse(uint8_t trigPin) {
  // Generate trigger pulse (Mode 0 / HC-SR04 style).
  if (ULTRASONIC_TRIG_ACTIVE_HIGH) {
    digitalWrite(trigPin, LOW);
    delayMicroseconds(ULTRASONIC_TRIG_PREPULSE_US);
    digitalWrite(trigPin, HIGH);
    delayMicroseconds(ULTRASONIC_TRIG_PULSE_US);
    digitalWrite(trigPin, LOW);
  } else {
    digitalWrite(trigPin, HIGH);
    delayMicroseconds(ULTRASONIC_TRIG_PREPULSE_US);
    digitalWrite(trigPin, LOW);
    delayMicroseconds(ULTRASONIC_TRIG_PULSE_US);
    digitalWrite(trigPin, HIGH);
  }
}

// float readUltrasonicDistanceCm(uint8_t echoPin, uint8_t trigPin, unsigned long* pulseUsOut = nullptr) {
//   triggerUltrasonicPulse(trigPin);
//   unsigned long durationUs = pulseIn(echoPin, HIGH, ULTRASONIC_ECHO_TIMEOUT_US);
//   if (pulseUsOut) *pulseUsOut = durationUs;
//   if (durationUs == 0) return NAN;

//   float distance = (durationUs * 0.0344f) / 2.0f;
//   if (distance < ULTRASONIC_MIN_CM || distance > ULTRASONIC_MAX_CM) return NAN;
//   return distance;
// }
float readUltrasonicDistanceCm(uint8_t echoPin, uint8_t trigPin, unsigned long* pulseUsOut = nullptr) {
  // Re-assert pin modes every time (important when system is “busy”)
  pinMode(trigPin, OUTPUT);
  digitalWrite(trigPin, ULTRASONIC_TRIG_ACTIVE_HIGH ? LOW : HIGH);
  pinMode(echoPin, INPUT);

  // Let line settle a moment
  delayMicroseconds(5);

  triggerUltrasonicPulse(trigPin);

  // Use stable pulse measurement instead of pulseIn()
  unsigned long durationUs = pulseInStable(echoPin, HIGH, ULTRASONIC_ECHO_TIMEOUT_US);

  if (pulseUsOut) *pulseUsOut = durationUs;
  if (durationUs == 0) return NAN;

  float distance = (durationUs * ultrasonicSoundSpeedCmPerUs()) / 2.0f;
  if (distance < ULTRASONIC_MIN_CM || distance > ULTRASONIC_MAX_CM) return NAN;
  return distance;
}

float readUltrasonicDistanceWithRetry(uint8_t echoPin, uint8_t trigPin, unsigned long* pulseUsOut = nullptr) {
  float validDistances[ULTRASONIC_RETRY_COUNT];
  unsigned long validPulses[ULTRASONIC_RETRY_COUNT];
  uint8_t validCount = 0;
  unsigned long lastPulse = 0;
  for (uint8_t attempt = 0; attempt < ULTRASONIC_RETRY_COUNT; ++attempt) {
    float distance = readUltrasonicDistanceCm(echoPin, trigPin, &lastPulse);
    if (!isnan(distance)) {
      if (validCount < ULTRASONIC_RETRY_COUNT) {
        validDistances[validCount] = distance;
        validPulses[validCount] = lastPulse;
        validCount++;
      }
    }
    if (attempt + 1 < ULTRASONIC_RETRY_COUNT) {
      delay(ULTRASONIC_RETRY_DELAY_MS);
    }
  }
  if (validCount > 0) {
    // Sort to get median valid sample (rejects spikes better than first-valid).
    for (uint8_t i = 0; i < validCount; ++i) {
      for (uint8_t j = i + 1; j < validCount; ++j) {
        if (validDistances[j] < validDistances[i]) {
          float d = validDistances[i];
          validDistances[i] = validDistances[j];
          validDistances[j] = d;
          unsigned long p = validPulses[i];
          validPulses[i] = validPulses[j];
          validPulses[j] = p;
        }
      }
    }
    uint8_t mid = validCount / 2;
    if (pulseUsOut) *pulseUsOut = validPulses[mid];
    return validDistances[mid];
  }
  if (pulseUsOut) *pulseUsOut = lastPulse;
  return NAN;
}

static inline float sanitizeUltrasonicDistanceCm(float rawCm) {
  if (isnan(rawCm)) return NAN;
  if (rawCm < ULTRASONIC_MIN_CM || rawCm > ULTRASONIC_MAX_CM) return NAN;
  return rawCm;
}

static inline float sanitizeUltrasonicDistanceForTankCm(float rawCm, float tankHeightCm) {
  float distanceCm = sanitizeUltrasonicDistanceCm(rawCm);
  if (isnan(distanceCm)) return NAN;
  if (tankHeightCm <= 0.0f) return distanceCm;
  const float tankMaxCm = min(ULTRASONIC_MAX_CM, max(ULTRASONIC_MIN_CM + 0.5f, tankHeightCm + ULTRASONIC_HEIGHT_MARGIN_CM));
  if (distanceCm > tankMaxCm) return NAN;
  return distanceCm;
}

static float medianFloatWindow(const float* values, uint8_t count) {
  if (count == 0) return NAN;
  float sorted[ULTRASONIC_POST_MEDIAN_WINDOW];
  for (uint8_t i = 0; i < count; ++i) sorted[i] = values[i];
  for (uint8_t i = 0; i < count; ++i) {
    for (uint8_t j = i + 1; j < count; ++j) {
      if (sorted[j] < sorted[i]) {
        float tmp = sorted[i];
        sorted[i] = sorted[j];
        sorted[j] = tmp;
      }
    }
  }
  return sorted[count / 2];
}

static float applyUltrasonicPostMedian(float value, float* buffer, uint8_t& count) {
  if (isnan(value)) return NAN;
  if (count < ULTRASONIC_POST_MEDIAN_WINDOW) {
    buffer[count++] = value;
  } else {
    for (uint8_t i = 0; i < ULTRASONIC_POST_MEDIAN_WINDOW - 1; ++i) {
      buffer[i] = buffer[i + 1];
    }
    buffer[ULTRASONIC_POST_MEDIAN_WINDOW - 1] = value;
  }
  return medianFloatWindow(buffer, count);
}

static int8_t expectedDistanceDirectionTank1() {
  if (!productionProcessEnabled) return 0;
  if (productionState == PROCESS_TRANSFER_1_TO_2) return 1;   // tank1 drains -> distance rises
  if (productionState == PROCESS_TRANSFER_2_TO_1) return -1;  // tank1 fills -> distance falls
  return 0;
}

static int8_t expectedDistanceDirectionTank2() {
  if (!productionProcessEnabled) return 0;
  if (productionState == PROCESS_TRANSFER_1_TO_2) return -1;  // tank2 fills -> distance falls
  if (productionState == PROCESS_TRANSFER_2_TO_1) return 1;   // tank2 drains -> distance rises
  return 0;
}

static bool transferPhaseActive() {
  return productionProcessEnabled
    && (productionState == PROCESS_TRANSFER_1_TO_2 || productionState == PROCESS_TRANSFER_2_TO_1);
}

static void updateUltrasonicPumpSettleWindow(unsigned long nowMs) {
  const bool pump12On = motor1.on;
  const bool pump21On = motor2.on;
  if (!ultrasonicPumpStateInitialized) {
    ultrasonicPumpStateInitialized = true;
    ultrasonicLastPump12On = pump12On;
    ultrasonicLastPump21On = pump21On;
    return;
  }
  if (pump12On != ultrasonicLastPump12On || pump21On != ultrasonicLastPump21On) {
    ultrasonicPumpSettleUntilMs = nowMs + ULTRASONIC_PUMP_SETTLE_MS;
    ultrasonicLastPump12On = pump12On;
    ultrasonicLastPump21On = pump21On;
  }
}

static float applyUltrasonicRateLimitCm(
  float valueCm,
  float previousCm,
  unsigned long& lastAcceptedMs,
  unsigned long nowMs,
  bool isTransferPhase
) {
  if (isnan(valueCm)) return previousCm;
  if (isnan(previousCm)) {
    lastAcceptedMs = nowMs;
    return valueCm;
  }

  float dtSec = 0.0f;
  if (lastAcceptedMs > 0 && nowMs >= lastAcceptedMs) {
    dtSec = (nowMs - lastAcceptedMs) / 1000.0f;
  }
  if (dtSec <= 0.0f) dtSec = max(0.05f, SAMPLE_INTERVAL_SEC);

  const float rateCmPerSec = isTransferPhase ? ULTRASONIC_MAX_RATE_TRANSFER_CM_PER_SEC : ULTRASONIC_MAX_RATE_IDLE_CM_PER_SEC;
  const float maxStep = max(0.05f, rateCmPerSec * dtSec);
  const float delta = valueCm - previousCm;
  const float limited = previousCm + clampf(delta, -maxStep, maxStep);
  lastAcceptedMs = nowMs;
  return limited;
}

static float stabilizeUltrasonicDistanceCm(float rawCm, float previousCm, float tankHeightCm, int8_t expectedDirection) {
  if (isnan(rawCm)) return previousCm;
  if (isnan(previousCm)) return rawCm;

  // Prevent one-shot jumps from immediately distorting tank level.
  float maxStep = min(ULTRASONIC_MAX_STEP_CM, max(2.0f, tankHeightCm * 0.5f));
  float delta = rawCm - previousCm;
  float jitterBand = (expectedDirection != 0)
    ? max(ULTRASONIC_JITTER_BAND_CM, ULTRASONIC_TRANSFER_JITTER_BAND_CM)
    : ULTRASONIC_JITTER_BAND_CM;
  if (fabs(delta) <= jitterBand) {
    return previousCm;
  }
  if (fabs(delta) > maxStep) {
    delta = (delta > 0.0f ? maxStep : -maxStep);
    rawCm = previousCm + delta;
  }

  float alpha = ULTRASONIC_EMA_ALPHA;
  if (expectedDirection == 0 && fabs(delta) >= ULTRASONIC_FAST_DELTA_CM && alpha < ULTRASONIC_FAST_ALPHA) {
    alpha = ULTRASONIC_FAST_ALPHA;
  }
  // During transfer, reject/strongly damp brief reverse jumps caused by turbulence.
  if (expectedDirection != 0) {
    bool reverse = (expectedDirection > 0) ? (delta < -ULTRASONIC_TRANSFER_REVERSE_TOL_CM)
                                           : (delta > ULTRASONIC_TRANSFER_REVERSE_TOL_CM);
    if (reverse) {
      if (expectedDirection > 0) {
        delta = max(delta, -ULTRASONIC_TRANSFER_MAX_REVERSE_STEP_CM);
      } else {
        delta = min(delta, ULTRASONIC_TRANSFER_MAX_REVERSE_STEP_CM);
      }
      rawCm = previousCm + delta;
      alpha = ULTRASONIC_TRANSFER_ALPHA_REVERSE;
    } else {
      alpha = max(alpha, ULTRASONIC_TRANSFER_ALPHA_FORWARD);
    }
  }

  return previousCm + (rawCm - previousCm) * alpha;
}

void measureTankLevelsFromUltrasonic() {
  const unsigned long nowMs = millis();
  updateUltrasonicPumpSettleWindow(nowMs);

  // Hold last stable value briefly when pump states change (relay bounce/turbulence window).
  if (nowMs < ultrasonicPumpSettleUntilMs) {
    if (!isnan(lastTank1DistanceCm)) {
      tank1DistanceCm = lastTank1DistanceCm;
      tank1LevelPct = distanceToLevelPct(tank1DistanceCm, TANK1_HEIGHT_CM);
      tank1LevelValid = true;
    }
    if (!isnan(lastTank2DistanceCm)) {
      tank2DistanceCm = lastTank2DistanceCm;
      tank2LevelPct = distanceToLevelPct(tank2DistanceCm, TANK2_HEIGHT_CM);
      tank2LevelValid = true;
    }
    return;
  }

  float d1 = NAN;
  float d2 = NAN;
  static bool readTank1First = true;
  if (ULTRASONIC_UART_MODE) {
    float cm = NAN;
    if (readUltrasonicDistanceUartCm(UltrasonicSerial1, cm)) d1 = cm;
    cm = NAN;
    if (readUltrasonicDistanceUartCm(UltrasonicSerial2, cm)) d2 = cm;
  } else {
    unsigned long pulse1 = 0;
    unsigned long pulse2 = 0;
    if (readTank1First) {
      d1 = readUltrasonicDistanceWithRetry(ULTRASONIC_TANK1_ECHO_PIN, ULTRASONIC_TANK1_TRIG_PIN, &pulse1);
      delay(ULTRASONIC_INTER_SENSOR_DELAY_MS);
      d2 = readUltrasonicDistanceWithRetry(ULTRASONIC_TANK2_ECHO_PIN, ULTRASONIC_TANK2_TRIG_PIN, &pulse2);
    } else {
      d2 = readUltrasonicDistanceWithRetry(ULTRASONIC_TANK2_ECHO_PIN, ULTRASONIC_TANK2_TRIG_PIN, &pulse2);
      delay(ULTRASONIC_INTER_SENSOR_DELAY_MS);
      d1 = readUltrasonicDistanceWithRetry(ULTRASONIC_TANK1_ECHO_PIN, ULTRASONIC_TANK1_TRIG_PIN, &pulse1);
    }
    readTank1First = !readTank1First;
    Serial.printf("[ultra] tank1 pulse=%lu us dist=%.2fcm miss=%u | tank2 pulse=%lu us dist=%.2fcm miss=%u\n",
                  pulse1, isnan(d1) ? -1.0f : d1, tank1UltrasonicMissCount,
                  pulse2, isnan(d2) ? -1.0f : d2, tank2UltrasonicMissCount);
  }
  // Raw sensor-reported distance before tank-height gating and level conversion.
  tank1RawDistanceCm = d1;
  tank2RawDistanceCm = d2;
  d1 = sanitizeUltrasonicDistanceForTankCm(d1, TANK1_HEIGHT_CM);
  d2 = sanitizeUltrasonicDistanceForTankCm(d2, TANK2_HEIGHT_CM);
  const bool transferPhase = transferPhaseActive();
  if (!isnan(d1)) {
    float stable1 = stabilizeUltrasonicDistanceCm(d1, lastTank1DistanceCm, TANK1_HEIGHT_CM, expectedDistanceDirectionTank1());
    float smoothed1 = applyUltrasonicPostMedian(stable1, tank1DistanceMedianBuf, tank1DistanceMedianCount);
    float limited1 = applyUltrasonicRateLimitCm(smoothed1, lastTank1DistanceCm, tank1LastDistanceAcceptedMs, nowMs, transferPhase);
    tank1DistanceCm = limited1;
    lastTank1DistanceCm = limited1;
    tank1UltrasonicMissCount = 0;
    tank1LevelValid = true;
  } else if (!isnan(lastTank1DistanceCm) && tank1UltrasonicMissCount < ULTRASONIC_MAX_CONSECUTIVE_MISSES) {
    tank1UltrasonicMissCount++;
    tank1DistanceCm = lastTank1DistanceCm;
    tank1LevelValid = true;
  } else {
    tank1UltrasonicMissCount = ULTRASONIC_MAX_CONSECUTIVE_MISSES;
    tank1DistanceCm = NAN;
    tank1LevelPct = 0.0f;
    tank1LevelValid = false;
    tank1DistanceMedianCount = 0;
    lastTank1DistanceCm = NAN;
    tank1LastDistanceAcceptedMs = 0;
  }

  if (tank1LevelValid && !isnan(tank1DistanceCm)) {
    tank1LevelPct = distanceToLevelPct(tank1DistanceCm, TANK1_HEIGHT_CM);
  }

  if (!isnan(d2)) {
    float stable2 = stabilizeUltrasonicDistanceCm(d2, lastTank2DistanceCm, TANK2_HEIGHT_CM, expectedDistanceDirectionTank2());
    float smoothed2 = applyUltrasonicPostMedian(stable2, tank2DistanceMedianBuf, tank2DistanceMedianCount);
    float limited2 = applyUltrasonicRateLimitCm(smoothed2, lastTank2DistanceCm, tank2LastDistanceAcceptedMs, nowMs, transferPhase);
    tank2DistanceCm = limited2;
    lastTank2DistanceCm = limited2;
    tank2UltrasonicMissCount = 0;
    tank2LevelValid = true;
  } else if (!isnan(lastTank2DistanceCm) && tank2UltrasonicMissCount < ULTRASONIC_MAX_CONSECUTIVE_MISSES) {
    tank2UltrasonicMissCount++;
    tank2DistanceCm = lastTank2DistanceCm;
    tank2LevelValid = true;
  } else {
    tank2UltrasonicMissCount = ULTRASONIC_MAX_CONSECUTIVE_MISSES;
    tank2DistanceCm = NAN;
    tank2LevelPct = 0.0f;
    tank2LevelValid = false;
    tank2DistanceMedianCount = 0;
    lastTank2DistanceCm = NAN;
    tank2LastDistanceAcceptedMs = 0;
  }

  if (tank2LevelValid && !isnan(tank2DistanceCm)) {
    tank2LevelPct = distanceToLevelPct(tank2DistanceCm, TANK2_HEIGHT_CM);
  }
}

void setProcessActuators(bool pump12On, bool pump21On, bool heater1On, bool heater2On) {
  if (!pump12On && motor1.on) rememberDutyBeforeOff(&motor1);
  motor1.on = pump12On;
  motor1.duty = pump12On ? 1.0f : 0.0f;
  if (!pump21On && motor2.on) rememberDutyBeforeOff(&motor2);
  motor2.on = pump21On;
  motor2.duty = pump21On ? 1.0f : 0.0f;
  if (!heater1On && heater1.on) rememberDutyBeforeOff(&heater1);
  heater1.on = heater1On;
  heater1.duty = heater1On ? 1.0f : 0.0f;
  if (!heater2On && heater2.on) rememberDutyBeforeOff(&heater2);
  heater2.on = heater2On;
  heater2.duty = heater2On ? 1.0f : 0.0f;
}

void applyProductionProcess(float dtSec) {
  if (!productionProcessEnabled) return;
  const float tank1TargetC = tank1EffectiveTempTargetC();
  const float tank2TargetC = tank2EffectiveTempTargetC();

  // Transition state machine.
  if (productionState == PROCESS_HEAT_TANK1) {
    // Heat phases should transition on temperature; level is handled in transfer phases.
    if (hasReachedTempTarget(tank1TempC, tank1TargetC)) {
      productionState = PROCESS_TRANSFER_1_TO_2;
    }
  } else if (productionState == PROCESS_TRANSFER_1_TO_2) {
    if (tank2LevelPct >= min(TANK2_LEVEL_TARGET_PCT, TANK2_HIGH_LEVEL_PCT)
        || tank1LevelPct <= TANK1_LOW_LEVEL_PCT) {
      productionState = PROCESS_HEAT_TANK2;
    }
  } else if (productionState == PROCESS_HEAT_TANK2) {
    // Heat phases should transition on temperature; level is handled in transfer phases.
    if (hasReachedTempTarget(tank2TempC, tank2TargetC)) {
      productionState = PROCESS_TRANSFER_2_TO_1;
    }
  } else if (productionState == PROCESS_TRANSFER_2_TO_1) {
    if (tank1LevelPct >= min(TANK1_LEVEL_TARGET_PCT, TANK1_HIGH_LEVEL_PCT)
        || tank2LevelPct <= TANK2_LOW_LEVEL_PCT) {
      productionState = PROCESS_HEAT_TANK1;
      processCycleCount++;
    }
  }

  // Enforce actuator sequence while active.
  if (productionState == PROCESS_HEAT_TANK1) {
    setProcessActuators(false, false, true, false);
  } else if (productionState == PROCESS_TRANSFER_1_TO_2) {
    setProcessActuators(true, false, false, false);
  } else if (productionState == PROCESS_HEAT_TANK2) {
    setProcessActuators(false, false, false, true);
  } else if (productionState == PROCESS_TRANSFER_2_TO_1) {
    setProcessActuators(false, true, false, false);
  } else {
    setProcessActuators(false, false, false, false);
  }

  // Fallback thermal model only when a DS18B20 sensor is missing for that tank.
  if (SIMULATE_TANK_TEMPS_WHEN_SENSOR_MISSING) {
    if (!tankTempSensor1Found) {
      if (heater1.on) tank1TempC += TANK_HEAT_RATE_C_PER_SEC * max(0.0f, heater1.duty) * dtSec;
      tank1TempC += (ambientTempC - tank1TempC) * TANK_COOL_RATE_PER_SEC * dtSec;
    }
    if (!tankTempSensor2Found) {
      if (heater2.on) tank2TempC += TANK_HEAT_RATE_C_PER_SEC * max(0.0f, heater2.duty) * dtSec;
      tank2TempC += (ambientTempC - tank2TempC) * TANK_COOL_RATE_PER_SEC * dtSec;
    }
  }
}

void setRuntimeConfigDefaults() {
  runtimeConfig.wifiSsid = DEFAULT_WIFI_SSID;
  runtimeConfig.wifiPassword = DEFAULT_WIFI_PASSWORD;
  runtimeConfig.mqttHost = DEFAULT_MQTT_BROKER_HOST;
  runtimeConfig.mqttPort = DEFAULT_MQTT_BROKER_PORT;
  runtimeConfig.mqttUsername = DEFAULT_MQTT_USERNAME;
  runtimeConfig.mqttPassword = DEFAULT_MQTT_PASSWORD;
}

void loadRuntimeConfigFromNvs() {
  setRuntimeConfigDefaults();
  if (!preferences.begin(PREFERENCES_NAMESPACE, true)) {
    Serial.println("[config] preferences open failed (read mode), using defaults");
    return;
  }

  runtimeConfig.wifiSsid = preferences.getString("wifi_ssid", runtimeConfig.wifiSsid);
  runtimeConfig.wifiPassword = preferences.getString("wifi_pass", runtimeConfig.wifiPassword);
  runtimeConfig.mqttHost = preferences.getString("mqtt_host", runtimeConfig.mqttHost);
  runtimeConfig.mqttPort = preferences.getUShort("mqtt_port", runtimeConfig.mqttPort);
  runtimeConfig.mqttUsername = preferences.getString("mqtt_user", runtimeConfig.mqttUsername);
  runtimeConfig.mqttPassword = preferences.getString("mqtt_pass", runtimeConfig.mqttPassword);
  preferences.end();
}

void saveRuntimeConfigToNvs() {
  if (!preferences.begin(PREFERENCES_NAMESPACE, false)) {
    Serial.println("[config] preferences open failed (write mode), config not saved");
    return;
  }

  preferences.putString("wifi_ssid", runtimeConfig.wifiSsid);
  preferences.putString("wifi_pass", runtimeConfig.wifiPassword);
  preferences.putString("mqtt_host", runtimeConfig.mqttHost);
  preferences.putUShort("mqtt_port", runtimeConfig.mqttPort);
  preferences.putString("mqtt_user", runtimeConfig.mqttUsername);
  preferences.putString("mqtt_pass", runtimeConfig.mqttPassword);
  preferences.end();
}

bool runtimeConfigLooksUsable() {
  if (runtimeConfig.wifiSsid.length() == 0 || runtimeConfig.wifiSsid == "YOUR_WIFI_SSID") return false;
  if (runtimeConfig.mqttHost.length() == 0) return false;
  if (runtimeConfig.mqttPort == 0) return false;
  return true;
}

String readSerialLineWithTimeout(unsigned long timeoutMs) {
  String line;
  unsigned long startMs = millis();
  while ((millis() - startMs) < timeoutMs) {
    while (Serial.available()) {
      char c = (char)Serial.read();
      if (c == '\r') continue;
      if (c == '\n') return line;
      line += c;
    }
    delay(10);
  }
  return line;
}

String promptConfigText(const char* label, const String& currentValue, bool hideCurrent) {
  String shown = hideCurrent ? (currentValue.length() > 0 ? "********" : "<empty>") : currentValue;
  Serial.printf("%s [%s]: ", label, shown.c_str());
  String in = readSerialLineWithTimeout(SERIAL_INPUT_TIMEOUT_MS);
  in.trim();
  if (in.length() == 0) return currentValue;
  return in;
}

uint16_t promptConfigPort(const char* label, uint16_t currentPort) {
  Serial.printf("%s [%u]: ", label, (unsigned int)currentPort);
  String in = readSerialLineWithTimeout(SERIAL_INPUT_TIMEOUT_MS);
  in.trim();
  if (in.length() == 0) return currentPort;
  long p = in.toInt();
  if (p < 1 || p > 65535) {
    Serial.println("[config] invalid port, keeping current value");
    return currentPort;
  }
  return (uint16_t)p;
}

bool serialConfigRequestedByUser() {
  Serial.println();
  Serial.printf("[config] press 'c' + Enter within %lus to edit WiFi/MQTT settings\n",
                STARTUP_CONFIG_WINDOW_MS / 1000UL);
  unsigned long startMs = millis();
  while ((millis() - startMs) < STARTUP_CONFIG_WINDOW_MS) {
    if (Serial.available()) {
      String in = readSerialLineWithTimeout(500);
      in.trim();
      in.toLowerCase();
      if (in == "c" || in == "config") return true;
      return false;
    }
    delay(20);
  }
  return false;
}

void runStartupConfigWizard() {
  bool forceSetup = !runtimeConfigLooksUsable();
  bool runWizard = forceSetup || serialConfigRequestedByUser();
  if (!runWizard) return;

  Serial.println("[config] entering startup configuration");
  runtimeConfig.wifiSsid = promptConfigText("WiFi SSID", runtimeConfig.wifiSsid, false);
  runtimeConfig.wifiPassword = promptConfigText("WiFi Password", runtimeConfig.wifiPassword, true);
  runtimeConfig.mqttHost = promptConfigText("MQTT Broker Host", runtimeConfig.mqttHost, false);
  runtimeConfig.mqttPort = promptConfigPort("MQTT Broker Port", runtimeConfig.mqttPort);
  runtimeConfig.mqttUsername = promptConfigText("MQTT Username", runtimeConfig.mqttUsername, false);
  runtimeConfig.mqttPassword = promptConfigText("MQTT Password", runtimeConfig.mqttPassword, true);

  saveRuntimeConfigToNvs();
  Serial.println("[config] settings saved to NVS");
}

void registerControlAction();
void registerControlActionTyped(const char* actionType);

String predictionSourceToString(PredictionInputSource src) {
  switch (src) {
    case PRED_SRC_LONG_RF: return "LONG_RF";
    case PRED_SRC_LONG_LSTM: return "LONG_LSTM";
    default: return "SHORT";
  }
}

const char* predictionSourceKey(PredictionInputSource src) {
  switch (src) {
    case PRED_SRC_LONG_RF: return "long_rf";
    case PRED_SRC_LONG_LSTM: return "long_lstm";
    default: return "short";
  }
}

static int predictionSourceIndex(PredictionInputSource src) {
  switch (src) {
    case PRED_SRC_LONG_RF: return 1;
    case PRED_SRC_LONG_LSTM: return 2;
    default: return 0;
  }
}

static PredictionChannelState& predictionStateFor(PredictionInputSource src) {
  return predictionStateBySource[predictionSourceIndex(src)];
}

static const PredictionChannelState& predictionStateForConst(PredictionInputSource src) {
  return predictionStateBySource[predictionSourceIndex(src)];
}

static bool predictionSourceSupportsDynamicProcess(PredictionInputSource src) {
  return src == PRED_SRC_LONG_RF || src == PRED_SRC_LONG_LSTM;
}

static bool predictionStateIsFresh(const PredictionChannelState& state, unsigned long nowMs) {
  if (!state.applied || state.lastAppliedMs == 0) return false;
  return (nowMs - state.lastAppliedMs) <= AI_DYNAMIC_PREDICTION_STALE_MS;
}

static int riskLevelWeight(const String& level) {
  String upper = level;
  upper.toUpperCase();
  if (upper == "HIGH") return 2;
  if (upper == "MEDIUM") return 1;
  return 0;
}

static bool suggestionTargetsProcessLoad(const AISuggestion& s) {
  return s.deviceName == "motor1"
    || s.deviceName == "motor2"
    || s.deviceName == "heater1"
    || s.deviceName == "heater2";
}

static bool predictionStateHasProcessPlan(const PredictionChannelState& state) {
  for (int i = 0; i < state.suggestionCount; ++i) {
    const AISuggestion& s = state.suggestions[i];
    if (s.hasProcessMode || suggestionTargetsProcessLoad(s)) return true;
  }
  return false;
}

static bool predictionStateSupportsProcessPlanning(PredictionInputSource src,
                                                   const PredictionChannelState& state,
                                                   unsigned long nowMs) {
  if (!predictionSourceSupportsDynamicProcess(src)) return false;
  if (!predictionStateIsFresh(state, nowMs)) return false;
  if (state.sourceMode != "ONLINE") return false;
  if (!state.sourceModelReady) return false;
  if (state.suggestionCount <= 0) return false;
  return predictionStateHasProcessPlan(state);
}

static const PredictionChannelState* selectDynamicProcessPredictionState(PredictionInputSource* srcOut = nullptr,
                                                                         String* reasonOut = nullptr) {
  const unsigned long nowMs = millis();
  const PredictionChannelState& lstmState = predictionStateForConst(PRED_SRC_LONG_LSTM);
  const PredictionChannelState& rfState = predictionStateForConst(PRED_SRC_LONG_RF);

  if (predictionStateSupportsProcessPlanning(PRED_SRC_LONG_LSTM, lstmState, nowMs)) {
    if (srcOut != nullptr) *srcOut = PRED_SRC_LONG_LSTM;
    if (reasonOut != nullptr) *reasonOut = "ready";
    return &lstmState;
  }
  if (predictionStateSupportsProcessPlanning(PRED_SRC_LONG_RF, rfState, nowMs)) {
    if (srcOut != nullptr) *srcOut = PRED_SRC_LONG_RF;
    if (reasonOut != nullptr) *reasonOut = "ready";
    return &rfState;
  }

  if (srcOut != nullptr) *srcOut = PRED_SRC_LONG_RF;
  if (reasonOut == nullptr) return nullptr;

  if (lstmState.sourceMode != "ONLINE" && rfState.sourceMode != "ONLINE") {
    *reasonOut = "predictor_not_online";
  } else if (!lstmState.sourceModelReady && !rfState.sourceModelReady) {
    *reasonOut = "predictor_not_trained";
  } else if (!predictionStateIsFresh(lstmState, nowMs) && !predictionStateIsFresh(rfState, nowMs)) {
    *reasonOut = "prediction_stale";
  } else if (lstmState.suggestionCount <= 0 && rfState.suggestionCount <= 0) {
    *reasonOut = "no_suggestions";
  } else {
    *reasonOut = "no_process_plan";
  }
  return nullptr;
}

static String activePredictionSourcesSummary() {
  String summary = "";
  const unsigned long nowMs = millis();
  for (int idx = 0; idx < PREDICTION_SOURCE_COUNT; ++idx) {
    PredictionInputSource src = (idx == 1) ? PRED_SRC_LONG_RF : ((idx == 2) ? PRED_SRC_LONG_LSTM : PRED_SRC_SHORT);
    const PredictionChannelState& state = predictionStateForConst(src);
    if (!predictionStateIsFresh(state, nowMs)) continue;
    if (summary.length() > 0) summary += "+";
    summary += predictionSourceToString(src);
  }
  if (summary.length() == 0) return "NONE";
  return summary;
}

static void rebuildFusedPredictionState() {
  const unsigned long nowMs = millis();
  predictorPeakRisk = false;
  predictorRiskLevel = "LOW";
  predictorPowerW = 0.0f;
  predictorHorizonSec = 0.0f;
  predictorSourceTimestampSec = 0.0;
  predictorSourceMode = "FUSED";
  predictorSourceModelReady = false;
  predictorLastAppliedMs = 0;
  aiSuggestionCount = 0;

  String usedDevices[MAX_AI_SUGGESTIONS];
  int usedDeviceCount = 0;

  const PredictionInputSource sourceOrder[PREDICTION_SOURCE_COUNT] = {
    PRED_SRC_SHORT,
    PRED_SRC_LONG_RF,
    PRED_SRC_LONG_LSTM,
  };

  for (int idx = 0; idx < PREDICTION_SOURCE_COUNT; ++idx) {
    const PredictionInputSource src = sourceOrder[idx];
    const PredictionChannelState& state = predictionStateForConst(src);
    if (!predictionStateIsFresh(state, nowMs)) continue;

    predictorPeakRisk = predictorPeakRisk || state.peakRisk;
    if (riskLevelWeight(state.riskLevel) > riskLevelWeight(predictorRiskLevel)) {
      predictorRiskLevel = state.riskLevel;
    }
    if (state.powerW >= predictorPowerW) {
      predictorPowerW = state.powerW;
      predictorHorizonSec = state.horizonSec;
    }
    predictorSourceTimestampSec = max(predictorSourceTimestampSec, state.sourceTimestampSec);
    predictorSourceModelReady = predictorSourceModelReady || state.sourceModelReady;
    predictorLastAppliedMs = max(predictorLastAppliedMs, state.lastAppliedMs);

    for (int i = 0; i < state.suggestionCount; ++i) {
      if (aiSuggestionCount >= MAX_AI_SUGGESTIONS) break;
      const AISuggestion& suggestion = state.suggestions[i];
      bool seenDevice = false;
      for (int j = 0; j < usedDeviceCount; ++j) {
        if (usedDevices[j] == suggestion.deviceName) {
          seenDevice = true;
          break;
        }
      }
      if (seenDevice) continue;
      aiSuggestions[aiSuggestionCount++] = suggestion;
      if (usedDeviceCount < MAX_AI_SUGGESTIONS) {
        usedDevices[usedDeviceCount++] = suggestion.deviceName;
      }
    }
  }
}

bool isAiDynamicProcessAvailable(String* reasonOut = nullptr) {
  PredictionInputSource selectedSrc = PRED_SRC_LONG_RF;
  return selectDynamicProcessPredictionState(&selectedSrc, reasonOut) != nullptr;
}

bool isAiDynamicProcessReady(String* reasonOut = nullptr) {
  if (!(controlPolicy == POLICY_AI_PREFERRED || controlPolicy == POLICY_HYBRID)) {
    if (reasonOut != nullptr) *reasonOut = "policy_not_ai";
    return false;
  }
  return isAiDynamicProcessAvailable(reasonOut);
}

String controlPolicyToString(ControlPolicy p) {
  switch (p) {
    case POLICY_RULE_ONLY: return "RULE_ONLY";
    case POLICY_AI_PREFERRED: return "AI_PREFERRED";
    case POLICY_NO_ENERGY_MANAGEMENT: return "NO_ENERGY_MANAGEMENT";
    default: return "HYBRID";
  }
}

String plantModeToString(PlantMode m) {
  switch (m) {
    case PLANT_HIGH_PROD: return "HIGH_PROD";
    case PLANT_LOW_PROD: return "LOW_PROD";
    case PLANT_ENERGY_SAVING: return "ENERGY_SAVING";
    default: return "NORMAL";
  }
}

String seasonModeToString(SeasonMode s) {
  switch (s) {
    case SEASON_SUMMER: return "SUMMER";
    case SEASON_WINTER: return "WINTER";
    case SEASON_SPRING: return "SPRING";
    case SEASON_AUTUMN: return "AUTUMN";
    default: return "DEFAULT";
  }
}

PlantMode plantModeFromString(const String& input) {
  String s = input;
  s.toUpperCase();
  if (s == "HIGH_PROD") return PLANT_HIGH_PROD;
  if (s == "LOW_PROD") return PLANT_LOW_PROD;
  if (s == "ENERGY_SAVING") return PLANT_ENERGY_SAVING;
  return PLANT_NORMAL;
}

SeasonMode seasonModeFromString(const String& input) {
  String s = input;
  s.toUpperCase();
  if (s == "SUMMER") return SEASON_SUMMER;
  if (s == "WINTER") return SEASON_WINTER;
  if (s == "SPRING") return SEASON_SPRING;
  if (s == "AUTUMN") return SEASON_AUTUMN;
  return SEASON_DEFAULT;
}

ControlPolicy controlPolicyFromString(const String& input) {
  String s = input;
  s.toUpperCase();
  if (s == "RULE_ONLY") return POLICY_RULE_ONLY;
  if (s == "AI_PREFERRED") return POLICY_AI_PREFERRED;
  if (s == "NO_ENERGY_MANAGEMENT") return POLICY_NO_ENERGY_MANAGEMENT;
  return POLICY_HYBRID;
}

String loadClassToString(LoadClass c) {
  switch (c) {
    case CLASS_CRITICAL: return "CRITICAL";
    case CLASS_ESSENTIAL: return "ESSENTIAL";
    case CLASS_IMPORTANT: return "IMPORTANT";
    case CLASS_SECONDARY: return "SECONDARY";
    default: return "NON_ESSENTIAL";
  }
}

LoadClass parseLoadClass(const String& s) {
  String u = s; u.toUpperCase();
  if (u == "CRITICAL") return CLASS_CRITICAL;
  if (u == "ESSENTIAL") return CLASS_ESSENTIAL;
  if (u == "IMPORTANT") return CLASS_IMPORTANT;
  if (u == "SECONDARY") return CLASS_SECONDARY;
  return CLASS_NON_ESSENTIAL;
}

void rememberDutyBeforeOff(LoadChannel* ld) {
  if (ld == nullptr) return;
  ld->lastDuty = clampf(ld->duty, 0.0f, 1.0f);
}

void restoreDutyOn(LoadChannel* ld) {
  if (ld == nullptr) return;
  float minDuty = (ld->loadClass == CLASS_CRITICAL) ? CRITICAL_MIN_DEVICE_DUTY : MIN_DEVICE_DUTY;
  float restored = clampf(ld->lastDuty, 0.0f, 1.0f);
  if (restored < minDuty) restored = minDuty;
  ld->duty = restored;
}

void forceAllLoadsOff(bool rememberLastDuty) {
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    LoadChannel* ld = loads[i];
    if (rememberLastDuty && ld->on) {
      rememberDutyBeforeOff(ld);
    }
    ld->on = false;
    ld->duty = 0.0f;
  }
}

static bool isProcessLoad(const LoadChannel* ld) {
  return ld == &motor1 || ld == &motor2 || ld == &heater1 || ld == &heater2;
}

static bool isAiDrivenPolicy() {
  return controlPolicy == POLICY_AI_PREFERRED || controlPolicy == POLICY_HYBRID;
}

void applyAiProcessOverridesFromSuggestions() {
  if (!productionProcessEnabled) return;
  if (!isAiDrivenPolicy()) return;
  PredictionInputSource selectedSrc = PRED_SRC_LONG_RF;
  String reason;
  const PredictionChannelState* selectedState = selectDynamicProcessPredictionState(&selectedSrc, &reason);
  if (selectedState == nullptr) return;

  bool hasProcessPlan = false;
  bool hasSuggestedMode = false;
  ProductionState suggestedMode = productionState;
  for (int i = 0; i < selectedState->suggestionCount; ++i) {
    const AISuggestion& s = selectedState->suggestions[i];
    LoadChannel* ld = findLoadByName(s.deviceName);
    if (ld != nullptr && isProcessLoad(ld)) {
      hasProcessPlan = true;
      if (!hasSuggestedMode && s.hasProcessMode) {
        suggestedMode = s.processMode;
        hasSuggestedMode = true;
      }
    }
  }
  if (!hasProcessPlan) return;
  if (hasSuggestedMode) {
    productionState = suggestedMode;
  }

  for (int i = 0; i < selectedState->suggestionCount; ++i) {
    const AISuggestion& s = selectedState->suggestions[i];
    LoadChannel* ld = findLoadByName(s.deviceName);
    if (ld == nullptr || !isProcessLoad(ld)) continue;
    if (!isPolicyManagedLoad(ld)) continue;
    if (ld->faultActive && ((s.hasOn && s.onValue) || (s.hasDuty && s.dutyValue > 0.0f))) continue;

    if (s.hasOn) {
      if (!s.onValue && ld->on) rememberDutyBeforeOff(ld);
      ld->on = s.onValue;
      if (s.onValue && !s.hasDuty) {
        restoreDutyOn(ld);
      }
    }
    if (s.hasDuty) {
      float d = clampf(s.dutyValue, 0.0f, 1.0f);
      ld->duty = d;
      if (d > 0.0f) ld->lastDuty = d;
      if (d <= 0.0f) ld->on = false;
      else if (!s.hasOn) ld->on = true;
    }
  }
}

LoadChannel* findLoadByName(const String& name) {
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    if (loads[i]->name == name) return loads[i];
  }
  return nullptr;
}

bool applySingleAiSuggestion(const AISuggestion& s) {
  LoadChannel* ld = findLoadByName(s.deviceName);
  if (ld == nullptr) return false;
  if (!isPolicyManagedLoad(ld)) return false;
  if (productionProcessEnabled && isProcessLoad(ld)) return false;

  // Guardrails: critical loads cannot be switched OFF
  if (s.hasOn && ld->loadClass == CLASS_CRITICAL && !s.onValue) return false;

  const bool wasOn = ld->on;
  const float oldDuty = ld->duty;
  if (s.hasOn) ld->on = s.onValue;
  if (s.hasDuty) {
    float dutyFloor = (ld->loadClass == CLASS_CRITICAL) ? CRITICAL_MIN_DEVICE_DUTY : MIN_DEVICE_DUTY;
    ld->duty = clampf(max(dutyFloor, s.dutyValue), 0.0f, 1.0f);
  } else if (s.hasOn && s.onValue && !wasOn) {
    restoreDutyOn(ld);
  } else if (s.hasOn && !s.onValue && wasOn) {
    rememberDutyBeforeOff(ld);
  }
  if (s.hasOn && wasOn && !ld->on) registerControlActionTyped("shed");
  else if (s.hasDuty && ld->duty < oldDuty) registerControlActionTyped("curtail");
  else if (s.hasDuty && ld->duty > oldDuty) registerControlActionTyped("restore");
  else registerControlAction();
  return true;
}

bool applyCachedAiSuggestion() {
  for (int i = 0; i < aiSuggestionCount; ++i) {
    if (applySingleAiSuggestion(aiSuggestions[i])) {
      return true;
    }
  }
  return false;
}

String isoTimestampUtc() {
  time_t now = time(nullptr);
  if (now < 100000) {
    // Fallback before NTP sync
    unsigned long ms = millis();
    return String("UNSYNCED_") + String(ms);
  }
  struct tm tmUtc;
  gmtime_r(&now, &tmUtc);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tmUtc);
  return String(buf);
}

float readVoltageSensorV(uint8_t pin, float ratio) {
  int raw = analogRead(pin);
  float sensed = (raw / ADC_MAX_COUNTS) * ADC_REFERENCE_V;
  return sensed * ratio;
}

bool tcaSelectChannel(uint8_t channel) {
  if (!USE_TCA9548A) return true;
  if (channel > 7) return false;
  Wire.beginTransmission(TCA9548A_ADDR);
  Wire.write(1 << channel);
  return Wire.endTransmission() == 0;
}

bool tcaPresent() {
  if (!USE_TCA9548A) return false;
  Wire.beginTransmission(TCA9548A_ADDR);
  return Wire.endTransmission() == 0;
}

bool initIna219OnChannel(uint8_t channel) {
  if (!tcaSelectChannel(channel)) return false;
  if (!ina219.begin()) return false;
  ina219.setCalibration_32V_2A();
  return true;
}

float readIna219CurrentRawA(uint8_t channel) {
  if (!tcaSelectChannel(channel)) return 0.0f;
  delayMicroseconds(200);
  float mA = ina219.getCurrent_mA();
  return mA / 1000.0f;
}

float readIna219CurrentA(uint8_t channel) {
  return fabs(readIna219CurrentRawA(channel));
}

float readIna219BusVoltageV(uint8_t channel) {
  if (!tcaSelectChannel(channel)) return NAN;
  delayMicroseconds(200);
  return ina219.getBusVoltage_V();
}

float readIna219PowerW(uint8_t channel) {
  if (!tcaSelectChannel(channel)) return NAN;
  delayMicroseconds(200);
  float mW = ina219.getPower_mW();
  return mW / 1000.0f;
}

void calibrateIna219CurrentOffsets() {
  if (!USE_INA219) return;
  Serial.println("[sensors] calibrating INA219 current offsets (no-load expected)...");
  bool prevOn[LOAD_COUNT];
  float prevDuty[LOAD_COUNT];
  unsigned long nowMs = millis();
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    prevOn[i] = loads[i]->on;
    prevDuty[i] = loads[i]->duty;
    loads[i]->on = false;
    loads[i]->duty = 0.0f;
    loads[i]->applyHardware(nowMs);
  }
  delay(CURRENT_ZERO_SETTLE_MS);
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    LoadChannel* ld = loads[i];
    ld->currentFilterInitialized = false;
    ld->filteredCurrentA = 0.0f;
    if (!ina219Present[i]) {
      ld->currentOffsetA = 0.0f;
      continue;
    }
    const uint8_t ch = INA219_CHANNELS[i];
    float acc = 0.0f;
    uint8_t count = 0;
    for (uint8_t s = 0; s < INA219_ZERO_CAL_SAMPLES; ++s) {
      float a = readIna219CurrentRawA(ch);
      if (!isnan(a)) {
        acc += a;
        count++;
      }
      delay(2);
    }
    ld->currentOffsetA = (count > 0) ? (acc / (float)count) : 0.0f;
    Serial.printf("[sensors] zero_%s ch=%u offset=%.4fA\n",
                  ld->name.c_str(),
                  ch,
                  ld->currentOffsetA);
  }
  nowMs = millis();
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    loads[i]->on = prevOn[i];
    loads[i]->duty = prevDuty[i];
    loads[i]->applyHardware(nowMs);
  }
}

float applyIna219CurrentCalibration(LoadChannel* ld, float rawCurrentA) {
  if (ld == nullptr || isnan(rawCurrentA)) return 0.0f;

  // Keep auto-zero tracking slow and only when load is effectively off.
  if (!ld->on && !ld->faultActive && ld->faultInjectA <= 0.0f) {
    if (fabs(rawCurrentA) <= INA219_OFFSET_TRACK_MAX_A) {
      ld->currentOffsetA = (ld->currentOffsetA * (1.0f - INA219_OFFSET_TRACK_ALPHA))
                         + (rawCurrentA * INA219_OFFSET_TRACK_ALPHA);
    }
  }

  float correctedA = (rawCurrentA - ld->currentOffsetA) * ld->currentGain;
  float absA = fabs(correctedA);
  if (absA < INA219_CURRENT_DEADBAND_A) absA = 0.0f;

  if (!ld->currentFilterInitialized) {
    ld->filteredCurrentA = absA;
    ld->currentFilterInitialized = true;
  } else {
    ld->filteredCurrentA += (absA - ld->filteredCurrentA) * INA219_CURRENT_FILTER_ALPHA;
  }
  if (ld->filteredCurrentA < INA219_CURRENT_DEADBAND_A) {
    ld->filteredCurrentA = 0.0f;
  }
  return max(0.0f, ld->filteredCurrentA);
}

bool controlCooldownOk() {
  unsigned long elapsedMs = millis() - lastControlActionMs;
  return elapsedMs >= (unsigned long)(CONTROL_COOLDOWN_SEC * 1000.0f);
}

void registerControlAction() {
  lastControlActionMs = millis();
  totalControlActions++;
}

void registerControlActionTyped(const char* actionType) {
  registerControlAction();
  if (strcmp(actionType, "shed") == 0) sheddingEventCount++;
  else if (strcmp(actionType, "curtail") == 0) curtailEventCount++;
  else if (strcmp(actionType, "restore") == 0) restoreEventCount++;
}

float scenePresetDurationSec(const String& preset) {
  String p = preset;
  p.toUpperCase();
  if (p == "WINTER_BALANCED") return 10.0f * 60.0f;
  if (p == "SPRING_RAMP") return 30.0f * 60.0f;
  if (p == "SUMMER_PEAK") return 60.0f * 60.0f;
  if (p == "AUTUMN_STABLE") return 24.0f * 60.0f * 60.0f;
  if (p == "PRESET_1") return 10.0f * 60.0f;
  if (p == "PRESET_2") return 30.0f * 60.0f;
  if (p == "PRESET_3") return 60.0f * 60.0f;
  if (p == "PRESET_4") return 24.0f * 60.0f * 60.0f;
  // Legacy aliases
  if (p == "TEN_MIN") return 10.0f * 60.0f;
  if (p == "THIRTY_MIN") return 30.0f * 60.0f;
  if (p == "ONE_HOUR") return 60.0f * 60.0f;
  if (p == "ONE_DAY") return 24.0f * 60.0f * 60.0f;
  return 0.0f;
}

void applySceneAutomation() {
  if (!sceneEnabled || sceneDurationSec <= 0.0f) {
    sceneTargetTempC = NAN;
    sceneTargetHumidityPct = NAN;
    return;
  }

  float elapsedSec = (millis() - sceneStartMs) / 1000.0f;
  if (elapsedSec >= sceneDurationSec) {
    if (sceneLoop) {
      elapsedSec = fmodf(elapsedSec, sceneDurationSec);
    } else {
      elapsedSec = sceneDurationSec;
      sceneEnabled = false;
      sceneTargetTempC = NAN;
      sceneTargetHumidityPct = NAN;
    }
  }

  float phase = elapsedSec / sceneDurationSec;
  String sceneKey = sceneName;
  sceneKey.toUpperCase();
  if (sceneKey == "PRESET_1") sceneKey = "WINTER_BALANCED";
  else if (sceneKey == "PRESET_2") sceneKey = "SPRING_RAMP";
  else if (sceneKey == "PRESET_3") sceneKey = "SUMMER_PEAK";
  else if (sceneKey == "PRESET_4") sceneKey = "AUTUMN_STABLE";

  PlantMode phaseMode = PLANT_NORMAL;
  SeasonMode phaseSeason = SEASON_DEFAULT;
  String phaseTariff = "OFFPEAK";
  bool phasePeak = false;
  float phaseTemp = NAN;
  float phaseHumidity = NAN;

  if (sceneKey == "WINTER_BALANCED") {
    phaseMode = PLANT_LOW_PROD;
    phaseSeason = SEASON_WINTER;
    phaseTariff = "OFFPEAK";
    phasePeak = false;
    phaseTemp = 19.0f;
    phaseHumidity = 50.0f;
  } else if (sceneKey == "SPRING_RAMP") {
    phaseMode = PLANT_NORMAL;
    phaseSeason = SEASON_SPRING;
    phaseTariff = "MIDPEAK";
    phasePeak = false;
    phaseTemp = 23.0f;
    phaseHumidity = 55.0f;
  } else if (sceneKey == "SUMMER_PEAK") {
    phaseMode = PLANT_HIGH_PROD;
    phaseSeason = SEASON_SUMMER;
    phaseTariff = "PEAK";
    phasePeak = true;
    phaseTemp = 30.0f;
    phaseHumidity = 62.0f;
  } else if (sceneKey == "AUTUMN_STABLE") {
    phaseMode = PLANT_NORMAL;
    phaseSeason = SEASON_AUTUMN;
    phaseTariff = "MIDPEAK";
    phasePeak = false;
    phaseTemp = 22.0f;
    phaseHumidity = 56.0f;
  } else if (phase < 0.15f) {
    phaseMode = PLANT_LOW_PROD;
    phaseSeason = SEASON_WINTER;
    phaseTariff = "OFFPEAK";
    phasePeak = false;
    phaseTemp = 19.0f;
    phaseHumidity = 50.0f;
  } else if (phase < 0.45f) {
    phaseMode = PLANT_HIGH_PROD;
    phaseSeason = SEASON_SPRING;
    phaseTariff = "MIDPEAK";
    phasePeak = false;
    phaseTemp = 23.0f;
    phaseHumidity = 55.0f;
  } else if (phase < 0.70f) {
    phaseMode = PLANT_HIGH_PROD;
    phaseSeason = SEASON_SUMMER;
    phaseTariff = "PEAK";
    phasePeak = true;
    phaseTemp = 30.0f;
    phaseHumidity = 62.0f;
  } else if (phase < 0.90f) {
    phaseMode = PLANT_NORMAL;
    phaseSeason = SEASON_AUTUMN;
    phaseTariff = "MIDPEAK";
    phasePeak = false;
    phaseTemp = 22.0f;
    phaseHumidity = 56.0f;
  } else {
    phaseMode = PLANT_ENERGY_SAVING;
    phaseSeason = SEASON_WINTER;
    phaseTariff = "OFFPEAK";
    phasePeak = false;
    phaseTemp = 18.0f;
    phaseHumidity = 50.0f;
  }

  if (sceneOverrideModeEnabled) phaseMode = sceneOverrideMode;
  if (sceneOverrideSeasonEnabled) phaseSeason = sceneOverrideSeason;
  if (sceneOverrideTariffEnabled) phaseTariff = sceneOverrideTariffState;
  if (sceneOverridePeakEnabled) phasePeak = sceneOverridePeakEvent;
  if (sceneOverrideTempEnabled) phaseTemp = sceneOverrideTempC;
  if (sceneOverrideHumidityEnabled) phaseHumidity = sceneOverrideHumidityPct;

  plantMode = phaseMode;
  seasonMode = phaseSeason;
  tariffState = phaseTariff;
  peakEvent = phasePeak;
  sceneTargetTempC = phaseTemp;
  sceneTargetHumidityPct = phaseHumidity;
}

void updateEnergyWindows(float totalPowerW, float elapsedSec) {
  float stepWh = (totalPowerW * max(0.0f, elapsedSec)) / 3600.0f;
  cumulativeEnergyWh += stepWh;
  currentMinuteBucketWh += stepWh;

  if (currentMinuteBucketStartMs == 0) {
    currentMinuteBucketStartMs = millis();
  }

  while ((millis() - currentMinuteBucketStartMs) >= 60000UL) {
    energyMinuteBucketsWh[energyBucketHead] = currentMinuteBucketWh;
    energyBucketHead = (energyBucketHead + 1) % ENERGY_BUCKETS_PER_DAY;
    if (energyBucketCount < ENERGY_BUCKETS_PER_DAY) {
      energyBucketCount++;
    }
    currentMinuteBucketWh = 0.0f;
    currentMinuteBucketStartMs += 60000UL;
  }
}

float getWindowEnergyWh(int windowMinutes) {
  int available = min(windowMinutes, energyBucketCount);
  float sumWh = currentMinuteBucketWh;
  for (int i = 0; i < available; ++i) {
    int idx = (energyBucketHead - 1 - i + ENERGY_BUCKETS_PER_DAY) % ENERGY_BUCKETS_PER_DAY;
    sumWh += energyMinuteBucketsWh[idx];
  }
  return sumWh;
}

float getWindowEnergyByDurationSec(float durationSec) {
  int minutes = (int)roundf(max(60.0f, durationSec) / 60.0f);
  return getWindowEnergyWh(minutes);
}

// -----------------------------
// Mode / season application
// -----------------------------
void applySeasonProfile() {
  seasonTempOffsetC = 0.0f;
  seasonHumidityOffset = 0.0f;
  switch (seasonMode) {
    case SEASON_SUMMER:
      seasonTempOffsetC = 6.0f;
      seasonHumidityOffset = 5.0f;
      break;
    case SEASON_WINTER:
      seasonTempOffsetC = -6.0f;
      seasonHumidityOffset = -5.0f;
      break;
    case SEASON_SPRING:
      seasonTempOffsetC = -2.0f;
      seasonHumidityOffset = 0.0f;
      break;
    case SEASON_AUTUMN:
      seasonTempOffsetC = -3.0f;
      seasonHumidityOffset = 0.0f;
      break;
    default:
      break;
  }
}

void applyPlantModeProfile() {
  // Apply a mode profile across all loads (similar intent as simulator)
  float motorDutyCap = 1.0f;
  float lightDutyCap = 1.0f;
  float heaterDutyCap = 1.0f;
  float motorLoadFactor = 1.0f;

  switch (plantMode) {
    case PLANT_HIGH_PROD:
      motorLoadFactor = 1.25f;
      break;
    case PLANT_LOW_PROD:
      motorLoadFactor = 0.80f;
      lightDutyCap = 0.70f;
      heaterDutyCap = 0.80f;
      break;
    case PLANT_ENERGY_SAVING:
      motorLoadFactor = 0.90f;
      lightDutyCap = 0.60f;
      heaterDutyCap = 0.80f;
      break;
    default:
      break;
  }

  motor1.loadFactor = motorLoadFactor;
  motor2.loadFactor = motorLoadFactor;

  // Cap duties so mode impacts all loads
  motor1.duty = min(motor1.duty, motorDutyCap);
  motor2.duty = min(motor2.duty, motorDutyCap);
  light1.duty = min(light1.duty, lightDutyCap);
  light2.duty = min(light2.duty, lightDutyCap);
  heater1.duty = min(heater1.duty, heaterDutyCap);
  heater2.duty = min(heater2.duty, heaterDutyCap);
}

// -----------------------------
// Rule-based control
// -----------------------------
int classWeight(LoadClass c) {
  switch (c) {
    case CLASS_CRITICAL: return 0;
    case CLASS_ESSENTIAL: return 1;
    case CLASS_IMPORTANT: return 2;
    case CLASS_SECONDARY: return 3;
    case CLASS_NON_ESSENTIAL: return 4;
  }
  return 4;
}

LoadChannel* selectCurtailCandidate(bool descendingImportance, bool allowPolicyOverrides) {
  LoadChannel* best = nullptr;
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    LoadChannel* ld = loads[i];
    if (!ld->on) continue;
    if (!allowPolicyOverrides && !isPolicyManagedLoad(ld)) continue;
    // Keep heaters under process control while production process is active.
    if (productionProcessEnabled && (ld == &heater1 || ld == &heater2)) continue;
    if (best == nullptr) {
      best = ld;
      continue;
    }
    int wBest = classWeight(best->loadClass);
    int wCur = classWeight(ld->loadClass);
    // For curtail/shedding: pick least important first (higher weight, higher priority value)
    if (!descendingImportance) {
      if (wCur > wBest || (wCur == wBest && ld->priority > best->priority)) best = ld;
    } else {
      // For restore: pick most important first (lower weight, lower priority value)
      if (wCur < wBest || (wCur == wBest && ld->priority < best->priority)) best = ld;
    }
  }
  return best;
}

static unsigned long automaticRestoreHoldoffMs() {
  return max(4000UL, (unsigned long)(max(4.0f, CONTROL_COOLDOWN_SEC * 2.0f) * 1000.0f));
}

static float desiredRestoreDuty(const LoadChannel* ld);

static void noteAutomaticCurtail(LoadChannel* ld, unsigned long nowMs) {
  if (ld == nullptr) return;
  ld->controlReduced = true;
  ld->lastAutoCurtailMs = nowMs;
  ld->restoreBlockedUntilMs = nowMs + automaticRestoreHoldoffMs();
}

static void noteAutomaticRestore(LoadChannel* ld, unsigned long nowMs) {
  if (ld == nullptr) return;
  ld->lastAutoRestoreMs = nowMs;
  if (ld->on && (desiredRestoreDuty(ld) - ld->duty) <= 0.01f) {
    ld->controlReduced = false;
  }
}

static float loadMinDuty(const LoadChannel* ld) {
  if (ld == nullptr) return MIN_DEVICE_DUTY;
  return (ld->loadClass == CLASS_CRITICAL) ? CRITICAL_MIN_DEVICE_DUTY : MIN_DEVICE_DUTY;
}

static float desiredRestoreDuty(const LoadChannel* ld) {
  if (ld == nullptr) return 0.0f;
  return clampf(max(loadMinDuty(ld), ld->lastDuty), 0.0f, 1.0f);
}

static float nextRestoreDuty(const LoadChannel* ld) {
  if (ld == nullptr) return 0.0f;
  const float desired = desiredRestoreDuty(ld);
  if (!ld->on) {
    return loadMinDuty(ld);
  }
  return min(desired, clampf(ld->duty, 0.0f, 1.0f) + max(0.01f, DUTY_STEP_SIZE));
}

static bool loadNeedsRestore(const LoadChannel* ld, unsigned long nowMs) {
  if (ld == nullptr) return false;
  if (!isPolicyManagedLoad(ld)) return false;
  if (!ld->controlReduced) return false;
  if (ld->faultActive || ld->restorePending) return false;
  if (productionProcessEnabled && isProcessLoad(ld)) return false;
  if (nowMs < ld->restoreBlockedUntilMs) return false;
  const float desired = desiredRestoreDuty(ld);
  if (!ld->on) return desired > 0.0f;
  return (desired - ld->duty) > 0.01f;
}

static LoadChannel* selectRestoreCandidate(unsigned long nowMs) {
  LoadChannel* best = nullptr;
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    LoadChannel* ld = loads[i];
    if (!loadNeedsRestore(ld, nowMs)) continue;
    if (best == nullptr) {
      best = ld;
      continue;
    }
    const int bestWeight = classWeight(best->loadClass);
    const int curWeight = classWeight(ld->loadClass);
    const bool bestIsOff = !best->on;
    const bool curIsOff = !ld->on;
    if (curWeight < bestWeight
        || (curWeight == bestWeight && curIsOff < bestIsOff)
        || (curWeight == bestWeight && curIsOff == bestIsOff && ld->priority < best->priority)) {
      best = ld;
    }
  }
  return best;
}

static float expectedPowerForDuty(const LoadChannel* ld, float targetDuty) {
  if (ld == nullptr) return 0.0f;
  const float clippedDuty = clampf(targetDuty, 0.0f, 1.0f);
  const float currentPower = max(0.0f, ld->powerW);
  const float currentDuty = max(0.01f, ld->duty);
  const float lastPower = max(0.0f, ld->lastActivePowerW);
  const float lastDuty = max(0.01f, ld->lastActiveDuty);

  if (lastPower > 0.0f) {
    return max(0.0f, lastPower * (clippedDuty / lastDuty));
  }
  if (ld->on && currentPower > 0.0f) {
    return max(0.0f, currentPower * (clippedDuty / currentDuty));
  }
  return 0.0f;
}

static float estimateRestorePowerDeltaW(const LoadChannel* ld) {
  if (ld == nullptr) return 0.0f;
  const float currentPower = max(0.0f, ld->powerW);
  const float expectedPower = expectedPowerForDuty(ld, max(0.0f, ld->duty));
  if (ld->on) {
    return max(0.0f, expectedPower - currentPower);
  }
  return expectedPower;
}

static float estimateRestorePowerDeltaWForDuty(const LoadChannel* ld, float targetDuty) {
  if (ld == nullptr) return 0.0f;
  const float currentPower = max(0.0f, ld->powerW);
  const float expectedPower = expectedPowerForDuty(ld, targetDuty);
  if (ld->on) {
    return max(0.0f, expectedPower - currentPower);
  }
  return expectedPower;
}

static float estimateRestoreCurrentA(const LoadChannel* ld) {
  if (ld == nullptr) return 0.0f;
  float voltageV = ld->voltageV;
  if (voltageV <= 0.1f) voltageV = loadBusVoltageV;
  const float expectedPower = expectedPowerForDuty(ld, max(0.0f, ld->duty));
  return max(0.0f, expectedPower / max(0.1f, voltageV));
}

static float estimateRestoreCurrentAForDuty(const LoadChannel* ld, float targetDuty) {
  if (ld == nullptr) return 0.0f;
  float voltageV = ld->voltageV;
  if (voltageV <= 0.1f) voltageV = loadBusVoltageV;
  const float expectedPower = expectedPowerForDuty(ld, targetDuty);
  return max(0.0f, expectedPower / max(0.1f, voltageV));
}

void applyAutomaticControl(float totalPowerW) {
  const unsigned long nowMs = millis();
  static unsigned long lastUvActionMs = 0;

  // Safety curtailment: active in all policies (including NO_ENERGY_MANAGEMENT).
  if (undervoltageActive || overvoltageActive) {
    const unsigned long uvActionCooldownMs = 500;
    if (nowMs - lastUvActionMs >= uvActionCooldownMs) {
      LoadChannel* target = selectCurtailCandidate(false, false);
      if (target != nullptr && target->loadClass != CLASS_CRITICAL) {
        const float minDuty = loadMinDuty(target);
        if (target->duty > minDuty) {
          target->duty = max(minDuty, target->duty - DUTY_STEP_SIZE);
          noteAutomaticCurtail(target, nowMs);
          registerControlActionTyped("curtail");
        } else {
          rememberDutyBeforeOff(target);
          target->on = false;
          target->duty = 0.0f;
          noteAutomaticCurtail(target, nowMs);
          registerControlActionTyped("shed");
        }
        lastUvActionMs = nowMs;
      }
    }
    return;
  }

  if (controlPolicy == POLICY_NO_ENERGY_MANAGEMENT) return;
  if (!controlCooldownOk()) return;
  float energyGoalWindowWh = getWindowEnergyByDurationSec(ENERGY_GOAL_DURATION_SEC);
  bool energyBudgetRisk = (MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH > 0.0f) && (energyGoalWindowWh >= MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH * 0.95f);
  bool effectivePeakRisk = predictorPeakRisk || energyBudgetRisk;
  String effectiveRiskLevel = predictorRiskLevel;
  if (energyBudgetRisk && energyGoalWindowWh > MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH * 1.01f) {
    effectiveRiskLevel = "HIGH";
  } else if (energyBudgetRisk && effectiveRiskLevel == "LOW") {
    effectiveRiskLevel = "MEDIUM";
  }

  // Ambient heater rules (skip when production process is active).
  if (!productionProcessEnabled) {
    if (ambientTempC >= HEATER_WARM_TEMP_C) {
      for (LoadChannel* heater : {&heater1, &heater2}) {
        if (!isPolicyManagedLoad(heater)) continue;
        if (heater->on && heater->duty > MIN_DEVICE_DUTY) {
          heater->duty = max(MIN_DEVICE_DUTY, heater->duty - HEATER_DUTY_STEP_SIZE);
          noteAutomaticCurtail(heater, nowMs);
          registerControlActionTyped("curtail");
          return;
        }
      }
    }
    if (ambientTempC >= HEATER_HOT_OFF_TEMP_C) {
      for (LoadChannel* heater : {&heater1, &heater2}) {
        if (!isPolicyManagedLoad(heater)) continue;
        if (heater->on) {
          rememberDutyBeforeOff(heater);
          heater->on = false;
          heater->duty = 0.0f;
          noteAutomaticCurtail(heater, nowMs);
          registerControlActionTyped("shed");
          return;
        }
      }
    }
  }

  // Overload rule
  if (totalPowerW > MAX_TOTAL_POWER_W) {
    LoadChannel* target = selectCurtailCandidate(false, false);
    if (target != nullptr) {
      const float minDuty = loadMinDuty(target);
      if (target->duty > minDuty) {
        target->duty = max(minDuty, target->duty - DUTY_STEP_SIZE);
        noteAutomaticCurtail(target, nowMs);
        registerControlActionTyped("curtail");
      } else if (target->loadClass != CLASS_CRITICAL) {
        rememberDutyBeforeOff(target);
        target->on = false;
        target->duty = 0.0f;
        noteAutomaticCurtail(target, nowMs);
        registerControlActionTyped("shed");
      }
      return;
    }
  }

  // AI-first mode: after safety/overload, try AI suggestions first.
  if (controlPolicy == POLICY_AI_PREFERRED) {
    if (applyCachedAiSuggestion()) return;
  }

  // Peak-risk rule (from predictor)
  if (effectivePeakRisk) {
    // Hybrid mode: AI suggestions get first chance under peak-risk.
    if (controlPolicy == POLICY_HYBRID) {
      if (applyCachedAiSuggestion()) return;
    }
    // Rule-only mode skips AI suggestions completely.
    LoadChannel* target = selectCurtailCandidate(false, false);
    if (target != nullptr) {
      const float minDuty = loadMinDuty(target);
      float dutyTarget = (effectiveRiskLevel == "HIGH" || effectiveRiskLevel == "MEDIUM") ? 0.35f : 0.50f;
      dutyTarget = max(dutyTarget, minDuty);
      if (target->duty > dutyTarget) {
        target->duty = dutyTarget;
        noteAutomaticCurtail(target, nowMs);
        registerControlActionTyped("curtail");
        return;
      }
      if (effectiveRiskLevel == "HIGH" && target->loadClass != CLASS_CRITICAL && target->duty <= MIN_DEVICE_DUTY) {
        rememberDutyBeforeOff(target);
        target->on = false;
        target->duty = 0.0f;
        noteAutomaticCurtail(target, nowMs);
        registerControlActionTyped("shed");
        return;
      }
    }
  }

  // Restore rule
  if (totalPowerW < (MAX_TOTAL_POWER_W * RESTORE_POWER_RATIO)) {
    const float restoreHeadroomMarginW = max(0.25f, MAX_TOTAL_POWER_W * 0.02f);
    LoadChannel* target = selectRestoreCandidate(nowMs);
    if (target != nullptr) {
      const float restoreDuty = nextRestoreDuty(target);
      const float projectedDelta = estimateRestorePowerDeltaWForDuty(target, restoreDuty);
      const float projectedTotal = totalPowerW + projectedDelta;
      if (projectedTotal > (MAX_TOTAL_POWER_W - restoreHeadroomMarginW)) {
        return;
      }
      if (!target->on || restoreDuty > target->duty + 0.01f) {
        target->on = true;
        target->duty = restoreDuty;
        noteAutomaticRestore(target, nowMs);
        registerControlActionTyped("restore");
        return;
      }
    }
  }
}

// -----------------------------
// MQTT command handling
// -----------------------------
void publishAck(JsonDocument& ack, JsonDocument& cmdDoc) {
  StaticJsonDocument<2048> out;
  out["timestamp"] = (double)(millis() / 1000.0f);
  out["plant_id"] = PLANT_IDENTIFIER;
  out["ack"] = ack.as<JsonObject>();
  out["cmd"] = cmdDoc.as<JsonObject>();
  String payload;
  serializeJson(out, payload);
  mqttClient.publish(topicCommandAck.c_str(), payload.c_str());
}

double normalizePredictionTimestampSec(double rawTs) {
  double ts = rawTs;
  // Accept unix epoch in milliseconds and convert to seconds.
  if (ts > 1e12) ts /= 1000.0;
  // Keep only plausible epoch-second values (2000..2100) to avoid uptime values.
  if (ts < 946684800.0 || ts > 4102444800.0) return 0.0;
  return ts;
}

bool rejectIfProcessLocked(JsonObject root, JsonDocument& ack, JsonDocument& cmdDoc) {
  // Keep lock status for observability but do not block runtime updates.
  (void)root;
  (void)ack;
  (void)cmdDoc;
  return false;
}

void applyControllerMode(JsonObject root, JsonDocument& ack) {
  if (root.containsKey("controller_mode") || root.containsKey("control_mode")) {
    // Deprecated: controller mode is no longer used for access control.
    ack["controller_mode"] = "DEPRECATED";
  }
}

void applyControlPolicy(JsonObject root, JsonDocument& ack) {
  if (!root.containsKey("control_policy")) return;
  String p = root["control_policy"].as<String>();
  p.toUpperCase();
  if (p == "RULE_ONLY") controlPolicy = POLICY_RULE_ONLY;
  else if (p == "AI_PREFERRED") controlPolicy = POLICY_AI_PREFERRED;
  else if (p == "NO_ENERGY_MANAGEMENT") controlPolicy = POLICY_NO_ENERGY_MANAGEMENT;
  else controlPolicy = POLICY_HYBRID;
  ack["control_policy"] = controlPolicyToString(controlPolicy);
}

bool applyPredictorMode(JsonObject root, JsonDocument& ack, JsonDocument& cmdDoc) {
  if (!root.containsKey("predictor_mode") && !root.containsKey("PREDICTOR_MODE")) return false;
  String mode = root.containsKey("predictor_mode") ? root["predictor_mode"].as<String>() : root["PREDICTOR_MODE"].as<String>();
  mode.toUpperCase();
  if (mode != "TRAIN_ONLY" && mode != "EVAL_ONLY" && mode != "ONLINE" && mode != "OFFLINE") {
    ack["ok"] = false;
    ack["msg"] = "invalid predictor_mode";
    publishAck(ack, cmdDoc);
    return true;
  }
  predictorMode = mode;
  predictorModeSet = true;
  ack["predictor_mode"] = predictorMode;
  return false;
}

void applyDeprecatedModeSeason(JsonObject root, JsonDocument& ack) {
  if (root.containsKey("mode")) ack["mode"] = "IGNORED (deprecated)";
  if (root.containsKey("season")) ack["season"] = "IGNORED (deprecated)";
}

void applyTariffPeak(JsonObject root, JsonDocument& ack) {
  if (root.containsKey("tariff_state")) {
    tariffState = root["tariff_state"].as<String>();
  }
  if (root.containsKey("peak_event")) {
    peakEvent = root["peak_event"].as<bool>();
  }
  if (root.containsKey("tariff_state")) ack["tariff_state"] = tariffState;
  if (root.containsKey("peak_event")) ack["peak_event"] = peakEvent;
}

bool applyProcessCommand(JsonObject root, JsonDocument& ack, JsonDocument& cmdDoc) {
  if (!root.containsKey("process") && !root.containsKey("production_goal")) return false;
  JsonObject proc = root.containsKey("process")
    ? root["process"].as<JsonObject>()
    : root["production_goal"].as<JsonObject>();
  const bool wasProcessEnabled = productionProcessEnabled;

  if (proc.containsKey("predictor_mode")) {
    String mode = proc["predictor_mode"].as<String>();
    mode.toUpperCase();
    if (mode == "TRAIN_ONLY" || mode == "EVAL_ONLY" || mode == "ONLINE" || mode == "OFFLINE") {
      predictorMode = mode;
      predictorModeSet = true;
    }
  }

    if (proc.containsKey("goal_cycles")) {
      PRODUCTION_GOAL_CYCLES = max<uint32_t>(1u, (uint32_t)proc["goal_cycles"].as<int>());
      processConfigGoalCyclesSet = true;
    }
  bool heightChanged = false;
  if (proc.containsKey("tank_height_cm")) {
    float h = proc["tank_height_cm"].as<float>();
    float clamped = clampf(h, 10.0f, 500.0f);
    TANK1_HEIGHT_CM = clamped;
    TANK2_HEIGHT_CM = clamped;
    heightChanged = true;
  }
  if (proc.containsKey("tank1_height_cm")) {
    float h = proc["tank1_height_cm"].as<float>();
    TANK1_HEIGHT_CM = clampf(h, 10.0f, 500.0f);
    heightChanged = true;
  }
  if (proc.containsKey("tank2_height_cm")) {
    float h = proc["tank2_height_cm"].as<float>();
    TANK2_HEIGHT_CM = clampf(h, 10.0f, 500.0f);
    heightChanged = true;
  }
  if (proc.containsKey("tank_low_level_pct")) {
    float v = proc["tank_low_level_pct"].as<float>();
    TANK1_LOW_LEVEL_PCT = clampLowPctForHeight(v, TANK1_HEIGHT_CM);
    TANK2_LOW_LEVEL_PCT = clampLowPctForHeight(v, TANK2_HEIGHT_CM);
  }
  if (proc.containsKey("tank_high_level_pct")) {
    float v = proc["tank_high_level_pct"].as<float>();
    TANK1_HIGH_LEVEL_PCT = clampHighPctForHeight(v, TANK1_HEIGHT_CM, TANK1_LOW_LEVEL_PCT);
    TANK2_HIGH_LEVEL_PCT = clampHighPctForHeight(v, TANK2_HEIGHT_CM, TANK2_LOW_LEVEL_PCT);
  }
  if (proc.containsKey("tank1_low_level_pct")) {
    TANK1_LOW_LEVEL_PCT = clampLowPctForHeight(proc["tank1_low_level_pct"].as<float>(), TANK1_HEIGHT_CM);
  }
  if (proc.containsKey("tank2_low_level_pct")) {
    TANK2_LOW_LEVEL_PCT = clampLowPctForHeight(proc["tank2_low_level_pct"].as<float>(), TANK2_HEIGHT_CM);
  }
  if (proc.containsKey("tank1_high_level_pct")) {
    TANK1_HIGH_LEVEL_PCT = clampHighPctForHeight(proc["tank1_high_level_pct"].as<float>(), TANK1_HEIGHT_CM, TANK1_LOW_LEVEL_PCT);
  }
  if (proc.containsKey("tank2_high_level_pct")) {
    TANK2_HIGH_LEVEL_PCT = clampHighPctForHeight(proc["tank2_high_level_pct"].as<float>(), TANK2_HEIGHT_CM, TANK2_LOW_LEVEL_PCT);
  }
  if (heightChanged) {
    TANK1_LOW_LEVEL_PCT = clampLowPctForHeight(TANK1_LOW_LEVEL_PCT, TANK1_HEIGHT_CM);
    TANK2_LOW_LEVEL_PCT = clampLowPctForHeight(TANK2_LOW_LEVEL_PCT, TANK2_HEIGHT_CM);
    TANK1_HIGH_LEVEL_PCT = clampHighPctForHeight(TANK1_HIGH_LEVEL_PCT, TANK1_HEIGHT_CM, TANK1_LOW_LEVEL_PCT);
    TANK2_HIGH_LEVEL_PCT = clampHighPctForHeight(TANK2_HIGH_LEVEL_PCT, TANK2_HEIGHT_CM, TANK2_LOW_LEVEL_PCT);
  }
  TANK1_LOW_LEVEL_PCT = min(TANK1_LOW_LEVEL_PCT, TANK1_HIGH_LEVEL_PCT);
  TANK2_LOW_LEVEL_PCT = min(TANK2_LOW_LEVEL_PCT, TANK2_HIGH_LEVEL_PCT);
  if (proc.containsKey("tank1_level_target_pct")) {
    TANK1_LEVEL_TARGET_PCT = clampf(proc["tank1_level_target_pct"].as<float>(), TANK1_LOW_LEVEL_PCT, TANK1_HIGH_LEVEL_PCT);
    processConfigTank1LevelSet = true;
  }
  if (proc.containsKey("tank2_level_target_pct")) {
    TANK2_LEVEL_TARGET_PCT = clampf(proc["tank2_level_target_pct"].as<float>(), TANK2_LOW_LEVEL_PCT, TANK2_HIGH_LEVEL_PCT);
    processConfigTank2LevelSet = true;
  }
  if (heightChanged) {
    TANK1_LEVEL_TARGET_PCT = clampf(TANK1_LEVEL_TARGET_PCT, TANK1_LOW_LEVEL_PCT, TANK1_HIGH_LEVEL_PCT);
    TANK2_LEVEL_TARGET_PCT = clampf(TANK2_LEVEL_TARGET_PCT, TANK2_LOW_LEVEL_PCT, TANK2_HIGH_LEVEL_PCT);
  }
  if (proc.containsKey("tank1_temp_target_c")) {
    TANK1_TEMP_TARGET_C = clampf(proc["tank1_temp_target_c"].as<float>(), 0.0f, 95.0f);
    processConfigTank1TempSet = true;
  }
  if (proc.containsKey("tank2_temp_target_c")) {
    TANK2_TEMP_TARGET_C = clampf(proc["tank2_temp_target_c"].as<float>(), 0.0f, 95.0f);
    processConfigTank2TempSet = true;
  }
  if (proc.containsKey("tank1_level_pct")) tank1LevelPct = clampf(proc["tank1_level_pct"].as<float>(), 0.0f, 100.0f);
  if (proc.containsKey("tank2_level_pct")) tank2LevelPct = clampf(proc["tank2_level_pct"].as<float>(), 0.0f, 100.0f);
  if (proc.containsKey("tank1_temp_c")) tank1TempC = proc["tank1_temp_c"].as<float>();
  if (proc.containsKey("tank2_temp_c")) tank2TempC = proc["tank2_temp_c"].as<float>();

  bool wantsStart = (proc.containsKey("enabled") && proc["enabled"].as<bool>())
    || (proc.containsKey("restart") && proc["restart"].as<bool>());
  if (wantsStart && emergencyStopActive) {
    ack["ok"] = false;
    ack["msg"] = "emergency_stop_active";
    ack["process"] = root["process"];
    publishAck(ack, cmdDoc);
    return true;
  }
  if (wantsStart && !processConfigReady()) {
    ack["ok"] = false;
    ack["msg"] = "process_config_incomplete: set required parameters before starting";
    ack["missing"] = processConfigMissing();
    publishAck(ack, cmdDoc);
    return true;
  }

  if (proc.containsKey("enabled")) {
    productionProcessEnabled = proc["enabled"].as<bool>();
    processLockActive = productionProcessEnabled;
    if (productionProcessEnabled) {
      if (!wasProcessEnabled) processCycleCount = 0;
      processStartMs = millis();
      if (productionState == PROCESS_IDLE) productionState = PROCESS_HEAT_TANK1;
    } else {
      productionState = PROCESS_IDLE;
    }
  }
  if (proc.containsKey("restart") && proc["restart"].as<bool>()) {
    productionProcessEnabled = true;
    processLockActive = true;
    processStartMs = millis();
    processCycleCount = 0;
    productionState = PROCESS_HEAT_TANK1;
  }

  JsonObject ackProcess = ack.createNestedObject("process");
  ackProcess["enabled"] = productionProcessEnabled;
  ackProcess["lock_active"] = processLockActive;
  ackProcess["state"] = productionStateToString(productionState);
  ackProcess["cycle_count"] = processCycleCount;
  ackProcess["goal_cycles"] = PRODUCTION_GOAL_CYCLES;
  ackProcess["predictor_mode"] = predictorMode;
  ackProcess["tank1_level_target_pct"] = TANK1_LEVEL_TARGET_PCT;
  ackProcess["tank2_level_target_pct"] = TANK2_LEVEL_TARGET_PCT;
  ackProcess["tank_low_level_pct"] = TANK1_LOW_LEVEL_PCT;
  ackProcess["tank_high_level_pct"] = TANK1_HIGH_LEVEL_PCT;
  ackProcess["tank1_low_level_pct"] = TANK1_LOW_LEVEL_PCT;
  ackProcess["tank2_low_level_pct"] = TANK2_LOW_LEVEL_PCT;
  ackProcess["tank1_high_level_pct"] = TANK1_HIGH_LEVEL_PCT;
  ackProcess["tank2_high_level_pct"] = TANK2_HIGH_LEVEL_PCT;
  ackProcess["tank1_temp_target_base_c"] = TANK1_TEMP_TARGET_C;
  ackProcess["tank2_temp_target_base_c"] = TANK2_TEMP_TARGET_C;
  ackProcess["tank1_temp_target_c"] = tank1EffectiveTempTargetC();
  ackProcess["tank2_temp_target_c"] = tank2EffectiveTempTargetC();
  ackProcess["temp_target_step_per_cycle_c"] = PROCESS_TEMP_TARGET_STEP_PER_CYCLE_C;
  ackProcess["tank_height_cm"] = TANK1_HEIGHT_CM;
  ackProcess["tank1_height_cm"] = TANK1_HEIGHT_CM;
  ackProcess["tank2_height_cm"] = TANK2_HEIGHT_CM;
  return false;
}

void handleDeviceSet(const String& devName, JsonObject setObj) {
  LoadChannel* ld = findLoadByName(devName);
  if (ld == nullptr) return;

  if (setObj.containsKey("clear_fault") && setObj["clear_fault"].as<bool>()) {
    ld->faultInjectA = 0.0f;
    const float limitA = (ld->faultLimitA > 0.0f) ? ld->faultLimitA : LOAD_OVERCURRENT_LIMIT_A;
    const unsigned long restoreDelayMs = (ld->faultRestoreDelayMs > 0) ? ld->faultRestoreDelayMs : OVERCURRENT_CLEAR_DELAY_MS;
    const bool overLimit = (ld->currentA >= limitA);
    if (!overLimit) {
      ld->faultActive = false;
      ld->faultLatched = false;
      ld->faultCode = "NONE";
      ld->health = "OK";
      ld->overcurrentStartMs = 0;
      ld->faultClearStartMs = 0;
      const bool processDemand = (productionProcessEnabled && processDemandsLoad(ld));
      const bool allowRestore = (ld->faultWasOn || processDemand);
      if ((ld->faultWasOn || processDemand) && allowRestore) {
        ld->restorePending = true;
        ld->restoreAtMs = millis() + restoreDelayMs;
        ld->on = false;
        ld->duty = 0.0f;
        ld->appliedDuty = 0.0f;
        ld->lastDutyUpdateMs = millis();
      } else {
        ld->restorePending = false;
        ld->on = false;
        ld->duty = 0.0f;
        ld->appliedDuty = 0.0f;
        ld->lastDutyUpdateMs = millis();
      }
    } else {
      Serial.printf("[fault] clear denied %s current=%.3fA\n",
                    ld->name.c_str(),
                    ld->currentA);
    }
  }
  if (setObj.containsKey("on")) {
    bool newOn = setObj["on"].as<bool>();
    if (!newOn && ld->on) {
      rememberDutyBeforeOff(ld);
    }
    ld->controlReduced = false;
    ld->restoreBlockedUntilMs = 0;
    ld->on = newOn;
    if (newOn && !setObj.containsKey("duty")) {
      restoreDutyOn(ld);
    }
  }
  if (setObj.containsKey("duty")) {
    ld->controlReduced = false;
    ld->restoreBlockedUntilMs = 0;
    ld->duty = clampf(setObj["duty"].as<float>(), 0.0f, 1.0f);
    if (ld->duty > 0.0f) {
      ld->lastDuty = ld->duty;
    }
  }
  if (setObj.containsKey("load_factor")) ld->loadFactor = clampf(setObj["load_factor"].as<float>(), 0.5f, 1.6f);
  if (setObj.containsKey("temperature_c")) ld->tempC = setObj["temperature_c"].as<float>();
  if (setObj.containsKey("fault_limit_a")) ld->faultLimitA = max(0.0f, setObj["fault_limit_a"].as<float>());
  if (setObj.containsKey("fault_trip_ms") || setObj.containsKey("fault_trip_delay_ms")) {
    unsigned long v = (unsigned long)(setObj.containsKey("fault_trip_ms")
      ? setObj["fault_trip_ms"].as<unsigned long>()
      : setObj["fault_trip_delay_ms"].as<unsigned long>());
    ld->faultTripDelayMs = max(0UL, v);
  }
  if (setObj.containsKey("fault_clear_ms") || setObj.containsKey("fault_clear_delay_ms")) {
    unsigned long v = (unsigned long)(setObj.containsKey("fault_clear_ms")
      ? setObj["fault_clear_ms"].as<unsigned long>()
      : setObj["fault_clear_delay_ms"].as<unsigned long>());
    ld->faultClearDelayMs = max(0UL, v);
  }
  if (setObj.containsKey("fault_restore_ms") || setObj.containsKey("fault_restore_delay_ms")) {
    unsigned long v = (unsigned long)(setObj.containsKey("fault_restore_ms")
      ? setObj["fault_restore_ms"].as<unsigned long>()
      : setObj["fault_restore_delay_ms"].as<unsigned long>());
    ld->faultRestoreDelayMs = max(0UL, v);
  }
  if (setObj.containsKey("override") || setObj.containsKey("policy_override")) {
    ld->policyOverride = setObj.containsKey("override")
      ? setObj["override"].as<bool>()
      : setObj["policy_override"].as<bool>();
  }
  if (setObj.containsKey("inject_current_a") || setObj.containsKey("fault_inject_a")) {
    float val = setObj.containsKey("inject_current_a") ? setObj["inject_current_a"].as<float>() : setObj["fault_inject_a"].as<float>();
    ld->faultInjectA = max(0.0f, val);
  }
  if (setObj.containsKey("clear_inject") && setObj["clear_inject"].as<bool>()) {
    ld->faultInjectA = 0.0f;
  }
}

void handleDeviceMeta(const String& devName, JsonObject metaObj) {
  LoadChannel* ld = findLoadByName(devName);
  if (ld == nullptr) return;
  if (metaObj.containsKey("priority")) ld->priority = (uint8_t)clampf(metaObj["priority"].as<int>(), 1, 99);
  if (metaObj.containsKey("class")) ld->loadClass = parseLoadClass(metaObj["class"].as<String>());
}

void handleCommandMessage(const String& payload) {
  StaticJsonDocument<4096> doc;
  StaticJsonDocument<2048> ack;
  ack["ok"] = true;
  ack["msg"] = "command applied";

  auto err = deserializeJson(doc, payload);
  if (err) {
    ack["ok"] = false;
    ack["msg"] = "invalid json";
    publishAck(ack, doc);
    return;
  }

  JsonObject root = doc.as<JsonObject>();

  if (rejectIfProcessLocked(root, ack, doc)) return;
  applyControllerMode(root, ack);
  applyControlPolicy(root, ack);
  if (applyPredictorMode(root, ack, doc)) return;
  applyDeprecatedModeSeason(root, ack);
  applyTariffPeak(root, ack);
  if (applyProcessCommand(root, ack, doc)) return;

  if (root.containsKey("scene")) {
    JsonObject scene = root["scene"].as<JsonObject>();
    if (scene.containsKey("enabled")) {
      sceneEnabled = scene["enabled"].as<bool>();
      sceneLockActive = sceneEnabled;
      if (sceneEnabled && sceneStartMs == 0) sceneStartMs = millis();
      if (!sceneEnabled) sceneLockActive = false;
    }
    if (scene.containsKey("preset")) {
      String preset = scene["preset"].as<String>();
      float presetDurationSec = scenePresetDurationSec(preset);
      if (presetDurationSec > 0.0f) {
        sceneName = preset;
        sceneName.toUpperCase();
        if (!scene.containsKey("duration_sec") && sceneDurationSec <= 0.0f) {
          // Keep backward compatibility if duration was never configured.
          sceneDurationSec = presetDurationSec;
        }
      }
    }
    if (scene.containsKey("duration_sec")) {
      sceneName = "CUSTOM";
      sceneDurationSec = max(10.0f, scene["duration_sec"].as<float>());
    }
    if (scene.containsKey("loop")) {
      sceneLoop = scene["loop"].as<bool>();
    }
    if (scene.containsKey("clear_overrides") && scene["clear_overrides"].as<bool>()) {
      sceneOverrideModeEnabled = false;
      sceneOverrideSeasonEnabled = false;
      sceneOverridePolicyEnabled = false;
      sceneOverrideTariffEnabled = false;
      sceneOverridePeakEnabled = false;
      sceneOverrideTempEnabled = false;
      sceneOverrideHumidityEnabled = false;
    }
    if (scene.containsKey("mode")) {
      sceneOverrideModeEnabled = true;
      sceneOverrideMode = plantModeFromString(scene["mode"].as<String>());
    }
    if (scene.containsKey("season")) {
      sceneOverrideSeasonEnabled = true;
      sceneOverrideSeason = seasonModeFromString(scene["season"].as<String>());
    }
    if (scene.containsKey("control_policy")) {
      sceneOverridePolicyEnabled = true;
      sceneOverridePolicy = controlPolicyFromString(scene["control_policy"].as<String>());
    }
    if (scene.containsKey("tariff_state")) {
      sceneOverrideTariffEnabled = true;
      sceneOverrideTariffState = scene["tariff_state"].as<String>();
      sceneOverrideTariffState.toUpperCase();
    }
    if (scene.containsKey("peak_event")) {
      sceneOverridePeakEnabled = true;
      sceneOverridePeakEvent = scene["peak_event"].as<bool>();
    }
    if (scene.containsKey("environment")) {
      JsonObject sceneEnv = scene["environment"].as<JsonObject>();
      if (sceneEnv.containsKey("temperature_c")) {
        sceneOverrideTempEnabled = true;
        sceneOverrideTempC = sceneEnv["temperature_c"].as<float>();
      }
      if (sceneEnv.containsKey("humidity_pct")) {
        sceneOverrideHumidityEnabled = true;
        sceneOverrideHumidityPct = clampf(sceneEnv["humidity_pct"].as<float>(), 0.0f, 100.0f);
      }
    }
    if (scene["restart"] | false) {
      sceneStartMs = millis();
    }

    JsonObject ackScene = ack.createNestedObject("scene");
    ackScene["enabled"] = sceneEnabled;
    ackScene["lock_active"] = sceneLockActive;
    ackScene["name"] = sceneName;
    ackScene["duration_sec"] = sceneDurationSec;
    ackScene["loop"] = sceneLoop;
    JsonObject ackSceneOverrides = ackScene.createNestedObject("overrides");
    if (sceneOverrideModeEnabled) ackSceneOverrides["mode"] = plantModeToString(sceneOverrideMode);
    if (sceneOverrideSeasonEnabled) ackSceneOverrides["season"] = seasonModeToString(sceneOverrideSeason);
    if (sceneOverridePolicyEnabled) ackSceneOverrides["control_policy"] = controlPolicyToString(sceneOverridePolicy);
    if (sceneOverrideTariffEnabled) ackSceneOverrides["tariff_state"] = sceneOverrideTariffState;
    if (sceneOverridePeakEnabled) ackSceneOverrides["peak_event"] = sceneOverridePeakEvent;
    if (sceneOverrideTempEnabled || sceneOverrideHumidityEnabled) {
      JsonObject ackSceneEnv = ackSceneOverrides.createNestedObject("environment");
      if (sceneOverrideTempEnabled) ackSceneEnv["temperature_c"] = sceneOverrideTempC;
      if (sceneOverrideHumidityEnabled) ackSceneEnv["humidity_pct"] = sceneOverrideHumidityPct;
    }
  }

  // Control block
  if (root.containsKey("control")) {
    JsonObject ctrl = root["control"].as<JsonObject>();
    if (ctrl.containsKey("MAX_TOTAL_POWER_W") || ctrl.containsKey("POWER_MAX_W") || ctrl.containsKey("P_MAX")) {
      MAX_TOTAL_POWER_W = ctrl.containsKey("MAX_TOTAL_POWER_W") ? ctrl["MAX_TOTAL_POWER_W"].as<float>()
                        : (ctrl.containsKey("POWER_MAX_W") ? ctrl["POWER_MAX_W"].as<float>() : ctrl["P_MAX"].as<float>());
    }
    if (ctrl.containsKey("UNDERVOLTAGE_THRESHOLD_V") || ctrl.containsKey("UV_THRESHOLD_V")) {
      float v = ctrl.containsKey("UNDERVOLTAGE_THRESHOLD_V")
        ? ctrl["UNDERVOLTAGE_THRESHOLD_V"].as<float>()
        : ctrl["UV_THRESHOLD_V"].as<float>();
      UNDERVOLTAGE_THRESHOLD_V = max(0.0f, v);
    }
    if (ctrl.containsKey("UNDERVOLTAGE_TRIP_DELAY_MS") || ctrl.containsKey("UV_TRIP_DELAY_MS")) {
      unsigned long v = (unsigned long)(ctrl.containsKey("UNDERVOLTAGE_TRIP_DELAY_MS")
        ? ctrl["UNDERVOLTAGE_TRIP_DELAY_MS"].as<unsigned long>()
        : ctrl["UV_TRIP_DELAY_MS"].as<unsigned long>());
      UNDERVOLTAGE_TRIP_DELAY_MS = max(0UL, v);
    }
    if (ctrl.containsKey("UNDERVOLTAGE_CLEAR_DELAY_MS") || ctrl.containsKey("UV_CLEAR_DELAY_MS")) {
      unsigned long v = (unsigned long)(ctrl.containsKey("UNDERVOLTAGE_CLEAR_DELAY_MS")
        ? ctrl["UNDERVOLTAGE_CLEAR_DELAY_MS"].as<unsigned long>()
        : ctrl["UV_CLEAR_DELAY_MS"].as<unsigned long>());
      UNDERVOLTAGE_CLEAR_DELAY_MS = max(0UL, v);
    }
    if (ctrl.containsKey("UNDERVOLTAGE_RESTORE_MARGIN_V") || ctrl.containsKey("UV_RESTORE_MARGIN_V")) {
      float v = ctrl.containsKey("UNDERVOLTAGE_RESTORE_MARGIN_V")
        ? ctrl["UNDERVOLTAGE_RESTORE_MARGIN_V"].as<float>()
        : ctrl["UV_RESTORE_MARGIN_V"].as<float>();
      UNDERVOLTAGE_RESTORE_MARGIN_V = max(0.0f, v);
    }
    if (ctrl.containsKey("OVERVOLTAGE_THRESHOLD_V") || ctrl.containsKey("OV_THRESHOLD_V")) {
      float v = ctrl.containsKey("OVERVOLTAGE_THRESHOLD_V")
        ? ctrl["OVERVOLTAGE_THRESHOLD_V"].as<float>()
        : ctrl["OV_THRESHOLD_V"].as<float>();
      OVERVOLTAGE_THRESHOLD_V = max(0.0f, v);
    }
    if (ctrl.containsKey("OVERVOLTAGE_TRIP_DELAY_MS") || ctrl.containsKey("OV_TRIP_DELAY_MS")) {
      unsigned long v = (unsigned long)(ctrl.containsKey("OVERVOLTAGE_TRIP_DELAY_MS")
        ? ctrl["OVERVOLTAGE_TRIP_DELAY_MS"].as<unsigned long>()
        : ctrl["OV_TRIP_DELAY_MS"].as<unsigned long>());
      OVERVOLTAGE_TRIP_DELAY_MS = max(0UL, v);
    }
    if (ctrl.containsKey("OVERVOLTAGE_CLEAR_DELAY_MS") || ctrl.containsKey("OV_CLEAR_DELAY_MS")) {
      unsigned long v = (unsigned long)(ctrl.containsKey("OVERVOLTAGE_CLEAR_DELAY_MS")
        ? ctrl["OVERVOLTAGE_CLEAR_DELAY_MS"].as<unsigned long>()
        : ctrl["OV_CLEAR_DELAY_MS"].as<unsigned long>());
      OVERVOLTAGE_CLEAR_DELAY_MS = max(0UL, v);
    }
    if (ctrl.containsKey("OVERVOLTAGE_RESTORE_MARGIN_V") || ctrl.containsKey("OV_RESTORE_MARGIN_V")) {
      float v = ctrl.containsKey("OVERVOLTAGE_RESTORE_MARGIN_V")
        ? ctrl["OVERVOLTAGE_RESTORE_MARGIN_V"].as<float>()
        : ctrl["OV_RESTORE_MARGIN_V"].as<float>();
      OVERVOLTAGE_RESTORE_MARGIN_V = max(0.0f, v);
    }
    if (ctrl.containsKey("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH") || ctrl.containsKey("MAX_ENERGY_CONSUMPTION_FOR_DAY_WH") || ctrl.containsKey("DAILY_ENERGY_CAP_WH")) {
      MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH =
        ctrl.containsKey("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH") ? ctrl["MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH"].as<float>()
      : (ctrl.containsKey("MAX_ENERGY_CONSUMPTION_FOR_DAY_WH") ? ctrl["MAX_ENERGY_CONSUMPTION_FOR_DAY_WH"].as<float>() : ctrl["DAILY_ENERGY_CAP_WH"].as<float>());
      if (MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH < 0.0f) MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = 0.0f;
    }
    if (ctrl.containsKey("ENERGY_GOAL_DURATION_SEC") || ctrl.containsKey("ENERGY_GOAL_DURATION_MIN")) {
      float sec = ctrl.containsKey("ENERGY_GOAL_DURATION_SEC")
        ? ctrl["ENERGY_GOAL_DURATION_SEC"].as<float>()
        : (ctrl["ENERGY_GOAL_DURATION_MIN"].as<float>() * 60.0f);
      ENERGY_GOAL_DURATION_SEC = max(60.0f, sec);
    }
    if (ctrl.containsKey("ENERGY_GOAL_RATE_W_PER_MIN") || ctrl.containsKey("ENERGY_GOAL_W_PER_MIN")) {
      float rate = ctrl.containsKey("ENERGY_GOAL_RATE_W_PER_MIN")
        ? ctrl["ENERGY_GOAL_RATE_W_PER_MIN"].as<float>()
        : ctrl["ENERGY_GOAL_W_PER_MIN"].as<float>();
      rate = max(0.0f, rate);
      MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = rate * (ENERGY_GOAL_DURATION_SEC / 60.0f);
    }
    if (ctrl.containsKey("ENERGY_GOAL_WH")) {
      MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = max(0.0f, ctrl["ENERGY_GOAL_WH"].as<float>());
    }
    if (ctrl.containsKey("RESTORE_POWER_RATIO") || ctrl.containsKey("RESTORE_THRESHOLD_RATIO") || ctrl.containsKey("RESTORE_FACTOR")) {
      RESTORE_POWER_RATIO = clampf(
        ctrl.containsKey("RESTORE_POWER_RATIO") ? ctrl["RESTORE_POWER_RATIO"].as<float>()
        : (ctrl.containsKey("RESTORE_THRESHOLD_RATIO") ? ctrl["RESTORE_THRESHOLD_RATIO"].as<float>() : ctrl["RESTORE_FACTOR"].as<float>()),
        0.0f, 1.0f
      );
    }
    if (ctrl.containsKey("CONTROL_COOLDOWN_SEC") || ctrl.containsKey("CONTROL_COOLDOWN_S") || ctrl.containsKey("COOLDOWN_SEC")) {
      CONTROL_COOLDOWN_SEC = max(0.0f, ctrl.containsKey("CONTROL_COOLDOWN_SEC") ? ctrl["CONTROL_COOLDOWN_SEC"].as<float>()
                              : (ctrl.containsKey("CONTROL_COOLDOWN_S") ? ctrl["CONTROL_COOLDOWN_S"].as<float>() : ctrl["COOLDOWN_SEC"].as<float>()));
    }
    if (ctrl.containsKey("CLEAR_VOLTAGE_INJECTION") && ctrl["CLEAR_VOLTAGE_INJECTION"].as<bool>()) {
      clearVoltageInjection();
    }
    if (ctrl.containsKey("VOLTAGE_INJECTION_MAGNITUDE_V") || ctrl.containsKey("VOLTAGE_INJECT_V") || ctrl.containsKey("VOLTAGE_FAULT_MAGNITUDE_V")) {
      float v = ctrl.containsKey("VOLTAGE_INJECTION_MAGNITUDE_V")
        ? ctrl["VOLTAGE_INJECTION_MAGNITUDE_V"].as<float>()
        : (ctrl.containsKey("VOLTAGE_INJECT_V") ? ctrl["VOLTAGE_INJECT_V"].as<float>() : ctrl["VOLTAGE_FAULT_MAGNITUDE_V"].as<float>());
      voltageInjectionMagnitudeV = max(0.0f, v);
      if (voltageInjectionMagnitudeV <= 0.0f) {
        voltageInjectionMode = VOLTAGE_INJECTION_NONE;
      }
    }
    if (ctrl.containsKey("VOLTAGE_INJECTION_MODE") || ctrl.containsKey("VOLTAGE_SCENARIO") || ctrl.containsKey("VOLTAGE_FAULT_MODE")) {
      String modeStr = ctrl.containsKey("VOLTAGE_INJECTION_MODE")
        ? ctrl["VOLTAGE_INJECTION_MODE"].as<String>()
        : (ctrl.containsKey("VOLTAGE_SCENARIO") ? ctrl["VOLTAGE_SCENARIO"].as<String>() : ctrl["VOLTAGE_FAULT_MODE"].as<String>());
      VoltageInjectionMode parsedMode = VOLTAGE_INJECTION_NONE;
      if (parseVoltageInjectionMode(modeStr, parsedMode)) {
        voltageInjectionMode = parsedMode;
        if (parsedMode == VOLTAGE_INJECTION_NONE) {
          voltageInjectionMagnitudeV = 0.0f;
        }
      }
    }
    if (ctrl.containsKey("INJECT_UNDERVOLTAGE") && ctrl["INJECT_UNDERVOLTAGE"].as<bool>()) {
      voltageInjectionMode = VOLTAGE_INJECTION_UNDERVOLTAGE;
    }
    if (ctrl.containsKey("INJECT_OVERVOLTAGE") && ctrl["INJECT_OVERVOLTAGE"].as<bool>()) {
      voltageInjectionMode = VOLTAGE_INJECTION_OVERVOLTAGE;
    }
    if (ctrl.containsKey("EMERGENCY_STOP") || ctrl.containsKey("E_STOP")) {
      emergencyStopActive = ctrl.containsKey("EMERGENCY_STOP") ? ctrl["EMERGENCY_STOP"].as<bool>()
        : ctrl["E_STOP"].as<bool>();
      if (emergencyStopActive) {
        productionProcessEnabled = false;
        processLockActive = false;
        productionState = PROCESS_IDLE;
        forceAllLoadsOff(true);
      }
    }
    if (ctrl.containsKey("MIN_DEVICE_DUTY") || ctrl.containsKey("MIN_DUTY")) {
      MIN_DEVICE_DUTY = clampf(ctrl.containsKey("MIN_DEVICE_DUTY") ? ctrl["MIN_DEVICE_DUTY"].as<float>() : ctrl["MIN_DUTY"].as<float>(), 0.0f, 1.0f);
    }
    if (ctrl.containsKey("DUTY_STEP_SIZE") || ctrl.containsKey("DUTY_STEP")) {
      DUTY_STEP_SIZE = clampf(ctrl.containsKey("DUTY_STEP_SIZE") ? ctrl["DUTY_STEP_SIZE"].as<float>() : ctrl["DUTY_STEP"].as<float>(), 0.01f, 1.0f);
    }
    if (ctrl.containsKey("DUTY_RAMP_RATE_PER_SEC") || ctrl.containsKey("DUTY_RAMP_SEC")) {
      if (ctrl.containsKey("DUTY_RAMP_SEC")) {
        float sec = max(0.0f, ctrl["DUTY_RAMP_SEC"].as<float>());
        DUTY_RAMP_RATE_PER_SEC = (sec <= 0.0f) ? 0.0f : (1.0f / sec);
      } else {
        DUTY_RAMP_RATE_PER_SEC = max(0.0f, ctrl["DUTY_RAMP_RATE_PER_SEC"].as<float>());
      }
    }
    if (ctrl.containsKey("CRITICAL_MIN_DEVICE_DUTY") || ctrl.containsKey("CRITICAL_MIN_DUTY")) {
      CRITICAL_MIN_DEVICE_DUTY = clampf(ctrl.containsKey("CRITICAL_MIN_DEVICE_DUTY") ? ctrl["CRITICAL_MIN_DEVICE_DUTY"].as<float>() : ctrl["CRITICAL_MIN_DUTY"].as<float>(), 0.0f, 1.0f);
    }
    if (ctrl.containsKey("HEATER_WARM_TEMP_C")) HEATER_WARM_TEMP_C = ctrl["HEATER_WARM_TEMP_C"].as<float>();
    if (ctrl.containsKey("HEATER_DUTY_STEP_SIZE") || ctrl.containsKey("HEATER_DUTY_STEP")) {
      HEATER_DUTY_STEP_SIZE = clampf(ctrl.containsKey("HEATER_DUTY_STEP_SIZE") ? ctrl["HEATER_DUTY_STEP_SIZE"].as<float>() : ctrl["HEATER_DUTY_STEP"].as<float>(), 0.01f, 1.0f);
    }
    if (ctrl.containsKey("HEATER_HOT_OFF_TEMP_C")) HEATER_HOT_OFF_TEMP_C = ctrl["HEATER_HOT_OFF_TEMP_C"].as<float>();
    if (ctrl.containsKey("PREDICTION_DELAY_SEC") || ctrl.containsKey("PREDICTOR_DELAY_SEC")) {
      predictionApplyDelaySec = max(0.0f, ctrl.containsKey("PREDICTION_DELAY_SEC") ? ctrl["PREDICTION_DELAY_SEC"].as<float>() : ctrl["PREDICTOR_DELAY_SEC"].as<float>());
    }
    if (ctrl.containsKey("AI_SUGGESTION_SOURCE") || ctrl.containsKey("PREDICTION_SOURCE") || ctrl.containsKey("SUGGESTION_SOURCE")) {
      ack["ai_suggestion_source"] = "IGNORED (fused multi-source mode)";
    }
    if (ctrl.containsKey("AI_DYNAMIC_PROCESS") || ctrl.containsKey("ENABLE_AI_DYNAMIC_PROCESS")) {
      // Deprecated toggle: dynamic process is policy-driven (HYBRID / AI_PREFERRED).
    }
    if (ctrl.containsKey("CALIBRATE_CURRENT") || ctrl.containsKey("CALIBRATE_CURRENT_OFFSETS")) {
      bool doCal = ctrl.containsKey("CALIBRATE_CURRENT")
        ? ctrl["CALIBRATE_CURRENT"].as<bool>()
        : ctrl["CALIBRATE_CURRENT_OFFSETS"].as<bool>();
      if (doCal) {
        calibrateCurrentZeroOffsets();
      }
    }

    JsonObject ackCtrl = ack.createNestedObject("control");
    ackCtrl["MAX_TOTAL_POWER_W"] = MAX_TOTAL_POWER_W;
    ackCtrl["UNDERVOLTAGE_THRESHOLD_V"] = UNDERVOLTAGE_THRESHOLD_V;
    ackCtrl["UNDERVOLTAGE_TRIP_DELAY_MS"] = UNDERVOLTAGE_TRIP_DELAY_MS;
    ackCtrl["UNDERVOLTAGE_CLEAR_DELAY_MS"] = UNDERVOLTAGE_CLEAR_DELAY_MS;
    ackCtrl["UNDERVOLTAGE_RESTORE_MARGIN_V"] = UNDERVOLTAGE_RESTORE_MARGIN_V;
    ackCtrl["UNDERVOLTAGE_ACTIVE"] = undervoltageActive;
    ackCtrl["OVERVOLTAGE_THRESHOLD_V"] = OVERVOLTAGE_THRESHOLD_V;
    ackCtrl["OVERVOLTAGE_TRIP_DELAY_MS"] = OVERVOLTAGE_TRIP_DELAY_MS;
    ackCtrl["OVERVOLTAGE_CLEAR_DELAY_MS"] = OVERVOLTAGE_CLEAR_DELAY_MS;
    ackCtrl["OVERVOLTAGE_RESTORE_MARGIN_V"] = OVERVOLTAGE_RESTORE_MARGIN_V;
    ackCtrl["OVERVOLTAGE_ACTIVE"] = overvoltageActive;
    ackCtrl["VOLTAGE_INJECTION_MODE"] = voltageInjectionModeToString(voltageInjectionMode);
    ackCtrl["VOLTAGE_INJECTION_MAGNITUDE_V"] = voltageInjectionMagnitudeV;
    ackCtrl["supply_v_raw"] = loadBusVoltageRawV;
    ackCtrl["supply_v_effective"] = loadBusVoltageV;
    ackCtrl["MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH"] = MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH;
    ackCtrl["ENERGY_GOAL_WH"] = MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH;
    ackCtrl["ENERGY_GOAL_DURATION_SEC"] = ENERGY_GOAL_DURATION_SEC;
    ackCtrl["ENERGY_GOAL_DURATION_MIN"] = ENERGY_GOAL_DURATION_SEC / 60.0f;
    ackCtrl["ENERGY_GOAL_RATE_W_PER_MIN"] = (ENERGY_GOAL_DURATION_SEC > 0.0f)
      ? (MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH / (ENERGY_GOAL_DURATION_SEC / 60.0f))
      : 0.0f;
    ackCtrl["RESTORE_POWER_RATIO"] = RESTORE_POWER_RATIO;
    ackCtrl["CONTROL_COOLDOWN_SEC"] = CONTROL_COOLDOWN_SEC;
    ackCtrl["EMERGENCY_STOP"] = emergencyStopActive;
    ackCtrl["DUTY_RAMP_RATE_PER_SEC"] = DUTY_RAMP_RATE_PER_SEC;
    ackCtrl["DUTY_RAMP_SEC"] = (DUTY_RAMP_RATE_PER_SEC > 0.0f) ? (1.0f / DUTY_RAMP_RATE_PER_SEC) : 0.0f;
    ackCtrl["PREDICTION_DELAY_SEC"] = predictionApplyDelaySec;
    ackCtrl["AI_DYNAMIC_PROCESS"] = (controlPolicy == POLICY_AI_PREFERRED || controlPolicy == POLICY_HYBRID);
    String dynamicReason;
    PredictionInputSource dynamicSrc = PRED_SRC_LONG_RF;
    const bool dynamicAvailable = (selectDynamicProcessPredictionState(&dynamicSrc, &dynamicReason) != nullptr);
    ackCtrl["AI_SUGGESTION_SOURCE"] = "FUSED";
    ackCtrl["AI_ACTIVE_SOURCES"] = activePredictionSourcesSummary();
    ackCtrl["AI_DYNAMIC_PROCESS_AVAILABLE"] = dynamicAvailable;
    ackCtrl["AI_DYNAMIC_PROCESS_REASON"] = dynamicReason;
    ackCtrl["AI_DYNAMIC_PROCESS_SOURCE"] = dynamicAvailable ? predictionSourceToString(dynamicSrc) : "NONE";
  }

  // Timing block
  if (root.containsKey("timing")) {
    JsonObject timing = root["timing"].as<JsonObject>();
    if (timing.containsKey("SAMPLE_INTERVAL_SEC") || timing.containsKey("DT_SEC")) {
      SAMPLE_INTERVAL_SEC = timing.containsKey("SAMPLE_INTERVAL_SEC") ? timing["SAMPLE_INTERVAL_SEC"].as<float>() : timing["DT_SEC"].as<float>();
      SAMPLE_INTERVAL_SEC = max(0.1f, SAMPLE_INTERVAL_SEC);
      sampleIntervalMs = (unsigned long)(SAMPLE_INTERVAL_SEC * 1000.0f);
    }
    JsonObject ackTiming = ack.createNestedObject("timing");
    ackTiming["SAMPLE_INTERVAL_SEC"] = SAMPLE_INTERVAL_SEC;
  }

  // Environment override (sensor + optional offset source)
  if (root.containsKey("environment")) {
    JsonObject env = root["environment"].as<JsonObject>();
    if (env.containsKey("temperature_c")) ambientTempC = env["temperature_c"].as<float>();
    if (env.containsKey("humidity_pct")) ambientHumidity = env["humidity_pct"].as<float>();
  }

  // Device commands
  if (root.containsKey("device")) {
    String dev = root["device"].as<String>();
    dev.toLowerCase();
    JsonObject setObj;
    const bool hasSet = root.containsKey("set") && root["set"].is<JsonObject>();
    if (hasSet) {
      setObj = root["set"].as<JsonObject>();
    }
    bool injectionOnly = false;
    bool faultConfigOnly = false;
    if (hasSet) {
      bool sawKey = false;
      injectionOnly = true;
      faultConfigOnly = true;
      for (JsonPair kv : setObj) {
        sawKey = true;
        const char* key = kv.key().c_str();
        if (strcmp(key, "inject_current_a") != 0
            && strcmp(key, "fault_inject_a") != 0
            && strcmp(key, "clear_inject") != 0) {
          injectionOnly = false;
        }
        if (strcmp(key, "inject_current_a") != 0
            && strcmp(key, "fault_inject_a") != 0
            && strcmp(key, "clear_inject") != 0
            && strcmp(key, "override") != 0
            && strcmp(key, "policy_override") != 0
            && strcmp(key, "fault_limit_a") != 0
            && strcmp(key, "fault_trip_ms") != 0
            && strcmp(key, "fault_trip_delay_ms") != 0
            && strcmp(key, "fault_clear_ms") != 0
            && strcmp(key, "fault_clear_delay_ms") != 0
            && strcmp(key, "fault_restore_ms") != 0
            && strcmp(key, "fault_restore_delay_ms") != 0
            && strcmp(key, "clear_fault") != 0) {
          faultConfigOnly = false;
        }
      }
      if (!sawKey) {
        injectionOnly = false;
        faultConfigOnly = false;
      }
    }
    bool nonActuationSetOnly = injectionOnly || faultConfigOnly;
    if (emergencyStopActive && hasSet && !nonActuationSetOnly) {
      ack["ok"] = false;
      ack["msg"] = "emergency_stop_active";
      ack["device"] = dev;
      publishAck(ack, doc);
      return;
    }
    if (hasSet) {
      LoadChannel* ld = findLoadByName(dev);
      if (ld && ld->faultActive) {
        bool wantsOn = setObj.containsKey("on") && setObj["on"].as<bool>();
        bool wantsDuty = setObj.containsKey("duty") && (setObj["duty"].as<float>() > 0.0f);
        if (wantsOn || wantsDuty) {
          ack["ok"] = false;
          ack["msg"] = "device_fault_lockout";
          ack["device"] = dev;
          ack["fault_code"] = ld->faultCode;
          publishAck(ack, doc);
          return;
        }
      }
      handleDeviceSet(dev, setObj);
    }
    if (root.containsKey("meta")) handleDeviceMeta(dev, root["meta"].as<JsonObject>());
    ack["device"] = dev;
  }

  publishAck(ack, doc);
}

void handlePredictionMessage(const String& payload, PredictionInputSource source) {
  PredictionChannelState& state = predictionStateFor(source);
  StaticJsonDocument<1024> doc;
  if (deserializeJson(doc, payload) != DeserializationError::Ok) return;
  JsonObject root = doc.as<JsonObject>();
  state.pendingSourceTimestampSec = normalizePredictionTimestampSec(root["timestamp"] | 0.0);
  state.pendingHorizonSec = root["horizon_sec"] | 0.0f;
  state.pendingSourceMode = String((const char*)(root["predictor_mode"] | "UNSET"));
  state.pendingSourceMode.toUpperCase();
  state.pendingSourceModelReady = root.containsKey("model_ready")
    ? root["model_ready"].as<bool>()
    : true;
  state.pendingPeakRisk = root["peak_risk"] | false;
  state.pendingRiskLevel = String((const char*)(root["risk_level"] | "LOW"));
  state.pendingPowerW = root["predicted_power_w"] | 0.0f;

  state.pendingSuggestionCount = 0;
  if (root.containsKey("suggestions") && root["suggestions"].is<JsonArray>()) {
    JsonArray arr = root["suggestions"].as<JsonArray>();
    for (JsonObject s : arr) {
      if (state.pendingSuggestionCount >= MAX_AI_SUGGESTIONS) break;
      if (!s.containsKey("device") || !s.containsKey("set")) continue;
      JsonObject setObj = s["set"].as<JsonObject>();

      AISuggestion item;
      item.deviceName = String((const char*)s["device"]);
      item.deviceName.toLowerCase();
      if (setObj.containsKey("on")) {
        item.hasOn = true;
        item.onValue = setObj["on"].as<bool>();
      }
      if (setObj.containsKey("duty")) {
        item.hasDuty = true;
        item.dutyValue = setObj["duty"].as<float>();
      }
      if (s.containsKey("process_mode")) {
        ProductionState parsedMode;
        String processModeText = String((const char*)s["process_mode"]);
        if (parseProductionState(processModeText, parsedMode)) {
          item.hasProcessMode = true;
          item.processMode = parsedMode;
        }
      }
      state.pendingSuggestions[state.pendingSuggestionCount++] = item;
    }
  }
  state.pendingApplyAtMs = millis() + (unsigned long)(predictionApplyDelaySec * 1000.0f);
  state.pendingReady = true;
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String body;
  body.reserve(length + 1);
  for (unsigned int i = 0; i < length; ++i) body += (char)payload[i];

  String t = String(topic);
  if (t == topicCommand) {
    handleCommandMessage(body);
  } else if (t == topicPrediction) {
    handlePredictionMessage(body, PRED_SRC_SHORT);
  } else if (t == topicPredictionLong) {
    handlePredictionMessage(body, PRED_SRC_LONG_RF);
  } else if (t == topicPredictionLongLstm) {
    handlePredictionMessage(body, PRED_SRC_LONG_LSTM);
  }
}

void applyPendingPredictionIfDue() {
  const unsigned long nowMs = millis();
  for (int idx = 0; idx < PREDICTION_SOURCE_COUNT; ++idx) {
    PredictionInputSource src = (idx == 1) ? PRED_SRC_LONG_RF : ((idx == 2) ? PRED_SRC_LONG_LSTM : PRED_SRC_SHORT);
    PredictionChannelState& state = predictionStateFor(src);
    if (!state.pendingReady) continue;
    if (nowMs < state.pendingApplyAtMs) continue;

    state.peakRisk = state.pendingPeakRisk;
    state.riskLevel = state.pendingRiskLevel;
    state.powerW = state.pendingPowerW;
    state.sourceTimestampSec = state.pendingSourceTimestampSec;
    state.horizonSec = state.pendingHorizonSec;
    state.sourceMode = state.pendingSourceMode;
    state.sourceModelReady = state.pendingSourceModelReady;
    state.lastAppliedMs = nowMs;
    state.applied = true;
    state.suggestionCount = state.pendingSuggestionCount;
    for (int i = 0; i < state.pendingSuggestionCount; ++i) {
      state.suggestions[i] = state.pendingSuggestions[i];
    }
    state.pendingReady = false;
  }
  rebuildFusedPredictionState();
}

// -----------------------------
// WiFi + MQTT lifecycle
// -----------------------------
static bool beginEnterpriseWifiConnection(const String& ssid,
                                          const String& pskFallback,
                                          const char* eapIdentity,
                                          const char* eapUsername,
                                          const char* eapPassword) {
  const char* identity = (strlen(eapIdentity) > 0) ? eapIdentity : eapUsername;
  const char* username = (strlen(eapUsername) > 0) ? eapUsername : identity;
  const char* password = (strlen(eapPassword) > 0) ? eapPassword : pskFallback.c_str();

  if (ssid.length() == 0 || strlen(username) == 0 || strlen(password) == 0) {
    Serial.println("[net] enterprise WiFi config incomplete (ssid/identity/username/password)");
    return false;
  }

#if DT_HAS_WIFI_EAP_CLIENT
  esp_wifi_sta_enterprise_disable();
  esp_eap_client_set_identity((const uint8_t*)identity, strlen(identity));
  esp_eap_client_set_username((const uint8_t*)username, strlen(username));
  esp_eap_client_set_password((const uint8_t*)password, strlen(password));
  esp_wifi_sta_enterprise_enable();
  WiFi.begin(ssid.c_str());
  return true;
#elif DT_HAS_WIFI_WPA2_ENTERPRISE
  esp_wifi_sta_wpa2_ent_disable();
  esp_wifi_sta_wpa2_ent_set_identity((uint8_t*)identity, strlen(identity));
  esp_wifi_sta_wpa2_ent_set_username((uint8_t*)username, strlen(username));
  esp_wifi_sta_wpa2_ent_set_password((uint8_t*)password, strlen(password));
  esp_wpa2_config_t wpa2Config = WPA2_CONFIG_INIT_DEFAULT();
  esp_wifi_sta_wpa2_ent_enable(&wpa2Config);
  WiFi.begin(ssid.c_str());
  return true;
#else
  Serial.println("[net] enterprise WiFi requested but this ESP32 core lacks enterprise APIs");
  return false;
#endif
}

static void disableEnterpriseWifi() {
#if DT_HAS_WIFI_EAP_CLIENT
  esp_wifi_sta_enterprise_disable();
#elif DT_HAS_WIFI_WPA2_ENTERPRISE
  esp_wifi_sta_wpa2_ent_disable();
#endif
}

static bool connectWifiProfile(const String& ssid,
                               const String& pskPassword,
                               bool useEnterprise,
                               const char* eapIdentity,
                               const char* eapUsername,
                               const char* eapPassword) {
  if (ssid.length() == 0) return false;

  Serial.printf("[net] trying SSID '%s'%s",
                ssid.c_str(),
                useEnterprise ? " (enterprise)" : "");
  WiFi.disconnect(true, true);
  delay(120);

  bool beginStarted = false;
  if (useEnterprise) {
    beginStarted = beginEnterpriseWifiConnection(ssid, pskPassword, eapIdentity, eapUsername, eapPassword);
  } else {
    disableEnterpriseWifi();
    WiFi.begin(ssid.c_str(), pskPassword.c_str());
    beginStarted = true;
  }
  if (!beginStarted) {
    Serial.println(" failed to start");
    return false;
  }

  const unsigned long startMs = millis();
  while (WiFi.status() != WL_CONNECTED && (millis() - startMs) < WIFI_CONNECT_TIMEOUT_MS) {
    delay(500);
    Serial.print(".");
  }
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println(" connected");
    return true;
  }
  Serial.println(" timeout");
  return false;
}

void ensureWifiConnected() {
  static unsigned long lastWifiAttemptMs = 0;
  if (WiFi.status() == WL_CONNECTED) return;
  WiFi.mode(WIFI_STA);
  if (lastWifiAttemptMs != 0 && (millis() - lastWifiAttemptMs) < WIFI_RETRY_BACKOFF_MS) return;
  lastWifiAttemptMs = millis();

  if (connectWifiProfile(runtimeConfig.wifiSsid,
                         runtimeConfig.wifiPassword,
                         WIFI_USE_ENTERPRISE,
                         DEFAULT_WIFI_EAP_IDENTITY,
                         DEFAULT_WIFI_EAP_USERNAME,
                         DEFAULT_WIFI_EAP_PASSWORD)) {
    WiFi.setSleep(false);
    configTime(0, 0, "pool.ntp.org", "time.nist.gov");
    return;
  }

  if (strlen(FALLBACK_WIFI_SSID) > 0 && String(FALLBACK_WIFI_SSID) != runtimeConfig.wifiSsid) {
    Serial.println("[net] primary WiFi failed, trying fallback SSID");
    if (connectWifiProfile(String(FALLBACK_WIFI_SSID),
                           String(FALLBACK_WIFI_PASSWORD),
                           WIFI_FALLBACK_USE_ENTERPRISE,
                           FALLBACK_WIFI_EAP_IDENTITY,
                           FALLBACK_WIFI_EAP_USERNAME,
                           FALLBACK_WIFI_EAP_PASSWORD)) {
      WiFi.setSleep(false);
      configTime(0, 0, "pool.ntp.org", "time.nist.gov");
      return;
    }
  }

  Serial.println("[net] WiFi connection failed; retrying shortly");
}

void ensureOtaReady() {
  if (WiFi.status() != WL_CONNECTED) {
    otaStarted = false;
    return;
  }
  if (otaStarted) return;
  ArduinoOTA.setHostname(OTA_HOSTNAME);
  if (strlen(OTA_PASSWORD) > 0) {
    ArduinoOTA.setPassword(OTA_PASSWORD);
  }
  ArduinoOTA.onStart([]() {
    Serial.println("[ota] start");
  });
  ArduinoOTA.onEnd([]() {
    Serial.println("[ota] end");
  });
  ArduinoOTA.onError([](ota_error_t error) {
    Serial.printf("[ota] error=%u\n", error);
  });
  ArduinoOTA.begin();
  otaStarted = true;
  Serial.printf("[ota] ready (host=%s)\n", OTA_HOSTNAME);
}

void ensureMqttConnected() {
  if (mqttClient.connected()) return;

  while (!mqttClient.connected()) {
    Serial.print("[mqtt] connecting...");
    String clientId = "esp32-" + String(PLANT_IDENTIFIER) + "-" + String((uint32_t)ESP.getEfuseMac(), HEX);
    bool ok = mqttClient.connect(clientId.c_str(), runtimeConfig.mqttUsername.c_str(), runtimeConfig.mqttPassword.c_str());
    if (ok) {
      Serial.println(" connected");
      mqttClient.subscribe(topicCommand.c_str());
      mqttClient.subscribe(topicPrediction.c_str());
      mqttClient.subscribe(topicPredictionLong.c_str());
      mqttClient.subscribe(topicPredictionLongLstm.c_str());
    } else {
      Serial.print(" failed rc=");
      Serial.print(mqttClient.state());
      Serial.println(" retry in 2s");
      delay(2000);
    }
  }
}

// -----------------------------
// Telemetry + serial print
// -----------------------------
void publishTelemetry(float totalPowerW, float totalCurrentA) {
  // Keep this static to avoid large stack pressure in loopTask.
  static StaticJsonDocument<10240> doc;
  doc.clear();
  const unsigned long nowMs = millis();
  float runtimeSec = max(1.0f, millis() / 1000.0f);
  float energy10mWh = getWindowEnergyWh(10);
  float energy30mWh = getWindowEnergyWh(30);
  float energy1hWh = getWindowEnergyWh(60);
  float energy1dWh = getWindowEnergyWh(24 * 60);
  float energyGoalWh = MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH;
  float goalWindowWh = getWindowEnergyByDurationSec(ENERGY_GOAL_DURATION_SEC);
  float energyLeftGoalWh = (energyGoalWh > 0.0f) ? max(0.0f, energyGoalWh - goalWindowWh) : 0.0f;
  float energyUsedRatioGoal = (energyGoalWh > 0.0f) ? (goalWindowWh / energyGoalWh) : 0.0f;

  doc["timestamp"] = isoTimestampUtc();

  JsonObject system = doc.createNestedObject("system");
  system["timestamp"] = nowMs / 1000.0;
  system["plant_id"] = PLANT_IDENTIFIER;
  system["mode"] = "PROCESS";
  system["control_policy"] = controlPolicyToString(controlPolicy);
  system["predictor_mode"] = predictorMode;
  String activeSources = activePredictionSourcesSummary();
  system["ai_suggestion_source"] = "FUSED";
  system["ai_source_active"] = activeSources;
  system["ai_dynamic_process"] = (controlPolicy == POLICY_AI_PREFERRED || controlPolicy == POLICY_HYBRID);
  String aiDynamicReason;
  PredictionInputSource dynamicProcessSrc = PRED_SRC_LONG_RF;
  const bool aiDynamicAvailable = (selectDynamicProcessPredictionState(&dynamicProcessSrc, &aiDynamicReason) != nullptr);
  system["ai_dynamic_process_available"] = aiDynamicAvailable;
  system["ai_dynamic_process_reason"] = aiDynamicReason;
  system["ai_dynamic_process_source"] = aiDynamicAvailable ? predictionSourceToString(dynamicProcessSrc) : "NONE";
  system["predictor_source_mode"] = predictorSourceMode;
  system["predictor_source_model_ready"] = predictorSourceModelReady;
  system["predictor_source_age_ms"] = (predictorLastAppliedMs == 0) ? -1.0f : (float)(nowMs - predictorLastAppliedMs);
  JsonObject predictionSources = system.createNestedObject("prediction_sources");
  for (int idx = 0; idx < PREDICTION_SOURCE_COUNT; ++idx) {
    PredictionInputSource src = (idx == 1) ? PRED_SRC_LONG_RF : ((idx == 2) ? PRED_SRC_LONG_LSTM : PRED_SRC_SHORT);
    const PredictionChannelState& srcState = predictionStateForConst(src);
    JsonObject srcJson = predictionSources.createNestedObject(predictionSourceKey(src));
    srcJson["applied"] = srcState.applied;
    srcJson["fresh"] = predictionStateIsFresh(srcState, nowMs);
    srcJson["mode"] = srcState.sourceMode;
    srcJson["model_ready"] = srcState.sourceModelReady;
    srcJson["age_ms"] = (srcState.lastAppliedMs == 0) ? -1.0f : (float)(nowMs - srcState.lastAppliedMs);
    srcJson["peak_risk"] = srcState.peakRisk;
    srcJson["risk_level"] = srcState.riskLevel;
    srcJson["predicted_power_w"] = srcState.powerW;
    srcJson["horizon_sec"] = srcState.horizonSec;
    srcJson["suggestion_count"] = srcState.suggestionCount;
  }
  system["emergency_stop"] = emergencyStopActive;
  system["season"] = "N/A";
  system["tariff_state"] = tariffState;
  system["peak_event"] = peakEvent;
  system["supply_v"] = loadBusVoltageV;
  system["supply_v_raw"] = loadBusVoltageRawV;
  system["total_power_w"] = totalPowerW;
  system["total_current_a"] = totalCurrentA;
  system["MAX_TOTAL_POWER_W"] = MAX_TOTAL_POWER_W;
  system["VOLTAGE_INJECTION_MODE"] = voltageInjectionModeToString(voltageInjectionMode);
  system["VOLTAGE_INJECTION_MAGNITUDE_V"] = voltageInjectionMagnitudeV;
  system["VOLTAGE_INJECTION_ACTIVE"] = (voltageInjectionMode != VOLTAGE_INJECTION_NONE) && (voltageInjectionMagnitudeV > 0.0f);
  system["UNDERVOLTAGE_THRESHOLD_V"] = UNDERVOLTAGE_THRESHOLD_V;
  system["UNDERVOLTAGE_TRIP_DELAY_MS"] = UNDERVOLTAGE_TRIP_DELAY_MS;
  system["UNDERVOLTAGE_CLEAR_DELAY_MS"] = UNDERVOLTAGE_CLEAR_DELAY_MS;
  system["UNDERVOLTAGE_RESTORE_MARGIN_V"] = UNDERVOLTAGE_RESTORE_MARGIN_V;
  system["UNDERVOLTAGE_ACTIVE"] = undervoltageActive;
  system["OVERVOLTAGE_THRESHOLD_V"] = OVERVOLTAGE_THRESHOLD_V;
  system["OVERVOLTAGE_TRIP_DELAY_MS"] = OVERVOLTAGE_TRIP_DELAY_MS;
  system["OVERVOLTAGE_CLEAR_DELAY_MS"] = OVERVOLTAGE_CLEAR_DELAY_MS;
  system["OVERVOLTAGE_RESTORE_MARGIN_V"] = OVERVOLTAGE_RESTORE_MARGIN_V;
  system["OVERVOLTAGE_ACTIVE"] = overvoltageActive;
  system["CONTROL_COOLDOWN_SEC"] = CONTROL_COOLDOWN_SEC;
  system["SAMPLE_INTERVAL_SEC"] = SAMPLE_INTERVAL_SEC;
  system["MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH"] = energyGoalWh;
  system["ENERGY_GOAL_VALUE_WH"] = energyGoalWh;
  system["ENERGY_GOAL_DURATION_SEC"] = ENERGY_GOAL_DURATION_SEC;
  system["ENERGY_GOAL_DURATION_MIN"] = ENERGY_GOAL_DURATION_SEC / 60.0f;
  system["ENERGY_GOAL_RATE_W_PER_MIN"] = (ENERGY_GOAL_DURATION_SEC > 0.0f)
    ? (energyGoalWh / (ENERGY_GOAL_DURATION_SEC / 60.0f))
    : 0.0f;
  system["ENERGY_CONSUMED_1D_WH"] = energy1dWh;
  system["ENERGY_REMAINING_1D_WH"] = (energyGoalWh > 0.0f) ? max(0.0f, energyGoalWh - energy1dWh) : 0.0f;
  system["ENERGY_USED_RATIO_1D"] = (energyGoalWh > 0.0f) ? (energy1dWh / energyGoalWh) : 0.0f;
  system["ENERGY_CONSUMED_GOAL_WINDOW_WH"] = goalWindowWh;
  system["ENERGY_REMAINING_GOAL_WINDOW_WH"] = energyLeftGoalWh;
  system["ENERGY_USED_RATIO_GOAL_WINDOW"] = energyUsedRatioGoal;
  system["scene_enabled"] = sceneEnabled;
  system["scene_lock_active"] = sceneLockActive;
  system["scene_name"] = sceneName;
  system["scene_duration_sec"] = sceneDurationSec;
  JsonObject scene = system.createNestedObject("scene");
  scene["enabled"] = sceneEnabled;
  scene["lock_active"] = sceneLockActive;
  scene["name"] = sceneName;
  scene["duration_sec"] = sceneDurationSec;
  scene["loop"] = sceneLoop;
  scene["elapsed_sec"] = sceneEnabled ? ((millis() - sceneStartMs) / 1000.0f) : 0.0f;
  if (!isnan(sceneTargetTempC)) scene["target_temperature_c"] = sceneTargetTempC;
  if (!isnan(sceneTargetHumidityPct)) scene["target_humidity_pct"] = sceneTargetHumidityPct;
  JsonObject sceneOverrides = scene.createNestedObject("overrides");
  if (sceneOverrideModeEnabled) sceneOverrides["mode"] = plantModeToString(sceneOverrideMode);
  if (sceneOverrideSeasonEnabled) sceneOverrides["season"] = seasonModeToString(sceneOverrideSeason);
  if (sceneOverridePolicyEnabled) sceneOverrides["control_policy"] = controlPolicyToString(sceneOverridePolicy);
  if (sceneOverrideTariffEnabled) sceneOverrides["tariff_state"] = sceneOverrideTariffState;
  if (sceneOverridePeakEnabled) sceneOverrides["peak_event"] = sceneOverridePeakEvent;
  if (sceneOverrideTempEnabled || sceneOverrideHumidityEnabled) {
    JsonObject sceneOverrideEnv = sceneOverrides.createNestedObject("environment");
    if (sceneOverrideTempEnabled) sceneOverrideEnv["temperature_c"] = sceneOverrideTempC;
    if (sceneOverrideHumidityEnabled) sceneOverrideEnv["humidity_pct"] = sceneOverrideHumidityPct;
  }
  system["total_energy_wh"] = cumulativeEnergyWh;
  bool processGoalReached = (processCycleCount >= PRODUCTION_GOAL_CYCLES);
  JsonObject productionGoal = system.createNestedObject("production_goal");
  productionGoal["enabled"] = productionProcessEnabled;
  productionGoal["lock_active"] = processLockActive;
  productionGoal["state"] = productionStateToString(productionState);
  productionGoal["cycle_count"] = processCycleCount;
  productionGoal["goal_cycles"] = PRODUCTION_GOAL_CYCLES;
  productionGoal["goal_reached"] = processGoalReached;

  JsonObject env = doc.createNestedObject("environment");
  env["temperature_c"] = ambientTempC;
  env["humidity_pct"] = ambientHumidity;

  JsonObject process = doc.createNestedObject("process");
  process["enabled"] = productionProcessEnabled;
  process["lock_active"] = processLockActive;
  process["state"] = productionStateToString(productionState);
  process["cycle_count"] = processCycleCount;
  process["goal_cycles"] = PRODUCTION_GOAL_CYCLES;
  process["goal_reached"] = processGoalReached;
  process["elapsed_sec"] = productionProcessEnabled ? ((millis() - processStartMs) / 1000.0f) : 0.0f;
  process["tank_height_cm"] = TANK1_HEIGHT_CM;
  process["tank1_height_cm"] = TANK1_HEIGHT_CM;
  process["tank2_height_cm"] = TANK2_HEIGHT_CM;
  process["tank_low_level_pct"] = TANK1_LOW_LEVEL_PCT;
  process["tank_high_level_pct"] = TANK1_HIGH_LEVEL_PCT;
  process["tank1_low_level_pct"] = TANK1_LOW_LEVEL_PCT;
  process["tank2_low_level_pct"] = TANK2_LOW_LEVEL_PCT;
  process["tank1_high_level_pct"] = TANK1_HIGH_LEVEL_PCT;
  process["tank2_high_level_pct"] = TANK2_HIGH_LEVEL_PCT;
  process["tank1_level_valid"] = tank1LevelValid;
  process["tank2_level_valid"] = tank2LevelValid;
  // Flat aliases keep compatibility with frontends/parsers expecting non-nested fields.
  if (tank1LevelValid) process["tank1_level_pct"] = tank1LevelPct;
  else process["tank1_level_pct"] = nullptr;
  if (tank2LevelValid) process["tank2_level_pct"] = tank2LevelPct;
  else process["tank2_level_pct"] = nullptr;
  process["tank1_temp_c"] = tank1TempC;
  process["tank2_temp_c"] = tank2TempC;
  process["tank1_temp_target_c"] = tank1EffectiveTempTargetC();
  process["tank2_temp_target_c"] = tank2EffectiveTempTargetC();
  process["temp_target_step_per_cycle_c"] = PROCESS_TEMP_TARGET_STEP_PER_CYCLE_C;
  if (tank1LevelValid && !isnan(tank1DistanceCm)) process["tank1_ultrasonic_distance_cm"] = tank1DistanceCm;
  else process["tank1_ultrasonic_distance_cm"] = nullptr;
  if (tank2LevelValid && !isnan(tank2DistanceCm)) process["tank2_ultrasonic_distance_cm"] = tank2DistanceCm;
  else process["tank2_ultrasonic_distance_cm"] = nullptr;
  if (!isnan(tank1RawDistanceCm)) process["tank1_ultrasonic_raw_cm"] = tank1RawDistanceCm;
  else process["tank1_ultrasonic_raw_cm"] = nullptr;
  if (!isnan(tank2RawDistanceCm)) process["tank2_ultrasonic_raw_cm"] = tank2RawDistanceCm;
  else process["tank2_ultrasonic_raw_cm"] = nullptr;
  JsonObject tank1 = process.createNestedObject("tank1");
  tank1["level_valid"] = tank1LevelValid;
  if (tank1LevelValid) tank1["level_pct"] = tank1LevelPct;
  else tank1["level_pct"] = nullptr;
  tank1["temperature_c"] = tank1TempC;
  tank1["target_level_pct"] = TANK1_LEVEL_TARGET_PCT;
  tank1["target_temp_base_c"] = TANK1_TEMP_TARGET_C;
  tank1["target_temp_c"] = tank1EffectiveTempTargetC();
  tank1["height_cm"] = TANK1_HEIGHT_CM;
  tank1["low_level_pct"] = TANK1_LOW_LEVEL_PCT;
  tank1["high_level_pct"] = TANK1_HIGH_LEVEL_PCT;
  if (tank1LevelValid && !isnan(tank1DistanceCm)) tank1["ultrasonic_distance_cm"] = tank1DistanceCm;
  else tank1["ultrasonic_distance_cm"] = nullptr;
  if (!isnan(tank1RawDistanceCm)) tank1["ultrasonic_raw_cm"] = tank1RawDistanceCm;
  else tank1["ultrasonic_raw_cm"] = nullptr;
  JsonObject tank2 = process.createNestedObject("tank2");
  tank2["level_valid"] = tank2LevelValid;
  if (tank2LevelValid) tank2["level_pct"] = tank2LevelPct;
  else tank2["level_pct"] = nullptr;
  tank2["temperature_c"] = tank2TempC;
  tank2["target_level_pct"] = TANK2_LEVEL_TARGET_PCT;
  tank2["target_temp_base_c"] = TANK2_TEMP_TARGET_C;
  tank2["target_temp_c"] = tank2EffectiveTempTargetC();
  tank2["height_cm"] = TANK2_HEIGHT_CM;
  tank2["low_level_pct"] = TANK2_LOW_LEVEL_PCT;
  tank2["high_level_pct"] = TANK2_HIGH_LEVEL_PCT;
  if (tank2LevelValid && !isnan(tank2DistanceCm)) tank2["ultrasonic_distance_cm"] = tank2DistanceCm;
  else tank2["ultrasonic_distance_cm"] = nullptr;
  if (!isnan(tank2RawDistanceCm)) tank2["ultrasonic_raw_cm"] = tank2RawDistanceCm;
  else tank2["ultrasonic_raw_cm"] = nullptr;
  JsonObject processKpi = process.createNestedObject("kpi");
  processKpi["balance_delta_level_pct"] = fabs(tank1LevelPct - tank2LevelPct);

  JsonObject energy = doc.createNestedObject("energy");
  energy["total_energy_wh"] = cumulativeEnergyWh;
  energy["window_wh_10m"] = energy10mWh;
  energy["window_wh_30m"] = energy30mWh;
  energy["window_wh_1h"] = energy1hWh;
  energy["window_wh_1d"] = energy1dWh;
  JsonObject budget = energy.createNestedObject("budget");
  budget["goal_window_wh"] = energyGoalWh;
  budget["window_duration_sec"] = ENERGY_GOAL_DURATION_SEC;
  budget["window_duration_min"] = ENERGY_GOAL_DURATION_SEC / 60.0f;
  budget["consumed_window_wh"] = goalWindowWh;
  budget["remaining_window_wh"] = energyLeftGoalWh;
  budget["used_ratio_window"] = energyUsedRatioGoal;
  // legacy aliases
  budget["goal_1d_wh"] = energyGoalWh;
  budget["consumed_1d_wh"] = energy1dWh;
  budget["remaining_1d_wh"] = (energyGoalWh > 0.0f) ? max(0.0f, energyGoalWh - energy1dWh) : 0.0f;
  budget["used_ratio_1d"] = (energyGoalWh > 0.0f) ? (energy1dWh / energyGoalWh) : 0.0f;
  energy["avg_power_w_10m"] = (energy10mWh * 3600.0f) / min(runtimeSec, 10.0f * 60.0f);
  energy["avg_power_w_30m"] = (energy30mWh * 3600.0f) / min(runtimeSec, 30.0f * 60.0f);
  energy["avg_power_w_1h"] = (energy1hWh * 3600.0f) / min(runtimeSec, 60.0f * 60.0f);
  energy["avg_power_w_1d"] = (energy1dWh * 3600.0f) / min(runtimeSec, 24.0f * 60.0f * 60.0f);

  JsonObject evaluation = doc.createNestedObject("evaluation");
  JsonObject controlEval = evaluation.createNestedObject("control");
  controlEval["total_control_actions"] = totalControlActions;
  controlEval["shedding_event_count"] = sheddingEventCount;
  controlEval["curtail_event_count"] = curtailEventCount;
  controlEval["restore_event_count"] = restoreEventCount;
  controlEval["control_actions_per_min"] = (totalControlActions * 60.0f) / runtimeSec;

  JsonObject stabilityEval = evaluation.createNestedObject("stability");
  stabilityEval["max_overshoot_w"] = maxOvershootW;
  stabilityEval["overshoot_event_count"] = overloadEventCount;
  stabilityEval["overshoot_energy_ws"] = overshootEnergyWs;
  stabilityEval["load_toggle_count"] = loadToggleCount;
  stabilityEval["last_peak_settling_time_sec"] = lastPeakSettlingSec;

  JsonObject predEval = evaluation.createNestedObject("prediction");
  double nowEpochSec = (double)time(nullptr);
  float predictionAgeMs = 0.0f;
  if (predictorSourceTimestampSec > 0.0 && nowEpochSec > 100000.0) {
    predictionAgeMs = max(0.0f, (float)((nowEpochSec - predictorSourceTimestampSec) * 1000.0));
  }
  predEval["prediction_age_ms"] = predictionAgeMs;
  predEval["apply_delay_sec"] = predictionApplyDelaySec;
  predEval["horizon_sec"] = predictorHorizonSec;

  JsonObject voltages = doc.createNestedObject("voltages");
  voltages["load_bus_v"] = loadBusVoltageV;
  voltages["aux_v"] = auxVoltageV;

  JsonObject pred = doc.createNestedObject("prediction_context");
  pred["peak_risk"] = predictorPeakRisk;
  pred["risk_level"] = predictorRiskLevel;
  pred["predicted_power_w"] = predictorPowerW;
  pred["ai_suggestion_count"] = aiSuggestionCount;

  JsonObject loadsJson = doc.createNestedObject("loads");
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    LoadChannel* ld = loads[i];
    JsonObject j = loadsJson.createNestedObject(ld->name);
    j["on"] = ld->on;
    j["duty"] = ld->duty;
    j["duty_applied"] = ld->appliedDuty;
    j["power_w"] = ld->powerW;
    j["current_a"] = ld->currentA;
    j["voltage_v"] = ld->voltageV;
    j["priority"] = ld->priority;
    j["class"] = loadClassToString(ld->loadClass);
    j["override"] = ld->policyOverride;
    j["health"] = ld->health;
    j["fault_code"] = ld->faultCode;
    j["fault_active"] = ld->faultActive;
    j["fault_latched"] = ld->faultLatched;
    j["fault_limit_a"] = ld->faultLimitA;
    j["fault_trip_ms"] = ld->faultTripDelayMs;
    j["fault_clear_ms"] = ld->faultClearDelayMs;
    j["fault_restore_ms"] = ld->faultRestoreDelayMs;
    j["inject_current_a"] = ld->faultInjectA;
    if (!isnan(ld->tempC)) j["temperature_c"] = ld->tempC;
  }

  String payload;
  if (doc.overflowed()) {
    Serial.println("[mqtt] telemetry json overflow (increase doc size or reduce fields)");
  }
  serializeJson(doc, payload);
  bool ok = mqttClient.publish(topicTelemetry.c_str(), payload.c_str());
  if (!ok) {
    Serial.printf("[mqtt] telemetry publish failed (len=%u, state=%d)\n",
                  (unsigned int)payload.length(), mqttClient.state());
  }
}

void printTick(float totalPowerW, float totalCurrentA) {
  float runtimeSec = max(1.0f, millis() / 1000.0f);
  float energy10mWh = getWindowEnergyWh(10);
  float energy30mWh = getWindowEnergyWh(30);
  float energy1hWh = getWindowEnergyWh(60);
  float energy1dWh = getWindowEnergyWh(24 * 60);
  float goalWindowWh = getWindowEnergyByDurationSec(ENERGY_GOAL_DURATION_SEC);

  Serial.println();
  Serial.println("======================================================================");
  Serial.print("[time] ");
  Serial.println(isoTimestampUtc());
  Serial.println("Loads:");
  Serial.println("----------------------------------------------------------------------");
  Serial.println("NAME       P CLASS          ON   DUTY  VOLT(V) POWER(W) CURR(A)");
  Serial.println("----------------------------------------------------------------------");

  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    LoadChannel* ld = loads[i];
    Serial.printf(
      "%-9s %1u %-14s %-4s %5.2f  %7.2f %8.2f %7.2f\n",
      ld->name.c_str(),
      ld->priority,
      loadClassToString(ld->loadClass).c_str(),
      ld->on ? "on" : "off",
      ld->duty,
      ld->voltageV,
      ld->powerW,
      ld->currentA
    );
  }
  Serial.println("----------------------------------------------------------------------");

  Serial.println("System / Env / Process / Eval:");
  Serial.println("----------------------------------------------------------------------");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "SECTION", "FIELD", "VALUE", "UNIT");
  Serial.println("----------------------------------------------------------------------");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "policy", controlPolicyToString(controlPolicy).c_str(), "-");
  String activeSources = activePredictionSourcesSummary();
  PredictionInputSource dynamicProcessSrc = PRED_SRC_LONG_RF;
  String aiDynamicReason;
  const bool aiDynamicAvailable = (selectDynamicProcessPredictionState(&dynamicProcessSrc, &aiDynamicReason) != nullptr);
  String aiDynamicSource = aiDynamicAvailable ? predictionSourceToString(dynamicProcessSrc) : "NONE";
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "ai_sources", activeSources.c_str(), "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "ai_dyn_process",
                (controlPolicy == POLICY_AI_PREFERRED || controlPolicy == POLICY_HYBRID) ? "on" : "off", "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "ai_dyn_avail", aiDynamicAvailable ? "yes" : "no", "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "ai_dyn_source", aiDynamicSource.c_str(), "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "ai_dyn_reason", aiDynamicReason.c_str(), "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "tariff", tariffState.c_str(), "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "peak_event", peakEvent ? "on" : "off", "-");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "supply_v", loadBusVoltageV, "V");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "supply_v_raw", loadBusVoltageRawV, "V");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "voltage_inject", voltageInjectionModeToString(voltageInjectionMode), "-");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "inject_mag", voltageInjectionMagnitudeV, "V");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "total_power", totalPowerW, "W");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "total_current", totalCurrentA, "A");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "max_power", MAX_TOTAL_POWER_W, "W");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "uv_threshold", UNDERVOLTAGE_THRESHOLD_V, "V");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "system", "uv_trip_delay", UNDERVOLTAGE_TRIP_DELAY_MS, "ms");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "system", "uv_clear_delay", UNDERVOLTAGE_CLEAR_DELAY_MS, "ms");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "uv_margin", UNDERVOLTAGE_RESTORE_MARGIN_V, "V");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "uv_active", undervoltageActive ? "yes" : "no", "-");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "ov_threshold", OVERVOLTAGE_THRESHOLD_V, "V");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "system", "ov_trip_delay", OVERVOLTAGE_TRIP_DELAY_MS, "ms");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "system", "ov_clear_delay", OVERVOLTAGE_CLEAR_DELAY_MS, "ms");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "system", "ov_margin", OVERVOLTAGE_RESTORE_MARGIN_V, "V");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "system", "ov_active", overvoltageActive ? "yes" : "no", "-");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "env", "ambient_temp", ambientTempC, "C");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "env", "humidity", ambientHumidity, "%");
  Serial.printf("%-12s | %-18s | %-18d | %-18s\n", "env", "ai_suggestions", aiSuggestionCount, "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "process", "enabled", productionProcessEnabled ? "on" : "off", "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "process", "lock", processLockActive ? "on" : "off", "-");
  Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "process", "state", productionStateToString(productionState), "-");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "process", "cycles", (unsigned long)processCycleCount, "count");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "process", "goal_cycles", (unsigned long)PRODUCTION_GOAL_CYCLES, "count");
  if (tank1LevelValid) Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "process", "tank1_level", tank1LevelPct, "%");
  else Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "process", "tank1_level", "N/A", "%");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "process", "tank1_temp", tank1TempC, "C");
  if (tank1LevelValid && !isnan(tank1DistanceCm)) Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "process", "tank1_dist", tank1DistanceCm, "cm");
  else Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "process", "tank1_dist", "N/A", "cm");
  if (tank2LevelValid) Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "process", "tank2_level", tank2LevelPct, "%");
  else Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "process", "tank2_level", "N/A", "%");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "process", "tank2_temp", tank2TempC, "C");
  if (tank2LevelValid && !isnan(tank2DistanceCm)) Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "process", "tank2_dist", tank2DistanceCm, "cm");
  else Serial.printf("%-12s | %-18s | %-18s | %-18s\n", "process", "tank2_dist", "N/A", "cm");
  Serial.println("----------------------------------------------------------------------");
  Serial.printf(
    "[energy] total=%.4fWh 10m=%.4fWh(%.2fW) 30m=%.4fWh(%.2fW) 1h=%.4fWh(%.2fW) 1d=%.4fWh(%.2fW)\n",
    cumulativeEnergyWh,
    energy10mWh, (energy10mWh * 3600.0f) / min(runtimeSec, 10.0f * 60.0f),
    energy30mWh, (energy30mWh * 3600.0f) / min(runtimeSec, 30.0f * 60.0f),
    energy1hWh, (energy1hWh * 3600.0f) / min(runtimeSec, 60.0f * 60.0f),
    energy1dWh, (energy1dWh * 3600.0f) / min(runtimeSec, 24.0f * 60.0f * 60.0f)
  );
  Serial.printf(
    "         goal=%.2fWh window=%.1fmin consumed=%.2fWh left=%.2fWh used=%.2f%%\n",
    MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH,
    ENERGY_GOAL_DURATION_SEC / 60.0f,
    goalWindowWh,
    (MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH > 0.0f) ? max(0.0f, MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH - goalWindowWh) : 0.0f,
    (MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH > 0.0f) ? (goalWindowWh / MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH) * 100.0f : 0.0f
  );
  double nowEpochSec = (double)time(nullptr);
  float predictionAgeMs = 0.0f;
  if (predictorSourceTimestampSec > 0.0 && nowEpochSec > 100000.0) {
    predictionAgeMs = max(0.0f, (float)((nowEpochSec - predictorSourceTimestampSec) * 1000.0));
  }
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "eval", "actions", totalControlActions, "count");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "eval", "shed_events", sheddingEventCount, "count");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "eval", "curtail_events", curtailEventCount, "count");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "eval", "restore_events", restoreEventCount, "count");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "eval", "overload_events", overloadEventCount, "count");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "eval", "max_overshoot", maxOvershootW, "W");
  Serial.printf("%-12s | %-18s | %-18lu | %-18s\n", "eval", "load_toggles", loadToggleCount, "count");
  Serial.printf("%-12s | %-18s | %-18.1f | %-18s\n", "eval", "pred_age", predictionAgeMs, "ms");
  Serial.printf("%-12s | %-18s | %-18.1f | %-18s\n", "eval", "apply_delay", predictionApplyDelaySec, "s");
  Serial.printf("%-12s | %-18s | %-18.2f | %-18s\n", "eval", "settle_time", lastPeakSettlingSec, "s");
  Serial.println("======================================================================");
}

// -----------------------------
// Setup / loop
// -----------------------------
void initializeLoadMetadata() {
  motor1.loadClass = CLASS_CRITICAL;     motor1.priority = 1;
  motor2.loadClass = CLASS_CRITICAL;     motor2.priority = 1;
  heater1.loadClass = CLASS_ESSENTIAL;   heater1.priority = 2;
  heater2.loadClass = CLASS_ESSENTIAL;   heater2.priority = 2;
  light1.loadClass = CLASS_IMPORTANT;    light1.priority = 3;
  light2.loadClass = CLASS_SECONDARY;    light2.priority = 4;
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    loads[i]->currentGain = INA219_CURRENT_GAIN[i];
    loads[i]->currentOffsetA = 0.0f;
    loads[i]->filteredCurrentA = 0.0f;
    loads[i]->currentFilterInitialized = false;
  }
}

void initializeAnalogInputs() {
  analogReadResolution(12);
  if (!USE_INA219) {
    analogSetPinAttenuation(MOTOR1_PINS.currentPin, ADC_11db);
    analogSetPinAttenuation(MOTOR2_PINS.currentPin, ADC_11db);
    analogSetPinAttenuation(LIGHT1_PINS.currentPin, ADC_11db);
    analogSetPinAttenuation(LIGHT2_PINS.currentPin, ADC_11db);
    analogSetPinAttenuation(HEATER1_PINS.currentPin, ADC_11db);
    analogSetPinAttenuation(HEATER2_PINS.currentPin, ADC_11db);
  }
  analogSetPinAttenuation(VOLTAGE_SENSOR_1_PIN, ADC_11db);
  analogSetPinAttenuation(VOLTAGE_SENSOR_2_PIN, ADC_11db);
}

void initializeTemperatureSensors() {
  ds18b20.begin();
  ds18b20.setWaitForConversion(true);
  int count = ds18b20.getDeviceCount();
  if (count >= 1) tankTempSensor1Found = ds18b20.getAddress(tankTempAddress1, 0);
  if (count >= 2) tankTempSensor2Found = ds18b20.getAddress(tankTempAddress2, 1);
  if (tankTempSensor1Found) ds18b20.setResolution(tankTempAddress1, 12);
  if (tankTempSensor2Found) ds18b20.setResolution(tankTempAddress2, 12);
  Serial.printf("[sensors] DS18B20 found=%d tank1=%s tank2=%s\n",
                count,
                tankTempSensor1Found ? "yes" : "no",
                tankTempSensor2Found ? "yes" : "no");

  dht22.setup(DHT22_PIN, DHTesp::DHT22);
}

void diagnoseAnalogPin(const char* name, uint8_t pin) {
  int raw = analogRead(pin);
  float voltage = (raw / ADC_MAX_COUNTS) * ADC_REFERENCE_V;
  Serial.printf("[sensors] %s pin=%u raw=%d v=%.3f\n", name, pin, raw, voltage);
  if (raw <= 2 || raw >= 4093) {
    Serial.printf("[sensors] WARN %s looks saturated (check wiring/divider)\n", name);
  }
}

float readAnalogVoltageAvg(uint8_t pin, uint8_t samples) {
  uint32_t acc = 0;
  for (uint8_t i = 0; i < samples; ++i) {
    acc += analogRead(pin);
    delayMicroseconds(200);
  }
  float raw = acc / (float)samples;
  return (raw / ADC_MAX_COUNTS) * ADC_REFERENCE_V;
}

void calibrateCurrentZeroOffsets() {
  if (USE_INA219) {
    calibrateIna219CurrentOffsets();
    return;
  }
  Serial.println("[sensors] calibrating current zero offsets (no-load expected)...");
  bool prevOn[LOAD_COUNT];
  float prevDuty[LOAD_COUNT];
  unsigned long nowMs = millis();
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    prevOn[i] = loads[i]->on;
    prevDuty[i] = loads[i]->duty;
    loads[i]->on = false;
    loads[i]->duty = 0.0f;
    loads[i]->applyHardware(nowMs);
  }
  delay(CURRENT_ZERO_SETTLE_MS);
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    LoadChannel* ld = loads[i];
    float v = readAnalogVoltageAvg(ld->pins.currentPin, CURRENT_ZERO_CAL_SAMPLES);
    ld->currentZeroV = v;
    Serial.printf("[sensors] zero_%s pin=%u v=%.3f\n",
                  ld->name.c_str(),
                  ld->pins.currentPin,
                  v);
  }
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    loads[i]->on = prevOn[i];
    loads[i]->duty = prevDuty[i];
    loads[i]->applyHardware(nowMs);
  }
}

void diagnoseSensors() {
  Serial.println("[sensors] running diagnostics...");

  // DHT22 needs at least 2s between reads.
  delay(2100);
  TempAndHumidity dhtData = dht22.getTempAndHumidity();
  if (isnan(dhtData.temperature) || isnan(dhtData.humidity)) {
    Serial.println("[sensors] DHT22 read failed (check wiring + pullup)");
  } else {
    Serial.printf("[sensors] DHT22 temp=%.2fC hum=%.2f%%\n", dhtData.temperature, dhtData.humidity);
  }

  float d1 = NAN;
  float d2 = NAN;
  if (ULTRASONIC_UART_MODE) {
    float cm = NAN;
    if (readUltrasonicDistanceUartCm(UltrasonicSerial1, cm)) d1 = cm;
    cm = NAN;
    if (readUltrasonicDistanceUartCm(UltrasonicSerial2, cm)) d2 = cm;
  } else {
    unsigned long pulse1 = 0;
    unsigned long pulse2 = 0;
    d1 = readUltrasonicDistanceWithRetry(ULTRASONIC_TANK1_ECHO_PIN, ULTRASONIC_TANK1_TRIG_PIN, &pulse1);
    delay(ULTRASONIC_INTER_SENSOR_DELAY_MS);
    d2 = readUltrasonicDistanceWithRetry(ULTRASONIC_TANK2_ECHO_PIN, ULTRASONIC_TANK2_TRIG_PIN, &pulse2);
    if (isnan(d1)) {
      Serial.printf("[sensors] Ultrasonic tank1: no echo (pulse=%luus)\n", pulse1);
    }
    if (isnan(d2)) {
      Serial.printf("[sensors] Ultrasonic tank2: no echo (pulse=%luus)\n", pulse2);
    }
  }
  d1 = sanitizeUltrasonicDistanceCm(d1);
  d2 = sanitizeUltrasonicDistanceCm(d2);
  if (!isnan(d1)) Serial.printf("[sensors] Ultrasonic tank1 distance=%.2fcm\n", d1);
  if (!isnan(d2)) Serial.printf("[sensors] Ultrasonic tank2 distance=%.2fcm\n", d2);

  if (USE_INA219) {
    for (size_t i = 0; i < LOAD_COUNT; ++i) {
      uint8_t ch = INA219_CHANNELS[i];
      if (!ina219Present[i]) {
        Serial.printf("[sensors] INA219 %s ch=%u not detected\n", loads[i]->name.c_str(), ch);
        continue;
      }
      float rawA = readIna219CurrentRawA(ch);
      float calibA = applyIna219CurrentCalibration(loads[i], rawA);
      Serial.printf("[sensors] INA219 %s ch=%u raw=%.4fA offset=%.4fA current=%.4fA\n",
                    loads[i]->name.c_str(), ch, rawA, loads[i]->currentOffsetA, calibA);
    }
  } else {
    diagnoseAnalogPin("current_motor1", MOTOR1_PINS.currentPin);
    diagnoseAnalogPin("current_motor2", MOTOR2_PINS.currentPin);
    diagnoseAnalogPin("current_light1", LIGHT1_PINS.currentPin);
    diagnoseAnalogPin("current_light2", LIGHT2_PINS.currentPin);
    diagnoseAnalogPin("current_heater1", HEATER1_PINS.currentPin);
    diagnoseAnalogPin("current_heater2", HEATER2_PINS.currentPin);
  }
  diagnoseAnalogPin("voltage_bus", VOLTAGE_SENSOR_1_PIN);
  diagnoseAnalogPin("voltage_aux", VOLTAGE_SENSOR_2_PIN);
}

void updateTemperatureSensors() {
  ds18b20.requestTemperatures();
  if (tankTempSensor1Found) {
    float t1 = ds18b20.getTempC(tankTempAddress1);
    if (isValidDs18Sample(t1)) {
      float corrected = t1 + TANK1_TEMP_OFFSET_C;
      if (!tank1TempInitialized) {
        tank1TempC = corrected;
        tank1TempInitialized = true;
      } else {
        tank1TempC = smoothBounded(tank1TempC, corrected, TANK_TEMP_FILTER_ALPHA, TANK_TEMP_MAX_STEP_C);
      }
      tank1TempInvalidCount = 0;
    } else {
      if (tank1TempInvalidCount < TEMP_INVALID_MAX_HOLD) {
        tank1TempInvalidCount++;
      } else {
        tank1TempC = 0.0f;
        tank1TempInitialized = false;
      }
    }
  } else {
    tank1TempC = 0.0f;
    tank1TempInitialized = false;
    tank1TempInvalidCount = TEMP_INVALID_MAX_HOLD;
  }
  if (tankTempSensor2Found) {
    float t2 = ds18b20.getTempC(tankTempAddress2);
    if (isValidDs18Sample(t2)) {
      float corrected = t2 + TANK2_TEMP_OFFSET_C;
      if (!tank2TempInitialized) {
        tank2TempC = corrected;
        tank2TempInitialized = true;
      } else {
        tank2TempC = smoothBounded(tank2TempC, corrected, TANK_TEMP_FILTER_ALPHA, TANK_TEMP_MAX_STEP_C);
      }
      tank2TempInvalidCount = 0;
    } else {
      if (tank2TempInvalidCount < TEMP_INVALID_MAX_HOLD) {
        tank2TempInvalidCount++;
      } else {
        tank2TempC = 0.0f;
        tank2TempInitialized = false;
      }
    }
  } else {
    tank2TempC = 0.0f;
    tank2TempInitialized = false;
    tank2TempInvalidCount = TEMP_INVALID_MAX_HOLD;
  }

  unsigned long nowMs = millis();
  if (nowMs - lastDhtReadMs >= DHT_READ_INTERVAL_MS) {
    lastDhtReadMs = nowMs;
    TempAndHumidity dhtData = dht22.getTempAndHumidity();
    if (!isnan(dhtData.temperature)) {
      float correctedTemp = dhtData.temperature + DHT_TEMP_OFFSET_C + seasonTempOffsetC;
      if (!ambientTempInitialized) {
        ambientTempC = correctedTemp;
        ambientTempInitialized = true;
      } else {
        ambientTempC = smoothBounded(ambientTempC, correctedTemp, AMBIENT_TEMP_FILTER_ALPHA, AMBIENT_TEMP_MAX_STEP_C);
      }
    }
    if (!isnan(dhtData.humidity)) {
      float correctedHum = clampf(dhtData.humidity + DHT_HUM_OFFSET_PCT + seasonHumidityOffset, 0.0f, 100.0f);
      if (!ambientHumidityInitialized) {
        ambientHumidity = correctedHum;
        ambientHumidityInitialized = true;
      } else {
        ambientHumidity = smoothBounded(ambientHumidity, correctedHum, AMBIENT_HUM_FILTER_ALPHA, AMBIENT_HUM_MAX_STEP_PCT);
      }
      ambientHumidity = clampf(ambientHumidity, 0.0f, 100.0f);
    }
  }
}

void setupTopics() {
  topicTelemetry = String("dt/") + PLANT_IDENTIFIER + "/telemetry";
  topicCommand = String("dt/") + PLANT_IDENTIFIER + "/cmd";
  topicStatus = String("dt/") + PLANT_IDENTIFIER + "/status";
  topicPrediction = String("dt/") + PLANT_IDENTIFIER + "/prediction";
  topicPredictionLong = String("dt/") + PLANT_IDENTIFIER + "/prediction_long";
  topicPredictionLongLstm = String("dt/") + PLANT_IDENTIFIER + "/prediction_long_lstm";
  topicCommandAck = String("dt/") + PLANT_IDENTIFIER + "/cmd_ack";
}

void setup() {
  Serial.begin(115200);
  delay(300);

  loadRuntimeConfigFromNvs();
  runStartupConfigWizard();
  if (!runtimeConfigLooksUsable()) {
    Serial.println("[config] missing/placeholder WiFi or MQTT settings, reboot after providing valid values");
    while (true) delay(1000);
  }

  setupTopics();
  initializeLoadMetadata();
  initializeAnalogInputs();
  forceAllLoadsOff(false);
  if (USE_INA219) {
    Wire.begin(I2C_SDA_RUNTIME_PIN, I2C_SCL_RUNTIME_PIN);
    if (USE_TCA9548A) {
      if (!tcaPresent()) {
        Serial.println("[sensors] TCA9548A not detected");
        while (true) delay(1000);
      }
      Serial.println("[sensors] TCA9548A detected");
      for (size_t i = 0; i < LOAD_COUNT; ++i) {
        uint8_t ch = INA219_CHANNELS[i];
        ina219Present[i] = initIna219OnChannel(ch);
        Serial.printf("[sensors] INA219 %s ch=%u %s\n",
                      loads[i]->name.c_str(),
                      ch,
                      ina219Present[i] ? "ok" : "missing");
      }
    } else {
      const size_t directIndex = 4; // light1
      ina219Present[directIndex] = initIna219OnChannel(INA219_CHANNELS[directIndex]);
      Serial.printf("[sensors] INA219 direct (%s) %s\n",
                    loads[directIndex]->name.c_str(),
                    ina219Present[directIndex] ? "ok" : "missing");
      for (size_t i = 1; i < LOAD_COUNT; ++i) {
        if (i == directIndex) continue;
        ina219Present[i] = false;
      }
    }
  }
  if (ULTRASONIC_UART_MODE) {
    UltrasonicSerial1.begin(ULTRASONIC_UART_BAUD, SERIAL_8N1, ULTRASONIC1_UART_RX_PIN, ULTRASONIC_UART_TX_PIN);
    UltrasonicSerial2.begin(ULTRASONIC_UART_BAUD, SERIAL_8N1, ULTRASONIC2_UART_RX_PIN, ULTRASONIC_UART_TX_PIN);
    UltrasonicSerial1.flush();
    UltrasonicSerial2.flush();
    Serial.println("[sensors] Ultrasonic UART mode enabled (JSN-SR04T)");
  } else {
    pinMode(ULTRASONIC_TANK1_TRIG_PIN, OUTPUT);
    pinMode(ULTRASONIC_TANK2_TRIG_PIN, OUTPUT);
    digitalWrite(ULTRASONIC_TANK1_TRIG_PIN, ULTRASONIC_TRIG_ACTIVE_HIGH ? LOW : HIGH);
    digitalWrite(ULTRASONIC_TANK2_TRIG_PIN, ULTRASONIC_TRIG_ACTIVE_HIGH ? LOW : HIGH);
    pinMode(ULTRASONIC_TANK1_ECHO_PIN, INPUT);
    pinMode(ULTRASONIC_TANK2_ECHO_PIN, INPUT);
  }

  heater1.smoothDuty = false;
  heater2.smoothDuty = false;

  motor1.begin();
  motor2.begin();
  light1.begin();
  light2.begin();
  heater1.begin();
  heater2.begin();

  calibrateCurrentZeroOffsets();
  initializeTemperatureSensors();
  diagnoseSensors();

  ensureWifiConnected();

  if (USE_INSECURE_TLS) {
    secureClient.setInsecure();
  }

  mqttClient.setServer(runtimeConfig.mqttHost.c_str(), runtimeConfig.mqttPort);
  // Telemetry JSON is large; increase MQTT client packet buffer from default (~256B).
  mqttClient.setBufferSize(12288);
  mqttClient.setCallback(mqttCallback);
  ensureMqttConnected();

  currentMinuteBucketStartMs = millis();
  sceneStartMs = millis();

  // Publish online status
  StaticJsonDocument<512> status;
  status["timestamp"] = isoTimestampUtc();
  status["plant_id"] = PLANT_IDENTIFIER;
  status["status"] = "ONLINE";
  status["cmd_topic"] = topicCommand;
  status["telemetry_topic"] = topicTelemetry;
  status["prediction_topic"] = topicPrediction;
  status["prediction_long_topic"] = topicPredictionLong;
  status["prediction_long_lstm_topic"] = topicPredictionLongLstm;
  String statusPayload;
  serializeJson(status, statusPayload);
  mqttClient.publish(topicStatus.c_str(), statusPayload.c_str(), true);

  Serial.println("=== ESP32 Industrial Controller Started ===");
  Serial.printf("Broker: %s:%u\n", runtimeConfig.mqttHost.c_str(), runtimeConfig.mqttPort);
  Serial.printf("WiFi SSID: %s\n", runtimeConfig.wifiSsid.c_str());
  Serial.printf("WiFi Auth: %s\n", WIFI_USE_ENTERPRISE ? "WPA2-Enterprise" : "WPA/WPA2-PSK");
  Serial.printf("Topics: telemetry=%s cmd=%s prediction=%s prediction_long=%s prediction_long_lstm=%s\n",
                topicTelemetry.c_str(), topicCommand.c_str(), topicPrediction.c_str(),
                topicPredictionLong.c_str(), topicPredictionLongLstm.c_str());
}

void loop() {
  ensureWifiConnected();
  ensureOtaReady();
  ensureMqttConnected();
  mqttClient.loop();
  ArduinoOTA.handle();
  applyPendingPredictionIfDue();

  const unsigned long nowMs = millis();
  if (nowMs - lastSampleMs < sampleIntervalMs) return;
  unsigned long elapsedMs = (lastSampleMs == 0) ? sampleIntervalMs : (nowMs - lastSampleMs);
  lastSampleMs = nowMs;
  float elapsedSec = elapsedMs / 1000.0f;

  // Legacy season/mode profiles are deprecated.

  measureTankLevelsFromUltrasonic();
  updateTemperatureSensors();
  if (!emergencyStopActive && productionProcessEnabled) {
    applyProductionProcess(elapsedSec);
    applyAiProcessOverridesFromSuggestions();
  }
  if (productionProcessEnabled && predictorMode != "ONLINE" && processCycleCount >= PRODUCTION_GOAL_CYCLES) {
    productionProcessEnabled = false;
    processLockActive = false;
    productionState = PROCESS_IDLE;
  }
  // Keep legacy scene fields aligned with production process for dashboard compatibility.
  sceneEnabled = productionProcessEnabled;
  sceneLockActive = processLockActive;
  sceneName = "TANK_PRODUCTION_PROCESS";
  sceneDurationSec = 0.0f;
  if (sceneEnabled && !productionProcessEnabled) {
    if (!isnan(sceneTargetTempC)) ambientTempC = sceneTargetTempC;
    if (!isnan(sceneTargetHumidityPct)) ambientHumidity = clampf(sceneTargetHumidityPct, 0.0f, 100.0f);
  }
  loadBusVoltageRawV = readVoltageSensorV(VOLTAGE_SENSOR_1_PIN, VOLTAGE_SENSOR_1_RATIO);
  auxVoltageRawV = readVoltageSensorV(VOLTAGE_SENSOR_2_PIN, VOLTAGE_SENSOR_2_RATIO);
  loadBusVoltageV = applyInjectedVoltageScenario(loadBusVoltageRawV);
  auxVoltageV = auxVoltageRawV;
  updateUndervoltageProtectionState(nowMs);
  updateOvervoltageProtectionState(nowMs);

  if (emergencyStopActive) {
    productionProcessEnabled = false;
    processLockActive = false;
    productionState = PROCESS_IDLE;
    forceAllLoadsOff(true);
  } else {
    // Optional peak event behavior.
    if (peakEvent) {
      motor1.loadFactor = clampf(motor1.loadFactor + 0.15f, 0.5f, 1.6f);
      motor2.loadFactor = clampf(motor2.loadFactor + 0.15f, 0.5f, 1.6f);
      heater1.on = true;
      heater2.on = true;
    }
  }

  // Apply hardware outputs first, then read sensors
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    loads[i]->applyHardware(nowMs);
  }

  float totalPowerW = 0.0f;
  float totalCurrentA = 0.0f;
  for (size_t i = 0; i < LOAD_COUNT; ++i) {
    if (USE_INA219) {
      if (ina219Present[i]) {
        uint8_t ch = INA219_CHANNELS[i];
        float busV = readIna219BusVoltageV(ch);
        float rawCurrentA = readIna219CurrentRawA(ch);
        loads[i]->currentA = applyIna219CurrentCalibration(loads[i], rawCurrentA);
        loads[i]->voltageV = isnan(busV) ? loadBusVoltageV : busV;
        float powerW = fabs(readIna219PowerW(ch));
        if (isnan(powerW)) powerW = 0.0f;
        if (loads[i]->currentA <= INA219_CURRENT_DEADBAND_A) {
          powerW = 0.0f;
        }
        loads[i]->powerW = max(0.0f, powerW);
      } else {
        loads[i]->currentA = 0.0f;
        loads[i]->voltageV = 0.0f;
        loads[i]->powerW = 0.0f;
      }
    } else {
      loads[i]->updateElectricalTelemetry(loadBusVoltageV);
    }
    if (loads[i]->on && loads[i]->powerW > 0.05f) {
      loads[i]->lastActivePowerW = loads[i]->powerW;
      loads[i]->lastActiveDuty = max(0.01f, loads[i]->duty);
    }
    if (loads[i]->faultInjectA > 0.0f && loads[i]->on && !loads[i]->faultActive) {
      loads[i]->currentA = max(0.0f, loads[i]->currentA + loads[i]->faultInjectA);
      loads[i]->powerW = max(0.0f, loads[i]->currentA * loads[i]->voltageV);
    }
    updateOvercurrentFault(loads[i], nowMs);
    totalPowerW += loads[i]->powerW;
    totalCurrentA += loads[i]->currentA;
    if (lastOnState[i] != loads[i]->on) {
      loadToggleCount++;
      lastOnState[i] = loads[i]->on;
    }
  }

  updateEnergyWindows(totalPowerW, elapsedSec);

  float overshootW = max(0.0f, totalPowerW - MAX_TOTAL_POWER_W);
  maxOvershootW = max(maxOvershootW, overshootW);
  overshootEnergyWs += (overshootW * elapsedSec);
  if (overshootW > 0.0f && !wasOverloadedLastTick) overloadEventCount++;
  wasOverloadedLastTick = overshootW > 0.0f;

  if (predictorPeakRisk && !peakRiskActive) {
    peakRiskActive = true;
    peakRiskStartMs = nowMs;
  }
  if (peakRiskActive && !predictorPeakRisk && totalPowerW <= (MAX_TOTAL_POWER_W * RESTORE_POWER_RATIO)) {
    lastPeakSettlingSec = (nowMs - peakRiskStartMs) / 1000.0f;
    peakRiskActive = false;
  }

  if (!emergencyStopActive) {
    applyAutomaticControl(totalPowerW);
  }

  publishTelemetry(totalPowerW, totalCurrentA);
  printTick(totalPowerW, totalCurrentA);
}
