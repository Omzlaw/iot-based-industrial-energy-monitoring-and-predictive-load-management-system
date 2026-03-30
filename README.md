# Industrial Digital Twin Platform (ESP32 + MQTT + Predictors + Dashboards)

This repository contains a full digital-twin stack for an industrial process:

- ESP32 edge controller for real sensors/actuators
- MQTT message bus (HiveMQ Cloud-compatible)
- Short-term and long-term prediction services
- Telemetry logger + history API
- React dashboard and Node-RED dashboard
- Optional Python simulator and PLC-integration variant

The active plant topic namespace is:

- `dt/<plant_id>/...` (default plant id: `factory1`)

---

## 1) Repository Map

| Path | Purpose |
|---|---|
| `Arduino/esp32_digital_twin_controller/esp32_digital_twin_controller.ino` | Main ESP32 controller (telemetry + closed-loop control + process orchestration). |
| `Arduino/esp32_digital_twin_controller/secrets.h.example` | Compile-time WiFi/MQTT/OTA config template for ESP32. |
| `PythonPredictor/main.py` | Short-term predictor (EMA + trend). |
| `PythonPredictor/long_term_predictor.py` | Long-term RandomForest predictor (+ process-aware/MPC suggestions). |
| `PythonPredictor/lstm_long_term_predictor.py` | Long-term LSTM predictor (+ process-aware/MPC suggestions). |
| `PythonPredictor/telemetry_logger.py` | Aggregates telemetry/prediction streams into JSONL logs and event logs. |
| `PythonPredictor/history_api.py` | Flask API serving historical telemetry/prediction series from logger output. |
| `DashboardReact/` | Main React dashboard (live MQTT + history charts + controls). |
| `Dashboard/flows_node_red.json` | Node-RED dashboard flow (legacy/alternate UI). |
| `PythonSimulator/` | Optional simulator and command publisher CLI. |
| `PLCIntegration/` | PLC-oriented variant (gateway + predictor + dashboard scaffold). |

---

## 2) MQTT Topic Contract

### Plant topics (ESP32 or simulator)

- Telemetry: `dt/<plant_id>/telemetry`
- Status: `dt/<plant_id>/status`
- Command input: `dt/<plant_id>/cmd`
- Command ack: `dt/<plant_id>/cmd_ack`

### Predictor topics

- Short prediction output: `dt/<plant_id>/prediction`
- Short predictor config input: `dt/<plant_id>/predictor_cmd`
- Long RF prediction output: `dt/<plant_id>/prediction_long`
- Long RF predictor config input: `dt/<plant_id>/predictor_long_cmd`
- Long LSTM prediction output: `dt/<plant_id>/prediction_long_lstm`
- Long LSTM predictor config input: `dt/<plant_id>/predictor_long_lstm_cmd`

---

## 3) Quick Start (Recommended Stack)

### 3.1 Configure environment files

1) Predictor services:

```bash
cp PythonPredictor/.env.example PythonPredictor/.env
```

Set at minimum:

- `HIVEMQ_USER`
- `HIVEMQ_PASS`
- `MQTT_BROKER_HOST`
- `MQTT_BROKER_PORT`
- `PLANT_IDENTIFIER`

2) React dashboard:

```bash
cp DashboardReact/.env.example DashboardReact/.env
```

3) ESP32 firmware:

- Copy `Arduino/esp32_digital_twin_controller/secrets.h.example` to `Arduino/esp32_digital_twin_controller/secrets.h`
- Fill WiFi/MQTT/OTA values.

### 3.2 Install Python dependencies

The requirements are split by service:

- `PythonPredictor/requirements.main.txt` (short predictor)
- `PythonPredictor/requirements.main_long.txt` (long RF)
- `PythonPredictor/requirements.lstm.txt` (long LSTM)
- `PythonPredictor/requirements.logger.txt` (logger)
- `PythonPredictor/requirements.history_api.txt` (history API)

Install per service (recommended) to avoid dependency clashes (especially LSTM/torch).

### 3.3 Run services

From repo root:

```bash
python PythonPredictor/main.py
python PythonPredictor/long_term_predictor.py
python PythonPredictor/lstm_long_term_predictor.py
python PythonPredictor/telemetry_logger.py
python PythonPredictor/history_api.py
```

### 3.4 Run React dashboard

```bash
cd DashboardReact
npm install
npm run dev
```

### 3.5 Flash ESP32

- Open `Arduino/esp32_digital_twin_controller/esp32_digital_twin_controller.ino` in Arduino IDE.
- Install required libraries:
  - ArduinoJson
  - PubSubClient
  - OneWire
  - DallasTemperature
  - DHTesp
  - Adafruit INA219
- Select board/port and upload.

---

## 4) ESP32 Controller (Current Behavior)

File: `Arduino/esp32_digital_twin_controller/esp32_digital_twin_controller.ino`

### 4.1 Core runtime behavior

- Default sampling interval: `SAMPLE_INTERVAL_SEC = 1.0` (1 second)
- Publishes rich telemetry JSON (system/environment/process/energy/evaluation/loads)
- Subscribes to all prediction streams (`prediction`, `prediction_long`, `prediction_long_lstm`)
- Accepts commands on `dt/<plant_id>/cmd`, publishes ack on `dt/<plant_id>/cmd_ack`
- Supports OTA (`ArduinoOTA`)
- Supports runtime WiFi/MQTT config persistence via `Preferences` (NVS)
- Supports optional WPA2-Enterprise WiFi (when ESP core APIs are available)

### 4.2 Control and protection features

- Policies: `RULE_ONLY`, `HYBRID`, `AI_PREFERRED`, `NO_ENERGY_MANAGEMENT`
- Prediction source selection:
  - `SHORT`
  - `LONG_RF`
  - `LONG_LSTM`
- Safety protections:
  - undervoltage (threshold + delay + restore margin)
  - overvoltage (threshold + delay + restore margin)
  - per-load overcurrent fault detection/trip/clear/restore delays
- Load control:
  - per-load class/priority
  - duty curtailment and load shedding
  - guardrails for critical loads
  - manual per-load policy override
- Production process state machine:
  - `HEAT_TANK1 -> TRANSFER_1_TO_2 -> HEAT_TANK2 -> TRANSFER_2_TO_1`

### 4.3 Key command blocks accepted by ESP32

- `control_policy`
- `predictor_mode`
- `process`
- `scene` (legacy automation block; still accepted)
- `control`
- `timing`
- `environment`
- `device` with `set` and `meta`

Common examples:

```json
{"control_policy":"HYBRID"}
{"predictor_mode":"ONLINE"}
{"timing":{"SAMPLE_INTERVAL_SEC":1.0}}
{"control":{"MAX_TOTAL_POWER_W":120,"ENERGY_GOAL_WH":500,"ENERGY_GOAL_DURATION_MIN":1440}}
{"process":{"enabled":true,"restart":true,"goal_cycles":4,"tank1_level_target_pct":70,"tank1_temp_target_c":45,"tank2_level_target_pct":70,"tank2_temp_target_c":55}}
{"device":"motor1","set":{"duty":0.7}}
{"device":"heater1","set":{"on":false}}
{"device":"lighting1","meta":{"class":"IMPORTANT","priority":3}}
```

### 4.4 Pin map (current code)

| Function | GPIO |
|---|---:|
| Motor1 relay / PWM | 16 / 13 |
| Motor2 relay / PWM | 17 / 26 |
| Light1 relay / PWM | 5 / 27 |
| Light2 relay / PWM | 18 / 14 |
| Heater1 relay | 19 |
| Heater2 relay | 21 |
| DS18B20 OneWire bus | 23 |
| DHT22 data | 22 |
| Ultrasonic tank1 trig / echo | 25 / 15 |
| Ultrasonic tank2 trig / echo | 4 / 39 |
| Voltage sensor 1 / 2 ADC | 34 / 35 |
| I2C SDA / SCL (INA219 mux) | 32 / 33 |

Important wiring note:

- Current sensing is configured for INA219 + TCA9548A (`USE_INA219 = true`).
- Several "currentPin" definitions overlap with voltage/echo/I2C pins. This is safe only because analog hall current reads are bypassed when INA219 is enabled.
- If you disable INA219, rework pin assignments first.

### 4.5 Security note

By default the firmware sets:

- `USE_INSECURE_TLS = true`

Use broker CA certificates in production and disable insecure TLS.

---

## 5) Predictor Services

### 5.1 Short-term predictor

File: `PythonPredictor/main.py`

- Method: EMA + linear trend
- Input: telemetry stream
- Output topic: `dt/<plant_id>/prediction`
- Produces:
  - `predicted_power_w`
  - energy projection fields
  - risk level / peak risk
  - actionable suggestions (duty/off)

Runtime config topic: `dt/<plant_id>/predictor_cmd`

Supported config families include:

- power/energy limits, horizon, EMA/risk margins
- duty and suggestion guardrails
- publish/sample window settings
- optional reset (`RESET`)

### 5.2 Long-term RF predictor

File: `PythonPredictor/long_term_predictor.py`

- Model: `RandomForestRegressor`
- Output topic: `dt/<plant_id>/prediction_long`
- Config topic: `dt/<plant_id>/predictor_long_cmd`
- Persists model artifacts:
  - `PythonPredictor/models/<plant_id>_long_rf.pkl`
  - `PythonPredictor/models/<plant_id>_long_rf.meta.json`

### 5.3 Long-term LSTM predictor

File: `PythonPredictor/lstm_long_term_predictor.py`

- Model: PyTorch LSTM
- Output topic: `dt/<plant_id>/prediction_long_lstm`
- Config topic: `dt/<plant_id>/predictor_long_lstm_cmd`
- Persists model artifacts:
  - `PythonPredictor/models/<plant_id>_long_lstm.pt`
  - `PythonPredictor/models/<plant_id>_long_lstm_x_scaler.pkl`
  - `PythonPredictor/models/<plant_id>_long_lstm_y_scaler.pkl`
  - `PythonPredictor/models/<plant_id>_long_lstm.meta.json`

### 5.4 Predictor modes

Long predictors support:

- `TRAIN_ONLY` - update model only
- `EVAL_ONLY` - run inference only
- `ONLINE` - train + infer
- `OFFLINE` - connected but inactive (no train/infer)

---

## 6) UK Time-of-Use (ToU) Tariff Logic

### 6.1 In dashboard / History API

`DashboardReact/src/App.jsx` uses UK clock rules for live ToU display, but tariff configuration is now owned by the History API backend and versioned over time for historical-cost accuracy.

Active tariff behavior:

- timezone: `Europe/London`
- weekends + UK bank holidays -> `OFFPEAK`
- configurable windows and rates:
  - `offpeak_start_min`, `offpeak_end_min`
  - `peak_start_min`, `peak_end_min`
  - `offpeak_rate_per_kwh`, `midpeak_rate_per_kwh`, `peak_rate_per_kwh`
  - `demand_charge_per_kw`
  - `demand_interval_min`

`MIDPEAK` is the shoulder period outside offpeak/peak windows.

The dashboard can edit/save this tariff config through the History API, and historical cost uses the tariff version that was active at each timestamp.

### 6.2 In long predictors

Both long predictors also include UK ToU/calendar features for training/inference context:

- `UK_TARIFF_TIMEZONE` (default `Europe/London`)
- `UK_TARIFF_OFFPEAK_START_HOUR` (default `0`)
- `UK_TARIFF_OFFPEAK_END_HOUR` (default `7`)
- `UK_TARIFF_PEAK_START_HOUR` (default `16`)
- `UK_TARIFF_PEAK_END_HOUR` (default `20`)

If telemetry provides explicit `system.tariff_state`, predictors use it; otherwise they derive ToU state from UK time/calendar.

### 6.3 Units: power vs energy-rate vs price-rate

- Power is in `W` (instantaneous).
- Energy rate in `Wh/min` is `W / 60`.
- Price rate is `currency/min` and is:
  - `(Wh/min) * (currency/Wh)`
  - equivalently `(W / 60) * (tariff_per_kWh / 1000)`

So `Wh/min` is not "just power", but it is directly proportional to power.

---

## 7) Telemetry Logging and History API

### 7.1 Logger

File: `PythonPredictor/telemetry_logger.py`

Subscribes to:

- telemetry
- short prediction
- long RF prediction
- long LSTM prediction

Writes aggregated JSONL logs to `LOGGER_OUTPUT_DIR` (default `PythonPredictor/telemetry_logs`):

- `<plant_id>_telemetry_YYYY-MM-DD.jsonl`

Optional event log:

- `<plant_id>_events_YYYY-MM-DD.jsonl`

Key env vars:

- `LOGGER_OUTPUT_DIR`
- `LOGGER_AGGREGATION_SEC` (default `60`)
- `LOGGER_EVENTS_ENABLED` (`1`/`0`)

### 7.2 History API

File: `PythonPredictor/history_api.py`

Default bind:

- host `0.0.0.0`
- port `8090`

Endpoints:

- `GET /health`
- `GET /api/history`
- `GET /api/tariff-config`
- `PUT /api/tariff-config`
- `GET /api/chapter4-export`

`/api/history` accepts:

- `from`, `to` (ISO or epoch)
- `step_sec`
- `max_points`
- `power_min`, `power_max`
- `series` (comma-separated): `actual,pred_short,pred_long_rf,pred_long_lstm`

Example:

```bash
curl "http://localhost:8090/api/history?from=2026-03-01T00:00:00Z&to=2026-03-05T00:00:00Z&step_sec=60&series=actual,pred_short"
```

`/api/history` rows also include backend-computed tariff/cost fields:

- `actual_tariff_state`
- `actual_tariff_rate_per_kwh`
- `actual_incremental_energy_cost`
- `actual_accumulated_energy_cost`
- `actual_max_demand_kw`
- `actual_demand_charge_cost`
- `actual_total_cost`
- `actual_demand_interval_min`

`/api/tariff-config` returns or saves the active/versioned tariff schedule used for historical cost.

### 7.3 Chapter 4 CSV export

File: `PythonPredictor/export_chapter4_data.py`

Purpose:

- reads raw telemetry/prediction JSONL logs from `LOGGER_OUTPUT_DIR`
- exports figure/table-ready CSV files for Chapter 4
- computes model metrics (`MAE`, `RMSE`, lag) from logged actual vs predicted power

Example:

```bash
python3 PythonPredictor/export_chapter4_data.py \
  --from 2026-03-01T00:00:00Z \
  --to 2026-03-07T23:59:59Z
```

Output directory:

- `PythonPredictor/chapter4_exports/<plant>_<from>_<to>/`

Key outputs:

- `table_4_1_per_load_full_load.csv`
- `table_4_2_supply_voltage_behaviour.csv`
- `table_4_3_model_performance_metrics.csv`
- `table_16_policy_performance_comparison.csv`
- `figure_4_1_load_voltage_vs_load_current.csv`
- `figure_4_2_supply_voltage_vs_total_current.csv`
- `figure_4_3_temperature_vs_time_heating.csv`
- `figure_4_4_ambient_temp_humidity_vs_time.csv`
- `figure_4_5_ultrasonic_vs_level.csv`
- `figure_4_6_power_vs_time_per_load.csv`
- `figure_4_7_total_system_power.csv`
- `figure_4_8_power_profiles_by_load_type.csv`
- `figure_4_9_cumulative_energy_over_time.csv`
- `figure_4_10_ema_prediction_vs_actual.csv`
- `figure_4_11_rf_prediction_vs_actual.csv`
- `figure_4_12_lstm_prediction_vs_actual.csv`
- `figure_4_13_load_states_priority_curtailment.csv`
- `figure_4_14_total_demand_with_control_response.csv`
- `figure_4_15_voltage_drop_with_control_response.csv`
- `figure_4_16_per_load_current_protection_response.csv`
- `figure_4_18_power_profiles_under_policies.csv`
- `figure_4_19_peak_demand_comparison.csv`
- `figure_4_20_energy_consumption_comparison.csv`
- `figure_4_21_integrated_system_operation_timeline.csv`
- `accumulated_cost_over_time.csv`
- `voltage_protection_events.csv`
- `system_timeseries_master.csv`
- `tank_timeseries_master.csv`
- `load_timeseries_master.csv`
- `prediction_timeseries_master.csv`

Notes:

- prediction comparison files are aligned to prediction target time using each message `horizon_sec`
- detailed per-load, duty, voltage, current, tank, and protection-response plots come from raw logs, not from `/api/history`
- cost exports use the backend tariff config/defaults and include energy cost, demand charge, and total accumulated cost over time

---

## 8) React Dashboard

Path: `DashboardReact/`

### 8.1 Features

- Live MQTT mode + demo mode toggle
- System/process/load controls
- Predictor mode control with fused short/RF/LSTM display
- Backend-saved ToU tariff editor (rates, windows, demand charge, demand interval)
- Cost and energy KPIs
- Live accumulated-cost chart
- Historical charting via History API
- Historical accumulated-cost chart
- Chapter 4 CSV zip export from History API
- Command ack feed

### 8.2 MQTT topics used

Subscribes:

- `telemetry`, `prediction`, `prediction_long`, `prediction_long_lstm`, `cmd_ack`

Publishes:

- `cmd`
- predictor config topics (`predictor_long_cmd`, `predictor_long_lstm_cmd`) when changing predictor settings
- short predictor config can be sent manually to `predictor_cmd` when needed

### 8.3 Key `.env` values

- `VITE_PLANT_ID`
- `VITE_BASE_PATH`
- `VITE_MQTT_URL`
- `VITE_MQTT_USERNAME`
- `VITE_MQTT_PASSWORD`
- `VITE_HISTORY_API_URL`
- `VITE_CURRENCY_CODE`
- `VITE_TARIFF_OFFPEAK_PER_KWH`
- `VITE_TARIFF_MIDPEAK_PER_KWH`
- `VITE_TARIFF_PEAK_PER_KWH`
- `VITE_TARIFF_DEMAND_CHARGE_PER_KW`
- `VITE_TARIFF_DEMAND_INTERVAL_MIN`
- UK ToU switch/window vars listed in section 6

---

## 9) Optional Components

### 9.1 Python simulator

Path: `PythonSimulator/`

- `main.py`: synthetic plant telemetry generator (legacy/optional when no hardware)
- `publish_cmd.py`: helper CLI for command publishing

### 9.2 Node-RED dashboard

Path: `Dashboard/`

- Import `Dashboard/flows_node_red.json`
- See `Dashboard/README.md` for setup

### 9.3 PLC integration variant

Path: `PLCIntegration/`

- Contains gateway/predictor/dashboard skeleton for PLC-authoritative deployments.
- See `PLCIntegration/README.md` and subfolder READMEs.

---

## 10) Deployment Scripts

Path: `scripts/deploy/`

- `react_vps_deploy.sh`
- `predictor_vps_deploy.sh`

See `scripts/deploy/README.md` for GitHub Actions and required secrets.

---

## 11) Troubleshooting Notes

- If dashboard is live but no history charts:
  - verify `VITE_HISTORY_API_URL`
  - verify `history_api.py` running and CORS settings
  - verify logger output path has JSONL files
- If long predictor is set to `EVAL_ONLY`/`ONLINE` but no outputs:
  - check `model_ready` in prediction payloads
  - run with enough samples or switch temporarily to `TRAIN_ONLY`
- If ESP32 voltage/current readings look wrong:
  - verify INA219 mux channel mapping and calibration
  - verify you are not relying on overlapping ADC pins with INA219 disabled
- If process start is rejected:
  - send required `process` config first (goal cycles, tank targets, predictor mode)

---

## 12) Security and Secrets

- Do not commit real `.env` files or `secrets.h`.
- Rotate MQTT credentials if they were exposed.
- Replace insecure TLS mode on ESP32 before production deployment.
