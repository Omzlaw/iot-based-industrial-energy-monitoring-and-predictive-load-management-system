#!/usr/bin/env python3
import os
import ssl
import json
import time
import numpy as np
import paho.mqtt.client as mqtt
from collections import deque
from datetime import datetime, timezone

try:
  from dotenv import load_dotenv
  load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:
  pass

DEFAULT_MQTT_BROKER_HOST = "2d7641080ba74f7a91be1b429b265933.s1.eu.hivemq.cloud"
DEFAULT_MQTT_BROKER_PORT = 8883

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", DEFAULT_MQTT_BROKER_HOST).strip() or DEFAULT_MQTT_BROKER_HOST
try:
  MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", str(DEFAULT_MQTT_BROKER_PORT)))
except (TypeError, ValueError):
  MQTT_BROKER_PORT = DEFAULT_MQTT_BROKER_PORT

MQTT_USERNAME = os.getenv("HIVEMQ_USER")
MQTT_PASSWORD = os.getenv("HIVEMQ_PASS")

PLANT_IDENTIFIER = os.getenv("PLANT_IDENTIFIER", "factory1").strip() or "factory1"
TOPIC_TELEMETRY_NAME  = f"dt/{PLANT_IDENTIFIER}/telemetry"
TOPIC_PREDICTION_NAME = f"dt/{PLANT_IDENTIFIER}/prediction"
TOPIC_PREDICTOR_CMD_NAME = f"dt/{PLANT_IDENTIFIER}/predictor_cmd"

# --- Short-term prediction settings ---
MAX_TOTAL_POWER_W = 200.0               # threshold for overload/risk logic
MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = 5000.0  # 24h energy cap; <=0 disables cap logic
FORECAST_HORIZON_SEC = 120                # forecast horizon (seconds)
TELEMETRY_SAMPLE_SEC = 5.0                 # expected telemetry sampling period
HISTORY_WINDOW_SEC = 180                 # history window used for trend
HISTORY_WINDOW_SAMPLES = int(HISTORY_WINDOW_SEC / TELEMETRY_SAMPLE_SEC)
EMA_ALPHA = 0.15                     # EMA smoothing factor
RISK_MARGIN_RATIO = 0.95               # risk margin relative to MAX_TOTAL_POWER_W

# --- Suggestion controls ---
MIN_DEVICE_DUTY = 0.3                   # min duty for non-critical loads
DUTY_STEP_SIZE = 0.1                  # duty decrement per suggestion
CRITICAL_MIN_DEVICE_DUTY = 0.4          # min duty for critical loads
MAX_SUGGESTIONS = 3               # max suggestions per publish
MOTOR_HIGH_TEMP_C = 70.0
MOTOR_DUTY_STEP_SIZE = 0.1
LOAD_OVERCURRENT_LIMIT_A = 2.0
FAULT_RISK_MARGIN = 0.90

# --- Publish controls ---
PUBLISH_INTERVAL_SEC = 5            # throttle publish frequency
# Keep this at 1 so short EMA starts publishing as soon as telemetry arrives.
# The fallback path in forecast_trend() already handles limited history safely.
MIN_SAMPLE_COUNT = 1                  # minimum samples before publishing
SMOOTH_WINDOW_SEC = 10           # rolling mean window length
SMOOTH_WINDOW_SAMPLES = int(SMOOTH_WINDOW_SEC / TELEMETRY_SAMPLE_SEC)

# Rolling buffers
power_ema_buffer = deque(maxlen=HISTORY_WINDOW_SAMPLES)
time_buffer = deque(maxlen=HISTORY_WINDOW_SAMPLES)
ema_power_w = None
last_publish_time = 0.0

try:
  import certifi
  _CA_CERTS = certifi.where()
except Exception:
  _CA_CERTS = None

def forecast_trend(t_arr, p_arr, horizon_sec):
  if len(p_arr) < 10:
    return float(p_arr[-1]) if len(p_arr) else 0.0

  t0 = t_arr[0]
  x = np.array(t_arr) - t0
  y = np.array(p_arr)
  A = np.vstack([x, np.ones(len(x))]).T
  m, c = np.linalg.lstsq(A, y, rcond=None)[0]
  x_future = (t_arr[-1] - t0) + horizon_sec
  return float(m * x_future + c)

def apply_config(cfg: dict):
  """Update runtime configuration via MQTT command."""
  global MAX_TOTAL_POWER_W, MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH, FORECAST_HORIZON_SEC, TELEMETRY_SAMPLE_SEC, HISTORY_WINDOW_SEC, HISTORY_WINDOW_SAMPLES, EMA_ALPHA, RISK_MARGIN_RATIO
  global MIN_DEVICE_DUTY, DUTY_STEP_SIZE, CRITICAL_MIN_DEVICE_DUTY, MAX_SUGGESTIONS
  global MOTOR_HIGH_TEMP_C, MOTOR_DUTY_STEP_SIZE, power_ema_buffer, time_buffer, ema_power_w
  global PUBLISH_INTERVAL_SEC, MIN_SAMPLE_COUNT, SMOOTH_WINDOW_SEC, SMOOTH_WINDOW_SAMPLES, last_publish_time

  if "MAX_TOTAL_POWER_W" in cfg or "POWER_MAX_W" in cfg or "P_MAX" in cfg:
    MAX_TOTAL_POWER_W = float(cfg.get("MAX_TOTAL_POWER_W", cfg.get("POWER_MAX_W", cfg.get("P_MAX"))))
  if "MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH" in cfg or "MAX_ENERGY_CONSUMPTION_FOR_DAY_WH" in cfg or "DAILY_ENERGY_CAP_WH" in cfg:
    MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = max(
      0.0,
      float(cfg.get("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH", cfg.get("MAX_ENERGY_CONSUMPTION_FOR_DAY_WH", cfg.get("DAILY_ENERGY_CAP_WH"))))
    )
  if "FORECAST_HORIZON_SEC" in cfg or "HORIZON_SEC" in cfg:
    FORECAST_HORIZON_SEC = int(cfg.get("FORECAST_HORIZON_SEC", cfg.get("HORIZON_SEC")))
  if "EMA_ALPHA" in cfg or "ALPHA" in cfg:
    EMA_ALPHA = max(0.01, min(1.0, float(cfg.get("EMA_ALPHA", cfg.get("ALPHA")))))
  if "RISK_MARGIN_RATIO" in cfg or "RISK_MARGIN" in cfg:
    RISK_MARGIN_RATIO = max(0.1, min(1.0, float(cfg.get("RISK_MARGIN_RATIO", cfg.get("RISK_MARGIN")))))
  if "MIN_DEVICE_DUTY" in cfg or "MIN_DUTY" in cfg or "CONTROL_MIN_DUTY" in cfg:
    MIN_DEVICE_DUTY = max(0.0, min(1.0, float(cfg.get("MIN_DEVICE_DUTY", cfg.get("MIN_DUTY", cfg.get("CONTROL_MIN_DUTY"))))))
  if "DUTY_STEP_SIZE" in cfg or "DUTY_STEP" in cfg or "CONTROL_DUTY_STEP" in cfg:
    DUTY_STEP_SIZE = max(0.01, min(1.0, float(cfg.get("DUTY_STEP_SIZE", cfg.get("DUTY_STEP", cfg.get("CONTROL_DUTY_STEP"))))))
  if "CRITICAL_MIN_DEVICE_DUTY" in cfg or "CRITICAL_MIN_DUTY" in cfg or "CONTROL_CRITICAL_MIN_DUTY" in cfg:
    CRITICAL_MIN_DEVICE_DUTY = max(0.0, min(1.0, float(cfg.get("CRITICAL_MIN_DEVICE_DUTY", cfg.get("CRITICAL_MIN_DUTY", cfg.get("CONTROL_CRITICAL_MIN_DUTY"))))))
  if "MAX_SUGGESTIONS" in cfg or "SUGGESTION_MAX" in cfg:
    MAX_SUGGESTIONS = max(1, int(cfg.get("MAX_SUGGESTIONS", cfg.get("SUGGESTION_MAX"))))
  if "MOTOR_HIGH_TEMP_C" in cfg or "MOTOR_TEMP_HIGH_THRESHOLD_C" in cfg or "MOTOR_TEMP_HIGH_C" in cfg:
    MOTOR_HIGH_TEMP_C = float(cfg.get("MOTOR_HIGH_TEMP_C", cfg.get("MOTOR_TEMP_HIGH_THRESHOLD_C", cfg.get("MOTOR_TEMP_HIGH_C"))))
  if "MOTOR_DUTY_STEP_SIZE" in cfg or "MOTOR_TEMP_DUTY_STEP" in cfg:
    MOTOR_DUTY_STEP_SIZE = max(0.01, min(1.0, float(cfg.get("MOTOR_DUTY_STEP_SIZE", cfg.get("MOTOR_TEMP_DUTY_STEP")))))
  if "HISTORY_WINDOW_SEC" in cfg or "WINDOW_SEC" in cfg or "TELEMETRY_SAMPLE_SEC" in cfg or "SAMPLE_SEC" in cfg:
    if "TELEMETRY_SAMPLE_SEC" in cfg or "SAMPLE_SEC" in cfg:
      TELEMETRY_SAMPLE_SEC = float(cfg.get("TELEMETRY_SAMPLE_SEC", cfg.get("SAMPLE_SEC")))
    if "HISTORY_WINDOW_SEC" in cfg or "WINDOW_SEC" in cfg:
      HISTORY_WINDOW_SEC = int(cfg.get("HISTORY_WINDOW_SEC", cfg.get("WINDOW_SEC")))
    HISTORY_WINDOW_SAMPLES = int(HISTORY_WINDOW_SEC / TELEMETRY_SAMPLE_SEC)
    new_power = deque(maxlen=HISTORY_WINDOW_SAMPLES)
    new_time = deque(maxlen=HISTORY_WINDOW_SAMPLES)
    new_power.extend(power_ema_buffer)
    new_time.extend(time_buffer)
    power_ema_buffer = new_power
    time_buffer = new_time
    SMOOTH_WINDOW_SAMPLES = max(1, int(SMOOTH_WINDOW_SEC / TELEMETRY_SAMPLE_SEC))
  if "PUBLISH_INTERVAL_SEC" in cfg or "PUBLISH_EVERY_SEC" in cfg:
    PUBLISH_INTERVAL_SEC = max(1, int(cfg.get("PUBLISH_INTERVAL_SEC", cfg.get("PUBLISH_EVERY_SEC"))))
  if "MIN_SAMPLE_COUNT" in cfg or "MIN_POINTS" in cfg:
    MIN_SAMPLE_COUNT = max(5, int(cfg.get("MIN_SAMPLE_COUNT", cfg.get("MIN_POINTS"))))
  if "SMOOTH_WINDOW_SEC" in cfg:
    SMOOTH_WINDOW_SEC = max(1, int(cfg["SMOOTH_WINDOW_SEC"]))
    SMOOTH_WINDOW_SAMPLES = max(1, int(SMOOTH_WINDOW_SEC / TELEMETRY_SAMPLE_SEC))
  if cfg.get("RESET", False):
    ema_power_w = None
    power_ema_buffer.clear()
    time_buffer.clear()
    last_publish_time = 0.0

  print("[predictor] config updated:")
  print(f"  - power_max_w={MAX_TOTAL_POWER_W} energy_cap_1d_wh={MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH} horizon_sec={FORECAST_HORIZON_SEC} sample_sec={TELEMETRY_SAMPLE_SEC} window_sec={HISTORY_WINDOW_SEC}")
  print(f"  - alpha={EMA_ALPHA} risk_margin={RISK_MARGIN_RATIO} min_points={MIN_SAMPLE_COUNT} publish_every_sec={PUBLISH_INTERVAL_SEC}")
  print(f"  - min_duty={MIN_DEVICE_DUTY} duty_step={DUTY_STEP_SIZE} critical_min_duty={CRITICAL_MIN_DEVICE_DUTY} suggestion_max={MAX_SUGGESTIONS}")
  print(f"  - motor_temp_high_c={MOTOR_HIGH_TEMP_C} motor_temp_duty_step={MOTOR_DUTY_STEP_SIZE}")
  print(f"  - smooth_window_sec={SMOOTH_WINDOW_SEC}")

def _parse_telemetry_payload(data: dict):
  system = data.get("system", {})
  energy = data.get("energy", {})
  loads = data.get("loads", {})
  process = data.get("process", {}) or {}
  if "total_power_w" not in system or "timestamp" not in system:
    return None
  total_power_w = float(system["total_power_w"])
  sample_ts_raw = system.get("timestamp", data.get("timestamp"))
  try:
    sample_ts = float(sample_ts_raw)
  except Exception:
    text = str(sample_ts_raw or "").strip()
    if not text:
      return None
    if text.endswith("Z"):
      text = text[:-1] + "+00:00"
    try:
      dt = datetime.fromisoformat(text)
      if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
      sample_ts = dt.timestamp()
    except Exception:
      return None
  current_total_energy_wh = float(energy.get("total_energy_wh", 0.0) or 0.0)
  budget = energy.get("budget", {}) or {}
  current_energy_goal_wh = float(
    budget.get("consumed_window_wh", (energy.get("window_wh", {}) or {}).get("last_1d", current_total_energy_wh)) or current_total_energy_wh
  )
  env_block = data.get("environment", {}) or {}
  env_temp = float(env_block.get("temperature_c", 0.0))
  env_humidity = float(env_block.get("humidity_pct", 0.0))
  return {
    "system": system,
    "energy": energy,
    "loads": loads,
    "process": process,
    "total_power_w": total_power_w,
    "sample_ts": sample_ts,
    "current_total_energy_wh": current_total_energy_wh,
    "current_energy_goal_wh": current_energy_goal_wh,
    "env_temp": env_temp,
    "env_humidity": env_humidity,
    "has_environment": bool(env_block),
  }

def _update_power_buffers(total_power_w: float, sample_ts: float) -> float:
  global ema_power_w
  ema_power_w = total_power_w if ema_power_w is None else (EMA_ALPHA * total_power_w + (1 - EMA_ALPHA) * ema_power_w)
  power_ema_buffer.append(ema_power_w)
  time_buffer.append(sample_ts)
  return ema_power_w

def _ready_to_publish() -> bool:
  if len(power_ema_buffer) < MIN_SAMPLE_COUNT:
    return False
  if (time.time() - last_publish_time) < PUBLISH_INTERVAL_SEC:
    return False
  return True

def _smoothed_power() -> float:
  if ema_power_w is None:
    return 0.0
  if len(power_ema_buffer) >= SMOOTH_WINDOW_SAMPLES:
    window = list(power_ema_buffer)[-SMOOTH_WINDOW_SAMPLES:]
    return float(np.mean(window))
  return ema_power_w

def _forecast_power(horizon_sec: float) -> float:
  baseline_power_w = float(power_ema_buffer[-1]) if len(power_ema_buffer) else 0.0
  raw_forecast_w = forecast_trend(list(time_buffer), list(power_ema_buffer), horizon_sec)
  # For low-power benches, linear extrapolation can swing negative and collapse the
  # short-horizon forecast to zero even while the recent EMA is clearly non-zero.
  if raw_forecast_w <= 0.0 and baseline_power_w > 0.0:
    return baseline_power_w
  return max(0.0, raw_forecast_w)

def _compute_energy_projection(
  predicted_power_w: float,
  current_total_energy_wh: float,
  current_energy_goal_wh: float,
  energy_cap_goal_wh: float,
  horizon_sec: float,
) -> dict:
  predicted_incremental_energy_wh = max(0.0, predicted_power_w) * (horizon_sec / 3600.0)
  predicted_total_energy_wh = current_total_energy_wh + predicted_incremental_energy_wh
  predicted_window_energy_wh = predicted_incremental_energy_wh
  projected_energy_goal_wh = predicted_window_energy_wh
  current_energy_goal_ratio = (current_energy_goal_wh / energy_cap_goal_wh) if energy_cap_goal_wh > 0.0 else 0.0
  projected_energy_goal_ratio = (projected_energy_goal_wh / energy_cap_goal_wh) if energy_cap_goal_wh > 0.0 else 0.0
  energy_window_pressure = (energy_cap_goal_wh > 0.0) and (current_energy_goal_wh >= energy_cap_goal_wh * RISK_MARGIN_RATIO)
  energy_budget_risk = (energy_cap_goal_wh > 0.0) and (projected_energy_goal_wh >= energy_cap_goal_wh * RISK_MARGIN_RATIO)
  return {
    "predicted_incremental_energy_wh": predicted_incremental_energy_wh,
    "predicted_window_energy_wh": predicted_window_energy_wh,
    "predicted_total_energy_wh": predicted_total_energy_wh,
    "projected_energy_goal_wh": projected_energy_goal_wh,
    "current_energy_goal_ratio": current_energy_goal_ratio,
    "projected_energy_goal_ratio": projected_energy_goal_ratio,
    "energy_window_pressure": energy_window_pressure,
    "energy_budget_risk": energy_budget_risk,
  }

def _determine_risk_levels(
  predicted_power_w: float,
  ema_power_value: float,
  energy_window_pressure: bool,
  energy_budget_risk: bool,
  energy_cap_goal_wh: float,
  current_energy_goal_wh: float,
  projected_energy_goal_wh: float,
  max_total_power_w: float,
) -> dict:
  power_limit_w = max(0.0, float(max_total_power_w or 0.0))
  power_peak_risk = (predicted_power_w > (power_limit_w * RISK_MARGIN_RATIO)) or (ema_power_value > power_limit_w)
  peak_risk = power_peak_risk or energy_window_pressure or energy_budget_risk
  if energy_cap_goal_wh > 0.0 and (
    projected_energy_goal_wh > energy_cap_goal_wh * 1.01 or current_energy_goal_wh > energy_cap_goal_wh * 1.01
  ):
    risk_level = "HIGH"
  elif predicted_power_w > power_limit_w * 1.05:
    risk_level = "HIGH"
  elif predicted_power_w > power_limit_w * RISK_MARGIN_RATIO or energy_window_pressure or energy_budget_risk:
    risk_level = "MEDIUM"
  else:
    risk_level = "LOW"
  return {
    "peak_risk": peak_risk,
    "power_peak_risk": power_peak_risk,
    "energy_window_pressure": energy_window_pressure,
    "risk_level": risk_level,
  }

def _resolve_process_protection(process: dict, base_reason: str) -> tuple[str, set]:
  reason_hint = base_reason
  protected_devices = set()
  if bool(process.get("enabled", False)):
    process_state = str(process.get("state", "IDLE")).upper()
    if process_state == "HEAT_TANK1":
      reason_hint = "PRODUCTION_PROCESS"
      protected_devices.update({"heater1"})
    elif process_state == "TRANSFER_1_TO_2":
      reason_hint = "PRODUCTION_PROCESS"
      protected_devices.update({"motor1"})
    elif process_state == "HEAT_TANK2":
      reason_hint = "PRODUCTION_PROCESS"
      protected_devices.update({"heater2"})
    elif process_state == "TRANSFER_2_TO_1":
      reason_hint = "PRODUCTION_PROCESS"
      protected_devices.update({"motor2"})
  return reason_hint, protected_devices

def _compute_production_pressure(process: dict) -> dict:
  if not bool(process.get("enabled", False)):
    return {"enabled": False, "pressure": 0.0, "components": {}}
  tank1 = process.get("tank1") or {}
  tank2 = process.get("tank2") or {}

  def _deficit(current, target):
    try:
      cur = float(current)
      tgt = float(target)
    except Exception:
      return None
    if tgt <= 0.0:
      return None
    return max(0.0, (tgt - cur) / tgt)

  components = {}
  deficits = []
  d = _deficit(tank1.get("level_pct"), tank1.get("target_level_pct"))
  if d is not None:
    components["tank1_level"] = round(d, 3)
    deficits.append(d)
  d = _deficit(tank2.get("level_pct"), tank2.get("target_level_pct"))
  if d is not None:
    components["tank2_level"] = round(d, 3)
    deficits.append(d)
  d = _deficit(tank1.get("temperature_c"), tank1.get("target_temp_c"))
  if d is not None:
    components["tank1_temp"] = round(d, 3)
    deficits.append(d)
  d = _deficit(tank2.get("temperature_c"), tank2.get("target_temp_c"))
  if d is not None:
    components["tank2_temp"] = round(d, 3)
    deficits.append(d)

  pressure = sum(deficits) / len(deficits) if deficits else 0.0
  return {"enabled": True, "pressure": round(pressure, 3), "components": components}

def _detect_fault_risk(loads: dict) -> dict:
  risk_loads = []
  fault_predicted = False
  for name, ld in (loads or {}).items():
    try:
      current_a = float(ld.get("current_a") or 0.0)
    except Exception:
      current_a = 0.0
    if current_a <= 0.0:
      continue
    if current_a >= LOAD_OVERCURRENT_LIMIT_A:
      fault_predicted = True
      risk_loads.append({
        "device": name,
        "current_a": round(current_a, 3),
        "level": "FAULT",
      })
    elif current_a >= LOAD_OVERCURRENT_LIMIT_A * FAULT_RISK_MARGIN:
      risk_loads.append({
        "device": name,
        "current_a": round(current_a, 3),
        "level": "RISK",
      })
  return {
    "fault_predicted": fault_predicted,
    "fault_risk_loads": risk_loads,
    "fault_limit_a": LOAD_OVERCURRENT_LIMIT_A,
  }

def _build_prediction_payload(
  system: dict,
  process: dict,
  loads: dict,
  env_temp: float,
  env_humidity: float,
  has_environment: bool,
  ema_power_value: float,
  smoothed_power_w: float,
  predicted_power_w: float,
  horizon_sec: float,
  energy_projection: dict,
  current_energy_goal_wh: float,
  energy_cap_goal_wh: float,
  energy_goal_duration_sec: float,
  max_total_power_w: float,
  risk_level: str,
  peak_risk: bool,
  energy_budget_risk: bool,
) -> dict:
  fault_risk = _detect_fault_risk(loads)
  production_pressure = _compute_production_pressure(process)
  return {
    "schema_version": "1.1",
    "producer": "short_term_predictor",
    "timestamp": time.time(),
    "plant_id": PLANT_IDENTIFIER,
    "predictor_mode": "ONLINE",
    "model_ready": True,
    "method": "EMA + LinearTrend",
    "ema_power_w": round(ema_power_value, 2),
    "smoothed_power_w": round(smoothed_power_w, 2),
    "predicted_power_w": round(predicted_power_w, 2),
    "predicted_energy_wh": round(energy_projection["predicted_incremental_energy_wh"], 4),
    "predicted_incremental_energy_wh": round(energy_projection["predicted_incremental_energy_wh"], 4),
    "predicted_total_energy_wh": round(energy_projection["predicted_total_energy_wh"], 4),
    "predicted_window_energy_wh": round(energy_projection["predicted_window_energy_wh"], 4),
    "horizon_sec": horizon_sec,
    "MAX_TOTAL_POWER_W": max_total_power_w,
    "MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH": energy_cap_goal_wh,
    "ENERGY_GOAL_VALUE_WH": energy_cap_goal_wh,
    "ENERGY_GOAL_DURATION_SEC": energy_goal_duration_sec,
    "risk_margin": RISK_MARGIN_RATIO,
    "risk_level": risk_level,
    "peak_risk": bool(peak_risk),
    "energy_budget_risk": bool(energy_budget_risk),
    "energy_window_pressure": bool(energy_projection["energy_window_pressure"]),
    "current_energy_1d_wh": round(current_energy_goal_wh, 4),
    "projected_energy_1d_wh": round(energy_projection["projected_energy_goal_wh"], 4),
    "current_energy_window_wh": round(current_energy_goal_wh, 4),
    "projected_energy_window_wh": round(energy_projection["projected_energy_goal_wh"], 4),
    "current_energy_goal_wh": round(current_energy_goal_wh, 4),
    "projected_energy_goal_wh": round(energy_projection["projected_energy_goal_wh"], 4),
    "current_energy_goal_ratio": round(energy_projection["current_energy_goal_ratio"], 6),
    "projected_energy_goal_ratio": round(energy_projection["projected_energy_goal_ratio"], 6),
    "sample_count": len(power_ema_buffer),
    "warming_up": len(power_ema_buffer) < 10,
    "env_temp_c": round(env_temp, 2),
    "env_humidity_pct": round(env_humidity, 2),
    "motor_temp_high_threshold_c": MOTOR_HIGH_TEMP_C,
    "system_context": {
      "mode": system.get("mode"),
      "controller_mode": system.get("controller_mode"),
      "control_policy": system.get("control_policy"),
      "season": system.get("season"),
      "tariff_state": system.get("tariff_state"),
      "peak_event": system.get("peak_event"),
      "supply_v": system.get("supply_v"),
      "total_current_a": system.get("total_current_a"),
      "process_enabled": bool(process.get("enabled", False)),
      "process_state": process.get("state"),
      "process_cycle_count": process.get("cycle_count", 0),
      "process_goal_cycles": process.get("goal_cycles", 0),
      "tank1_level_pct": ((process.get("tank1") or {}).get("level_pct")),
      "tank2_level_pct": ((process.get("tank2") or {}).get("level_pct")),
    },
    "input_completeness": {
      "has_system": bool(system),
      "has_environment": bool(has_environment),
      "has_loads": bool(loads),
      "load_count": len(loads),
    },
    "fault_risk": fault_risk,
    "production_pressure": production_pressure,
  }

def _apply_motor_temp_suggestions(loads: dict, suggestions: list, sugg_reason: str) -> tuple[list, str]:
  for motor in ("motor1", "motor2"):
    ld = loads.get(motor)
    if not ld:
      continue
    motor_temp = float(ld.get("temperature_c") or 0.0)
    if motor_temp >= MOTOR_HIGH_TEMP_C and ld.get("duty", 1.0) > CRITICAL_MIN_DEVICE_DUTY:
      new_duty = max(CRITICAL_MIN_DEVICE_DUTY, ld.get("duty", 1.0) - MOTOR_DUTY_STEP_SIZE)
      suggestions.insert(0, {
        "device": motor,
        "set": {"duty": round(new_duty, 2)},
        "action": "motor_temp",
        "reason": "MOTOR_TEMP",
        "class": str(ld.get("class", "CRITICAL")).upper(),
        "priority": int(ld.get("priority", 99)),
        "current_temp_c": round(motor_temp, 2),
        "threshold_temp_c": MOTOR_HIGH_TEMP_C,
        "current_duty": round(float(ld.get("duty", 1.0)), 2),
        "proposed_duty": round(new_duty, 2),
        "guardrails": {
          "critical_can_turn_off": False,
          "critical_min_duty": CRITICAL_MIN_DEVICE_DUTY,
          "noncritical_min_duty": MIN_DEVICE_DUTY,
        },
      })
      if len(suggestions) > MAX_SUGGESTIONS:
        suggestions = suggestions[:MAX_SUGGESTIONS]
      sugg_reason = "MOTOR_TEMP"
      break
  return suggestions, sugg_reason

def _apply_fault_suggestions(loads: dict, suggestions: list, sugg_reason: str) -> tuple[list, str]:
  fault_info = _detect_fault_risk(loads)
  if not fault_info["fault_risk_loads"]:
    return suggestions, sugg_reason

  existing = {(s.get("device"), tuple(sorted((s.get("set") or {}).items()))) for s in suggestions}
  for item in fault_info["fault_risk_loads"]:
    if item["level"] != "FAULT":
      continue
    device = item["device"]
    key = (device, tuple(sorted(({"on": False}).items())))
    if key in existing:
      continue
    suggestions.insert(0, {
      "device": device,
      "set": {"on": False},
      "reason": "OVERCURRENT_FAULT",
    })
    existing.add(key)
    if len(suggestions) >= MAX_SUGGESTIONS:
      break

  if suggestions and sugg_reason == "NONE":
    sugg_reason = "FAULT_RISK"
  return suggestions, sugg_reason

def _class_weight(cls: str) -> int:
  """Map class -> weight for load-shedding order."""
  cls_u = str(cls).upper()
  if cls_u == "NON_ESSENTIAL":
    return 4
  if cls_u == "SECONDARY":
    return 3
  if cls_u == "IMPORTANT":
    return 2
  if cls_u == "ESSENTIAL":
    return 1
  if cls_u == "CRITICAL":
    return 0
  return 2

def suggest_actions(
  loads: dict,
  total_power_w: float,
  predicted_power_w: float,
  risk_level: str,
  max_total_power_w: float,
  reason_hint: str = "NONE",
  protected_devices=None,
  production_pressure: float = 0.0,
  process_enabled: bool = False,
):
  """Return a list of suggested device actions and the reason."""
  power_limit_w = max(0.0, float(max_total_power_w or 0.0))
  if total_power_w > power_limit_w:
    reason = "OVERLOAD"
  elif reason_hint and reason_hint != "NONE":
    reason = reason_hint
  elif risk_level in ("MEDIUM", "HIGH"):
    reason = "PEAK_RISK"
  else:
    return [], "NONE"

  candidates = []
  protected = protected_devices or set()
  for name, ld in loads.items():
    if name in protected:
      continue
    if not ld.get("on", True):
      continue
    prio = int(ld.get("priority", 99))
    weight = _class_weight(ld.get("class", "IMPORTANT"))
    if process_enabled:
      cls = str(ld.get("class", "IMPORTANT")).upper()
      if production_pressure >= 0.8 and cls not in ("NON_ESSENTIAL",):
        continue
      if production_pressure >= 0.5 and cls not in ("NON_ESSENTIAL", "SECONDARY"):
        continue
    candidates.append((weight, prio, name, ld))

  candidates.sort(reverse=True)
  if not candidates:
    return [], reason

  suggestions = []
  for _, _, name, ld in candidates:
    if len(suggestions) >= MAX_SUGGESTIONS:
      break
    duty = float(ld.get("duty", 1.0))
    cls = str(ld.get("class", "IMPORTANT")).upper()
    prio = int(ld.get("priority", 99))
    min_duty = CRITICAL_MIN_DEVICE_DUTY if cls == "CRITICAL" else MIN_DEVICE_DUTY
    if duty > min_duty:
      new_duty = max(min_duty, duty - DUTY_STEP_SIZE)
      suggestions.append({
        "device": name,
        "set": {"duty": round(new_duty, 2)},
        "action": "curtail",
        "reason": reason,
        "class": cls,
        "priority": prio,
        "current_duty": round(duty, 2),
        "proposed_duty": round(new_duty, 2),
        "guardrails": {
          "critical_can_turn_off": False,
          "critical_min_duty": CRITICAL_MIN_DEVICE_DUTY,
          "noncritical_min_duty": MIN_DEVICE_DUTY,
        },
      })
    elif cls != "CRITICAL":
      suggestions.append({
        "device": name,
        "set": {"on": False},
        "action": "shed",
        "reason": reason,
        "class": cls,
        "priority": prio,
        "guardrails": {
          "critical_can_turn_off": False,
          "critical_min_duty": CRITICAL_MIN_DEVICE_DUTY,
          "noncritical_min_duty": MIN_DEVICE_DUTY,
        },
      })

  return suggestions, reason

def on_connect(client, userdata, flags, reason_code, properties):
  """Subscribe to telemetry + config command topics."""
  if reason_code == 0:
    print(f"[predictor] connected: telemetry={TOPIC_TELEMETRY_NAME} cmd={TOPIC_PREDICTOR_CMD_NAME}")
    client.subscribe(TOPIC_TELEMETRY_NAME)
    client.subscribe(TOPIC_PREDICTOR_CMD_NAME)
  else:
    print(f"[predictor] connect failed: reason_code={reason_code}")

def on_message(client, userdata, msg):
  """Handle incoming telemetry and publish short-term prediction."""
  global ema_power_w, last_publish_time

  try:
    data = json.loads(msg.payload.decode("utf-8"))
  except Exception as exc:
    print(f"[predictor] invalid json on topic={msg.topic}: {exc}")
    return
  if msg.topic == TOPIC_PREDICTOR_CMD_NAME:
    if isinstance(data, dict):
      apply_config(data)
    else:
      print(f"[predictor] ignored non-object config payload on topic={msg.topic}")
    return
  if not isinstance(data, dict):
    print(f"[predictor] ignored non-object telemetry payload on topic={msg.topic}")
    return
  parsed = _parse_telemetry_payload(data)
  if parsed is None:
    return

  system = parsed["system"]
  loads = parsed["loads"]
  process = parsed["process"]
  total_power_w = parsed["total_power_w"]
  sample_ts = parsed["sample_ts"]
  current_total_energy_wh = parsed["current_total_energy_wh"]
  current_energy_goal_wh = parsed["current_energy_goal_wh"]
  env_temp = parsed["env_temp"]
  env_humidity = parsed["env_humidity"]
  has_environment = parsed["has_environment"]
  budget = parsed["energy"].get("budget", {}) if "energy" in parsed else {}

  ema_power_value = _update_power_buffers(total_power_w, sample_ts)
  if not _ready_to_publish():
    return

  smoothed_power_w = _smoothed_power()
  predicted_power_w = _forecast_power(FORECAST_HORIZON_SEC)
  max_total_power_w = float(system.get("MAX_TOTAL_POWER_W", MAX_TOTAL_POWER_W) or MAX_TOTAL_POWER_W)
  energy_cap_goal_wh = float(
    system.get("ENERGY_GOAL_VALUE_WH", system.get("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH", MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH)) or 0.0
  )
  energy_goal_duration_sec = float(system.get("ENERGY_GOAL_DURATION_SEC", budget.get("window_duration_sec", 24 * 3600.0)) or 24 * 3600.0)

  projected = _compute_energy_projection(
    predicted_power_w,
    current_total_energy_wh,
    current_energy_goal_wh,
    energy_cap_goal_wh,
    FORECAST_HORIZON_SEC,
  )
  risk = _determine_risk_levels(
    predicted_power_w,
    ema_power_value,
    projected["energy_window_pressure"],
    projected["energy_budget_risk"],
    energy_cap_goal_wh,
    current_energy_goal_wh,
    projected["projected_energy_goal_wh"],
    max_total_power_w,
  )
  if projected["energy_budget_risk"] or projected["energy_window_pressure"]:
    reason_hint = "ENERGY_GOAL_WINDOW"
  elif risk["power_peak_risk"]:
    reason_hint = "PEAK_RISK"
  else:
    reason_hint = "NONE"
  reason_hint, protected_devices = _resolve_process_protection(process, reason_hint)
  production_pressure = _compute_production_pressure(process)

  payload = _build_prediction_payload(
    system,
    process,
    loads,
    env_temp,
    env_humidity,
    has_environment,
    ema_power_value,
    smoothed_power_w,
    predicted_power_w,
    FORECAST_HORIZON_SEC,
    projected,
    current_energy_goal_wh,
    energy_cap_goal_wh,
    energy_goal_duration_sec,
    max_total_power_w,
    risk["risk_level"],
    risk["peak_risk"],
    projected["energy_budget_risk"],
  )

  suggestions, sugg_reason = suggest_actions(
    loads,
    total_power_w,
    predicted_power_w,
    risk["risk_level"],
    max_total_power_w,
    reason_hint,
    protected_devices=protected_devices,
    production_pressure=production_pressure["pressure"],
    process_enabled=production_pressure["enabled"],
  )
  suggestions, sugg_reason = _apply_motor_temp_suggestions(loads, suggestions, sugg_reason)
  suggestions, sugg_reason = _apply_fault_suggestions(loads, suggestions, sugg_reason)
  payload["suggestions"] = suggestions
  payload["suggestion_reason"] = sugg_reason

  publish_info = client.publish(TOPIC_PREDICTION_NAME, json.dumps(payload), qos=0)
  if publish_info.rc != mqtt.MQTT_ERR_SUCCESS:
    print(f"[predictor] publish failed rc={publish_info.rc} topic={TOPIC_PREDICTION_NAME}")
    return
  last_publish_time = time.time()
  print(f"[predictor] ts={sample_ts} ema={round(ema_power_value,2)} smooth={round(smoothed_power_w,2)} "
        f"pred={round(predicted_power_w,2)} level={risk['risk_level']} samples={len(power_ema_buffer)}")
  if suggestions:
    print(f"[predictor] suggestions ({sugg_reason}): {suggestions}")

def main():
  print("[predictor] starting short-term predictor...")
  client = mqtt.Client(
    client_id=f"predictor-{PLANT_IDENTIFIER}-{int(time.time())}",
    protocol=mqtt.MQTTv5,
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
  )

  if MQTT_USERNAME and MQTT_PASSWORD:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
  else:
    print("[predictor] warning: HIVEMQ_USER/HIVEMQ_PASS not set")

  if _CA_CERTS:
    client.tls_set(
      ca_certs=_CA_CERTS,
      cert_reqs=ssl.CERT_REQUIRED,
      tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )
  else:
    client.tls_set_context(ssl.create_default_context())

  client.on_connect = on_connect
  client.on_message = on_message

  client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
  client.loop_forever()

if __name__ == "__main__":
  main()
