#!/usr/bin/env python3
import os
import ssl
import json
import sys
import time
import pickle
import threading
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import paho.mqtt.client as mqtt
from sklearn.ensemble import RandomForestRegressor

from model_feature_schema import LONG_TERM_FEATURE_NAMES

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:
    pass


def _prepare_numpy_pickle_compat() -> None:
    """Allow NumPy 1.x runtimes to read pickles created against NumPy 2.x internals."""
    core_module = getattr(np, "core", None)
    if core_module is None:
        return
    sys.modules.setdefault("numpy._core", core_module)
    for suffix in ("multiarray", "umath", "numerictypes", "_multiarray_umath"):
        module = getattr(core_module, suffix, None)
        if module is not None:
            sys.modules.setdefault(f"numpy._core.{suffix}", module)

DEFAULT_MQTT_BROKER_HOST = "2d7641080ba74f7a91be1b429b265933.s1.eu.hivemq.cloud"
DEFAULT_MQTT_BROKER_PORT = 8883

MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", DEFAULT_MQTT_BROKER_HOST).strip() or DEFAULT_MQTT_BROKER_HOST
try:
    MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", str(DEFAULT_MQTT_BROKER_PORT)))
except (TypeError, ValueError):
    MQTT_BROKER_PORT = DEFAULT_MQTT_BROKER_PORT

MQTT_USERNAME = os.getenv("HIVEMQ_USER")
MQTT_PASSWORD = os.getenv("HIVEMQ_PASS")


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)).strip() or str(default)))
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)).strip() or str(default))
    except Exception:
        return float(default)


def _parse_optional_epoch_seconds(value) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        raw = float(text)
        if raw > 1e12:
            raw /= 1000.0
        return raw if raw >= 946684800.0 else None
    except Exception:
        pass
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.astimezone()
        return dt.timestamp()
    except Exception:
        return None


PLANT_IDENTIFIER = os.getenv("PLANT_IDENTIFIER", "factory1").strip() or "factory1"
TOPIC_TELEMETRY_NAME = f"dt/{PLANT_IDENTIFIER}/telemetry"
TOPIC_PREDICTION_LONG_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long"
TOPIC_PREDICTION_LONG_STATUS_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long_status"
TOPIC_PREDICTOR_LONG_CMD_NAME = f"dt/{PLANT_IDENTIFIER}/predictor_long_cmd"

# Long-term prediction settings
FORECAST_HORIZON_SEC = _env_int("LONG_TERM_FORECAST_HORIZON_SEC", 1800)  # 30 minutes
TELEMETRY_SAMPLE_SEC = _env_float("LONG_TERM_TELEMETRY_SAMPLE_SEC", 5.0)  # simulator default is every 5 seconds
FORECAST_HORIZON_STEPS = int(FORECAST_HORIZON_SEC / TELEMETRY_SAMPLE_SEC)
TRAIN_INTERVAL_SEC = _env_int("LONG_TERM_TRAIN_INTERVAL_SEC", 60)
TRAIN_INTERVAL_STEPS = int(TRAIN_INTERVAL_SEC / TELEMETRY_SAMPLE_SEC)
MIN_TRAIN_SAMPLE_COUNT = _env_int("LONG_TERM_MIN_TRAIN_SAMPLE_COUNT", 300)
RISK_MARGIN_RATIO = 0.95
MAX_SUGGESTIONS = 3
# Fallback only; primary source is telemetry.system/budget (user-set cap per selected window).
MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = 0.0
LOAD_OVERCURRENT_LIMIT_A = 2.0
FAULT_RISK_MARGIN = 0.90
MPC_ENABLED = True
MPC_DUTY_STEPS = [0.4, 0.6, 0.8, 1.0]
MPC_MIN_EFFECTIVE_DUTY = 0.4
MPC_PRESSURE_DUTY_PENALTY = 25.0
MPC_LOW_LEVEL_MARGIN_PCT = 10.0
MPC_LOW_LEVEL_DUTY_PENALTY = 20.0
MPC_PROCESS_PLANNER_ENABLED = True
MPC_PROCESS_PROGRESS_WEIGHT = 160.0
MPC_PROCESS_POWER_WEIGHT = 1.0
MPC_PROCESS_CAP_PENALTY = 3.0
MPC_PROCESS_SWITCH_PENALTY = 1.5
MPC_PROCESS_LOW_LEVEL_BUFFER_PCT = 1.0
MPC_PROCESS_HIGH_LEVEL_BUFFER_PCT = 1.0
MPC_PROCESS_MIN_PROGRESS = 0.002
PROCESS_LOAD_DEVICES = ("motor1", "motor2", "heater1", "heater2")
PREDICTOR_MODE = os.getenv("LONG_TERM_PREDICTOR_MODE", "ONLINE").strip().upper()
VALID_PREDICTOR_MODES = {"TRAIN_ONLY", "EVAL_ONLY", "ONLINE", "OFFLINE"}
UK_TARIFF_TIMEZONE = os.getenv("UK_TARIFF_TIMEZONE", "Europe/London").strip() or "Europe/London"

def _clamp_hour(value: int) -> int:
    return max(0, min(23, int(value)))

UK_TARIFF_OFFPEAK_START_HOUR = _clamp_hour(_env_int("UK_TARIFF_OFFPEAK_START_HOUR", 0))
UK_TARIFF_OFFPEAK_END_HOUR = _clamp_hour(_env_int("UK_TARIFF_OFFPEAK_END_HOUR", 7))
UK_TARIFF_PEAK_START_HOUR = _clamp_hour(_env_int("UK_TARIFF_PEAK_START_HOUR", 16))
UK_TARIFF_PEAK_END_HOUR = _clamp_hour(_env_int("UK_TARIFF_PEAK_END_HOUR", 20))

UK_TARIFF_OFFPEAK_START_MIN = UK_TARIFF_OFFPEAK_START_HOUR * 60
UK_TARIFF_OFFPEAK_END_MIN = UK_TARIFF_OFFPEAK_END_HOUR * 60
UK_TARIFF_PEAK_START_MIN = UK_TARIFF_PEAK_START_HOUR * 60
UK_TARIFF_PEAK_END_MIN = UK_TARIFF_PEAK_END_HOUR * 60
LOGGER_OUTPUT_DIR = (os.getenv("LOGGER_OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "telemetry_logs")).strip()
                     or os.path.join(os.path.dirname(__file__), "telemetry_logs"))
BOOTSTRAP_FROM_HISTORY = str(os.getenv("BOOTSTRAP_FROM_HISTORY", "1")).strip().lower() in {"1", "true", "yes", "on"}
BOOTSTRAP_FROM_HISTORY_ON_STARTUP = str(os.getenv("BOOTSTRAP_FROM_HISTORY_ON_STARTUP", "0")).strip().lower() in {"1", "true", "yes", "on"}
BOOTSTRAP_HISTORY_MAX_RECORDS = max(0, _env_int("BOOTSTRAP_HISTORY_MAX_RECORDS", 20000))
BOOTSTRAP_RANGE_START_SEC = _parse_optional_epoch_seconds(os.getenv("BOOTSTRAP_RANGE_FROM"))
BOOTSTRAP_RANGE_END_SEC = _parse_optional_epoch_seconds(os.getenv("BOOTSTRAP_RANGE_TO"))

# Feature engineering
LAG_STEPS = [5, 60]      # lag steps
ROLLING_WINDOWS = [10, 60, 300]    # rolling window steps

# Rolling buffers (last hour)
power_buffer = deque(maxlen=3600)
temp_buffer = deque(maxlen=3600)
humidity_buffer = deque(maxlen=3600)
time_buffer = deque(maxlen=3600)
system_supply_v = 0.0
system_total_current_a = 0.0
process_target_tank1_level_pct = 0.0
process_target_tank2_level_pct = 0.0
process_target_tank1_temp_c = 0.0
process_target_tank2_temp_c = 0.0
process_goal_cycles = 0.0
process_cycle_count = 0.0
process_progress_ratio = 0.0
process_state_code = 0.0
process_tank1_level_pct = 0.0
process_tank2_level_pct = 0.0
process_tank1_temp_c = 0.0
process_tank2_temp_c = 0.0
process_tank1_height_cm = 0.0
process_tank2_height_cm = 0.0
process_tank1_low_level_pct = 0.0
process_tank2_low_level_pct = 0.0
process_tank1_high_level_pct = 0.0
process_tank2_high_level_pct = 0.0
system_max_total_power_w = 0.0
system_energy_goal_wh = 0.0
system_current_energy_window_wh = 0.0
system_remaining_energy_window_wh = 0.0
system_energy_goal_ratio = 0.0
system_control_policy_code = 0.0
system_manual_override_flag = 0.0
system_tariff_offpeak_flag = 1.0
system_tariff_midpeak_flag = 0.0
system_tariff_peak_flag = 0.0
system_tariff_rate_per_kwh = 0.0
system_peak_event_flag = 0.0
system_weekday_sin = 0.0
system_weekday_cos = 1.0
system_weekend_flag = 0.0
system_bank_holiday_flag = 0.0
system_minute_of_day_norm = 0.0
system_minutes_to_peak_norm = 0.0
system_minutes_to_offpeak_norm = 0.0
process_elapsed_sec = 0.0
process_last_duration_sec = 0.0
process_avg_duration_sec = 0.0
process_time_since_end_sec = 0.0
_process_last_enabled = False
_process_start_ts = None
_process_end_ts = None
_process_duration_count = 0
event_total_control_actions_delta = 0.0
event_shedding_count_delta = 0.0
event_curtail_count_delta = 0.0
event_restore_count_delta = 0.0
event_load_toggle_count_delta = 0.0
event_overload_count_delta = 0.0
load_fault_active_count = 0.0
load_fault_latched_count = 0.0
_last_total_control_actions = None
_last_shedding_event_count = None
_last_curtail_event_count = None
_last_restore_event_count = None
_last_load_toggle_count = None
_last_overload_event_count = None
load_total_count = 0.0
load_on_count = 0.0
load_on_ratio = 0.0
load_duty_sum_on = 0.0
load_duty_sum_all = 0.0
load_duty_avg_on = 0.0
load_duty_avg_all = 0.0
load_priority_avg_on = 0.0
load_priority_min_on = 0.0
load_priority_max_on = 0.0
load_on_critical = 0.0
load_on_essential = 0.0
load_on_important = 0.0
load_on_secondary = 0.0
load_on_non_essential = 0.0
LOAD_FEATURE_ORDER = ("motor1", "motor2", "heater1", "heater2", "lighting1", "lighting2")
LOAD_DUTY_FEATURE_ORDER = ("motor1", "motor2", "lighting1", "lighting2")
load_goal_window_energy_wh_by_name: Dict[str, float] = {name: 0.0 for name in LOAD_FEATURE_ORDER}
load_on_by_name: Dict[str, float] = {name: 0.0 for name in LOAD_FEATURE_ORDER}
load_duty_by_name: Dict[str, float] = {name: 0.0 for name in LOAD_FEATURE_ORDER}
uk_bank_holidays_cache: Dict[int, set] = {}

# Training data
feature_buffer = deque(maxlen=5000)
target_buffer = deque(maxlen=5000)
pending_feature_buffer = deque(maxlen=10000)

# Evaluation tracking (for EVAL_ONLY)
pending_forecasts = deque(maxlen=5000)  # each: (target_ts, predicted_window_energy_wh, start_total_energy_wh)
eval_errors = deque(maxlen=10000)       # prediction error samples
eval_last_process_enabled = False

model = RandomForestRegressor(
    n_estimators=400,
    max_depth=12,
    min_samples_split=8,
    min_samples_leaf=4,
    max_features="sqrt",
    bootstrap=True,
    random_state=42,
    n_jobs=-1,
)
model_ready = False
warned_eval_without_model = False
processing_lock = threading.RLock()
bootstrap_thread = None
startup_bootstrap_requested = False
status_phase = "starting"
status_message = "service_starting"
bootstrap_active = False
bootstrap_total_records = 0
bootstrap_processed_records = 0
last_bootstrap_started_at = 0.0
last_bootstrap_completed_at = 0.0
last_trained_at = 0.0
last_saved_at = 0.0
last_loaded_at = 0.0
last_status_error = ""
model_meta_warning_fields: list[str] = []

MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
RF_MODEL_PATH = os.path.join(MODEL_DIR, f"{PLANT_IDENTIFIER}_long_rf.pkl")
RF_META_PATH = os.path.join(MODEL_DIR, f"{PLANT_IDENTIFIER}_long_rf.meta.json")
RF_ANALYTICS_PATH = os.path.join(MODEL_DIR, f"{PLANT_IDENTIFIER}_long_rf.analytics.json")

try:
    import certifi
    _CA_CERTS = certifi.where()
except Exception:
    _CA_CERTS = None

def _model_meta():
    return {
        "model_type": "RandomForestRegressor",
        "target_type": "rolling_energy_window_wh",
        "plant_id": PLANT_IDENTIFIER,
        "forecast_horizon_sec": int(FORECAST_HORIZON_SEC),
        "telemetry_sample_sec": float(TELEMETRY_SAMPLE_SEC),
        "lag_steps": list(LAG_STEPS),
        "rolling_windows": list(ROLLING_WINDOWS),
        "feature_version": 18,
    }


def _model_meta_compat(saved_meta: dict) -> tuple[bool, list[str], dict]:
    current_meta = _model_meta()
    hard_keys = ("model_type", "target_type", "plant_id", "lag_steps", "rolling_windows", "feature_version")
    soft_keys = ("forecast_horizon_sec", "telemetry_sample_sec")
    for key in hard_keys:
        if saved_meta.get(key) != current_meta.get(key):
            return False, [], current_meta
    warnings = [key for key in soft_keys if saved_meta.get(key) != current_meta.get(key)]
    return True, warnings, current_meta

def _normalize_predictor_mode(mode_value: str) -> str:
    mode = str(mode_value or "OFFLINE").strip().upper()
    if mode not in VALID_PREDICTOR_MODES:
        return "OFFLINE"
    return mode

def _training_enabled() -> bool:
    return PREDICTOR_MODE in ("TRAIN_ONLY", "ONLINE")

def _prediction_enabled() -> bool:
    return PREDICTOR_MODE in ("EVAL_ONLY", "ONLINE")

def reset_runtime_buffers():
    global system_supply_v, system_total_current_a
    global system_current_energy_window_wh, system_remaining_energy_window_wh, system_energy_goal_ratio
    global event_total_control_actions_delta, event_shedding_count_delta, event_curtail_count_delta
    global event_restore_count_delta, event_load_toggle_count_delta, event_overload_count_delta
    global load_fault_active_count, load_fault_latched_count
    global _last_total_control_actions, _last_shedding_event_count, _last_curtail_event_count
    global _last_restore_event_count, _last_load_toggle_count, _last_overload_event_count
    global process_elapsed_sec, process_last_duration_sec, process_avg_duration_sec, process_time_since_end_sec
    global _process_last_enabled, _process_start_ts, _process_end_ts, _process_duration_count
    global load_goal_window_energy_wh_by_name, load_on_by_name, load_duty_by_name
    power_buffer.clear()
    temp_buffer.clear()
    humidity_buffer.clear()
    time_buffer.clear()
    system_supply_v = 0.0
    system_total_current_a = 0.0
    feature_buffer.clear()
    target_buffer.clear()
    pending_feature_buffer.clear()
    process_elapsed_sec = 0.0
    process_last_duration_sec = 0.0
    process_avg_duration_sec = 0.0
    process_time_since_end_sec = 0.0
    system_current_energy_window_wh = 0.0
    system_remaining_energy_window_wh = 0.0
    system_energy_goal_ratio = 0.0
    event_total_control_actions_delta = 0.0
    event_shedding_count_delta = 0.0
    event_curtail_count_delta = 0.0
    event_restore_count_delta = 0.0
    event_load_toggle_count_delta = 0.0
    event_overload_count_delta = 0.0
    load_fault_active_count = 0.0
    load_fault_latched_count = 0.0
    _last_total_control_actions = None
    _last_shedding_event_count = None
    _last_curtail_event_count = None
    _last_restore_event_count = None
    _last_load_toggle_count = None
    _last_overload_event_count = None
    _process_last_enabled = False
    _process_start_ts = None
    _process_end_ts = None
    _process_duration_count = 0
    load_goal_window_energy_wh_by_name = {name: 0.0 for name in LOAD_FEATURE_ORDER}
    load_on_by_name = {name: 0.0 for name in LOAD_FEATURE_ORDER}
    load_duty_by_name = {name: 0.0 for name in LOAD_FEATURE_ORDER}

def load_model_state():
    global model, model_ready, last_loaded_at, status_phase, status_message, last_status_error, model_meta_warning_fields
    try:
        if not (os.path.exists(RF_MODEL_PATH) and os.path.exists(RF_META_PATH)):
            return False
        with open(RF_META_PATH, "r", encoding="utf-8") as f:
            saved_meta = json.load(f)
        compat_ok, warning_fields, current_meta = _model_meta_compat(saved_meta if isinstance(saved_meta, dict) else {})
        if not compat_ok:
            print("[predictor-long] saved model meta mismatch; ignoring saved model")
            return False
        _prepare_numpy_pickle_compat()
        with open(RF_MODEL_PATH, "rb") as f:
            model = pickle.load(f)
        model_ready = True
        last_loaded_at = time.time()
        status_phase = "ready"
        status_message = "saved_model_loaded"
        last_status_error = ""
        model_meta_warning_fields = list(warning_fields)
        if not os.path.exists(RF_ANALYTICS_PATH):
            _save_rf_analytics()
        if warning_fields:
            print(
                "[predictor-long] loaded saved model with compatibility warning:"
                f" soft_fields={warning_fields} saved={saved_meta} runtime={current_meta}"
            )
        print(f"[predictor-long] loaded saved model: {RF_MODEL_PATH}")
        return True
    except Exception as e:
        status_phase = "error"
        status_message = "load_model_failed"
        last_status_error = str(e)
        model_meta_warning_fields = []
        print(f"[predictor-long] failed to load saved model: {e}")
        return False

def save_model_state():
    global last_saved_at, status_message
    try:
        os.makedirs(MODEL_DIR, exist_ok=True)
        with open(RF_MODEL_PATH, "wb") as f:
            pickle.dump(model, f)
        with open(RF_META_PATH, "w", encoding="utf-8") as f:
            json.dump(_model_meta(), f, indent=2)
        last_saved_at = time.time()
        status_message = "model_saved"
    except Exception as e:
        print(f"[predictor-long] failed to save model: {e}")


def _feature_names() -> list[str]:
    return list(LONG_TERM_FEATURE_NAMES)


def _save_rf_analytics(X: Optional[np.ndarray] = None, y: Optional[np.ndarray] = None) -> None:
    if model is None or not hasattr(model, "feature_importances_"):
        return
    try:
        os.makedirs(MODEL_DIR, exist_ok=True)
        feature_names = _feature_names()
        importances = np.asarray(getattr(model, "feature_importances_", []), dtype=float)
        if importances.size == 0:
            return
        feature_count = min(len(feature_names), int(importances.size))
        feature_names = feature_names[:feature_count]
        importances = importances[:feature_count]
        total_importance = float(importances.sum())
        normalized = (importances / total_importance) if total_importance > 0 else importances
        order = np.argsort(normalized)[::-1]
        top_features = [
            {
                "rank": int(rank + 1),
                "feature": feature_names[int(idx)],
                "importance": float(normalized[int(idx)]),
            }
            for rank, idx in enumerate(order[:15])
        ]

        analytics = {
            "model": "RandomForestRegressor",
            "generated_at": float(time.time()),
            "feature_count": feature_count,
            "all_features": [
                {"feature": feature_names[i], "importance": float(normalized[i])}
                for i in range(feature_count)
            ],
            "top_features": top_features,
        }

        if X is not None and y is not None:
            x_arr = np.asarray(X, dtype=float)
            y_arr = np.asarray(y, dtype=float).reshape(-1)
            if x_arr.ndim == 2 and y_arr.size == x_arr.shape[0] and x_arr.shape[1] >= feature_count:
                corr_idx = order[: min(12, feature_count)]
                corr_input = np.column_stack([x_arr[:, corr_idx], y_arr])
                corr = np.corrcoef(corr_input, rowvar=False)
                corr = np.nan_to_num(corr, nan=0.0, posinf=0.0, neginf=0.0)
                analytics["correlation_heatmap"] = {
                    "labels": [feature_names[int(idx)] for idx in corr_idx] + ["target_future_window_energy_wh"],
                    "matrix": corr.tolist(),
                }

        with open(RF_ANALYTICS_PATH, "w", encoding="utf-8") as handle:
            json.dump(analytics, handle, indent=2)
    except Exception as exc:
        print(f"[predictor-long] failed to save analytics: {exc}")


def _load_bootstrap_payloads() -> list[dict]:
    global last_status_error
    if not BOOTSTRAP_FROM_HISTORY or BOOTSTRAP_HISTORY_MAX_RECORDS <= 0:
        return []

    log_dir = Path(LOGGER_OUTPUT_DIR)
    if not log_dir.exists():
        last_status_error = f"log_dir_not_found:{log_dir}"
        print(f"[predictor-long] bootstrap skipped: log dir not found: {log_dir}")
        return []

    files = sorted(log_dir.glob(f"{PLANT_IDENTIFIER}_telemetry_*.jsonl"))
    if not files:
        files = sorted(log_dir.rglob(f"{PLANT_IDENTIFIER}_telemetry_*.jsonl"))
    if not files:
        last_status_error = f"no_matching_telemetry_files:{log_dir}"
        print(f"[predictor-long] bootstrap skipped: no telemetry logs for {PLANT_IDENTIFIER} in {log_dir}")
        return []

    payloads = deque(maxlen=BOOTSTRAP_HISTORY_MAX_RECORDS)
    for path in files:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except Exception:
                        continue
                    topic = str(record.get("topic", "") or "")
                    if topic and topic != TOPIC_TELEMETRY_NAME:
                        continue
                    payload = record.get("payload") if isinstance(record.get("payload"), dict) else record
                    if not isinstance(payload, dict):
                        continue
                    system = payload.get("system", {}) or {}
                    if "total_power_w" not in system:
                        continue
                    ts = parse_ts(payload)
                    if BOOTSTRAP_RANGE_START_SEC is not None and ts < BOOTSTRAP_RANGE_START_SEC:
                        continue
                    if BOOTSTRAP_RANGE_END_SEC is not None and ts > BOOTSTRAP_RANGE_END_SEC:
                        continue
                    payloads.append((ts, payload))
        except Exception as exc:
            print(f"[predictor-long] bootstrap skipped unreadable log {path}: {exc}")

    ordered = sorted(payloads, key=lambda item: item[0])
    return [payload for _, payload in ordered]


def _status_payload(extra: Optional[dict] = None) -> dict:
    payload = {
        "schema_version": "1.0",
        "producer": "long_term_predictor",
        "message_type": "predictor_status",
        "timestamp": time.time(),
        "plant_id": PLANT_IDENTIFIER,
        "source": "long_rf",
        "method": "RandomForestRegressor",
        "predictor_mode": PREDICTOR_MODE,
        "phase": status_phase,
        "message": status_message,
        "model_ready": bool(model_ready),
        "bootstrap_active": bool(bootstrap_active),
        "bootstrap_total_records": int(bootstrap_total_records),
        "bootstrap_processed_records": int(bootstrap_processed_records),
        "bootstrap_progress_ratio": (
            round(float(bootstrap_processed_records) / float(bootstrap_total_records), 4)
            if bootstrap_total_records > 0 else 0.0
        ),
        "train_samples": int(len(feature_buffer)),
        "model_path": RF_MODEL_PATH,
        "meta_path": RF_META_PATH,
        "analytics_path": RF_ANALYTICS_PATH,
        "logger_output_dir": LOGGER_OUTPUT_DIR,
        "bootstrap_range_from": (datetime.utcfromtimestamp(BOOTSTRAP_RANGE_START_SEC).isoformat() + "Z") if BOOTSTRAP_RANGE_START_SEC is not None else "",
        "bootstrap_range_to": (datetime.utcfromtimestamp(BOOTSTRAP_RANGE_END_SEC).isoformat() + "Z") if BOOTSTRAP_RANGE_END_SEC is not None else "",
        "forecast_horizon_sec": int(FORECAST_HORIZON_SEC),
        "telemetry_sample_sec": float(TELEMETRY_SAMPLE_SEC),
        "model_meta_warning_fields": list(model_meta_warning_fields),
        "min_train_sample_count": int(MIN_TRAIN_SAMPLE_COUNT),
        "last_bootstrap_started_at": (datetime.utcfromtimestamp(last_bootstrap_started_at).isoformat() + "Z") if last_bootstrap_started_at > 0 else "",
        "last_bootstrap_completed_at": (datetime.utcfromtimestamp(last_bootstrap_completed_at).isoformat() + "Z") if last_bootstrap_completed_at > 0 else "",
        "last_trained_at": (datetime.utcfromtimestamp(last_trained_at).isoformat() + "Z") if last_trained_at > 0 else "",
        "last_saved_at": (datetime.utcfromtimestamp(last_saved_at).isoformat() + "Z") if last_saved_at > 0 else "",
        "last_loaded_at": (datetime.utcfromtimestamp(last_loaded_at).isoformat() + "Z") if last_loaded_at > 0 else "",
        "last_error": last_status_error,
    }
    if extra:
        payload.update(extra)
    return payload


def _publish_status(client=None, extra: Optional[dict] = None) -> None:
    if client is None:
        return
    try:
        client.publish(TOPIC_PREDICTION_LONG_STATUS_NAME, json.dumps(_status_payload(extra)), qos=1, retain=True)
    except Exception as exc:
        print(f"[predictor-long] failed to publish status: {exc}")


def _train_model_from_buffers(client=None) -> bool:
    global model_ready, warned_eval_without_model, last_trained_at
    global status_phase, status_message, last_status_error
    if len(feature_buffer) < MIN_TRAIN_SAMPLE_COUNT:
        return False
    status_phase = "training"
    status_message = "training_model"
    last_status_error = ""
    _publish_status(client)
    X = np.array(feature_buffer, dtype=float)
    y = np.array(target_buffer, dtype=float)
    model.fit(X, y)
    model_ready = True
    warned_eval_without_model = False
    last_trained_at = time.time()
    status_phase = "ready"
    status_message = "training_complete"
    print(f"[predictor-long] trained model on {len(X)} samples")
    _save_rf_analytics(X, y)
    save_model_state()
    _publish_status(client)
    return True


def bootstrap_from_history(client=None) -> int:
    global bootstrap_active, bootstrap_total_records, bootstrap_processed_records
    global last_bootstrap_started_at, last_bootstrap_completed_at
    global status_phase, status_message, last_status_error, model_ready, warned_eval_without_model
    if not _training_enabled():
        return 0
    model_ready = False
    warned_eval_without_model = False
    bootstrap_active = True
    bootstrap_total_records = 0
    bootstrap_processed_records = 0
    last_bootstrap_started_at = time.time()
    status_phase = "bootstrap_loading"
    status_message = "loading_history"
    last_status_error = ""
    _publish_status(client)
    payloads = _load_bootstrap_payloads()
    if not payloads:
        bootstrap_active = False
        last_bootstrap_completed_at = time.time()
        status_phase = "ready" if model_ready else "idle"
        status_message = "no_history_payloads"
        _publish_status(client, {"logger_output_dir": str(Path(LOGGER_OUTPUT_DIR).resolve())})
        return 0

    bootstrap_total_records = len(payloads)
    status_phase = "bootstrap_replaying"
    status_message = "replaying_history"
    _publish_status(client, {"logger_output_dir": str(Path(LOGGER_OUTPUT_DIR).resolve())})

    last_progress_publish = 0.0
    with processing_lock:
        reset_runtime_buffers()
        for idx, payload in enumerate(payloads, start=1):
            _process_telemetry_payload(payload, client=None, publish=False, bootstrap=True)
            bootstrap_processed_records = idx
            now = time.time()
            if idx == bootstrap_total_records or idx == 1 or idx % 250 == 0 or (now - last_progress_publish) >= 1.0:
                last_progress_publish = now
                _publish_status(client)

    _train_model_from_buffers(client=client)
    bootstrap_active = False
    last_bootstrap_completed_at = time.time()
    status_phase = "ready" if model_ready else "waiting_for_training"
    status_message = "bootstrap_complete"
    _publish_status(client)

    print(
        f"[predictor-long] bootstrap replayed {len(payloads)} historical telemetry samples "
        f"from {LOGGER_OUTPUT_DIR}; train_samples={len(feature_buffer)} model_ready={model_ready}"
    )
    return len(payloads)


def _start_bootstrap_thread(client=None) -> bool:
    global bootstrap_thread, status_message
    if bootstrap_thread is not None and bootstrap_thread.is_alive():
        status_message = "bootstrap_already_running"
        _publish_status(client)
        return False

    def _worker():
        global status_phase, status_message, last_status_error
        try:
            bootstrap_from_history(client=client)
        except Exception as exc:
            status_phase = "error"
            status_message = "bootstrap_failed"
            last_status_error = str(exc)
            print(f"[predictor-long] bootstrap failed: {exc}")
            _publish_status(client)

    bootstrap_thread = threading.Thread(target=_worker, name=f"{PLANT_IDENTIFIER}-rf-bootstrap", daemon=True)
    bootstrap_thread.start()
    return True

def _normalize_epoch_seconds(raw) -> Optional[float]:
    try:
        ts = float(raw)
    except Exception:
        return None
    # Accept ms epoch too.
    if ts > 1e12:
        ts = ts / 1000.0
    # Reject non-epoch values such as MCU uptime seconds.
    if ts < 946684800.0:  # 2000-01-01 UTC
        return None
    return ts


def parse_ts(payload):
    """Parse a timestamp from payload (ISO or epoch)."""
    ts = payload.get("timestamp")
    if isinstance(ts, str):
        raw = ts.strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(raw).timestamp()
        except Exception:
            parsed = _normalize_epoch_seconds(raw)
            if parsed is not None:
                return parsed
    else:
        parsed = _normalize_epoch_seconds(ts)
        if parsed is not None:
            return parsed

    parsed = _normalize_epoch_seconds(payload.get("system", {}).get("timestamp"))
    if parsed is not None:
        return parsed
    return time.time()

def _load_uk_tz():
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(UK_TARIFF_TIMEZONE)
    except Exception:
        return None

UK_TZ = _load_uk_tz()

def _to_uk_datetime(ts_epoch: float) -> datetime:
    if UK_TZ is not None:
        return datetime.fromtimestamp(ts_epoch, tz=UK_TZ)
    # Fallback keeps behavior deterministic even without zoneinfo/tzdata.
    return datetime.utcfromtimestamp(ts_epoch)

def _normalize_tariff_state(value: Any) -> str:
    key = str(value or "").strip().upper().replace("_", "").replace("-", "")
    if key == "PEAK":
        return "PEAK"
    if key in ("MIDPEAK", "MID"):
        return "MIDPEAK"
    if key == "OFFPEAK":
        return "OFFPEAK"
    return ""

def _to_float_or_none(value: Any) -> Optional[float]:
    try:
        n = float(value)
    except Exception:
        return None
    if not np.isfinite(n):
        return None
    return n

def _is_minute_in_window(minute_of_day: int, start_min: int, end_min: int) -> bool:
    minute = minute_of_day % 1440
    start = start_min % 1440
    end = end_min % 1440
    if start == end:
        return True
    if start < end:
        return start <= minute < end
    return minute >= start or minute < end

def _minutes_until_window_start(minute_of_day: int, start_min: int) -> int:
    minute = minute_of_day % 1440
    start = start_min % 1440
    return (start - minute) % 1440

def _day_of_week_utc(year: int, month: int, day: int) -> int:
    return datetime(year, month, day).weekday()

def _days_in_month_utc(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    this_month = datetime(year, month, 1)
    return int((next_month - this_month).days)

def _add_days_utc(year: int, month: int, day: int, delta: int) -> Dict[str, int]:
    dt = datetime(year, month, day) + timedelta(days=delta)
    return {"year": dt.year, "month": dt.month, "day": dt.day}

def _first_weekday_of_month_utc(year: int, month: int, weekday: int) -> int:
    for day in range(1, 8):
        if _day_of_week_utc(year, month, day) == weekday:
            return day
    return 1

def _last_weekday_of_month_utc(year: int, month: int, weekday: int) -> int:
    for day in range(_days_in_month_utc(year, month), 0, -1):
        if _day_of_week_utc(year, month, day) == weekday:
            return day
    return _days_in_month_utc(year, month)

def _compute_easter_sunday_utc(year: int) -> Dict[str, int]:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + (2 * e) + (2 * i) - h - k) % 7
    m = (a + (11 * h) + (22 * l)) // 451
    month = (h + l - (7 * m) + 114) // 31
    day = ((h + l - (7 * m) + 114) % 31) + 1
    return {"year": year, "month": month, "day": day}

def _build_uk_bank_holiday_set(year: int) -> set:
    out = set()

    # New Year's Day substitute.
    new_year_dow = _day_of_week_utc(year, 1, 1)
    new_year_observed = 1
    if new_year_dow == 5:
        new_year_observed = 3
    elif new_year_dow == 6:
        new_year_observed = 2
    out.add((year, 1, new_year_observed))

    # Good Friday + Easter Monday.
    easter = _compute_easter_sunday_utc(year)
    gf = _add_days_utc(easter["year"], easter["month"], easter["day"], -2)
    em = _add_days_utc(easter["year"], easter["month"], easter["day"], 1)
    out.add((gf["year"], gf["month"], gf["day"]))
    out.add((em["year"], em["month"], em["day"]))

    # May + August holidays.
    out.add((year, 5, _first_weekday_of_month_utc(year, 5, 0)))  # first Monday
    out.add((year, 5, _last_weekday_of_month_utc(year, 5, 0)))   # last Monday
    out.add((year, 8, _last_weekday_of_month_utc(year, 8, 0)))   # last Monday

    # Christmas + Boxing substitute handling.
    christmas_dow = _day_of_week_utc(year, 12, 25)
    christmas_observed = 25
    if christmas_dow in (5, 6):
        christmas_observed = 27
    out.add((year, 12, christmas_observed))

    boxing_dow = _day_of_week_utc(year, 12, 26)
    boxing_observed = 26
    if boxing_dow in (5, 6):
        boxing_observed = 28
    out.add((year, 12, boxing_observed))
    return out

def _is_uk_bank_holiday(year: int, month: int, day: int) -> bool:
    if year not in uk_bank_holidays_cache:
        uk_bank_holidays_cache[year] = _build_uk_bank_holiday_set(year)
    return (year, month, day) in uk_bank_holidays_cache[year]

def _extract_tariff_rate_per_kwh(system: Dict[str, Any]) -> float:
    tariff_block = system.get("tariff", {})
    if not isinstance(tariff_block, dict):
        tariff_block = {}
    candidates = [
        system.get("tariff_rate_per_kwh"),
        system.get("tariff_price_per_kwh"),
        system.get("energy_price_per_kwh"),
        system.get("price_per_kwh"),
        tariff_block.get("rate_per_kwh"),
    ]
    for candidate in candidates:
        value = _to_float_or_none(candidate)
        if value is not None and value >= 0.0:
            return float(value)
    return 0.0

def _resolve_tariff_state(system: Dict[str, Any], minute_of_day: int, is_weekend: bool, is_bank_holiday: bool) -> str:
    explicit_state = _normalize_tariff_state(system.get("tariff_state"))
    if explicit_state:
        return explicit_state
    if is_weekend or is_bank_holiday:
        return "OFFPEAK"
    if _is_minute_in_window(minute_of_day, UK_TARIFF_OFFPEAK_START_MIN, UK_TARIFF_OFFPEAK_END_MIN):
        return "OFFPEAK"
    if _is_minute_in_window(minute_of_day, UK_TARIFF_PEAK_START_MIN, UK_TARIFF_PEAK_END_MIN):
        return "PEAK"
    return "MIDPEAK"


def compute_production_pressure(process: Dict[str, Any]) -> Dict[str, Any]:
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


def detect_fault_risk(loads: Dict[str, Any]) -> Dict[str, Any]:
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


def _update_eval_errors(sample_ts: float, current_total_energy_wh: float) -> None:
    while pending_forecasts and pending_forecasts[0][0] <= sample_ts:
        _, pred_window_energy_wh, start_total_energy_wh = pending_forecasts.popleft()
        actual_window_energy_wh = max(0.0, float(current_total_energy_wh) - float(start_total_energy_wh))
        eval_errors.append(float(pred_window_energy_wh) - actual_window_energy_wh)


def _eval_metrics() -> Optional[Dict[str, float]]:
    if not eval_errors:
        return None
    err = np.array(eval_errors, dtype=float)
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err ** 2)))
    return {"mae_wh": round(mae, 4), "rmse_wh": round(rmse, 4), "count": int(len(eval_errors))}


def _update_load_aggregates(loads: Dict[str, Any]) -> None:
    global load_total_count, load_on_count, load_on_ratio
    global load_duty_sum_on, load_duty_sum_all, load_duty_avg_on, load_duty_avg_all
    global load_priority_avg_on, load_priority_min_on, load_priority_max_on
    global load_on_critical, load_on_essential, load_on_important, load_on_secondary, load_on_non_essential
    global load_goal_window_energy_wh_by_name, load_on_by_name, load_duty_by_name
    metrics = _compute_load_aggregates(loads)
    load_total_count = float(metrics["load_total_count"])
    load_on_count = float(metrics["load_on_count"])
    load_on_ratio = float(metrics["load_on_ratio"])
    load_duty_sum_all = float(metrics["load_duty_sum_all"])
    load_duty_sum_on = float(metrics["load_duty_sum_on"])
    load_duty_avg_all = float(metrics["load_duty_avg_all"])
    load_duty_avg_on = float(metrics["load_duty_avg_on"])
    load_priority_avg_on = float(metrics["load_priority_avg_on"])
    load_priority_min_on = float(metrics["load_priority_min_on"])
    load_priority_max_on = float(metrics["load_priority_max_on"])
    load_on_critical = float(metrics["load_on_critical"])
    load_on_essential = float(metrics["load_on_essential"])
    load_on_important = float(metrics["load_on_important"])
    load_on_secondary = float(metrics["load_on_secondary"])
    load_on_non_essential = float(metrics["load_on_non_essential"])
    load_on_by_name = {
        name: float(metrics.get(f"load_{name}_on_ratio", 0.0))
        for name in LOAD_FEATURE_ORDER
    }
    load_duty_by_name = {
        name: float(metrics.get(f"load_{name}_duty_applied_avg", 0.0))
        for name in LOAD_FEATURE_ORDER
    }
    load_goal_window_energy_wh_by_name = {
        name: float(metrics.get(f"load_{name}_goal_window_energy_wh", 0.0))
        for name in LOAD_FEATURE_ORDER
    }

def _compute_load_aggregates(loads: Dict[str, Any]) -> Dict[str, float]:
    total = len(loads)
    on_count = 0.0
    duty_sum_all = 0.0
    duty_sum_on = 0.0
    class_counts = {
        "CRITICAL": 0,
        "ESSENTIAL": 0,
        "IMPORTANT": 0,
        "SECONDARY": 0,
        "NON_ESSENTIAL": 0,
    }

    for _, ld in (loads or {}).items():
        try:
            on = bool(ld.get("on", False))
        except Exception:
            on = False
        on_ratio = _safe_float(ld.get("on_ratio"), 1.0 if on else 0.0)
        on_ratio = max(0.0, min(1.0, float(on_ratio)))
        try:
            duty = float(ld.get("duty_applied_avg", ld.get("duty_applied", ld.get("duty", 0.0))))
        except Exception:
            duty = 0.0
        duty = max(0.0, min(1.0, float(duty)))
        duty_sum_all += duty
        on_count += on_ratio
        if on_ratio > 0.0:
            duty_sum_on += duty
            cls = str(ld.get("class", "")).upper()
            if cls in class_counts:
                class_counts[cls] += on_ratio

    load_goal_window_energy = {
        f"load_{name}_goal_window_energy_wh": float(
            _safe_float((loads or {}).get(name, {}).get("energy_goal_window_wh"), load_goal_window_energy_wh_by_name.get(name, 0.0))
        )
        for name in LOAD_FEATURE_ORDER
    }
    load_live_state = {}
    for name in LOAD_FEATURE_ORDER:
        load_payload = (loads or {}).get(name, {}) or {}
        load_live_state[f"load_{name}_on_ratio"] = max(
            0.0,
            min(1.0, float(_safe_float(load_payload.get("on_ratio"), 1.0 if bool(load_payload.get("on", False)) else 0.0))),
        )
        load_live_state[f"load_{name}_duty_applied_avg"] = max(
            0.0,
            min(1.0, float(_safe_float(load_payload.get("duty_applied_avg", load_payload.get("duty_applied", load_payload.get("duty", 0.0))), load_duty_by_name.get(name, 0.0)))),
        )

    return {
        "load_total_count": float(total),
        "load_on_count": float(on_count),
        "load_on_ratio": (on_count / total) if total > 0 else 0.0,
        "load_duty_sum_all": float(duty_sum_all),
        "load_duty_sum_on": float(duty_sum_on),
        "load_duty_avg_all": float(duty_sum_all / total) if total > 0 else 0.0,
        "load_duty_avg_on": float(duty_sum_on / on_count) if on_count > 0 else 0.0,
        "load_priority_avg_on": 0.0,
        "load_priority_min_on": 0.0,
        "load_priority_max_on": 0.0,
        "load_on_critical": float(class_counts["CRITICAL"]),
        "load_on_essential": float(class_counts["ESSENTIAL"]),
        "load_on_important": float(class_counts["IMPORTANT"]),
        "load_on_secondary": float(class_counts["SECONDARY"]),
        "load_on_non_essential": float(class_counts["NON_ESSENTIAL"]),
        **load_live_state,
        **load_goal_window_energy,
    }

def _encode_control_policy(policy_value: Any) -> float:
    policy = str(policy_value or "").strip().upper()
    if policy == "RULE_ONLY":
        return 0.0
    if policy == "AI_PREFERRED":
        return 1.0
    if policy == "HYBRID":
        return 2.0
    return 0.0


def _update_system_context(system: Dict[str, Any], sample_ts: float) -> None:
    global system_max_total_power_w, system_energy_goal_wh
    global system_control_policy_code, system_manual_override_flag
    global system_tariff_offpeak_flag, system_tariff_midpeak_flag, system_tariff_peak_flag
    global system_tariff_rate_per_kwh, system_peak_event_flag
    global system_supply_v, system_total_current_a
    global system_weekday_sin, system_weekday_cos, system_weekend_flag, system_bank_holiday_flag
    global system_minute_of_day_norm, system_minutes_to_peak_norm, system_minutes_to_offpeak_norm

    system_max_total_power_w = float(system.get("MAX_TOTAL_POWER_W", 0.0) or 0.0)
    energy_goal = system.get(
        "ENERGY_GOAL_VALUE_WH",
        system.get("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH", system.get("MAX_ENERGY_CONSUMPTION_FOR_DAY_WH", 0.0)),
    )
    system_energy_goal_wh = float(energy_goal or 0.0)
    system_control_policy_code = _encode_control_policy(system.get("control_policy"))
    system_manual_override_flag = 1.0 if bool(system.get("manual_override", False)) else 0.0
    system_supply_v = float(system.get("supply_v", system_supply_v) or 0.0)
    system_total_current_a = float(system.get("total_current_a", system_total_current_a) or 0.0)

    uk_dt = _to_uk_datetime(sample_ts)
    weekday_idx = int(uk_dt.weekday())  # Monday=0 ... Sunday=6
    minute_of_day = int((uk_dt.hour * 60) + uk_dt.minute)
    weekend = weekday_idx >= 5
    bank_holiday = _is_uk_bank_holiday(uk_dt.year, uk_dt.month, uk_dt.day)

    tariff_state = _resolve_tariff_state(system, minute_of_day, weekend, bank_holiday)
    system_tariff_offpeak_flag = 1.0 if tariff_state == "OFFPEAK" else 0.0
    system_tariff_midpeak_flag = 1.0 if tariff_state == "MIDPEAK" else 0.0
    system_tariff_peak_flag = 1.0 if tariff_state == "PEAK" else 0.0
    system_tariff_rate_per_kwh = _extract_tariff_rate_per_kwh(system)
    system_peak_event_flag = 1.0 if bool(system.get("peak_event", tariff_state == "PEAK")) else 0.0

    weekday_rad = 2 * np.pi * (weekday_idx / 7.0)
    system_weekday_sin = float(np.sin(weekday_rad))
    system_weekday_cos = float(np.cos(weekday_rad))
    system_weekend_flag = 1.0 if weekend else 0.0
    system_bank_holiday_flag = 1.0 if bank_holiday else 0.0
    system_minute_of_day_norm = float(minute_of_day / 1439.0) if minute_of_day > 0 else 0.0
    system_minutes_to_peak_norm = float(_minutes_until_window_start(minute_of_day, UK_TARIFF_PEAK_START_MIN) / 1440.0)
    system_minutes_to_offpeak_norm = float(_minutes_until_window_start(minute_of_day, UK_TARIFF_OFFPEAK_START_MIN) / 1440.0)


def _counter_delta(current_value: Any, previous_attr: str) -> float:
    previous_value = globals().get(previous_attr)
    current = _safe_float(current_value, 0.0)
    if previous_value is None:
        delta = 0.0
    elif current >= float(previous_value):
        delta = current - float(previous_value)
    else:
        delta = current
    globals()[previous_attr] = current
    return float(max(0.0, delta))


def _update_event_context(data: Dict[str, Any], energy: Dict[str, Any], loads: Dict[str, Any]) -> None:
    global system_current_energy_window_wh, system_remaining_energy_window_wh, system_energy_goal_ratio
    global event_total_control_actions_delta, event_shedding_count_delta, event_curtail_count_delta
    global event_restore_count_delta, event_load_toggle_count_delta, event_overload_count_delta
    global load_fault_active_count, load_fault_latched_count

    budget = energy.get("budget", {}) or {}
    evaluation = data.get("evaluation", {}) or {}
    control_eval = evaluation.get("control", {}) or {}
    stability_eval = evaluation.get("stability", {}) or {}

    system_current_energy_window_wh = float(
        budget.get("consumed_window_wh", system_current_energy_window_wh) or 0.0
    )
    system_remaining_energy_window_wh = float(
        budget.get("remaining_window_wh", max(0.0, system_energy_goal_wh - system_current_energy_window_wh)) or 0.0
    )
    system_energy_goal_ratio = (
        system_current_energy_window_wh / system_energy_goal_wh
        if system_energy_goal_wh > 0.0 else 0.0
    )

    event_total_control_actions_delta = _counter_delta(control_eval.get("total_control_actions"), "_last_total_control_actions")
    event_shedding_count_delta = _counter_delta(control_eval.get("shedding_event_count"), "_last_shedding_event_count")
    event_curtail_count_delta = _counter_delta(control_eval.get("curtail_event_count"), "_last_curtail_event_count")
    event_restore_count_delta = _counter_delta(control_eval.get("restore_event_count"), "_last_restore_event_count")
    event_load_toggle_count_delta = _counter_delta(stability_eval.get("load_toggle_count"), "_last_load_toggle_count")
    event_overload_count_delta = _counter_delta(stability_eval.get("overload_event_count"), "_last_overload_event_count")

    load_fault_active_count = 0.0
    load_fault_latched_count = 0.0
    for load_data in loads.values():
        if not isinstance(load_data, dict):
            continue
        if bool(load_data.get("fault_active", False)):
            load_fault_active_count += 1.0
        if bool(load_data.get("fault_latched", False)):
            load_fault_latched_count += 1.0

def _update_process_context(process: Dict[str, Any]) -> bool:
    global process_target_tank1_level_pct, process_target_tank2_level_pct
    global process_target_tank1_temp_c, process_target_tank2_temp_c, process_goal_cycles
    global process_cycle_count, process_progress_ratio, process_state_code
    global process_tank1_level_pct, process_tank2_level_pct
    global process_tank1_temp_c, process_tank2_temp_c
    global process_tank1_height_cm, process_tank2_height_cm
    global process_tank1_low_level_pct, process_tank2_low_level_pct
    global process_tank1_high_level_pct, process_tank2_high_level_pct

    tank1 = process.get("tank1", {}) or {}
    tank2 = process.get("tank2", {}) or {}
    process_enabled = bool(process.get("enabled", False))

    if "target_level_pct" in tank1:
        process_target_tank1_level_pct = float(tank1.get("target_level_pct", process_target_tank1_level_pct))
    if "target_level_pct" in tank2:
        process_target_tank2_level_pct = float(tank2.get("target_level_pct", process_target_tank2_level_pct))
    if "target_temp_c" in tank1:
        process_target_tank1_temp_c = float(tank1.get("target_temp_c", process_target_tank1_temp_c))
    if "target_temp_c" in tank2:
        process_target_tank2_temp_c = float(tank2.get("target_temp_c", process_target_tank2_temp_c))
    if "goal_cycles" in process:
        process_goal_cycles = float(process.get("goal_cycles", process_goal_cycles))

    if "tank1_height_cm" in process:
        process_tank1_height_cm = float(process.get("tank1_height_cm", process_tank1_height_cm))
    elif "height_cm" in tank1:
        process_tank1_height_cm = float(tank1.get("height_cm", process_tank1_height_cm))
    if "tank2_height_cm" in process:
        process_tank2_height_cm = float(process.get("tank2_height_cm", process_tank2_height_cm))
    elif "height_cm" in tank2:
        process_tank2_height_cm = float(tank2.get("height_cm", process_tank2_height_cm))

    global_low = process.get("tank_low_level_pct", None)
    global_high = process.get("tank_high_level_pct", None)
    if "tank1_low_level_pct" in process:
        process_tank1_low_level_pct = float(process.get("tank1_low_level_pct", process_tank1_low_level_pct))
    elif "low_level_pct" in tank1:
        process_tank1_low_level_pct = float(tank1.get("low_level_pct", process_tank1_low_level_pct))
    elif global_low is not None:
        process_tank1_low_level_pct = float(global_low)
    if "tank2_low_level_pct" in process:
        process_tank2_low_level_pct = float(process.get("tank2_low_level_pct", process_tank2_low_level_pct))
    elif "low_level_pct" in tank2:
        process_tank2_low_level_pct = float(tank2.get("low_level_pct", process_tank2_low_level_pct))
    elif global_low is not None:
        process_tank2_low_level_pct = float(global_low)

    if "tank1_high_level_pct" in process:
        process_tank1_high_level_pct = float(process.get("tank1_high_level_pct", process_tank1_high_level_pct))
    elif "high_level_pct" in tank1:
        process_tank1_high_level_pct = float(tank1.get("high_level_pct", process_tank1_high_level_pct))
    elif global_high is not None:
        process_tank1_high_level_pct = float(global_high)
    if "tank2_high_level_pct" in process:
        process_tank2_high_level_pct = float(process.get("tank2_high_level_pct", process_tank2_high_level_pct))
    elif "high_level_pct" in tank2:
        process_tank2_high_level_pct = float(tank2.get("high_level_pct", process_tank2_high_level_pct))
    elif global_high is not None:
        process_tank2_high_level_pct = float(global_high)

    if "level_pct" in tank1:
        process_tank1_level_pct = float(tank1.get("level_pct", process_tank1_level_pct))
    if "level_pct" in tank2:
        process_tank2_level_pct = float(tank2.get("level_pct", process_tank2_level_pct))
    if "temperature_c" in tank1:
        process_tank1_temp_c = float(tank1.get("temperature_c", process_tank1_temp_c))
    if "temperature_c" in tank2:
        process_tank2_temp_c = float(tank2.get("temperature_c", process_tank2_temp_c))
    if "cycle_count" in process:
        process_cycle_count = float(process.get("cycle_count", process_cycle_count))
    if process_goal_cycles > 0:
        process_progress_ratio = max(0.0, min(1.0, process_cycle_count / process_goal_cycles))
    state = str(process.get("state", "IDLE")).upper()
    state_map = {"IDLE": 0.0, "HEAT_TANK1": 1.0, "TRANSFER_1_TO_2": 2.0, "HEAT_TANK2": 3.0, "TRANSFER_2_TO_1": 4.0}
    process_state_code = state_map.get(state, 0.0)
    return process_enabled

def _update_process_time_features(sample_ts: float, process: Dict[str, Any], process_enabled: bool) -> None:
    global process_elapsed_sec, process_last_duration_sec, process_avg_duration_sec, process_time_since_end_sec
    global _process_last_enabled, _process_start_ts, _process_end_ts, _process_duration_count

    if "elapsed_sec" in process:
        try:
            process_elapsed_sec = float(process.get("elapsed_sec", process_elapsed_sec))
        except Exception:
            pass

    if process_enabled and not _process_last_enabled:
        _process_start_ts = sample_ts

    if (not process_enabled) and _process_last_enabled:
        if _process_start_ts is None:
            duration = 0.0
        else:
            duration = max(0.0, sample_ts - _process_start_ts)
        process_last_duration_sec = duration
        _process_duration_count += 1
        if _process_duration_count == 1:
            process_avg_duration_sec = duration
        else:
            process_avg_duration_sec = process_avg_duration_sec + (duration - process_avg_duration_sec) / _process_duration_count
        _process_end_ts = sample_ts

    if process_enabled:
        if _process_start_ts is not None and "elapsed_sec" not in process:
            process_elapsed_sec = max(0.0, sample_ts - _process_start_ts)
        process_time_since_end_sec = 0.0
    else:
        if _process_end_ts is not None:
            process_time_since_end_sec = max(0.0, sample_ts - _process_end_ts)
        else:
            process_time_since_end_sec = 0.0

    _process_last_enabled = process_enabled

def _handle_eval_only(client, sample_ts: float, process: Dict[str, Any], process_enabled: bool) -> None:
    global eval_last_process_enabled
    if process_enabled and not eval_last_process_enabled:
        eval_errors.clear()
        pending_forecasts.clear()
    if eval_last_process_enabled and not process_enabled:
        metrics = _eval_metrics()
        if metrics:
            summary_payload = {
                "schema_version": "1.1",
                "producer": "long_term_predictor",
                "message_type": "evaluation_summary",
                "timestamp": float(sample_ts),
                "generated_at": time.time(),
                "plant_id": PLANT_IDENTIFIER,
                "method": "RandomForestRegressor",
                "predictor_mode": PREDICTOR_MODE,
                "model_ready": bool(model_ready),
                "forecast_accuracy": metrics,
                "train_samples": len(feature_buffer),
                "process_context": process,
            }
            client.publish(TOPIC_PREDICTION_LONG_NAME, json.dumps(summary_payload), qos=0)
    eval_last_process_enabled = process_enabled

def time_features(ts_epoch: float):
    """Encode time-of-day as sin/cos."""
    dt = datetime.fromtimestamp(ts_epoch)
    hour = dt.hour + dt.minute / 60.0
    rad = 2 * np.pi * (hour / 24.0)
    return np.sin(rad), np.cos(rad)

def feature_vector():
    """Build a feature vector from recent history."""
    if not power_buffer:
        return None
    p_arr = np.array(power_buffer, dtype=float)
    features = []
    def _lag_value(lag: int) -> float:
        return float(p_arr[-lag]) if len(p_arr) > lag else float(p_arr[0])

    def _roll_mean(window_size: int) -> float:
        if len(p_arr) >= window_size:
            return float(p_arr[-window_size:].mean())
        return float(p_arr.mean())

    def _roll_std(window_size: int) -> float:
        if len(p_arr) >= window_size:
            return float(p_arr[-window_size:].std())
        return float(p_arr.std())

    features.append(float(p_arr[-1]))
    features.append(_lag_value(5))
    features.append(_lag_value(60))
    features.append(_roll_mean(10))
    features.append(_roll_mean(60))
    features.append(_roll_std(60))
    features.append(_roll_mean(300))
    features.append(float(temp_buffer[-1]) if temp_buffer else 0.0)
    features.append(float(humidity_buffer[-1]) if humidity_buffer else 0.0)
    features.append(float(system_supply_v))
    sin_h, cos_h = time_features(time_buffer[-1])
    features.append(float(sin_h))
    features.append(float(cos_h))
    features.append(float(system_weekday_sin))
    features.append(float(system_weekday_cos))
    features.append(float(system_weekend_flag))
    features.append(float(system_bank_holiday_flag))
    features.append(float(system_current_energy_window_wh))
    features.append(float(system_tariff_rate_per_kwh))
    features.append(float(process_state_code))
    features.append(float(process_progress_ratio))
    features.append(float(process_cycle_count))
    features.append(float(process_tank1_level_pct))
    features.append(float(process_tank2_level_pct))
    features.append(float(process_tank1_temp_c))
    features.append(float(process_tank2_temp_c))
    features.append(float(load_on_count))
    features.append(float(load_duty_sum_all))
    features.append(float(load_fault_active_count))
    features.append(float(load_fault_latched_count))
    features.append(float(load_on_critical))
    features.append(float(load_on_essential))
    features.append(float(load_on_important))
    features.append(float(load_on_secondary))
    features.append(float(load_on_non_essential))
    for name in LOAD_FEATURE_ORDER:
        features.append(float(load_on_by_name.get(name, 0.0)))
    if len(features) != len(LONG_TERM_FEATURE_NAMES):
        raise RuntimeError(f"feature_vector length mismatch: {len(features)} != {len(LONG_TERM_FEATURE_NAMES)}")
    return features


def _feature_vector_with_load_overrides(load_metrics: Dict[str, float], power_override: Optional[float] = None):
    if not power_buffer:
        return None
    p_arr = np.array(power_buffer, dtype=float)
    if power_override is not None and len(p_arr) > 0:
        p_arr[-1] = power_override
    features = []
    def _lag_value(lag: int) -> float:
        return float(p_arr[-lag]) if len(p_arr) > lag else float(p_arr[0])
    def _roll_mean(window_size: int) -> float:
        if len(p_arr) >= window_size:
            return float(p_arr[-window_size:].mean())
        return float(p_arr.mean())
    def _roll_std(window_size: int) -> float:
        if len(p_arr) >= window_size:
            return float(p_arr[-window_size:].std())
        return float(p_arr.std())

    features.append(float(p_arr[-1]))
    features.append(_lag_value(5))
    features.append(_lag_value(60))
    features.append(_roll_mean(10))
    features.append(_roll_mean(60))
    features.append(_roll_std(60))
    features.append(_roll_mean(300))
    features.append(float(temp_buffer[-1]) if temp_buffer else 0.0)
    features.append(float(humidity_buffer[-1]) if humidity_buffer else 0.0)
    features.append(float(system_supply_v))
    sin_h, cos_h = time_features(time_buffer[-1])
    features.append(float(sin_h))
    features.append(float(cos_h))
    features.append(float(system_weekday_sin))
    features.append(float(system_weekday_cos))
    features.append(float(system_weekend_flag))
    features.append(float(system_bank_holiday_flag))
    features.append(float(system_current_energy_window_wh))
    features.append(float(system_tariff_rate_per_kwh))
    features.append(float(process_state_code))
    features.append(float(process_progress_ratio))
    features.append(float(process_cycle_count))
    features.append(float(process_tank1_level_pct))
    features.append(float(process_tank2_level_pct))
    features.append(float(process_tank1_temp_c))
    features.append(float(process_tank2_temp_c))
    features.append(float(load_metrics.get("load_on_count", load_on_count)))
    features.append(float(load_metrics.get("load_duty_sum_all", load_duty_sum_all)))
    features.append(float(load_fault_active_count))
    features.append(float(load_fault_latched_count))
    features.append(float(load_metrics.get("load_on_critical", load_on_critical)))
    features.append(float(load_metrics.get("load_on_essential", load_on_essential)))
    features.append(float(load_metrics.get("load_on_important", load_on_important)))
    features.append(float(load_metrics.get("load_on_secondary", load_on_secondary)))
    features.append(float(load_metrics.get("load_on_non_essential", load_on_non_essential)))
    for name in LOAD_FEATURE_ORDER:
        features.append(float(load_metrics.get(f"load_{name}_on_ratio", load_on_by_name.get(name, 0.0))))
    if len(features) != len(LONG_TERM_FEATURE_NAMES):
        raise RuntimeError(f"feature_vector override length mismatch: {len(features)} != {len(LONG_TERM_FEATURE_NAMES)}")
    return features

def _estimate_load_power_at_duty(current_power_w: float, current_duty: float, new_duty: float) -> float:
    if current_power_w <= 0.0:
        return max(0.0, new_duty) * current_power_w
    if current_duty <= 0.0:
        return current_power_w * max(0.0, new_duty)
    return current_power_w * max(0.0, new_duty / current_duty)

def _read_tank_bounds(process: Dict[str, Any], tank_key: str) -> Dict[str, float]:
    tank = process.get(tank_key, {}) or {}
    return {
        "level_pct": float(tank.get("level_pct", 0.0) or 0.0),
        "low_level_pct": float(tank.get("low_level_pct", process.get(f"{tank_key}_low_level_pct", 0.0)) or 0.0),
    }

def _pressure_for_state(production_pressure: Dict[str, Any], process_state: str) -> float:
    if not isinstance(production_pressure, dict):
        return float(production_pressure or 0.0)
    components = production_pressure.get("components", {}) or {}
    if process_state == "TRANSFER_1_TO_2":
        return float(components.get("tank2_level", production_pressure.get("pressure", 0.0)))
    if process_state == "TRANSFER_2_TO_1":
        return float(components.get("tank1_level", production_pressure.get("pressure", 0.0)))
    if process_state == "HEAT_TANK1":
        return float(components.get("tank1_temp", production_pressure.get("pressure", 0.0)))
    if process_state == "HEAT_TANK2":
        return float(components.get("tank2_temp", production_pressure.get("pressure", 0.0)))
    return float(production_pressure.get("pressure", 0.0))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _window_energy_to_avg_power(window_energy_wh: float, horizon_sec: Optional[float] = None) -> float:
    horizon = max(1.0, float(horizon_sec or FORECAST_HORIZON_SEC))
    return max(0.0, float(window_energy_wh)) * 3600.0 / horizon


def _read_process_tank(process: Dict[str, Any], tank_key: str) -> Dict[str, float]:
    tank = process.get(tank_key, {}) or {}
    low_key = f"{tank_key}_low_level_pct"
    high_key = f"{tank_key}_high_level_pct"
    return {
        "level_pct": _safe_float(tank.get("level_pct"), 0.0),
        "target_level_pct": _safe_float(tank.get("target_level_pct"), 0.0),
        "temperature_c": _safe_float(tank.get("temperature_c"), 0.0),
        "target_temp_c": _safe_float(tank.get("target_temp_c"), 0.0),
        "low_level_pct": _safe_float(
            tank.get("low_level_pct", process.get(low_key, process.get("tank_low_level_pct", 0.0))),
            0.0,
        ),
        "high_level_pct": _safe_float(
            tank.get("high_level_pct", process.get(high_key, process.get("tank_high_level_pct", 100.0))),
            100.0,
        ),
    }


def _process_mode_progress(mode: str, t1: Dict[str, float], t2: Dict[str, float]) -> float:
    lvl1_def = max(0.0, (t1["target_level_pct"] - t1["level_pct"]) / 100.0)
    lvl2_def = max(0.0, (t2["target_level_pct"] - t2["level_pct"]) / 100.0)
    temp1_def = 0.0 if t1["target_temp_c"] <= 0 else max(0.0, (t1["target_temp_c"] - t1["temperature_c"]) / max(1.0, t1["target_temp_c"]))
    temp2_def = 0.0 if t2["target_temp_c"] <= 0 else max(0.0, (t2["target_temp_c"] - t2["temperature_c"]) / max(1.0, t2["target_temp_c"]))
    lvl1_surplus = max(0.0, (t1["level_pct"] - t1["target_level_pct"]) / 100.0)
    lvl2_surplus = max(0.0, (t2["level_pct"] - t2["target_level_pct"]) / 100.0)

    if mode == "HEAT_TANK1":
        return temp1_def * 1.3
    if mode == "HEAT_TANK2":
        return temp2_def * 1.3
    if mode == "TRANSFER_1_TO_2":
        return (lvl2_def * 1.25) + (lvl1_surplus * 0.6) - (lvl1_def * 0.35)
    if mode == "TRANSFER_2_TO_1":
        return (lvl1_def * 1.25) + (lvl2_surplus * 0.6) - (lvl2_def * 0.35)
    return 0.0


def _blocked_process_devices(loads: Dict[str, Any]) -> set:
    blocked = set()
    for dev in PROCESS_LOAD_DEVICES:
        ld = (loads or {}).get(dev, {}) or {}
        if bool(ld.get("fault_active", False)):
            blocked.add(dev)
            continue
        current_a = _safe_float(ld.get("current_a"), 0.0)
        fault_limit = _safe_float(ld.get("fault_limit_a"), LOAD_OVERCURRENT_LIMIT_A)
        if fault_limit > 0 and current_a >= fault_limit:
            blocked.add(dev)
    return blocked


def build_mpc_process_plan_suggestions(
    loads: Dict[str, Any],
    process: Dict[str, Any],
    total_power_w: float,
    production_pressure: Dict[str, Any],
    max_total_power_w: float,
    blocked_devices: Optional[set] = None,
):
    if not MPC_PROCESS_PLANNER_ENABLED or not MPC_ENABLED or not model_ready:
        return []
    if not bool(process.get("enabled", False)):
        return []

    blocked = set(blocked_devices or set())
    process_state = str(process.get("state", "IDLE")).upper()
    t1 = _read_process_tank(process, "tank1")
    t2 = _read_process_tank(process, "tank2")

    candidates = []
    if "heater1" not in blocked:
        if t1["target_temp_c"] > 0.0 and (t1["temperature_c"] < t1["target_temp_c"] or process_state == "HEAT_TANK1"):
            candidates.append(("HEAT_TANK1", "heater1"))
    if "heater2" not in blocked:
        if t2["target_temp_c"] > 0.0 and (t2["temperature_c"] < t2["target_temp_c"] or process_state == "HEAT_TANK2"):
            candidates.append(("HEAT_TANK2", "heater2"))
    if "motor1" not in blocked:
        if t1["level_pct"] > (t1["low_level_pct"] + MPC_PROCESS_LOW_LEVEL_BUFFER_PCT) and t2["level_pct"] < (t2["high_level_pct"] - MPC_PROCESS_HIGH_LEVEL_BUFFER_PCT):
            candidates.append(("TRANSFER_1_TO_2", "motor1"))
    if "motor2" not in blocked:
        if t2["level_pct"] > (t2["low_level_pct"] + MPC_PROCESS_LOW_LEVEL_BUFFER_PCT) and t1["level_pct"] < (t1["high_level_pct"] - MPC_PROCESS_HIGH_LEVEL_BUFFER_PCT):
            candidates.append(("TRANSFER_2_TO_1", "motor2"))
    if not candidates:
        return []

    process_power_now = 0.0
    for dev in PROCESS_LOAD_DEVICES:
        process_power_now += max(0.0, _safe_float((loads or {}).get(dev, {}).get("power_w"), 0.0))

    best_plan = None
    best_score = None
    for mode, dev in candidates:
        progress = _process_mode_progress(mode, t1, t2)
        if progress <= 0.0 and mode != process_state:
            continue

        ld = (loads or {}).get(dev, {}) or {}
        cur_duty = _safe_float(ld.get("duty"), 1.0)
        cur_power = _safe_float(ld.get("power_w"), 0.0)
        duty_floor = max(MPC_MIN_EFFECTIVE_DUTY, 0.5 if dev.startswith("heater") else MPC_MIN_EFFECTIVE_DUTY)
        duty_steps = sorted(set(
            [round(float(d), 2) for d in MPC_DUTY_STEPS] +
            [round(cur_duty, 2), round(max(0.0, cur_duty - 0.1), 2), round(min(1.0, cur_duty + 0.1), 2)]
        ))

        for duty in duty_steps:
            if duty < duty_floor:
                continue

            candidate_loads = {k: dict(v) for k, v in (loads or {}).items()}
            for pdev in PROCESS_LOAD_DEVICES:
                pld = dict(candidate_loads.get(pdev, {}))
                pld["on"] = False
                pld["duty"] = 0.0
                candidate_loads[pdev] = pld

            active = dict(candidate_loads.get(dev, {}))
            active["on"] = True
            active["duty"] = duty
            candidate_loads[dev] = active

            candidate_process_power = _estimate_load_power_at_duty(cur_power, max(0.01, cur_duty), duty)
            total_power_candidate = max(0.0, total_power_w - process_power_now + candidate_process_power)
            load_metrics = _compute_load_aggregates(candidate_loads)
            feat = _feature_vector_with_load_overrides(load_metrics, power_override=total_power_candidate)
            if feat is None:
                continue
            predicted_window_energy_wh = max(0.0, float(model.predict([feat])[0]))
            predicted_power = _window_energy_to_avg_power(predicted_window_energy_wh)

            pressure = _pressure_for_state(production_pressure, mode)
            progress_weighted = max(0.0, progress) * (1.0 + 0.5 * max(0.0, pressure))
            score = (predicted_power * MPC_PROCESS_POWER_WEIGHT) - (progress_weighted * MPC_PROCESS_PROGRESS_WEIGHT)
            if mode != process_state:
                score += MPC_PROCESS_SWITCH_PENALTY
            if max_total_power_w > 0.0 and predicted_power > max_total_power_w:
                score += (predicted_power - max_total_power_w) * MPC_PROCESS_CAP_PENALTY
            if progress_weighted < MPC_PROCESS_MIN_PROGRESS and mode != process_state:
                score += 10.0

            if best_score is None or score < best_score:
                best_score = score
                best_plan = {
                    "mode": mode,
                    "device": dev,
                    "duty": duty,
                    "predicted_window_energy_wh": predicted_window_energy_wh,
                    "predicted_power_w": predicted_power,
                    "score": score,
                }

    if best_plan is None:
        return []

    out = []
    for dev in PROCESS_LOAD_DEVICES:
        if dev == best_plan["device"]:
            out.append({
                "device": dev,
                "set": {"on": True, "duty": round(best_plan["duty"], 2)},
                "action": "process_mode",
                "reason": "MPC_PROCESS_NEXT_STEP",
                "process_mode": best_plan["mode"],
                "expected_power_w": round(best_plan["predicted_power_w"], 2),
                "expected_window_energy_wh": round(best_plan["predicted_window_energy_wh"], 4),
                "score": round(best_plan["score"], 4),
            })
        else:
            out.append({
                "device": dev,
                "set": {"on": False},
                "action": "process_mode",
                "reason": "MPC_PROCESS_NEXT_STEP",
                "process_mode": best_plan["mode"],
            })
    return out

def build_mpc_suggestions(
    loads: Dict[str, Any],
    process: Dict[str, Any],
    total_power_w: float,
    production_pressure: Dict[str, Any],
    max_total_power_w: float,
):
    if not MPC_ENABLED or not model_ready:
        return []
    if not bool(process.get("enabled", False)):
        return []

    process_state = str(process.get("state", "IDLE")).upper()
    src_tank = None
    if process_state == "TRANSFER_1_TO_2":
        device = "motor1"
        src_tank = _read_tank_bounds(process, "tank1")
    elif process_state == "TRANSFER_2_TO_1":
        device = "motor2"
        src_tank = _read_tank_bounds(process, "tank2")
    elif process_state == "HEAT_TANK1":
        device = "heater1"
    elif process_state == "HEAT_TANK2":
        device = "heater2"
    else:
        return []

    ld = (loads or {}).get(device)
    if not ld:
        return []

    try:
        cur_duty = float(ld.get("duty", 1.0))
    except Exception:
        cur_duty = 1.0
    try:
        cur_power = float(ld.get("power_w", 0.0))
    except Exception:
        cur_power = 0.0
    cur_on = bool(ld.get("on", True))

    pressure = _pressure_for_state(production_pressure, process_state)
    duty_steps = sorted(set([
        round(float(d), 2) for d in MPC_DUTY_STEPS
    ] + [
        round(cur_duty, 2),
        round(max(0.0, cur_duty - 0.1), 2),
        round(min(1.0, cur_duty + 0.1), 2),
    ]))

    best = None
    best_score = None
    best_predicted = None
    best_predicted_energy = None
    for duty in duty_steps:
        if duty < MPC_MIN_EFFECTIVE_DUTY:
            continue
        candidate_loads = {k: dict(v) for k, v in (loads or {}).items()}
        candidate = dict(candidate_loads.get(device, {}))
        candidate["on"] = True
        candidate["duty"] = duty
        candidate_loads[device] = candidate

        candidate_power = _estimate_load_power_at_duty(cur_power, cur_duty, duty)
        total_power_candidate = total_power_w - (cur_power if cur_on else 0.0) + candidate_power
        load_metrics = _compute_load_aggregates(candidate_loads)
        feat = _feature_vector_with_load_overrides(load_metrics, power_override=total_power_candidate)
        if feat is None:
            continue
        predicted_window_energy_wh = max(0.0, float(model.predict([feat])[0]))
        predicted_power = _window_energy_to_avg_power(predicted_window_energy_wh)

        score = predicted_power
        score += pressure * (1.0 - duty) * MPC_PRESSURE_DUTY_PENALTY

        if src_tank is not None:
            low_level = src_tank.get("low_level_pct")
            level = src_tank.get("level_pct")
            if MPC_LOW_LEVEL_MARGIN_PCT > 0 and level is not None and low_level is not None:
                margin = max(0.0, float(MPC_LOW_LEVEL_MARGIN_PCT))
                if level <= low_level + margin:
                    factor = (low_level + margin - level) / margin if margin > 0 else 1.0
                    score += factor * max(0.0, duty - MPC_MIN_EFFECTIVE_DUTY) * MPC_LOW_LEVEL_DUTY_PENALTY

        if max_total_power_w > 0 and predicted_power > max_total_power_w:
            score += (predicted_power - max_total_power_w) * 2.0

        if best_score is None or score < best_score:
            best_score = score
            best = duty
            best_predicted = predicted_power
            best_predicted_energy = predicted_window_energy_wh

    if best is None:
        return []
    if abs(best - cur_duty) < 0.05:
        return []

    return [{
        "device": device,
        "set": {"duty": round(best, 2)},
        "action": "optimize",
        "reason": "MPC_OPTIMIZATION",
        "expected_power_w": None if best_predicted is None else round(best_predicted, 2),
        "expected_window_energy_wh": None if best_predicted_energy is None else round(best_predicted_energy, 4),
    }]

def _class_weight(cls: str) -> int:
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

def build_long_term_suggestions(
    loads: dict,
    total_power_w: float,
    predicted_power_w: float,
    max_total_power_w: float,
    current_energy_1d_wh: float,
    predicted_window_energy_wh: float,
    max_energy_1d_wh: float,
    protected_devices=None,
    production_pressure: float = 0.0,
    process_enabled: bool = False,
):
    if max_total_power_w <= 0 and max_energy_1d_wh <= 0:
        return [], "NONE", "LOW", False, False, False, 0.0, 0.0

    power_peak_risk = (predicted_power_w > (max_total_power_w * RISK_MARGIN_RATIO)) or (total_power_w > max_total_power_w)
    projected_energy_1d_wh = max(0.0, float(predicted_window_energy_wh))
    current_energy_ratio = (current_energy_1d_wh / max_energy_1d_wh) if max_energy_1d_wh > 0.0 else 0.0
    projected_energy_ratio = (projected_energy_1d_wh / max_energy_1d_wh) if max_energy_1d_wh > 0.0 else 0.0
    energy_window_pressure = (max_energy_1d_wh > 0.0) and (current_energy_1d_wh >= max_energy_1d_wh * RISK_MARGIN_RATIO)
    energy_budget_risk = (max_energy_1d_wh > 0.0) and (projected_energy_1d_wh >= max_energy_1d_wh * RISK_MARGIN_RATIO)
    peak_risk = power_peak_risk or energy_window_pressure or energy_budget_risk

    if (max_energy_1d_wh > 0.0 and (projected_energy_1d_wh > max_energy_1d_wh * 1.01 or current_energy_1d_wh > max_energy_1d_wh * 1.01)) or predicted_power_w > max_total_power_w * 1.05:
        risk_level = "HIGH"
    elif peak_risk or energy_budget_risk:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    if not (peak_risk or energy_budget_risk):
        return [], "NONE", risk_level, False, energy_window_pressure, energy_budget_risk, current_energy_ratio, projected_energy_ratio

    if energy_budget_risk or energy_window_pressure:
        reason = "ENERGY_GOAL_WINDOW"
    elif power_peak_risk:
        reason = "LONG_TERM_RISK"
    else:
        reason = "NONE"
    protected = protected_devices or set()
    candidates = []
    for name, ld in loads.items():
        if name in protected:
            continue
        if not ld.get("on", True):
            continue
        if process_enabled:
            cls = str(ld.get("class", "IMPORTANT")).upper()
            if production_pressure >= 0.8 and cls not in ("NON_ESSENTIAL",):
                continue
            if production_pressure >= 0.5 and cls not in ("NON_ESSENTIAL", "SECONDARY"):
                continue
        candidates.append((_class_weight(ld.get("class", "IMPORTANT")), int(ld.get("priority", 99)), name, ld))
    candidates.sort(reverse=True)

    suggestions = []
    for _, _, name, ld in candidates:
        if len(suggestions) >= MAX_SUGGESTIONS:
            break
        cls = str(ld.get("class", "IMPORTANT")).upper()
        cur_duty = float(ld.get("duty", 1.0))
        if cur_duty > 0.4:
            suggestions.append({
                "device": name,
                "set": {"duty": round(max(0.4, cur_duty - 0.1), 2)},
                "action": "curtail",
                "reason": reason,
                "class": cls,
                "priority": int(ld.get("priority", 99)),
            })
        elif cls != "CRITICAL":
            suggestions.append({
                "device": name,
                "set": {"on": False},
                "action": "shed",
                "reason": reason,
                "class": cls,
                "priority": int(ld.get("priority", 99)),
            })
    return suggestions, reason, risk_level, peak_risk, energy_window_pressure, energy_budget_risk, current_energy_ratio, projected_energy_ratio

def apply_config(cfg: dict):
    """Update long-term predictor settings at runtime."""
    global FORECAST_HORIZON_SEC, TELEMETRY_SAMPLE_SEC, FORECAST_HORIZON_STEPS, TRAIN_INTERVAL_SEC, TRAIN_INTERVAL_STEPS
    global MIN_TRAIN_SAMPLE_COUNT, RISK_MARGIN_RATIO, MAX_SUGGESTIONS, MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH
    global PREDICTOR_MODE, model_ready, model, warned_eval_without_model, model_meta_warning_fields
    global LOGGER_OUTPUT_DIR, BOOTSTRAP_FROM_HISTORY, BOOTSTRAP_FROM_HISTORY_ON_STARTUP, BOOTSTRAP_HISTORY_MAX_RECORDS
    global BOOTSTRAP_RANGE_START_SEC, BOOTSTRAP_RANGE_END_SEC
    global status_phase, status_message, last_status_error

    if "PREDICTOR_MODE" in cfg or "MODE" in cfg:
        PREDICTOR_MODE = _normalize_predictor_mode(cfg.get("PREDICTOR_MODE", cfg.get("MODE")))
        warned_eval_without_model = False

    if cfg.get("RESET_BUFFERS", False):
        reset_runtime_buffers()
    if cfg.get("RESET_MODEL", False):
        model = RandomForestRegressor(
            n_estimators=400,
            max_depth=12,
            min_samples_split=8,
            min_samples_leaf=4,
            max_features="sqrt",
            bootstrap=True,
            random_state=42,
            n_jobs=-1,
        )
        model_ready = False
        warned_eval_without_model = False
        model_meta_warning_fields = []
    if cfg.get("SAVE_MODEL_NOW", False) and model_ready:
        save_model_state()
    if "FORECAST_HORIZON_SEC" in cfg or "HORIZON_SEC" in cfg:
        FORECAST_HORIZON_SEC = int(cfg.get("FORECAST_HORIZON_SEC", cfg.get("HORIZON_SEC")))
    if "TELEMETRY_SAMPLE_SEC" in cfg or "SAMPLE_SEC" in cfg:
        TELEMETRY_SAMPLE_SEC = float(cfg.get("TELEMETRY_SAMPLE_SEC", cfg.get("SAMPLE_SEC")))
    if "TRAIN_INTERVAL_SEC" in cfg or "TRAIN_EVERY_SEC" in cfg:
        TRAIN_INTERVAL_SEC = int(cfg.get("TRAIN_INTERVAL_SEC", cfg.get("TRAIN_EVERY_SEC")))
    if "MIN_TRAIN_SAMPLE_COUNT" in cfg or "MIN_TRAIN_SAMPLES" in cfg:
        MIN_TRAIN_SAMPLE_COUNT = int(cfg.get("MIN_TRAIN_SAMPLE_COUNT", cfg.get("MIN_TRAIN_SAMPLES")))
    if "RISK_MARGIN_RATIO" in cfg or "RISK_MARGIN" in cfg:
        RISK_MARGIN_RATIO = max(0.1, min(1.0, float(cfg.get("RISK_MARGIN_RATIO", cfg.get("RISK_MARGIN")))))
    if "MAX_SUGGESTIONS" in cfg or "SUGGESTION_MAX" in cfg:
        MAX_SUGGESTIONS = max(1, int(cfg.get("MAX_SUGGESTIONS", cfg.get("SUGGESTION_MAX"))))
    if "MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH" in cfg or "MAX_ENERGY_CONSUMPTION_FOR_DAY_WH" in cfg or "DAILY_ENERGY_CAP_WH" in cfg:
        MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = max(
            0.0,
            float(cfg.get("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH", cfg.get("MAX_ENERGY_CONSUMPTION_FOR_DAY_WH", cfg.get("DAILY_ENERGY_CAP_WH")))),
        )
    if "LOGGER_OUTPUT_DIR" in cfg:
        LOGGER_OUTPUT_DIR = str(cfg.get("LOGGER_OUTPUT_DIR") or LOGGER_OUTPUT_DIR).strip() or LOGGER_OUTPUT_DIR
    if "BOOTSTRAP_HISTORY_MAX_RECORDS" in cfg:
        BOOTSTRAP_HISTORY_MAX_RECORDS = max(0, int(cfg.get("BOOTSTRAP_HISTORY_MAX_RECORDS", BOOTSTRAP_HISTORY_MAX_RECORDS)))
    if "BOOTSTRAP_FROM_HISTORY" in cfg:
        text = str(cfg.get("BOOTSTRAP_FROM_HISTORY")).strip().lower()
        BOOTSTRAP_FROM_HISTORY = text in {"1", "true", "yes", "on"}
    if "BOOTSTRAP_FROM_HISTORY_ON_STARTUP" in cfg:
        text = str(cfg.get("BOOTSTRAP_FROM_HISTORY_ON_STARTUP")).strip().lower()
        BOOTSTRAP_FROM_HISTORY_ON_STARTUP = text in {"1", "true", "yes", "on"}
    if "BOOTSTRAP_RANGE_FROM" in cfg:
        BOOTSTRAP_RANGE_START_SEC = _parse_optional_epoch_seconds(cfg.get("BOOTSTRAP_RANGE_FROM"))
    if "BOOTSTRAP_RANGE_TO" in cfg:
        BOOTSTRAP_RANGE_END_SEC = _parse_optional_epoch_seconds(cfg.get("BOOTSTRAP_RANGE_TO"))
    FORECAST_HORIZON_STEPS = max(1, int(FORECAST_HORIZON_SEC / TELEMETRY_SAMPLE_SEC))
    TRAIN_INTERVAL_STEPS = max(1, int(TRAIN_INTERVAL_SEC / TELEMETRY_SAMPLE_SEC))
    if cfg.get("BOOTSTRAP_FROM_HISTORY_NOW", False):
        model_ready = False
        warned_eval_without_model = False
        model_meta_warning_fields = []
        status_phase = "bootstrap_requested"
        status_message = "bootstrap_command_received"
        last_status_error = ""
        if not _start_bootstrap_thread(cfg.get("_mqtt_client")):
            status_phase = "bootstrap_running"
    _publish_status(cfg.get("_mqtt_client"))
    print("[predictor-long] config updated:")
    print(f"  - mode={PREDICTOR_MODE} train_enabled={_training_enabled()} predict_enabled={_prediction_enabled()}")
    print(f"  - horizon_sec={FORECAST_HORIZON_SEC} sample_sec={TELEMETRY_SAMPLE_SEC}")
    print(f"  - train_every_sec={TRAIN_INTERVAL_SEC} min_train_samples={MIN_TRAIN_SAMPLE_COUNT}")
    print(f"  - risk_margin={RISK_MARGIN_RATIO} max_suggestions={MAX_SUGGESTIONS} energy_cap_fallback_wh={MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH}")
    print(f"  - logger_output_dir={LOGGER_OUTPUT_DIR} bootstrap_from_history={BOOTSTRAP_FROM_HISTORY} bootstrap_on_startup={BOOTSTRAP_FROM_HISTORY_ON_STARTUP} bootstrap_records={BOOTSTRAP_HISTORY_MAX_RECORDS}")
    print(f"  - bootstrap_range_from={BOOTSTRAP_RANGE_START_SEC} bootstrap_range_to={BOOTSTRAP_RANGE_END_SEC}")

def on_connect(client, userdata, flags, reason_code, properties):
    global startup_bootstrap_requested, status_phase, status_message
    if reason_code == 0:
        print(f"[predictor-long] connected: telemetry={TOPIC_TELEMETRY_NAME} cmd={TOPIC_PREDICTOR_LONG_CMD_NAME}")
        client.subscribe(TOPIC_TELEMETRY_NAME)
        client.subscribe(TOPIC_PREDICTOR_LONG_CMD_NAME)
        _publish_status(client)
        if startup_bootstrap_requested and not model_ready:
            startup_bootstrap_requested = False
            status_phase = "bootstrap_requested"
            status_message = "startup_bootstrap_after_connect"
            _start_bootstrap_thread(client)
    else:
        print(f"[predictor-long] connect failed: reason_code={reason_code}")

def _process_telemetry_payload(data, client=None, publish: bool = True, bootstrap: bool = False):
    global model_ready, warned_eval_without_model, eval_last_process_enabled
    global process_target_tank1_level_pct, process_target_tank2_level_pct
    global process_target_tank1_temp_c, process_target_tank2_temp_c, process_goal_cycles
    global process_cycle_count, process_progress_ratio, process_state_code
    global process_tank1_level_pct, process_tank2_level_pct
    global process_tank1_temp_c, process_tank2_temp_c
    global last_trained_at, status_phase, status_message, last_status_error

    system = data.get("system", {})
    energy = data.get("energy", {})
    loads = data.get("loads", {})
    process = data.get("process", {}) or {}
    if "total_power_w" not in system:
        return
    sample_ts = parse_ts(data)
    _update_system_context(system, sample_ts)
    _update_load_aggregates(loads)
    total_power_w = float(system["total_power_w"])
    current_total_energy_wh = float(data.get("energy", {}).get("total_energy_wh", 0.0) or 0.0)
    budget = energy.get("budget", {}) or {}
    current_energy_window_wh = float(
        budget.get("consumed_window_wh", (energy.get("window_wh", {}) or {}).get("last_1d", current_total_energy_wh)) or current_total_energy_wh
    )
    env = data.get("environment", {})
    ambient_temp_c = float(env.get("temperature_c", 0.0))
    ambient_humidity = float(env.get("humidity_pct", 0.0))
    process_enabled = _update_process_context(process)
    _update_process_time_features(sample_ts, process, process_enabled)
    _update_event_context(data, energy, loads)

    if not bootstrap and PREDICTOR_MODE == "EVAL_ONLY" and client is not None:
        _handle_eval_only(client, sample_ts, process, process_enabled)

    if not bootstrap:
        _update_eval_errors(sample_ts, current_total_energy_wh)

    time_buffer.append(sample_ts)
    power_buffer.append(total_power_w)
    temp_buffer.append(ambient_temp_c)
    humidity_buffer.append(ambient_humidity)

    features = feature_vector()
    if features is None:
        return

    if _training_enabled():
        # Queue current features and pair them with a future target after horizon steps.
        pending_feature_buffer.append((features, current_total_energy_wh))
        if len(pending_feature_buffer) > FORECAST_HORIZON_STEPS:
            past_features, start_total_energy_wh = pending_feature_buffer.popleft()
            feature_buffer.append(past_features)
            target_buffer.append(max(0.0, current_total_energy_wh - float(start_total_energy_wh)))

        # Train periodically.
        if not bootstrap and len(feature_buffer) >= MIN_TRAIN_SAMPLE_COUNT and (len(feature_buffer) % TRAIN_INTERVAL_STEPS == 0):
            _train_model_from_buffers(client=client)

    if bootstrap or not _prediction_enabled():
        return
    if not model_ready:
        if not warned_eval_without_model:
            print("[predictor-long] mode requires prediction, but model is not ready. Train first or load saved weights.")
            warned_eval_without_model = True
            status_phase = "waiting_for_training"
            status_message = "prediction_requested_without_model"
            _publish_status(client)
        return

    # predict long-term horizon
    predicted_window_energy_wh = max(0.0, float(model.predict([features])[0]))
    predicted_incremental_energy_wh = predicted_window_energy_wh
    predicted_power_w = _window_energy_to_avg_power(predicted_window_energy_wh)
    predicted_total_energy_wh = current_total_energy_wh + predicted_incremental_energy_wh
    projected_energy_window_wh = predicted_window_energy_wh
    max_total_power_w = float(system.get("MAX_TOTAL_POWER_W", 0.0) or 0.0)
    max_energy_window_wh = float(
        system.get(
            "ENERGY_GOAL_VALUE_WH",
            budget.get("goal_window_wh", system.get("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH", MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH)),
        ) or 0.0
    )
    energy_goal_duration_sec = float(system.get("ENERGY_GOAL_DURATION_SEC", budget.get("window_duration_sec", 60 * 60.0)) or 60 * 60.0)
    protected_devices = set()
    if bool(process.get("enabled", False)):
        process_state = str(process.get("state", "IDLE")).upper()
        if process_state == "HEAT_TANK1":
            protected_devices.update({"heater1"})
        elif process_state == "TRANSFER_1_TO_2":
            protected_devices.update({"motor1"})
        elif process_state == "HEAT_TANK2":
            protected_devices.update({"heater2"})
        elif process_state == "TRANSFER_2_TO_1":
            protected_devices.update({"motor2"})
    production_pressure = compute_production_pressure(process)
    (
        suggestions,
        suggestion_reason,
        risk_level,
        peak_risk,
        energy_window_pressure,
        energy_budget_risk,
        current_energy_ratio,
        projected_energy_ratio,
    ) = build_long_term_suggestions(
        loads,
        total_power_w,
        predicted_power_w,
        max_total_power_w,
        current_energy_window_wh,
        predicted_window_energy_wh,
        max_energy_window_wh,
        protected_devices=protected_devices,
        production_pressure=production_pressure["pressure"],
        process_enabled=production_pressure["enabled"],
    )
    blocked_process = _blocked_process_devices(loads)
    mpc_process_suggestions = build_mpc_process_plan_suggestions(
        loads,
        process,
        total_power_w,
        production_pressure,
        max_total_power_w,
        blocked_devices=blocked_process,
    )
    if mpc_process_suggestions:
        # Process plan suggestions must be applied as a full set (all process loads).
        non_process = [s for s in suggestions if s.get("device") not in PROCESS_LOAD_DEVICES]
        keep_count = max(MAX_SUGGESTIONS, len(mpc_process_suggestions))
        suggestions = (mpc_process_suggestions + non_process)[:keep_count]
        suggestion_reason = "MPC_PROCESS_NEXT_STEP"
    else:
        mpc_suggestions = build_mpc_suggestions(
            loads,
            process,
            total_power_w,
            production_pressure,
            max_total_power_w,
        )
        if mpc_suggestions:
            existing_devices = {s.get("device") for s in suggestions}
            for item in mpc_suggestions:
                if len(suggestions) >= MAX_SUGGESTIONS:
                    break
                if item.get("device") in existing_devices:
                    continue
                suggestions.insert(0, item)
                existing_devices.add(item.get("device"))
            if suggestion_reason == "NONE":
                suggestion_reason = "MPC_OPTIMIZATION"
    fault_risk = detect_fault_risk(loads)
    if fault_risk["fault_risk_loads"]:
        for item in fault_risk["fault_risk_loads"]:
            if item["level"] != "FAULT":
                continue
            dev = item["device"]
            # Remove any conflicting "turn on" advice for the same load.
            filtered = []
            for s in suggestions:
                if s.get("device") != dev:
                    filtered.append(s)
                    continue
                set_obj = s.get("set", {}) or {}
                wants_on = bool(set_obj.get("on", False))
                wants_duty = _safe_float(set_obj.get("duty"), 0.0) > 0.0
                if wants_on or wants_duty:
                    continue
                filtered.append(s)
            suggestions = filtered
            suggestions.insert(0, {
                "device": dev,
                "set": {"on": False},
                "reason": "OVERCURRENT_FAULT",
            })
        if suggestion_reason == "NONE":
            suggestion_reason = "FAULT_RISK"
    if PREDICTOR_MODE == "EVAL_ONLY":
        pending_forecasts.append((sample_ts + FORECAST_HORIZON_SEC, predicted_window_energy_wh, current_total_energy_wh))
    payload = {
            "schema_version": "1.1",
            "producer": "long_term_predictor",
            # Use source telemetry sample time for temporal alignment across components.
            "timestamp": float(sample_ts),
            "generated_at": time.time(),
            "plant_id": PLANT_IDENTIFIER,
            "method": "RandomForestRegressor",
            "predictor_mode": PREDICTOR_MODE,
            "model_ready": bool(model_ready),
            "predicted_power_w": round(predicted_power_w, 2),
            "predicted_energy_wh": round(predicted_incremental_energy_wh, 4),
            "predicted_incremental_energy_wh": round(predicted_incremental_energy_wh, 4),
            "predicted_total_energy_wh": round(predicted_total_energy_wh, 4),
            "predicted_window_energy_wh": round(predicted_window_energy_wh, 4),
            "horizon_sec": FORECAST_HORIZON_SEC,
            "train_samples": len(feature_buffer),
            "MAX_TOTAL_POWER_W": max_total_power_w,
            "MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH": max_energy_window_wh,
            "ENERGY_GOAL_VALUE_WH": max_energy_window_wh,
            "ENERGY_GOAL_DURATION_SEC": energy_goal_duration_sec,
            "risk_margin": RISK_MARGIN_RATIO,
            "risk_level": risk_level,
            "peak_risk": bool(peak_risk),
            "energy_window_pressure": bool(energy_window_pressure),
            "energy_budget_risk": bool(energy_budget_risk),
            "current_energy_window_wh": round(current_energy_window_wh, 4),
            "projected_energy_window_wh": round(projected_energy_window_wh, 4),
            "current_energy_1d_wh": round(current_energy_window_wh, 4),
            "projected_energy_1d_wh": round(projected_energy_window_wh, 4),
            "current_energy_goal_ratio": round(current_energy_ratio, 6),
            "projected_energy_goal_ratio": round(projected_energy_ratio, 6),
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
                "process_lock_active": bool(process.get("lock_active", False)),
                "process_state": process.get("state"),
                "process_cycle_count": process.get("cycle_count", 0),
                "process_goal_cycles": process.get("goal_cycles", 0),
                "tank1_level_pct": ((process.get("tank1") or {}).get("level_pct")),
                "tank2_level_pct": ((process.get("tank2") or {}).get("level_pct")),
                "tank1_temp_c": ((process.get("tank1") or {}).get("temperature_c")),
                "tank2_temp_c": ((process.get("tank2") or {}).get("temperature_c")),
            },
            "environment_context": {
                "temperature_c": ambient_temp_c,
                "humidity_pct": ambient_humidity,
            },
            "process_context": process,
            "input_completeness": {
                "has_system": bool(system),
                "has_environment": bool(env),
                "has_loads": bool(loads),
                "has_process": bool(process),
                "load_count": len(loads),
            },
            "features": {
                "feature_version": _model_meta()["feature_version"],
                "lags": LAG_STEPS,
                "roll_windows": ROLLING_WINDOWS,
                "includes_tariff_tou_features": True,
                "uk_tou_windows": {
                    "offpeak": [UK_TARIFF_OFFPEAK_START_HOUR, UK_TARIFF_OFFPEAK_END_HOUR],
                    "peak": [UK_TARIFF_PEAK_START_HOUR, UK_TARIFF_PEAK_END_HOUR],
                },
            },
            "suggestions": suggestions,
            "suggestion_reason": suggestion_reason,
            "fault_risk": fault_risk,
            "production_pressure": production_pressure,
        }
    if PREDICTOR_MODE == "EVAL_ONLY":
        metrics = _eval_metrics()
        if metrics:
            payload["forecast_accuracy"] = metrics
    if publish and client is not None:
        client.publish(TOPIC_PREDICTION_LONG_NAME, json.dumps(payload), qos=0)
    return payload


def on_message(client, userdata, msg):
    data = json.loads(msg.payload.decode("utf-8"))
    if msg.topic == TOPIC_PREDICTOR_LONG_CMD_NAME:
        data["_mqtt_client"] = client
        apply_config(data)
        return
    if bootstrap_active:
        return
    if not processing_lock.acquire(blocking=False):
        return
    try:
        _process_telemetry_payload(data, client=client, publish=True, bootstrap=False)
    finally:
        processing_lock.release()

def main():
    global PREDICTOR_MODE, status_phase, status_message, startup_bootstrap_requested
    PREDICTOR_MODE = _normalize_predictor_mode(PREDICTOR_MODE)
    print("[predictor-long] starting long-term predictor...")
    print(f"[predictor-long] mode={PREDICTOR_MODE} train_enabled={_training_enabled()} predict_enabled={_prediction_enabled()}")
    load_model_state()
    startup_bootstrap_requested = False
    if not model_ready:
        status_phase = "waiting_for_training"
        status_message = "no_saved_model_loaded"
    client = mqtt.Client(
        client_id=f"predictor-long-{PLANT_IDENTIFIER}-{int(time.time())}",
        protocol=mqtt.MQTTv5,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )

    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    else:
        print("[predictor-long] warning: HIVEMQ_USER/HIVEMQ_PASS not set")

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
