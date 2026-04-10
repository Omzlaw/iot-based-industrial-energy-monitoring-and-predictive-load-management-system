#!/usr/bin/env python3
import io
import json
import os
import ssl
import tempfile
import threading
import time
import uuid
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

try:
    import paho.mqtt.client as mqtt
    MQTT_AVAILABLE = True
    MQTT_IMPORT_ERROR = ""
except Exception as exc:
    mqtt = None
    MQTT_AVAILABLE = False
    MQTT_IMPORT_ERROR = str(exc)

try:
    from export_chapter4_data import export_chapter4_dataset
    CHAPTER4_EXPORT_AVAILABLE = True
    CHAPTER4_EXPORT_IMPORT_ERROR = ""
except Exception as exc:
    export_chapter4_dataset = None
    CHAPTER4_EXPORT_AVAILABLE = False
    CHAPTER4_EXPORT_IMPORT_ERROR = str(exc)

try:
    from plot_chapter4_figures import generate_png_figures
    CHAPTER4_PLOTS_AVAILABLE = True
    CHAPTER4_PLOTS_IMPORT_ERROR = ""
except Exception as exc:
    generate_png_figures = None
    CHAPTER4_PLOTS_AVAILABLE = False
    CHAPTER4_PLOTS_IMPORT_ERROR = str(exc)

try:
    from generate_synthetic_industrial_telemetry import generate_dataset, parse_start
    SYNTHETIC_DATA_AVAILABLE = True
    SYNTHETIC_DATA_IMPORT_ERROR = ""
except Exception as exc:
    generate_dataset = None
    parse_start = None
    SYNTHETIC_DATA_AVAILABLE = False
    SYNTHETIC_DATA_IMPORT_ERROR = str(exc)

try:
    from offline_prediction_replay import prepare_offline_prediction_log_dir
    OFFLINE_PREDICTION_REPLAY_AVAILABLE = True
    OFFLINE_PREDICTION_REPLAY_IMPORT_ERROR = ""
except Exception as exc:
    prepare_offline_prediction_log_dir = None
    OFFLINE_PREDICTION_REPLAY_AVAILABLE = False
    OFFLINE_PREDICTION_REPLAY_IMPORT_ERROR = str(exc)

try:
    from offline_policy_replay import prepare_offline_policy_log_dir
    OFFLINE_POLICY_REPLAY_AVAILABLE = True
    OFFLINE_POLICY_REPLAY_IMPORT_ERROR = ""
except Exception as exc:
    prepare_offline_policy_log_dir = None
    OFFLINE_POLICY_REPLAY_AVAILABLE = False
    OFFLINE_POLICY_REPLAY_IMPORT_ERROR = str(exc)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:
    pass

PLANT_IDENTIFIER = os.getenv("PLANT_IDENTIFIER", "factory1").strip() or "factory1"
LOGGER_OUTPUT_DIR = (os.getenv("LOGGER_OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "telemetry_logs")).strip()
                     or os.path.join(os.path.dirname(__file__), "telemetry_logs"))
DEFAULT_MQTT_BROKER_HOST = "2d7641080ba74f7a91be1b429b265933.s1.eu.hivemq.cloud"
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", DEFAULT_MQTT_BROKER_HOST).strip() or DEFAULT_MQTT_BROKER_HOST
try:
    MQTT_BROKER_PORT = int(os.getenv("MQTT_BROKER_PORT", "8883"))
except (TypeError, ValueError):
    MQTT_BROKER_PORT = 8883
MQTT_USERNAME = os.getenv("HIVEMQ_USER")
MQTT_PASSWORD = os.getenv("HIVEMQ_PASS")
HISTORY_API_HOST = os.getenv("HISTORY_API_HOST", "0.0.0.0").strip() or "0.0.0.0"
try:
    HISTORY_API_PORT = int(os.getenv("HISTORY_API_PORT", "8090"))
except (TypeError, ValueError):
    HISTORY_API_PORT = 8090
HISTORY_API_CORS_ORIGINS = os.getenv("HISTORY_API_CORS_ORIGINS", "*").strip() or "*"
TARIFF_CONFIG_PATH = (os.getenv("TARIFF_CONFIG_PATH", os.path.join(os.path.dirname(__file__), "tariff_config.json")).strip()
                      or os.path.join(os.path.dirname(__file__), "tariff_config.json"))
TARIFF_TIMEZONE = os.getenv("TARIFF_TIMEZONE", "Europe/London").strip() or "Europe/London"
EXPERIMENT_RUNS_PATH = (os.getenv("EXPERIMENT_RUNS_PATH", os.path.join(os.path.dirname(__file__), "experiment_runs.json")).strip()
                        or os.path.join(os.path.dirname(__file__), "experiment_runs.json"))
SYNTHETIC_TELEMETRY_OUTPUT_DIR = (
    os.getenv("SYNTHETIC_TELEMETRY_OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "telemetry_logs_synthetic")).strip()
    or os.path.join(os.path.dirname(__file__), "telemetry_logs_synthetic")
)
LONG_RF_MODEL_DIR_ENV = os.getenv("LONG_RF_MODEL_DIR", "").strip()
LONG_LSTM_MODEL_DIR_ENV = os.getenv("LONG_LSTM_MODEL_DIR", "").strip()

TOPIC_TELEMETRY_NAME = f"dt/{PLANT_IDENTIFIER}/telemetry"
TOPIC_PREDICTION_NAME = f"dt/{PLANT_IDENTIFIER}/prediction"
TOPIC_PREDICTION_LONG_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long"
TOPIC_PREDICTION_LONG_LSTM_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long_lstm"
TOPIC_PREDICTOR_LONG_CMD_NAME = f"dt/{PLANT_IDENTIFIER}/predictor_long_cmd"
TOPIC_PREDICTOR_LONG_LSTM_CMD_NAME = f"dt/{PLANT_IDENTIFIER}/predictor_long_lstm_cmd"
TOPIC_SYNTHETIC_TRAINING_STATUS_NAME = f"dt/{PLANT_IDENTIFIER}/synthetic_training_status"
TOPIC_EXPERIMENT_EXPORT_STATUS_NAME = f"dt/{PLANT_IDENTIFIER}/experiment_export_status"

log_dir = Path(LOGGER_OUTPUT_DIR)
tariff_config_path = Path(TARIFF_CONFIG_PATH)
experiment_runs_path = Path(EXPERIMENT_RUNS_PATH)
models_dir = Path(os.path.join(os.path.dirname(__file__), "models"))
synthetic_telemetry_output_dir = Path(SYNTHETIC_TELEMETRY_OUTPUT_DIR).expanduser().resolve()
experiment_exports_path = experiment_runs_path.parent / "experiment_exports"
app = Flask(__name__)
synthetic_job_lock = threading.RLock()
synthetic_job_state: Dict[str, Any] = {}
experiment_export_lock = threading.RLock()

try:
    import certifi
    _CA_CERTS = certifi.where()
except Exception:
    _CA_CERTS = None

if HISTORY_API_CORS_ORIGINS == "*":
    CORS(app)
else:
    cors_origins = [origin.strip() for origin in HISTORY_API_CORS_ORIGINS.split(",") if origin.strip()]
    CORS(app, resources={r"/api/*": {"origins": cors_origins}, r"/health": {"origins": cors_origins}})


def _utc_iso(ts: Optional[float]) -> str:
    if not ts or ts <= 0:
        return ""
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _copy_synthetic_job_state() -> Dict[str, Any]:
    with synthetic_job_lock:
        return json.loads(json.dumps(synthetic_job_state)) if synthetic_job_state else {}


def _is_synthetic_job_active(state: Optional[Dict[str, Any]] = None) -> bool:
    current = state if state is not None else _copy_synthetic_job_state()
    phase = str((current or {}).get("phase") or "").lower()
    return phase in {"queued", "starting", "generating", "publishing_bootstrap", "running"}


def _set_synthetic_job_state(**updates: Any) -> Dict[str, Any]:
    with synthetic_job_lock:
        previous = dict(synthetic_job_state) if synthetic_job_state else {}
        previous.update(updates)
        previous["updated_at"] = _utc_iso(time.time())
        synthetic_job_state.clear()
        synthetic_job_state.update(previous)
        return dict(synthetic_job_state)


def _synthetic_status_payload(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    state = _copy_synthetic_job_state()
    payload = {
        "schema_version": "1.0",
        "producer": "history_api",
        "message_type": "synthetic_training_status",
        "timestamp": time.time(),
        "plant_id": PLANT_IDENTIFIER,
        "job": state,
    }
    if extra:
        payload.update(extra)
    return payload


def _normalize_export_naming_mode(value: Any, default: str = "report") -> str:
    text = str(value or default).strip().lower()
    return text if text in {"report", "standard"} else default


def _normalize_export_job(job: Optional[Dict[str, Any]], naming_mode: str) -> Dict[str, Any]:
    payload = dict(job or {})
    cache_path_text = str(payload.get("cache_path") or "").strip()
    cache_exists = False
    if cache_path_text:
        try:
            cache_exists = Path(cache_path_text).exists()
        except Exception:
            cache_exists = False
    size_raw = payload.get("size_bytes")
    try:
        size_bytes = int(size_raw)
    except Exception:
        size_bytes = 0
    progress_raw = payload.get("progress_ratio")
    try:
        progress_ratio = float(progress_raw)
    except Exception:
        progress_ratio = 0.0
    return {
        "naming_mode": _normalize_export_naming_mode(payload.get("naming_mode"), naming_mode),
        "status": str(payload.get("status") or "idle").strip().lower() or "idle",
        "phase": str(payload.get("phase") or "").strip(),
        "message": str(payload.get("message") or "").strip(),
        "progress_ratio": max(0.0, min(1.0, progress_ratio)),
        "requested_at": str(payload.get("requested_at") or ""),
        "started_at": str(payload.get("started_at") or ""),
        "finished_at": str(payload.get("finished_at") or ""),
        "updated_at": str(payload.get("updated_at") or ""),
        "archive_name": str(payload.get("archive_name") or ""),
        "download_ready": bool(payload.get("download_ready")) and cache_exists,
        "error": str(payload.get("error") or ""),
        "size_bytes": max(0, size_bytes),
    }


def _experiment_export_status_payload(run: Dict[str, Any], job: Dict[str, Any], naming_mode: str) -> Dict[str, Any]:
    normalized_job = _normalize_export_job(job, naming_mode)
    normalized_job["run_id"] = str(run.get("id") or "")
    normalized_job["template_id"] = str(run.get("template_id") or "")
    normalized_job["template_name"] = str(run.get("template_name") or "")
    return {
        "schema_version": "1.0",
        "producer": "history_api",
        "message_type": "experiment_export_status",
        "timestamp": time.time(),
        "plant_id": PLANT_IDENTIFIER,
        "run_id": str(run.get("id") or ""),
        "template_id": str(run.get("template_id") or ""),
        "template_name": str(run.get("template_name") or ""),
        "naming_mode": _normalize_export_naming_mode(naming_mode, "report"),
        "job": normalized_job,
    }


def _publish_experiment_export_status(client, run: Dict[str, Any], job: Dict[str, Any], naming_mode: str) -> None:
    try:
        _publish_mqtt_json(
            client,
            TOPIC_EXPERIMENT_EXPORT_STATUS_NAME,
            _experiment_export_status_payload(run, job, naming_mode),
            retain=True,
            qos=1,
        )
    except Exception:
        pass


def _connect_mqtt_client(client_id_prefix: str):
    if not MQTT_AVAILABLE or mqtt is None:
        raise RuntimeError(f"mqtt_unavailable:{MQTT_IMPORT_ERROR or 'paho_mqtt_not_installed'}")
    connected = threading.Event()
    failure: Dict[str, str] = {"error": ""}
    client = mqtt.Client(
        client_id=f"{client_id_prefix}-{PLANT_IDENTIFIER}-{uuid.uuid4().hex[:8]}",
        protocol=mqtt.MQTTv5,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )

    if MQTT_USERNAME and MQTT_PASSWORD:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    if _CA_CERTS:
        client.tls_set(
            ca_certs=_CA_CERTS,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
    else:
        client.tls_set_context(ssl.create_default_context())

    def _on_connect(_client, _userdata, _flags, reason_code, _properties):
        if reason_code == 0:
            connected.set()
        else:
            failure["error"] = f"connect_failed:{reason_code}"
            connected.set()

    client.on_connect = _on_connect
    client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    client.loop_start()
    if not connected.wait(timeout=10.0):
        client.loop_stop()
        client.disconnect()
        raise RuntimeError("connect_timeout")
    if failure["error"]:
        client.loop_stop()
        client.disconnect()
        raise RuntimeError(failure["error"])
    return client


def _publish_mqtt_json(client, topic: str, payload: Dict[str, Any], *, retain: bool = False, qos: int = 1) -> None:
    if client is None:
        return
    info = client.publish(topic, json.dumps(payload), qos=qos, retain=retain)
    try:
        info.wait_for_publish(timeout=5.0)
    except TypeError:
        info.wait_for_publish()
    if hasattr(info, "rc") and int(getattr(info, "rc", 0)) != 0:
        raise RuntimeError(f"mqtt_publish_failed:{topic}:{getattr(info, 'rc', 'unknown')}")
    if hasattr(info, "is_published") and not info.is_published():
        raise RuntimeError(f"mqtt_publish_timeout:{topic}")


def _publish_synthetic_job_status(client, **extra: Any) -> None:
    try:
        _publish_mqtt_json(client, TOPIC_SYNTHETIC_TRAINING_STATUS_NAME, _synthetic_status_payload(extra if extra else None), retain=True, qos=1)
    except Exception:
        pass


def _count_synthetic_telemetry_files(path: Path) -> int:
    try:
        return sum(1 for _ in Path(path).glob(f"{PLANT_IDENTIFIER}_telemetry_*.jsonl"))
    except Exception:
        return 0


def _load_json_file(path: Path) -> Optional[Dict[str, Any]]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _count_jsonl_records(paths: List[Path]) -> int:
    total = 0
    for path in paths:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for _ in handle:
                    total += 1
        except Exception:
            continue
    return total


def _extract_logged_topic_payload(record: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    topic = str(record.get("topic") or "")
    payload = record.get("payload")
    if isinstance(payload, dict):
        return topic, payload
    if topic and isinstance(record.get("system"), dict):
        return topic, record
    return topic, None


def _history_log_paths_for_range(start_ms: int, end_ms: int) -> List[Path]:
    paths: List[Path] = []
    for day in _iter_days(start_ms, end_ms):
        path = log_dir / f"{PLANT_IDENTIFIER}_telemetry_{day.isoformat()}.jsonl"
        if path.exists():
            paths.append(path)
    return paths


def _log_dir_paths_for_range(source_log_dir: Path, start_ms: int, end_ms: int) -> List[Path]:
    paths: List[Path] = []
    for day in _iter_days(start_ms, end_ms):
        path = Path(source_log_dir) / f"{PLANT_IDENTIFIER}_telemetry_{day.isoformat()}.jsonl"
        if path.exists():
            paths.append(path)
    return paths


def _count_history_telemetry_records(start_ms: int, end_ms: int) -> int:
    total = 0
    for path in _history_log_paths_for_range(start_ms, end_ms):
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        record = json.loads(text)
                    except Exception:
                        continue
                    topic, payload = _extract_logged_topic_payload(record if isinstance(record, dict) else {})
                    if topic != TOPIC_TELEMETRY_NAME or not isinstance(payload, dict):
                        continue
                    system = payload.get("system") or {}
                    ts_ms = (
                        _parse_epoch_ms(payload.get("timestamp"))
                        or _parse_epoch_ms(system.get("timestamp"))
                        or _parse_epoch_ms(record.get("logged_at_utc"))
                    )
                    if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                        continue
                    total += 1
        except Exception:
            continue
    return total


def _scan_log_dir_window(source_log_dir: Path) -> Tuple[Optional[int], Optional[int]]:
    min_ts: Optional[int] = None
    max_ts: Optional[int] = None
    try:
        paths = sorted(Path(source_log_dir).glob(f"{PLANT_IDENTIFIER}_telemetry_*.jsonl"))
    except Exception:
        return None, None
    for path in paths:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        record = json.loads(text)
                    except Exception:
                        continue
                    topic, payload = _extract_logged_topic_payload(record if isinstance(record, dict) else {})
                    if topic != TOPIC_TELEMETRY_NAME or not isinstance(payload, dict):
                        continue
                    system = payload.get("system") or {}
                    ts_ms = (
                        _parse_epoch_ms(payload.get("timestamp"))
                        or _parse_epoch_ms(system.get("timestamp"))
                        or _parse_epoch_ms(record.get("logged_at_utc"))
                    )
                    if ts_ms is None:
                        continue
                    if min_ts is None or ts_ms < min_ts:
                        min_ts = ts_ms
                    if max_ts is None or ts_ms > max_ts:
                        max_ts = ts_ms
        except Exception:
            continue
    return min_ts, max_ts


def _resolve_experiment_source_mode(value: Any, default: str = "live") -> str:
    text = str(value or default).strip().lower()
    return text if text in {"live", "synthetic", "history"} else default


def _resolve_offline_experiment_source(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    source_mode = _resolve_experiment_source_mode(payload.get("source_mode"), "live")
    if source_mode == "live":
        return None

    if source_mode == "synthetic":
        dataset_dir = _resolve_existing_synthetic_dir(payload.get("existing_output_dir"))
        if dataset_dir is None:
            raise ValueError("invalid_synthetic_dataset")
        start_ms, end_ms = _scan_log_dir_window(dataset_dir)
        if start_ms is None or end_ms is None or end_ms < start_ms:
            raise ValueError("synthetic_dataset_not_exportable")
        info = _synthetic_dataset_info(dataset_dir) or {}
        return {
            "source_mode": "synthetic",
            "export_log_dir": str(dataset_dir),
            "export_start_ms": int(start_ms),
            "export_end_ms": int(end_ms),
            "source_label": str(info.get("name") or dataset_dir.name),
            "source_summary": info.get("summary"),
        }

    start_ms = _parse_epoch_ms(payload.get("history_from"))
    end_ms = _parse_epoch_ms(payload.get("history_to"))
    if start_ms is None or end_ms is None or end_ms <= start_ms:
        raise ValueError("invalid_history_export_range")
    if not _history_log_paths_for_range(start_ms, end_ms):
        raise ValueError("no_history_files_in_range")
    return {
        "source_mode": "history",
        "export_log_dir": str(log_dir),
        "export_start_ms": int(start_ms),
        "export_end_ms": int(end_ms),
        "source_label": "live_history_logs",
        "source_summary": None,
    }


def _synthetic_dataset_info(dataset_dir: Path) -> Optional[Dict[str, Any]]:
    dataset_dir = dataset_dir.resolve()
    if not dataset_dir.exists() or not dataset_dir.is_dir():
        return None
    try:
        dataset_dir.relative_to(synthetic_telemetry_output_dir.resolve())
    except Exception:
        return None

    summary_path = dataset_dir / "synthetic_dataset_summary.json"
    summary = _load_json_file(summary_path) if summary_path.exists() else None
    jsonl_files = sorted(dataset_dir.glob(f"{PLANT_IDENTIFIER}_telemetry_*.jsonl"))
    if not jsonl_files and summary is None:
        return None

    records_written = int(summary.get("records_written") or 0) if summary else 0
    if records_written <= 0 and jsonl_files:
        records_written = _count_jsonl_records(jsonl_files)

    mtimes = [path.stat().st_mtime for path in jsonl_files if path.exists()]
    if summary_path.exists():
        mtimes.append(summary_path.stat().st_mtime)
    modified_at = max(mtimes) if mtimes else dataset_dir.stat().st_mtime

    return {
        "name": dataset_dir.name,
        "path": str(dataset_dir),
        "record_count": records_written,
        "file_count": len(jsonl_files),
        "modified_at": _utc_iso(modified_at),
        "summary": summary,
        "start_utc": (summary or {}).get("start_utc", ""),
        "days": int((summary or {}).get("days") or 0),
        "step_sec": int((summary or {}).get("step_sec") or 0),
        "synthetic_profile": (summary or {}).get("synthetic_profile", ""),
    }


def _list_synthetic_datasets(limit: int = 50) -> List[Dict[str, Any]]:
    root = synthetic_telemetry_output_dir.resolve()
    if not root.exists() or not root.is_dir():
        return []
    datasets: List[Dict[str, Any]] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        info = _synthetic_dataset_info(child)
        if info is not None:
            datasets.append(info)
    datasets.sort(key=lambda item: str(item.get("modified_at") or ""), reverse=True)
    return datasets[: max(1, min(limit, 200))]


def _resolve_existing_synthetic_dir(raw_value: Any) -> Optional[Path]:
    text = str(raw_value or "").strip()
    if not text:
        return None
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = synthetic_telemetry_output_dir / candidate
    try:
        resolved = candidate.resolve()
        resolved.relative_to(synthetic_telemetry_output_dir.resolve())
    except Exception:
        return None
    return resolved if resolved.exists() and resolved.is_dir() else None


def _env_number(*keys: str, default: float) -> float:
    for key in keys:
        raw = os.getenv(key)
        if raw is None:
            continue
        try:
            value = float(str(raw).strip())
            if value == value:
                return value
        except Exception:
            continue
    return default


def _env_bool(*keys: str, default: bool) -> bool:
    for key in keys:
        raw = os.getenv(key)
        if raw is None:
            continue
        text = str(raw).strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
    return default


def _normalize_minute_of_day(value: Any, fallback: int = 0) -> int:
    try:
        number = int(round(float(value)))
    except Exception:
        return fallback
    return ((number % 1440) + 1440) % 1440


def _normalize_nonnegative_float(value: Any, fallback: float = 0.0) -> float:
    try:
        number = float(value)
        if number != number:
            return fallback
        return max(0.0, number)
    except Exception:
        return fallback


def _normalize_positive_int(value: Any, fallback: int = 1) -> int:
    try:
        number = int(round(float(value)))
    except Exception:
        return fallback
    return max(1, number)


def _iso_utc_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


DEFAULT_TARIFF_VERSION = {
    "effective_from": "1970-01-01T00:00:00+00:00",
    "timezone": TARIFF_TIMEZONE,
    "use_dashboard_tou": _env_bool("TARIFF_USE_UK_TOU", "VITE_TARIFF_USE_UK_TOU", default=True),
    "offpeak_start_min": _normalize_minute_of_day(
        _env_number("TARIFF_UK_OFFPEAK_START_HOUR", "VITE_TARIFF_UK_OFFPEAK_START_HOUR", default=0) * 60,
        0,
    ),
    "offpeak_end_min": _normalize_minute_of_day(
        _env_number("TARIFF_UK_OFFPEAK_END_HOUR", "VITE_TARIFF_UK_OFFPEAK_END_HOUR", default=7) * 60,
        7 * 60,
    ),
    "peak_start_min": _normalize_minute_of_day(
        _env_number("TARIFF_UK_PEAK_START_HOUR", "VITE_TARIFF_UK_PEAK_START_HOUR", default=16) * 60,
        16 * 60,
    ),
    "peak_end_min": _normalize_minute_of_day(
        _env_number("TARIFF_UK_PEAK_END_HOUR", "VITE_TARIFF_UK_PEAK_END_HOUR", default=20) * 60,
        20 * 60,
    ),
    "offpeak_rate_per_kwh": _normalize_nonnegative_float(
        _env_number("TARIFF_OFFPEAK_PER_KWH", "VITE_TARIFF_OFFPEAK_PER_KWH", default=0.12),
        0.12,
    ),
    "midpeak_rate_per_kwh": _normalize_nonnegative_float(
        _env_number("TARIFF_MIDPEAK_PER_KWH", "VITE_TARIFF_MIDPEAK_PER_KWH", default=0.18),
        0.18,
    ),
    "peak_rate_per_kwh": _normalize_nonnegative_float(
        _env_number("TARIFF_PEAK_PER_KWH", "VITE_TARIFF_PEAK_PER_KWH", default=0.28),
        0.28,
    ),
    "demand_charge_per_kw": _normalize_nonnegative_float(
        _env_number("TARIFF_DEMAND_CHARGE_PER_KW", "VITE_TARIFF_DEMAND_CHARGE_PER_KW", default=0.0),
        0.0,
    ),
    "demand_interval_min": _normalize_positive_int(
        _env_number("TARIFF_DEMAND_INTERVAL_MIN", "VITE_TARIFF_DEMAND_INTERVAL_MIN", default=30),
        30,
    ),
    "weekends_offpeak": True,
    "bank_holidays_offpeak": True,
}

POLICY_OPTIONS = {"RULE_ONLY", "HYBRID", "AI_PREFERRED", "NO_ENERGY_MANAGEMENT"}
PREDICTOR_MODE_OPTIONS = {"TRAIN_ONLY", "EVAL_ONLY", "ONLINE", "OFFLINE"}
RUN_STATUS_OPTIONS = {"created", "running", "completed", "failed", "cancelled"}


def _default_experiment_store() -> Dict[str, Any]:
    return {
        "plant_id": PLANT_IDENTIFIER,
        "path": str(experiment_runs_path),
        "runs": [],
    }


EXPERIMENT_LOAD_OPTIONS = ["motor1", "motor2", "heater1", "heater2", "lighting1", "lighting2"]
REPORT_EXPORT_SPECIAL_NAMES = {
    "figure_4_13_4_14_combined.png": "priority_curtailment_and_demand_response_combined.png",
}
MODEL_FILE_GROUPS = {
    "rf": [
        "{plant}_long_rf.pkl",
        "{plant}_long_rf.meta.json",
        "{plant}_long_rf.analytics.json",
    ],
    "lstm": [
        "{plant}_long_lstm.pt",
        "{plant}_long_lstm.meta.json",
        "{plant}_long_lstm.analytics.json",
        "{plant}_long_lstm_x_scaler.pkl",
        "{plant}_long_lstm_y_scaler.pkl",
    ],
}
PRIMARY_MODEL_FILENAMES = {
    "rf": "{plant}_long_rf.pkl",
    "lstm": "{plant}_long_lstm.pt",
}
MODEL_DIR_ENV_MAP = {
    "rf": LONG_RF_MODEL_DIR_ENV,
    "lstm": LONG_LSTM_MODEL_DIR_ENV,
}
MODEL_SERVICE_DIR_HINTS = {
    "rf": ("long", "long-rf", "predictor-long-rf", "rf"),
    "lstm": ("lstm", "long-lstm", "predictor-long-lstm"),
}


def _normalize_experiment_load_name(value: Any, default: str = "motor1") -> str:
    text = str(value or default).strip().lower()
    aliases = {
        "light1": "lighting1",
        "light2": "lighting2",
    }
    text = aliases.get(text, text)
    return text if text in EXPERIMENT_LOAD_OPTIONS else default


def _standard_export_file_name(filename: str) -> str:
    special = REPORT_EXPORT_SPECIAL_NAMES.get(filename)
    if special:
        return special
    parts = filename.split("_")
    if len(parts) >= 3 and parts[0] in {"figure", "table"}:
        idx = 1
        while idx < len(parts) - 1 and parts[idx].isdigit():
            idx += 1
        remainder = "_".join(parts[idx:])
        if remainder:
            return remainder
    return filename


def _archive_relative_path(path: Path, run_dir: Path, naming_mode: str) -> str:
    relative = path.relative_to(run_dir)
    if naming_mode != "standard":
        return str(relative)
    if relative.parts and relative.parts[0] in {"png_figures", "model_analytics"} and len(relative.parts) >= 2:
        renamed = _standard_export_file_name(relative.name)
        return str(Path(relative.parts[0]) / renamed)
    if len(relative.parts) == 1:
        return _standard_export_file_name(relative.name)
    return str(relative)


def _model_file_paths(model_type: Optional[str] = None) -> List[Path]:
    selected_types = [model_type] if model_type in MODEL_FILE_GROUPS else list(MODEL_FILE_GROUPS.keys())
    paths: List[Path] = []
    seen = set()
    for selected in selected_types:
        model_dir = _resolve_model_dir(selected)
        for pattern in MODEL_FILE_GROUPS.get(selected, []):
            path = model_dir / pattern.format(plant=PLANT_IDENTIFIER)
            if str(path) in seen:
                continue
            seen.add(str(path))
            paths.append(path)
    return paths


def _model_search_roots() -> List[Path]:
    script_dir = Path(__file__).resolve().parent
    cwd = Path.cwd().resolve()
    roots: List[Path] = []
    for candidate in [
        script_dir,
        cwd,
        script_dir.parent,
        cwd.parent,
        script_dir.parent.parent,
        cwd.parent.parent,
        script_dir.parent.parent.parent,
        cwd.parent.parent.parent,
    ]:
        if candidate.exists() and candidate.is_dir() and candidate not in roots:
            roots.append(candidate)
    return roots


def _discover_model_dir(model_type: str) -> Optional[Path]:
    env_dir = MODEL_DIR_ENV_MAP.get(model_type)
    if env_dir:
        candidate = Path(env_dir).expanduser().resolve()
        return candidate

    primary_name = PRIMARY_MODEL_FILENAMES.get(model_type, "").format(plant=PLANT_IDENTIFIER)
    if not primary_name:
        return models_dir.resolve()

    direct_candidates: List[Path] = []
    for root in _model_search_roots():
        direct = root / "models"
        if direct not in direct_candidates:
            direct_candidates.append(direct)
        shared_models = root / "shared" / "models"
        if shared_models not in direct_candidates:
            direct_candidates.append(shared_models)
        current_models = root / "current" / "models"
        if current_models not in direct_candidates:
            direct_candidates.append(current_models)
        for service_name in MODEL_SERVICE_DIR_HINTS.get(model_type, ()):
            for candidate in (
                root / service_name / "shared" / "models",
                root / service_name / "current" / "models",
                root / service_name / "models",
            ):
                if candidate not in direct_candidates:
                    direct_candidates.append(candidate)
    for candidate in direct_candidates:
        if (candidate / primary_name).exists():
            return candidate.resolve()

    for root in _model_search_roots():
        try:
            for match in root.rglob(primary_name):
                if match.is_file():
                    return match.parent.resolve()
        except Exception:
            continue
    return models_dir.resolve()


def _resolve_model_dir(model_type: str) -> Path:
    discovered = _discover_model_dir(model_type)
    return discovered if discovered is not None else models_dir.resolve()


def _list_saved_models() -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for model_type, patterns in MODEL_FILE_GROUPS.items():
        model_dir = _resolve_model_dir(model_type)
        for pattern in patterns:
            path = model_dir / pattern.format(plant=PLANT_IDENTIFIER)
            items.append(
                {
                    "model_type": model_type,
                    "name": path.name,
                    "path": str(path),
                    "resolved_dir": str(model_dir),
                    "exists": path.exists(),
                    "size_bytes": path.stat().st_size if path.exists() else 0,
                    "modified_at": (
                        datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
                        if path.exists()
                        else None
                    ),
                }
            )
    return items


def _delete_saved_models(model_type: Optional[str] = None) -> List[str]:
    deleted: List[str] = []
    for path in _model_file_paths(model_type):
        if not path.exists():
            continue
        path.unlink()
        deleted.append(path.name)
    return deleted


def _normalize_policy(value: Any, default: str) -> str:
    text = str(value or default).strip().upper()
    return text if text in POLICY_OPTIONS else default


def _normalize_predictor_mode(value: Any, default: str) -> str:
    text = str(value or default).strip().upper()
    return text if text in PREDICTOR_MODE_OPTIONS else default


def _load_set_command(
    device: str,
    *,
    on: Optional[bool] = None,
    duty: Optional[float] = None,
    override: Optional[bool] = None,
    fault_limit_a: Optional[float] = None,
    inject_current_a: Optional[float] = None,
    clear_inject: bool = False,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"device": str(device), "set": {}}
    set_obj = payload["set"]
    if on is not None:
        set_obj["on"] = bool(on)
    if duty is not None:
        set_obj["duty"] = float(duty)
    if override is not None:
        set_obj["override"] = bool(override)
    if fault_limit_a is not None:
        set_obj["fault_limit_a"] = float(fault_limit_a)
    if inject_current_a is not None:
        set_obj["inject_current_a"] = float(inject_current_a)
    if clear_inject:
        set_obj["clear_inject"] = True
    return payload


def _all_load_reset_commands(*, clear_inject: bool = False) -> List[Dict[str, Any]]:
    return [
        _load_set_command(device, on=False, duty=0.0, override=False, clear_inject=clear_inject)
        for device in EXPERIMENT_LOAD_OPTIONS
    ]


def _experiment_templates() -> List[Dict[str, Any]]:
    return [
        {
            "id": "sensor_validation",
            "name": "Sensor Validation",
            "description": "Runs sequential full-load and staged multi-load checks for electrical validation and supply-voltage behaviour.",
            "notes": [
                "Best for Table 4.1, Table 4.2, Figure 4.1, and Figure 4.2.",
                "Table 4.1 still needs datasheet values added manually in the report.",
            ],
            "recommended_outputs": [
                "table_4_1_per_load_full_load.csv",
                "table_4_2_supply_voltage_behaviour.csv",
                "figure_4_1_load_voltage_vs_load_current.csv",
                "figure_4_2_supply_voltage_vs_total_current.csv",
            ],
            "defaults": {
                "single_load_duration_sec": 30,
                "stage_duration_sec": 45,
                "predictor_mode": "OFFLINE",
                "max_total_power_w": None,
                "heater_hot_off_temp_c": 80.0,
            },
            "fields": [
                {"name": "single_load_duration_sec", "label": "Single-Load Hold (s)", "type": "number", "min": 5, "step": 1},
                {"name": "stage_duration_sec", "label": "Low/Medium/High Hold (s)", "type": "number", "min": 5, "step": 1},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "heater_hot_off_temp_c", "label": "Heater Hot-Off Temp (C)", "type": "number", "min": 0, "step": 0.1},
            ],
        },
        {
            "id": "policy_comparison",
            "name": "Policy Comparison",
            "description": "Runs the four main control policies sequentially and records one export window for policy-performance comparison.",
            "notes": [
                "Best for Figure 4.18, Figure 4.19, Figure 4.20, and Table 16.",
                "For physical hardware, initial tank/thermal state is not reset automatically between policy phases.",
            ],
            "recommended_outputs": [
                "table_16_policy_performance_comparison.csv",
                "figure_4_18_power_profiles_under_policies.csv",
                "figure_4_19_peak_demand_comparison.csv",
                "figure_4_20_energy_consumption_comparison.csv",
            ],
            "defaults": {
                "policy_duration_sec": 600,
                "cooldown_sec": 30,
                "start_process": True,
                "restart_process_each_policy": True,
                "predictor_mode": "ONLINE",
                "max_total_power_w": None,
                "energy_goal_wh": None,
                "energy_goal_duration_min": None,
            },
            "fields": [
                {"name": "policy_duration_sec", "label": "Duration per Policy (s)", "type": "number", "min": 30, "step": 1},
                {"name": "cooldown_sec", "label": "Cooldown Between Policies (s)", "type": "number", "min": 0, "step": 1},
                {"name": "start_process", "label": "Start Process for Each Policy", "type": "boolean"},
                {"name": "restart_process_each_policy", "label": "Restart Process Each Policy", "type": "boolean"},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "energy_goal_wh", "label": "Energy Goal Window (Wh)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "energy_goal_duration_min", "label": "Energy Goal Duration (min)", "type": "number", "min": 1, "step": 1, "optional": True},
            ],
        },
        {
            "id": "ambient_monitoring",
            "name": "Ambient Monitoring",
            "description": "Captures ambient temperature and humidity over a controlled idle window.",
            "notes": [
                "Best for Figure 4.4.",
                "Keeps loads off and the process stopped so the ambient trace is easy to interpret.",
            ],
            "recommended_outputs": [
                "figure_4_4_ambient_temp_humidity_vs_time.csv",
            ],
            "defaults": {
                "run_duration_sec": 600,
                "predictor_mode": "OFFLINE",
            },
            "fields": [
                {"name": "run_duration_sec", "label": "Run Duration (s)", "type": "number", "min": 30, "step": 1},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
            ],
        },
        {
            "id": "prediction_comparison",
            "name": "Prediction Comparison",
            "description": "Runs one stable scenario for aligned EMA, RF, and LSTM prediction-vs-actual exports.",
            "notes": [
                "Best for Figure 4.10, Figure 4.11, Figure 4.12, and Table 4.3.",
                "Use a repeatable load/process scenario so all three models see the same demand window.",
            ],
            "recommended_outputs": [
                "table_4_3_model_performance_metrics.csv",
                "figure_4_10_ema_prediction_vs_actual.csv",
                "figure_4_11_rf_prediction_vs_actual.csv",
                "figure_4_12_lstm_prediction_vs_actual.csv",
            ],
            "defaults": {
                "control_policy": "NO_ENERGY_MANAGEMENT",
                "run_duration_sec": 900,
                "start_process": True,
                "restart_process": True,
                "predictor_mode": "ONLINE",
                "max_total_power_w": None,
                "energy_goal_wh": None,
                "energy_goal_duration_min": None,
            },
            "fields": [
                {"name": "control_policy", "label": "Control Policy", "type": "select", "options": sorted(POLICY_OPTIONS)},
                {"name": "run_duration_sec", "label": "Run Duration (s)", "type": "number", "min": 30, "step": 1},
                {"name": "start_process", "label": "Start Process", "type": "boolean"},
                {"name": "restart_process", "label": "Restart Process", "type": "boolean"},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "energy_goal_wh", "label": "Energy Goal Window (Wh)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "energy_goal_duration_min", "label": "Energy Goal Duration (min)", "type": "number", "min": 1, "step": 1, "optional": True},
            ],
        },
        {
            "id": "heating_cycle",
            "name": "Heating Cycle",
            "description": "Runs one heating/process window for temperature-response evidence.",
            "notes": [
                "Best for Figure 4.3 and heating-related discussion.",
            ],
            "recommended_outputs": [
                "figure_4_3_temperature_vs_time_heating.csv",
            ],
            "defaults": {
                "run_duration_sec": 900,
                "restart_process": True,
                "predictor_mode": "ONLINE",
                "max_total_power_w": None,
            },
            "fields": [
                {"name": "run_duration_sec", "label": "Run Duration (s)", "type": "number", "min": 30, "step": 1},
                {"name": "restart_process", "label": "Restart Process", "type": "boolean"},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0, "step": 0.01, "optional": True},
            ],
        },
        {
            "id": "ultrasonic_validation",
            "name": "Ultrasonic Validation",
            "description": "Runs a process window to capture tank level, ultrasonic distance, and process timing for level-validation plots.",
            "notes": [
                "Best for Figure 4.5.",
                "Manual actual-level measurements are still needed if the report compares against ground truth.",
            ],
            "recommended_outputs": [
                "figure_4_5_ultrasonic_vs_level.csv",
            ],
            "defaults": {
                "run_duration_sec": 900,
                "restart_process": True,
                "control_policy": "NO_ENERGY_MANAGEMENT",
                "predictor_mode": "ONLINE",
                "max_total_power_w": None,
                "heater_hot_off_temp_c": 80.0,
            },
            "fields": [
                {"name": "run_duration_sec", "label": "Run Duration (s)", "type": "number", "min": 30, "step": 1},
                {"name": "restart_process", "label": "Restart Process", "type": "boolean"},
                {"name": "control_policy", "label": "Control Policy", "type": "select", "options": sorted(POLICY_OPTIONS)},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "heater_hot_off_temp_c", "label": "Heater Hot-Off Temp (C)", "type": "number", "min": 0, "step": 0.1},
            ],
        },
        {
            "id": "load_behavior",
            "name": "Load Behaviour",
            "description": "Runs a scripted single- and multi-load schedule to capture per-load power, total power, and cumulative energy trends.",
            "notes": [
                "Best for Figure 4.6, Figure 4.7, Figure 4.8, and Figure 4.9.",
                "The schedule includes both binary heater stages and PWM-style motor/light stages.",
            ],
            "recommended_outputs": [
                "figure_4_6_power_vs_time_per_load.csv",
                "figure_4_7_total_system_power.csv",
                "figure_4_8_power_profiles_by_load_type.csv",
                "figure_4_9_cumulative_energy_over_time.csv",
            ],
            "defaults": {
                "stage_duration_sec": 45,
                "control_policy": "NO_ENERGY_MANAGEMENT",
                "predictor_mode": "OFFLINE",
                "max_total_power_w": None,
                "heater_hot_off_temp_c": 80.0,
            },
            "fields": [
                {"name": "stage_duration_sec", "label": "Stage Duration (s)", "type": "number", "min": 5, "step": 1},
                {"name": "control_policy", "label": "Control Policy", "type": "select", "options": sorted(POLICY_OPTIONS)},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "heater_hot_off_temp_c", "label": "Heater Hot-Off Temp (C)", "type": "number", "min": 0, "step": 0.1},
            ],
        },
        {
            "id": "priority_curtailment",
            "name": "Priority Curtailment",
            "description": "Builds demand in stages under rule-based control to show threshold response and priority-based shedding.",
            "notes": [
                "Best for the combined Figure 4.13 / Figure 4.14 discussion.",
                "Uses rule-only control by default so the curtailment sequence is easier to interpret.",
            ],
            "recommended_outputs": [
                "figure_4_13_load_states_priority_curtailment.csv",
                "figure_4_14_total_demand_with_control_response.csv",
            ],
            "defaults": {
                "stage_duration_sec": 45,
                "control_policy": "RULE_ONLY",
                "predictor_mode": "OFFLINE",
                "max_total_power_w": 15.0,
                "heater_hot_off_temp_c": 80.0,
            },
            "fields": [
                {"name": "stage_duration_sec", "label": "Stage Duration (s)", "type": "number", "min": 5, "step": 1},
                {"name": "control_policy", "label": "Control Policy", "type": "select", "options": sorted(POLICY_OPTIONS)},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0.1, "step": 0.01},
                {"name": "heater_hot_off_temp_c", "label": "Heater Hot-Off Temp (C)", "type": "number", "min": 0, "step": 0.1},
            ],
        },
        {
            "id": "voltage_protection",
            "name": "Voltage Protection",
            "description": "Injects undervoltage and overvoltage scenarios with controlled hold and recovery periods.",
            "notes": [
                "Best for Figure 4.15 and voltage protection event analysis.",
            ],
            "recommended_outputs": [
                "figure_4_15_voltage_drop_with_control_response.csv",
                "voltage_protection_events.csv",
            ],
            "defaults": {
                "undervoltage_magnitude_v": 1.0,
                "undervoltage_hold_sec": 30,
                "recovery_sec": 20,
                "overvoltage_magnitude_v": 1.0,
                "overvoltage_hold_sec": 30,
            },
            "fields": [
                {"name": "undervoltage_magnitude_v", "label": "Undervoltage Magnitude (V)", "type": "number", "min": 0.01, "step": 0.01},
                {"name": "undervoltage_hold_sec", "label": "Undervoltage Hold (s)", "type": "number", "min": 1, "step": 1},
                {"name": "recovery_sec", "label": "Recovery Delay (s)", "type": "number", "min": 0, "step": 1},
                {"name": "overvoltage_magnitude_v", "label": "Overvoltage Magnitude (V)", "type": "number", "min": 0.01, "step": 0.01},
                {"name": "overvoltage_hold_sec", "label": "Overvoltage Hold (s)", "type": "number", "min": 1, "step": 1},
            ],
        },
        {
            "id": "overcurrent_response",
            "name": "Overcurrent Response",
            "description": "Injects a load-level overcurrent condition and captures fault isolation and recovery behaviour.",
            "notes": [
                "Best for Figure 4.16.",
                "Use a single target load per run so the resulting event is easy to explain.",
            ],
            "recommended_outputs": [
                "figure_4_16_per_load_current_protection_response.csv",
            ],
            "defaults": {
                "target_load": "motor1",
                "pre_fault_sec": 20,
                "fault_hold_sec": 20,
                "recovery_sec": 20,
                "fault_limit_a": 0.25,
                "inject_current_a": 1.5,
                "control_policy": "NO_ENERGY_MANAGEMENT",
                "predictor_mode": "OFFLINE",
                "heater_hot_off_temp_c": 80.0,
            },
            "fields": [
                {"name": "target_load", "label": "Target Load", "type": "select", "options": list(EXPERIMENT_LOAD_OPTIONS)},
                {"name": "pre_fault_sec", "label": "Pre-Fault Hold (s)", "type": "number", "min": 1, "step": 1},
                {"name": "fault_hold_sec", "label": "Fault Hold (s)", "type": "number", "min": 1, "step": 1},
                {"name": "recovery_sec", "label": "Recovery Hold (s)", "type": "number", "min": 0, "step": 1},
                {"name": "fault_limit_a", "label": "Fault Limit (A)", "type": "number", "min": 0.01, "step": 0.01},
                {"name": "inject_current_a", "label": "Injected Current (A)", "type": "number", "min": 0.01, "step": 0.01},
                {"name": "control_policy", "label": "Control Policy", "type": "select", "options": sorted(POLICY_OPTIONS)},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "heater_hot_off_temp_c", "label": "Heater Hot-Off Temp (C)", "type": "number", "min": 0, "step": 0.1},
            ],
        },
        {
            "id": "integrated_operation",
            "name": "Integrated Operation",
            "description": "Runs a representative closed-loop process window with sensing, prediction, and control all active.",
            "notes": [
                "Best for Figure 4.21 and the integrated-system discussion.",
            ],
            "recommended_outputs": [
                "figure_4_21_integrated_system_operation_timeline.csv",
            ],
            "defaults": {
                "control_policy": "AI_PREFERRED",
                "run_duration_sec": 1200,
                "restart_process": True,
                "predictor_mode": "ONLINE",
                "max_total_power_w": None,
                "energy_goal_wh": None,
                "energy_goal_duration_min": None,
            },
            "fields": [
                {"name": "control_policy", "label": "Control Policy", "type": "select", "options": sorted(POLICY_OPTIONS)},
                {"name": "run_duration_sec", "label": "Run Duration (s)", "type": "number", "min": 30, "step": 1},
                {"name": "restart_process", "label": "Restart Process", "type": "boolean"},
                {"name": "predictor_mode", "label": "Predictor Mode", "type": "select", "options": sorted(PREDICTOR_MODE_OPTIONS)},
                {"name": "max_total_power_w", "label": "Max Power Threshold (W)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "energy_goal_wh", "label": "Energy Goal Window (Wh)", "type": "number", "min": 0, "step": 0.01, "optional": True},
                {"name": "energy_goal_duration_min", "label": "Energy Goal Duration (min)", "type": "number", "min": 1, "step": 1, "optional": True},
            ],
        },
    ]


def _default_tariff_store() -> Dict[str, Any]:
    return {
        "plant_id": PLANT_IDENTIFIER,
        "config_path": str(tariff_config_path),
        "versions": [dict(DEFAULT_TARIFF_VERSION)],
    }


def _safe_float(value) -> Optional[float]:
    try:
        number = float(value)
        if number != number:  # NaN
            return None
        return number
    except Exception:
        return None


def _parse_epoch_ms(value) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        raw = float(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        try:
            raw = float(text)
        except Exception:
            raw = None
        if raw is None:
            iso_text = text
            if iso_text.endswith("Z"):
                iso_text = iso_text[:-1] + "+00:00"
            try:
                dt = datetime.fromisoformat(iso_text)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp() * 1000)
            except Exception:
                return None
    if raw is None:
        return None
    if raw > 1e12:
        return int(raw)
    if raw > 946684800:  # likely epoch seconds
        return int(raw * 1000)
    return None


def _parse_range_value(value: Optional[str], fallback_ms: int) -> int:
    parsed = _parse_epoch_ms(value)
    if parsed is None:
        return fallback_ms
    return parsed


def _iter_days(start_ms: int, end_ms: int) -> Iterable[date]:
    start_day = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).date()
    day = start_day
    while day <= end_day:
        yield day
        day = day + timedelta(days=1)


def _day_of_week_utc(year: int, month: int, day: int) -> int:
    return datetime(year, month, day, tzinfo=timezone.utc).weekday()


def _days_in_month_utc(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return (next_month - timedelta(days=1)).day


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


def _compute_easter_sunday_utc(year: int) -> Tuple[int, int]:
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
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return month, day


_BANK_HOLIDAY_CACHE: Dict[int, set] = {}


def _build_uk_bank_holiday_set(year: int) -> set:
    out = set()

    new_year = _day_of_week_utc(year, 1, 1)
    new_year_observed = 1
    if new_year == 5:  # Saturday
        new_year_observed = 3
    elif new_year == 6:  # Sunday
        new_year_observed = 2
    out.add(f"{year:04d}-01-{new_year_observed:02d}")

    easter_month, easter_day = _compute_easter_sunday_utc(year)
    easter_dt = datetime(year, easter_month, easter_day, tzinfo=timezone.utc)
    out.add((easter_dt - timedelta(days=2)).date().isoformat())  # Good Friday
    out.add((easter_dt + timedelta(days=1)).date().isoformat())  # Easter Monday

    out.add(f"{year:04d}-05-{_first_weekday_of_month_utc(year, 5, 0):02d}")  # Early May Monday
    out.add(f"{year:04d}-05-{_last_weekday_of_month_utc(year, 5, 0):02d}")   # Spring bank holiday
    out.add(f"{year:04d}-08-{_last_weekday_of_month_utc(year, 8, 0):02d}")   # Summer bank holiday

    christmas = _day_of_week_utc(year, 12, 25)
    christmas_observed = 25
    if christmas in {5, 6}:
        christmas_observed = 27
    out.add(f"{year:04d}-12-{christmas_observed:02d}")

    boxing = _day_of_week_utc(year, 12, 26)
    boxing_observed = 26
    if boxing in {5, 6}:
        boxing_observed = 28
    out.add(f"{year:04d}-12-{boxing_observed:02d}")

    return out


def _is_uk_bank_holiday(local_dt: datetime) -> bool:
    year = local_dt.year
    if year not in _BANK_HOLIDAY_CACHE:
        _BANK_HOLIDAY_CACHE[year] = _build_uk_bank_holiday_set(year)
    return local_dt.date().isoformat() in _BANK_HOLIDAY_CACHE[year]


def _normalize_tariff_version(raw: Dict[str, Any], fallback_effective_from: str) -> Dict[str, Any]:
    effective_ms = _parse_epoch_ms(raw.get("effective_from")) if isinstance(raw, dict) else None
    effective_from = _iso_utc_from_ms(effective_ms) if effective_ms is not None else fallback_effective_from
    return {
        "effective_from": effective_from,
        "timezone": str(raw.get("timezone") or TARIFF_TIMEZONE).strip() or TARIFF_TIMEZONE,
        "use_dashboard_tou": bool(raw.get("use_dashboard_tou", raw.get("useDashboardTou", True))),
        "offpeak_start_min": _normalize_minute_of_day(raw.get("offpeak_start_min", raw.get("offpeakStartMin")), DEFAULT_TARIFF_VERSION["offpeak_start_min"]),
        "offpeak_end_min": _normalize_minute_of_day(raw.get("offpeak_end_min", raw.get("offpeakEndMin")), DEFAULT_TARIFF_VERSION["offpeak_end_min"]),
        "peak_start_min": _normalize_minute_of_day(raw.get("peak_start_min", raw.get("peakStartMin")), DEFAULT_TARIFF_VERSION["peak_start_min"]),
        "peak_end_min": _normalize_minute_of_day(raw.get("peak_end_min", raw.get("peakEndMin")), DEFAULT_TARIFF_VERSION["peak_end_min"]),
        "offpeak_rate_per_kwh": _normalize_nonnegative_float(raw.get("offpeak_rate_per_kwh", raw.get("offpeakRatePerKwh")), DEFAULT_TARIFF_VERSION["offpeak_rate_per_kwh"]),
        "midpeak_rate_per_kwh": _normalize_nonnegative_float(raw.get("midpeak_rate_per_kwh", raw.get("midpeakRatePerKwh")), DEFAULT_TARIFF_VERSION["midpeak_rate_per_kwh"]),
        "peak_rate_per_kwh": _normalize_nonnegative_float(raw.get("peak_rate_per_kwh", raw.get("peakRatePerKwh")), DEFAULT_TARIFF_VERSION["peak_rate_per_kwh"]),
        "demand_charge_per_kw": _normalize_nonnegative_float(raw.get("demand_charge_per_kw", raw.get("demandChargePerKw")), DEFAULT_TARIFF_VERSION["demand_charge_per_kw"]),
        "demand_interval_min": _normalize_positive_int(raw.get("demand_interval_min", raw.get("demandIntervalMin")), DEFAULT_TARIFF_VERSION["demand_interval_min"]),
        "weekends_offpeak": bool(raw.get("weekends_offpeak", True)),
        "bank_holidays_offpeak": bool(raw.get("bank_holidays_offpeak", True)),
    }


def _load_tariff_store() -> Dict[str, Any]:
    default_store = _default_tariff_store()
    if not tariff_config_path.exists():
        return default_store
    try:
        payload = json.loads(tariff_config_path.read_text(encoding="utf-8"))
        versions = payload.get("versions") if isinstance(payload, dict) else None
        if not isinstance(versions, list) or not versions:
            return default_store
        normalized_versions = [
            _normalize_tariff_version(item if isinstance(item, dict) else {}, DEFAULT_TARIFF_VERSION["effective_from"])
            for item in versions
        ]
        normalized_versions.sort(key=lambda item: _parse_epoch_ms(item.get("effective_from")) or 0)
        return {
            "plant_id": str(payload.get("plant_id") or PLANT_IDENTIFIER),
            "config_path": str(tariff_config_path),
            "versions": normalized_versions,
        }
    except Exception:
        return default_store


def _write_tariff_store(store: Dict[str, Any]) -> Dict[str, Any]:
    normalized_versions = [
        _normalize_tariff_version(item if isinstance(item, dict) else {}, DEFAULT_TARIFF_VERSION["effective_from"])
        for item in (store.get("versions") or [])
    ]
    if not normalized_versions:
        normalized_versions = [dict(DEFAULT_TARIFF_VERSION)]
    normalized_versions.sort(key=lambda item: _parse_epoch_ms(item.get("effective_from")) or 0)
    normalized_store = {
        "plant_id": str(store.get("plant_id") or PLANT_IDENTIFIER),
        "config_path": str(tariff_config_path),
        "versions": normalized_versions,
    }
    tariff_config_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(tariff_config_path.parent), prefix="tariff_", suffix=".json") as handle:
        json.dump(normalized_store, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_name = handle.name
    os.replace(temp_name, tariff_config_path)
    return normalized_store


def _resolve_tariff_version_for_ms(store: Dict[str, Any], ts_ms: Optional[int]) -> Dict[str, Any]:
    versions = list(store.get("versions") or [])
    if not versions:
        return dict(DEFAULT_TARIFF_VERSION)
    if ts_ms is None:
        return dict(versions[-1])
    active = versions[0]
    for version in versions:
        effective_ms = _parse_epoch_ms(version.get("effective_from")) or 0
        if effective_ms <= ts_ms:
            active = version
        else:
            break
    return dict(active)


def _normalize_optional_float(value: Any, *, minimum: Optional[float] = None) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
        if number != number:
            return None
        if minimum is not None:
            number = max(minimum, number)
        return number
    except Exception:
        return None


def _template_map() -> Dict[str, Dict[str, Any]]:
    return {item["id"]: item for item in _experiment_templates()}


def _normalize_experiment_config(template_id: str, raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = dict(raw or {})
    if template_id == "sensor_validation":
        heater_hot_off = _normalize_nonnegative_float(payload.get("heater_hot_off_temp_c"), 80.0) or 80.0
        return {
            "single_load_duration_sec": _normalize_positive_int(payload.get("single_load_duration_sec"), 30),
            "stage_duration_sec": _normalize_positive_int(payload.get("stage_duration_sec"), 45),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "OFFLINE"),
            "max_total_power_w": _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.0),
            "heater_hot_off_temp_c": heater_hot_off,
            "heater_warm_temp_c": max(0.0, heater_hot_off - 5.0),
        }
    if template_id == "policy_comparison":
        return {
            "policy_duration_sec": _normalize_positive_int(payload.get("policy_duration_sec"), 600),
            "cooldown_sec": int(max(0, round(float(payload.get("cooldown_sec", 30) or 30)))),
            "start_process": bool(payload.get("start_process", True)),
            "restart_process_each_policy": bool(payload.get("restart_process_each_policy", True)),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "ONLINE"),
            "max_total_power_w": _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.0),
            "energy_goal_wh": _normalize_optional_float(payload.get("energy_goal_wh"), minimum=0.0),
            "energy_goal_duration_min": _normalize_optional_float(payload.get("energy_goal_duration_min"), minimum=1.0),
            "policies": ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"],
        }
    if template_id == "ambient_monitoring":
        return {
            "run_duration_sec": _normalize_positive_int(payload.get("run_duration_sec"), 600),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "OFFLINE"),
        }
    if template_id == "prediction_comparison":
        return {
            "control_policy": _normalize_policy(payload.get("control_policy"), "NO_ENERGY_MANAGEMENT"),
            "run_duration_sec": _normalize_positive_int(payload.get("run_duration_sec"), 900),
            "start_process": bool(payload.get("start_process", True)),
            "restart_process": bool(payload.get("restart_process", True)),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "ONLINE"),
            "max_total_power_w": _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.0),
            "energy_goal_wh": _normalize_optional_float(payload.get("energy_goal_wh"), minimum=0.0),
            "energy_goal_duration_min": _normalize_optional_float(payload.get("energy_goal_duration_min"), minimum=1.0),
        }
    if template_id == "heating_cycle":
        return {
            "run_duration_sec": _normalize_positive_int(payload.get("run_duration_sec"), 900),
            "restart_process": bool(payload.get("restart_process", True)),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "ONLINE"),
            "max_total_power_w": _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.0),
        }
    if template_id == "ultrasonic_validation":
        heater_hot_off = _normalize_nonnegative_float(payload.get("heater_hot_off_temp_c"), 80.0) or 80.0
        return {
            "run_duration_sec": _normalize_positive_int(payload.get("run_duration_sec"), 900),
            "restart_process": bool(payload.get("restart_process", True)),
            "control_policy": _normalize_policy(payload.get("control_policy"), "NO_ENERGY_MANAGEMENT"),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "ONLINE"),
            "max_total_power_w": _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.0),
            "heater_hot_off_temp_c": heater_hot_off,
            "heater_warm_temp_c": max(0.0, heater_hot_off - 5.0),
        }
    if template_id == "load_behavior":
        heater_hot_off = _normalize_nonnegative_float(payload.get("heater_hot_off_temp_c"), 80.0) or 80.0
        return {
            "stage_duration_sec": _normalize_positive_int(payload.get("stage_duration_sec"), 45),
            "control_policy": _normalize_policy(payload.get("control_policy"), "NO_ENERGY_MANAGEMENT"),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "OFFLINE"),
            "max_total_power_w": _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.0),
            "heater_hot_off_temp_c": heater_hot_off,
            "heater_warm_temp_c": max(0.0, heater_hot_off - 5.0),
        }
    if template_id == "priority_curtailment":
        heater_hot_off = _normalize_nonnegative_float(payload.get("heater_hot_off_temp_c"), 80.0) or 80.0
        max_total_power = _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.1)
        return {
            "stage_duration_sec": _normalize_positive_int(payload.get("stage_duration_sec"), 45),
            "control_policy": _normalize_policy(payload.get("control_policy"), "RULE_ONLY"),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "OFFLINE"),
            "max_total_power_w": max_total_power if max_total_power is not None else 15.0,
            "heater_hot_off_temp_c": heater_hot_off,
            "heater_warm_temp_c": max(0.0, heater_hot_off - 5.0),
        }
    if template_id == "voltage_protection":
        return {
            "undervoltage_magnitude_v": _normalize_nonnegative_float(payload.get("undervoltage_magnitude_v"), 1.0) or 1.0,
            "undervoltage_hold_sec": _normalize_positive_int(payload.get("undervoltage_hold_sec"), 30),
            "recovery_sec": int(max(0, round(float(payload.get("recovery_sec", 20) or 20)))),
            "overvoltage_magnitude_v": _normalize_nonnegative_float(payload.get("overvoltage_magnitude_v"), 1.0) or 1.0,
            "overvoltage_hold_sec": _normalize_positive_int(payload.get("overvoltage_hold_sec"), 30),
        }
    if template_id == "overcurrent_response":
        heater_hot_off = _normalize_nonnegative_float(payload.get("heater_hot_off_temp_c"), 80.0) or 80.0
        return {
            "target_load": _normalize_experiment_load_name(payload.get("target_load"), "motor1"),
            "pre_fault_sec": _normalize_positive_int(payload.get("pre_fault_sec"), 20),
            "fault_hold_sec": _normalize_positive_int(payload.get("fault_hold_sec"), 20),
            "recovery_sec": int(max(0, round(float(payload.get("recovery_sec", 20) or 20)))),
            "fault_limit_a": _normalize_nonnegative_float(payload.get("fault_limit_a"), 0.25) or 0.25,
            "inject_current_a": _normalize_nonnegative_float(payload.get("inject_current_a"), 1.5) or 1.5,
            "control_policy": _normalize_policy(payload.get("control_policy"), "NO_ENERGY_MANAGEMENT"),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "OFFLINE"),
            "heater_hot_off_temp_c": heater_hot_off,
            "heater_warm_temp_c": max(0.0, heater_hot_off - 5.0),
        }
    if template_id == "integrated_operation":
        return {
            "control_policy": _normalize_policy(payload.get("control_policy"), "AI_PREFERRED"),
            "run_duration_sec": _normalize_positive_int(payload.get("run_duration_sec"), 1200),
            "restart_process": bool(payload.get("restart_process", True)),
            "predictor_mode": _normalize_predictor_mode(payload.get("predictor_mode"), "ONLINE"),
            "max_total_power_w": _normalize_optional_float(payload.get("max_total_power_w"), minimum=0.0),
            "energy_goal_wh": _normalize_optional_float(payload.get("energy_goal_wh"), minimum=0.0),
            "energy_goal_duration_min": _normalize_optional_float(payload.get("energy_goal_duration_min"), minimum=1.0),
        }
    raise ValueError(f"unsupported_template:{template_id}")


def _plan_step(step_number: int, action: str, label: str, params: Optional[Dict[str, Any]] = None, duration_sec: Optional[int] = None) -> Dict[str, Any]:
    out = {
        "id": f"s{step_number:02d}",
        "action": str(action),
        "label": str(label),
        "status": "pending",
        "params": dict(params or {}),
    }
    if duration_sec is not None:
        out["duration_sec"] = int(max(0, duration_sec))
    return out


def _apply_system_params(
    config: Dict[str, Any],
    *,
    control_policy: Optional[str] = None,
    predictor_mode: Optional[str] = None,
    control_overrides: Optional[Dict[str, Any]] = None,
    predictor_short_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {}
    if control_policy:
        params["control_policy"] = control_policy
    if predictor_mode:
        params["predictor_mode"] = predictor_mode
    if config.get("max_total_power_w") is not None:
        params["max_total_power_w"] = float(config["max_total_power_w"])
    if config.get("energy_goal_wh") is not None:
        params["energy_goal_wh"] = float(config["energy_goal_wh"])
    if config.get("energy_goal_duration_min") is not None:
        params["energy_goal_duration_min"] = float(config["energy_goal_duration_min"])
    if control_overrides:
        params["control"] = dict(control_overrides)
    if predictor_short_overrides:
        params["predictor_short"] = dict(predictor_short_overrides)
    return params


def _heater_control_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if config.get("heater_warm_temp_c") is not None:
        out["HEATER_WARM_TEMP_C"] = float(config["heater_warm_temp_c"])
    if config.get("heater_hot_off_temp_c") is not None:
        out["HEATER_HOT_OFF_TEMP_C"] = float(config["heater_hot_off_temp_c"])
    return out


def _plan_set_loads_step(step_number: int, label: str, loads: List[Dict[str, Any]]) -> Dict[str, Any]:
    return _plan_step(step_number, "set_loads", label, {"loads": list(loads)})


def _build_experiment_plan(template_id: str, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    steps: List[Dict[str, Any]] = []
    step_number = 1
    heater_overrides = _heater_control_overrides(config)
    if template_id == "sensor_validation":
        steps.append(_plan_step(step_number, "apply_system", "Apply sensor-validation settings", _apply_system_params(
            config,
            control_policy="NO_ENERGY_MANAGEMENT",
            predictor_mode=config.get("predictor_mode"),
            control_overrides=heater_overrides,
        )))
        step_number += 1
        steps.append(_plan_set_loads_step(step_number, "Reset all loads", _all_load_reset_commands(clear_inject=True)))
        step_number += 1

        for device in EXPERIMENT_LOAD_OPTIONS:
            single_stage = _all_load_reset_commands()
            for item in single_stage:
                if item["device"] == device:
                    item["set"]["on"] = True
                    item["set"]["duty"] = 1.0
            steps.append(_plan_set_loads_step(step_number, f"Enable {device} at full load", single_stage))
            step_number += 1
            steps.append(_plan_step(step_number, "wait", f"Observe {device}", duration_sec=int(config.get("single_load_duration_sec", 30) or 30)))
            step_number += 1

        staged_profiles = [
            ("Low-load stage", [
                _load_set_command("lighting2", on=True, duty=0.35, override=False),
            ]),
            ("Medium-load stage", [
                _load_set_command("lighting1", on=True, duty=0.60, override=False),
                _load_set_command("motor1", on=True, duty=0.65, override=False),
            ]),
            ("High-load stage", [
                _load_set_command("motor1", on=True, duty=1.0, override=False),
                _load_set_command("motor2", on=True, duty=1.0, override=False),
                _load_set_command("heater1", on=True, duty=1.0, override=False),
                _load_set_command("heater2", on=True, duty=1.0, override=False),
                _load_set_command("lighting1", on=True, duty=1.0, override=False),
                _load_set_command("lighting2", on=True, duty=1.0, override=False),
            ]),
        ]
        for label, active_loads in staged_profiles:
            staged_commands = _all_load_reset_commands()
            staged_commands.extend(active_loads)
            steps.append(_plan_set_loads_step(step_number, label, staged_commands))
            step_number += 1
            steps.append(_plan_step(step_number, "wait", f"Observe {label.lower()}", duration_sec=int(config.get("stage_duration_sec", 45) or 45)))
            step_number += 1

        steps.append(_plan_set_loads_step(step_number, "Shutdown validation loads", _all_load_reset_commands(clear_inject=True)))
        return steps
    if template_id == "policy_comparison":
        policies = list(config.get("policies") or [])
        for index, policy in enumerate(policies):
            steps.append(_plan_step(step_number, "apply_system", f"Apply {policy}", _apply_system_params(config, control_policy=policy, predictor_mode=config.get("predictor_mode"))))
            step_number += 1
            if config.get("start_process", True):
                steps.append(_plan_step(step_number, "start_process", f"Start process for {policy}", {
                    "restart": bool(config.get("restart_process_each_policy", True)),
                }))
                step_number += 1
            steps.append(_plan_step(step_number, "wait", f"Observe {policy}", duration_sec=int(config.get("policy_duration_sec", 600) or 600)))
            step_number += 1
            if config.get("start_process", True):
                steps.append(_plan_step(step_number, "stop_process", f"Stop process after {policy}"))
                step_number += 1
            if index < (len(policies) - 1) and int(config.get("cooldown_sec", 0) or 0) > 0:
                steps.append(_plan_step(step_number, "wait", f"Cooldown after {policy}", duration_sec=int(config.get("cooldown_sec", 0) or 0)))
                step_number += 1
        return steps
    if template_id == "ambient_monitoring":
        steps.append(_plan_step(step_number, "apply_system", "Apply ambient-monitoring settings", _apply_system_params(
            config,
            control_policy="NO_ENERGY_MANAGEMENT",
            predictor_mode=config.get("predictor_mode"),
        )))
        step_number += 1
        steps.append(_plan_step(step_number, "stop_process", "Ensure process is stopped"))
        step_number += 1
        steps.append(_plan_set_loads_step(step_number, "Disable all loads", _all_load_reset_commands(clear_inject=True)))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", "Observe ambient conditions", duration_sec=int(config.get("run_duration_sec", 600) or 600)))
        return steps
    if template_id == "prediction_comparison":
        steps.append(_plan_step(step_number, "apply_system", f"Apply {config.get('control_policy')} prediction comparison settings", _apply_system_params(config, control_policy=config.get("control_policy"), predictor_mode=config.get("predictor_mode"))))
        step_number += 1
        if config.get("start_process", True):
            steps.append(_plan_step(step_number, "start_process", "Start prediction comparison process", {
                "restart": bool(config.get("restart_process", True)),
            }))
            step_number += 1
        steps.append(_plan_step(step_number, "wait", "Observe prediction comparison window", duration_sec=int(config.get("run_duration_sec", 900) or 900)))
        step_number += 1
        if config.get("start_process", True):
            steps.append(_plan_step(step_number, "stop_process", "Stop prediction comparison process"))
        return steps
    if template_id == "heating_cycle":
        steps.append(_plan_step(step_number, "apply_system", "Apply heating-cycle settings", _apply_system_params(config, predictor_mode=config.get("predictor_mode"))))
        step_number += 1
        steps.append(_plan_step(step_number, "start_process", "Start heating cycle", {
            "restart": bool(config.get("restart_process", True)),
        }))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", "Observe heating cycle", duration_sec=int(config.get("run_duration_sec", 900) or 900)))
        step_number += 1
        steps.append(_plan_step(step_number, "stop_process", "Stop heating cycle"))
        return steps
    if template_id == "ultrasonic_validation":
        steps.append(_plan_step(step_number, "apply_system", "Apply ultrasonic-validation settings", _apply_system_params(
            config,
            control_policy=config.get("control_policy"),
            predictor_mode=config.get("predictor_mode"),
            control_overrides=heater_overrides,
        )))
        step_number += 1
        steps.append(_plan_step(step_number, "start_process", "Start ultrasonic validation process", {
            "restart": bool(config.get("restart_process", True)),
        }))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", "Observe ultrasonic and tank levels", duration_sec=int(config.get("run_duration_sec", 900) or 900)))
        step_number += 1
        steps.append(_plan_step(step_number, "stop_process", "Stop ultrasonic validation process"))
        return steps
    if template_id == "load_behavior":
        stage_duration = int(config.get("stage_duration_sec", 45) or 45)
        steps.append(_plan_step(step_number, "apply_system", "Apply load-behaviour settings", _apply_system_params(
            config,
            control_policy=config.get("control_policy"),
            predictor_mode=config.get("predictor_mode"),
            control_overrides=heater_overrides,
        )))
        step_number += 1
        stages = [
            ("Reset all loads", _all_load_reset_commands(clear_inject=True)),
            ("Heater-only stage", [
                *_all_load_reset_commands(),
                _load_set_command("heater1", on=True, duty=1.0, override=False),
                _load_set_command("heater2", on=True, duty=1.0, override=False),
            ]),
            ("Motor low-duty stage", [
                *_all_load_reset_commands(),
                _load_set_command("motor1", on=True, duty=0.45, override=False),
                _load_set_command("motor2", on=True, duty=0.45, override=False),
            ]),
            ("Motor high-duty stage", [
                *_all_load_reset_commands(),
                _load_set_command("motor1", on=True, duty=0.85, override=False),
                _load_set_command("motor2", on=True, duty=0.85, override=False),
            ]),
            ("Lighting low-duty stage", [
                *_all_load_reset_commands(),
                _load_set_command("lighting1", on=True, duty=0.35, override=False),
                _load_set_command("lighting2", on=True, duty=0.35, override=False),
            ]),
            ("Lighting high-duty stage", [
                *_all_load_reset_commands(),
                _load_set_command("lighting1", on=True, duty=0.80, override=False),
                _load_set_command("lighting2", on=True, duty=0.80, override=False),
            ]),
            ("Mixed-load stage", [
                *_all_load_reset_commands(),
                _load_set_command("heater1", on=True, duty=1.0, override=False),
                _load_set_command("motor1", on=True, duty=0.60, override=False),
                _load_set_command("lighting1", on=True, duty=0.50, override=False),
            ]),
            ("All-load stage", [
                *_all_load_reset_commands(),
                _load_set_command("motor1", on=True, duty=1.0, override=False),
                _load_set_command("motor2", on=True, duty=1.0, override=False),
                _load_set_command("heater1", on=True, duty=1.0, override=False),
                _load_set_command("heater2", on=True, duty=1.0, override=False),
                _load_set_command("lighting1", on=True, duty=1.0, override=False),
                _load_set_command("lighting2", on=True, duty=1.0, override=False),
            ]),
        ]
        for label, loads in stages:
            steps.append(_plan_set_loads_step(step_number, label, loads))
            step_number += 1
            if label != "Reset all loads":
                steps.append(_plan_step(step_number, "wait", f"Observe {label.lower()}", duration_sec=stage_duration))
                step_number += 1
        steps.append(_plan_set_loads_step(step_number, "Shutdown load-behaviour schedule", _all_load_reset_commands(clear_inject=True)))
        return steps
    if template_id == "priority_curtailment":
        stage_duration = int(config.get("stage_duration_sec", 45) or 45)
        steps.append(_plan_step(step_number, "apply_system", "Apply priority-curtailment settings", _apply_system_params(
            config,
            control_policy=config.get("control_policy"),
            predictor_mode=config.get("predictor_mode"),
            control_overrides=heater_overrides,
        )))
        step_number += 1
        stages = [
            ("Reset all loads", _all_load_reset_commands(clear_inject=True)),
            ("Low-priority load stage", [
                *_all_load_reset_commands(),
                _load_set_command("lighting2", on=True, duty=1.0, override=False),
                _load_set_command("lighting1", on=True, duty=0.70, override=False),
            ]),
            ("Essential-load stage", [
                *_all_load_reset_commands(),
                _load_set_command("lighting2", on=True, duty=1.0, override=False),
                _load_set_command("lighting1", on=True, duty=1.0, override=False),
                _load_set_command("heater1", on=True, duty=1.0, override=False),
                _load_set_command("heater2", on=True, duty=1.0, override=False),
            ]),
            ("Full-demand stage", [
                *_all_load_reset_commands(),
                _load_set_command("motor1", on=True, duty=1.0, override=False),
                _load_set_command("motor2", on=True, duty=1.0, override=False),
                _load_set_command("heater1", on=True, duty=1.0, override=False),
                _load_set_command("heater2", on=True, duty=1.0, override=False),
                _load_set_command("lighting1", on=True, duty=1.0, override=False),
                _load_set_command("lighting2", on=True, duty=1.0, override=False),
            ]),
        ]
        for label, loads in stages:
            steps.append(_plan_set_loads_step(step_number, label, loads))
            step_number += 1
            if label != "Reset all loads":
                steps.append(_plan_step(step_number, "wait", f"Observe {label.lower()}", duration_sec=stage_duration))
                step_number += 1
        steps.append(_plan_set_loads_step(step_number, "Shutdown curtailment scenario", _all_load_reset_commands(clear_inject=True)))
        return steps
    if template_id == "voltage_protection":
        steps.append(_plan_step(step_number, "clear_voltage_injection", "Clear existing voltage injection"))
        step_number += 1
        steps.append(_plan_step(step_number, "voltage_injection", "Inject undervoltage", {
            "mode": "UNDERVOLTAGE",
            "magnitude_v": float(config.get("undervoltage_magnitude_v", 1.0) or 1.0),
        }))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", "Hold undervoltage", duration_sec=int(config.get("undervoltage_hold_sec", 30) or 30)))
        step_number += 1
        steps.append(_plan_step(step_number, "clear_voltage_injection", "Clear undervoltage injection"))
        step_number += 1
        if int(config.get("recovery_sec", 0) or 0) > 0:
            steps.append(_plan_step(step_number, "wait", "Voltage recovery", duration_sec=int(config.get("recovery_sec", 0) or 0)))
            step_number += 1
        steps.append(_plan_step(step_number, "voltage_injection", "Inject overvoltage", {
            "mode": "OVERVOLTAGE",
            "magnitude_v": float(config.get("overvoltage_magnitude_v", 1.0) or 1.0),
        }))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", "Hold overvoltage", duration_sec=int(config.get("overvoltage_hold_sec", 30) or 30)))
        step_number += 1
        steps.append(_plan_step(step_number, "clear_voltage_injection", "Clear overvoltage injection"))
        step_number += 1
        if int(config.get("recovery_sec", 0) or 0) > 0:
            steps.append(_plan_step(step_number, "wait", "Voltage recovery", duration_sec=int(config.get("recovery_sec", 0) or 0)))
        return steps
    if template_id == "overcurrent_response":
        target_load = _normalize_experiment_load_name(config.get("target_load"), "motor1")
        pre_fault_sec = int(config.get("pre_fault_sec", 20) or 20)
        fault_hold_sec = int(config.get("fault_hold_sec", 20) or 20)
        recovery_sec = int(config.get("recovery_sec", 20) or 20)
        steps.append(_plan_step(step_number, "apply_system", "Apply overcurrent-response settings", _apply_system_params(
            config,
            control_policy=config.get("control_policy"),
            predictor_mode=config.get("predictor_mode"),
            control_overrides=heater_overrides,
        )))
        step_number += 1
        steps.append(_plan_set_loads_step(step_number, "Reset all loads", _all_load_reset_commands(clear_inject=True)))
        step_number += 1
        steps.append(_plan_set_loads_step(step_number, f"Arm {target_load} fault response", [
            _load_set_command(target_load, on=True, duty=1.0, override=False, fault_limit_a=float(config.get("fault_limit_a", 0.25) or 0.25), clear_inject=True),
        ]))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", f"Observe {target_load} before fault injection", duration_sec=pre_fault_sec))
        step_number += 1
        steps.append(_plan_set_loads_step(step_number, f"Inject overcurrent on {target_load}", [
            _load_set_command(target_load, on=True, duty=1.0, override=False, inject_current_a=float(config.get("inject_current_a", 1.5) or 1.5)),
        ]))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", f"Hold overcurrent on {target_load}", duration_sec=fault_hold_sec))
        step_number += 1
        steps.append(_plan_set_loads_step(step_number, f"Clear injected fault on {target_load}", [
            _load_set_command(target_load, on=True, duty=1.0, override=False, clear_inject=True),
        ]))
        step_number += 1
        if recovery_sec > 0:
            steps.append(_plan_step(step_number, "wait", f"Observe {target_load} recovery", duration_sec=recovery_sec))
            step_number += 1
        steps.append(_plan_set_loads_step(step_number, "Shutdown overcurrent scenario", _all_load_reset_commands(clear_inject=True)))
        return steps
    if template_id == "integrated_operation":
        steps.append(_plan_step(step_number, "apply_system", "Apply integrated-operation settings", _apply_system_params(
            config,
            control_policy=config.get("control_policy"),
            predictor_mode=config.get("predictor_mode"),
        )))
        step_number += 1
        steps.append(_plan_step(step_number, "start_process", "Start integrated operation process", {
            "restart": bool(config.get("restart_process", True)),
        }))
        step_number += 1
        steps.append(_plan_step(step_number, "wait", "Observe integrated operation window", duration_sec=int(config.get("run_duration_sec", 1200) or 1200)))
        step_number += 1
        steps.append(_plan_step(step_number, "stop_process", "Stop integrated operation process"))
        return steps
    raise ValueError(f"unsupported_template:{template_id}")


def _load_experiment_store() -> Dict[str, Any]:
    default_store = _default_experiment_store()
    if not experiment_runs_path.exists():
        return default_store
    try:
        payload = json.loads(experiment_runs_path.read_text(encoding="utf-8"))
        runs = payload.get("runs") if isinstance(payload, dict) else None
        if not isinstance(runs, list):
            return default_store
        valid_runs = [dict(item) for item in runs if isinstance(item, dict) and item.get("id")]
        valid_runs.sort(key=lambda item: _parse_epoch_ms(item.get("created_at")) or 0, reverse=True)
        return {
            "plant_id": str(payload.get("plant_id") or PLANT_IDENTIFIER),
            "path": str(experiment_runs_path),
            "runs": valid_runs,
        }
    except Exception:
        return default_store


def _write_experiment_store(store: Dict[str, Any]) -> Dict[str, Any]:
    runs = [dict(item) for item in (store.get("runs") or []) if isinstance(item, dict) and item.get("id")]
    runs.sort(key=lambda item: _parse_epoch_ms(item.get("created_at")) or 0, reverse=True)
    normalized_store = {
        "plant_id": str(store.get("plant_id") or PLANT_IDENTIFIER),
        "path": str(experiment_runs_path),
        "runs": runs,
    }
    experiment_runs_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(experiment_runs_path.parent), prefix="experiment_runs_", suffix=".json") as handle:
        json.dump(normalized_store, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_name = handle.name
    os.replace(temp_name, experiment_runs_path)
    return normalized_store


def _find_run(store: Dict[str, Any], run_id: str) -> Optional[Dict[str, Any]]:
    for run in store.get("runs") or []:
        if str(run.get("id")) == str(run_id):
            return run
    return None


def _cached_experiment_export_path(run_id: str, naming_mode: str, archive_name: str) -> Path:
    return experiment_exports_path / str(run_id) / _normalize_export_naming_mode(naming_mode, "report") / archive_name


def _serialize_export_jobs(raw_jobs: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw_jobs, dict):
        return {}
    jobs: Dict[str, Dict[str, Any]] = {}
    for key, value in raw_jobs.items():
        naming_mode = _normalize_export_naming_mode(key, "report")
        jobs[naming_mode] = _normalize_export_job(value if isinstance(value, dict) else {}, naming_mode)
    return jobs


def _serialize_run(run: Dict[str, Any]) -> Dict[str, Any]:
    steps = list(run.get("plan") or [])
    completed_steps = sum(1 for step in steps if str(step.get("status")) == "completed")
    running_step = next((step for step in steps if str(step.get("status")) == "running"), None)
    export_jobs = _serialize_export_jobs(run.get("export_jobs"))
    return {
        **run,
        "plan": steps,
        "step_count": len(steps),
        "completed_steps": completed_steps,
        "current_step": running_step or (steps[completed_steps] if completed_steps < len(steps) else None),
        "can_export": bool(run.get("started_at")) and bool(run.get("finished_at")) and str(run.get("status")) == "completed",
        "export_jobs": export_jobs,
    }


def _record_run_event(run: Dict[str, Any], event_type: str, step_id: Optional[str] = None, detail: Optional[str] = None) -> Dict[str, Any]:
    now_iso = _iso_utc_from_ms(int(datetime.now(tz=timezone.utc).timestamp() * 1000))
    events = list(run.get("events") or [])
    event = {"type": event_type, "ts": now_iso}
    if step_id:
        event["step_id"] = step_id
    if detail:
        event["detail"] = str(detail)
    events.append(event)
    run["events"] = events[-200:]
    run["updated_at"] = now_iso

    steps = list(run.get("plan") or [])
    step_index = None
    target_step = None
    if step_id:
        for index, step in enumerate(steps):
            if str(step.get("id")) == str(step_id):
                step_index = index
                target_step = step
                break

    if event_type == "run_started":
        run["status"] = "running"
        run["started_at"] = run.get("started_at") or now_iso
        run["current_step_index"] = 0
    elif event_type == "step_started" and target_step is not None:
        target_step["status"] = "running"
        target_step["started_at"] = now_iso
        run["status"] = "running"
        run["current_step_index"] = int(step_index or 0)
    elif event_type == "step_completed" and target_step is not None:
        target_step["status"] = "completed"
        target_step["completed_at"] = now_iso
        run["current_step_index"] = int((step_index or 0) + 1)
    elif event_type == "run_completed":
        run["status"] = "completed"
        run["finished_at"] = now_iso
    elif event_type == "run_failed":
        run["status"] = "failed"
        run["finished_at"] = now_iso
        run["failure_detail"] = str(detail or run.get("failure_detail") or "run_failed")
    elif event_type == "run_cancelled":
        run["status"] = "cancelled"
        run["finished_at"] = now_iso
        run["failure_detail"] = str(detail or "cancelled")

    run["plan"] = steps
    return run


def _set_run_export_job(
    run_id: str,
    naming_mode: str,
    updates: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    naming_mode = _normalize_export_naming_mode(naming_mode, "report")
    with experiment_export_lock:
        store = _load_experiment_store()
        run = _find_run(store, run_id)
        if run is None:
            return None, None
        export_jobs = run.setdefault("export_jobs", {})
        current = dict(export_jobs.get(naming_mode) or {})
        current.update(updates or {})
        current["naming_mode"] = naming_mode
        current["updated_at"] = _utc_iso(time.time())
        export_jobs[naming_mode] = current
        updated = _write_experiment_store(store)
        saved = _find_run(updated, run_id) or run
        saved_jobs = _serialize_export_jobs(saved.get("export_jobs"))
        return saved, saved_jobs.get(naming_mode) or _normalize_export_job(current, naming_mode)


def _queue_experiment_export_job(run_id: str, naming_mode: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool]:
    naming_mode = _normalize_export_naming_mode(naming_mode, "report")
    with experiment_export_lock:
        store = _load_experiment_store()
        run = _find_run(store, run_id)
        if run is None:
            return None, None, False
        start_ms = _parse_epoch_ms(run.get("export_start_ms")) or _parse_epoch_ms(run.get("started_at"))
        end_ms = _parse_epoch_ms(run.get("export_end_ms")) or _parse_epoch_ms(run.get("finished_at"))
        if start_ms is None or end_ms is None or end_ms < start_ms:
            return run, None, False
        export_jobs = run.setdefault("export_jobs", {})
        existing = dict(export_jobs.get(naming_mode) or {})
        normalized_existing = _normalize_export_job(existing, naming_mode)
        cache_path_text = str(existing.get("cache_path") or "").strip()
        if normalized_existing.get("download_ready") and cache_path_text:
            updated = _write_experiment_store(store)
            saved = _find_run(updated, run_id) or run
            saved_jobs = _serialize_export_jobs(saved.get("export_jobs"))
            return saved, saved_jobs.get(naming_mode) or normalized_existing, False
        if normalized_existing.get("status") in {"queued", "running"}:
            updated = _write_experiment_store(store)
            saved = _find_run(updated, run_id) or run
            saved_jobs = _serialize_export_jobs(saved.get("export_jobs"))
            return saved, saved_jobs.get(naming_mode) or normalized_existing, False

        queued_at = _utc_iso(time.time())
        export_jobs[naming_mode] = {
            "naming_mode": naming_mode,
            "status": "queued",
            "phase": "queued",
            "message": "Waiting to start export",
            "progress_ratio": 0.0,
            "requested_at": queued_at,
            "started_at": "",
            "finished_at": "",
            "updated_at": queued_at,
            "archive_name": str(existing.get("archive_name") or ""),
            "cache_path": "",
            "download_ready": False,
            "error": "",
            "size_bytes": 0,
        }
        updated = _write_experiment_store(store)
        saved = _find_run(updated, run_id) or run
        saved_jobs = _serialize_export_jobs(saved.get("export_jobs"))
        return saved, saved_jobs.get(naming_mode), True


def _run_experiment_export_job(run_id: str, naming_mode: str) -> None:
    naming_mode = _normalize_export_naming_mode(naming_mode, "report")
    client = None
    run = None
    try:
        try:
            client = _connect_mqtt_client("history-export")
        except Exception:
            client = None

        run, job = _set_run_export_job(
            run_id,
            naming_mode,
            {
                "status": "running",
                "phase": "starting",
                "message": "Starting export job",
                "progress_ratio": 0.02,
                "started_at": _utc_iso(time.time()),
                "finished_at": "",
                "download_ready": False,
                "error": "",
                "size_bytes": 0,
            },
        )
        if run is None or job is None:
            return
        _publish_experiment_export_status(client, run, job, naming_mode)

        def _update_export_progress(phase: str, ratio: float, message: str) -> None:
            nonlocal run, job
            run, job = _set_run_export_job(
                run_id,
                naming_mode,
                {
                    "status": "running",
                    "phase": str(phase),
                    "message": str(message),
                    "progress_ratio": max(0.0, min(1.0, float(ratio))),
                    "download_ready": False,
                    "error": "",
                },
            )
            if run is not None and job is not None:
                _publish_experiment_export_status(client, run, job, naming_mode)

        start_ms = _parse_epoch_ms(run.get("export_start_ms")) or _parse_epoch_ms(run.get("started_at"))
        end_ms = _parse_epoch_ms(run.get("export_end_ms")) or _parse_epoch_ms(run.get("finished_at"))
        if start_ms is None or end_ms is None or end_ms < start_ms:
            raise ValueError("run_not_exportable")
        export_log_dir_raw = str(run.get("export_log_dir") or "").strip()
        export_log_dir = Path(export_log_dir_raw).resolve() if export_log_dir_raw else None
        source_mode = _resolve_experiment_source_mode(run.get("source_mode"), "live")
        offline_prediction_replay = (
            str(run.get("template_id") or "").strip().lower() == "prediction_comparison"
            and source_mode in {"synthetic", "history"}
        )
        offline_policy_replay_config = None
        if (
            str(run.get("template_id") or "").strip().lower() == "policy_comparison"
            and source_mode in {"synthetic", "history"}
        ):
            config = run.get("config") or {}
            offline_policy_replay_config = {
                "policies": list(config.get("policies") or ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]),
                "max_total_power_w": config.get("max_total_power_w"),
                "energy_goal_wh": config.get("energy_goal_wh"),
            }
        archive_buffer, archive_name = _chapter4_export_buffer(
            start_ms,
            end_ms,
            archive_name=f"experiment_{run.get('template_id')}_{run_id}.zip",
            run_metadata=_serialize_run(run),
            export_log_dir=export_log_dir,
            offline_prediction_replay=offline_prediction_replay,
            offline_policy_replay_config=offline_policy_replay_config,
            naming_mode=naming_mode,
            progress_callback=_update_export_progress,
        )
        archive_path = _cached_experiment_export_path(run_id, naming_mode, archive_name)
        archive_path.parent.mkdir(parents=True, exist_ok=True)
        with archive_path.open("wb") as handle:
            handle.write(archive_buffer.getvalue())
        size_bytes = archive_path.stat().st_size if archive_path.exists() else 0
        run, job = _set_run_export_job(
            run_id,
            naming_mode,
            {
                "status": "ready",
                "phase": "ready",
                "message": "Export ready to download",
                "progress_ratio": 1.0,
                "archive_name": archive_name,
                "cache_path": str(archive_path),
                "finished_at": _utc_iso(time.time()),
                "download_ready": True,
                "error": "",
                "size_bytes": int(size_bytes),
            },
        )
        if run is not None and job is not None:
            _publish_experiment_export_status(client, run, job, naming_mode)
    except Exception as exc:
        error_text = f"experiment_export_failed: {exc}"
        if run is None:
            store = _load_experiment_store()
            run = _find_run(store, run_id)
        run, job = _set_run_export_job(
            run_id,
            naming_mode,
            {
                "status": "error",
                "phase": "error",
                "message": "Export failed",
                "finished_at": _utc_iso(time.time()),
                "download_ready": False,
                "error": error_text,
            },
        )
        if run is not None and job is not None:
            _publish_experiment_export_status(client, run, job, naming_mode)
    finally:
        if client is not None:
            try:
                client.loop_stop()
            except Exception:
                pass
            try:
                client.disconnect()
            except Exception:
                pass


def _chapter4_export_buffer(
    start_ms: int,
    end_ms: int,
    *,
    archive_name: Optional[str] = None,
    run_metadata: Optional[Dict[str, Any]] = None,
    export_log_dir: Optional[Path] = None,
    offline_prediction_replay: bool = False,
    offline_policy_replay_config: Optional[Dict[str, Any]] = None,
    prediction_step_sec: int = 5,
    max_lag_sec: int = 600,
    naming_mode: str = "report",
    progress_callback=None,
):
    with tempfile.TemporaryDirectory(prefix="chapter4_export_") as temp_dir:
        def _emit_progress(phase: str, ratio: float, message: str) -> None:
            if progress_callback is None:
                return
            try:
                progress_callback(phase, ratio, message)
            except Exception:
                pass

        _emit_progress("preparing_source", 0.05, "Preparing export source")
        effective_log_dir = Path(export_log_dir).resolve() if export_log_dir is not None else log_dir
        replay_summary = None
        policy_replay_summary = None
        export_start_ms = int(start_ms)
        export_end_ms = int(end_ms)
        if offline_prediction_replay:
            if not OFFLINE_PREDICTION_REPLAY_AVAILABLE or prepare_offline_prediction_log_dir is None:
                raise ValueError(
                    "offline_prediction_replay_unavailable"
                    + (f":{OFFLINE_PREDICTION_REPLAY_IMPORT_ERROR}" if OFFLINE_PREDICTION_REPLAY_IMPORT_ERROR else "")
                )
            replay_dir = Path(temp_dir) / "_offline_prediction_logs"
            _emit_progress("offline_prediction_replay", 0.10, "Replaying offline predictions")
            replay_summary = prepare_offline_prediction_log_dir(
                source_log_dir=effective_log_dir,
                output_dir=replay_dir,
                plant_id=PLANT_IDENTIFIER,
                start_ms=start_ms,
                end_ms=end_ms,
                rf_model_dir=_resolve_model_dir("rf"),
                lstm_model_dir=_resolve_model_dir("lstm"),
                progress_callback=lambda update: _emit_progress(
                    "offline_prediction_replay",
                    0.10 + (float(update.get("progress_ratio") or 0.0) * 0.55),
                    str(update.get("message") or "Replaying offline predictions"),
                ),
            )
            effective_log_dir = replay_dir
        if offline_policy_replay_config is not None:
            if not OFFLINE_POLICY_REPLAY_AVAILABLE or prepare_offline_policy_log_dir is None:
                raise ValueError(
                    "offline_policy_replay_unavailable"
                    + (f":{OFFLINE_POLICY_REPLAY_IMPORT_ERROR}" if OFFLINE_POLICY_REPLAY_IMPORT_ERROR else "")
                )
            replay_dir = Path(temp_dir) / "_offline_policy_logs"
            _emit_progress("offline_policy_replay", 0.10, "Preparing offline policy replay")
            policy_replay_summary = prepare_offline_policy_log_dir(
                source_log_dir=effective_log_dir,
                output_dir=replay_dir,
                plant_id=PLANT_IDENTIFIER,
                start_ms=start_ms,
                end_ms=end_ms,
                policies=list(offline_policy_replay_config.get("policies") or []),
                max_total_power_w=offline_policy_replay_config.get("max_total_power_w"),
                energy_goal_wh=offline_policy_replay_config.get("energy_goal_wh"),
                rf_model_dir=_resolve_model_dir("rf"),
                lstm_model_dir=_resolve_model_dir("lstm"),
                progress_callback=lambda update: _emit_progress(
                    "offline_policy_replay",
                    0.10 + (float(update.get("progress_ratio") or 0.0) * 0.70),
                    str(update.get("message") or "Running offline policy replay"),
                ),
            )
            effective_log_dir = replay_dir
            export_start_ms = int(policy_replay_summary.get("replay_start_ms") or export_start_ms)
            export_end_ms = int(policy_replay_summary.get("replay_end_ms") or export_end_ms)
        _emit_progress("exporting_csv", 0.55, "Building CSV exports")
        result = export_chapter4_dataset(
            log_dir=effective_log_dir,
            out_dir=temp_dir,
            plant_id=PLANT_IDENTIFIER,
            start_ms=export_start_ms,
            end_ms=export_end_ms,
            prediction_step_sec=prediction_step_sec,
            max_lag_sec=max_lag_sec,
            run_metadata=run_metadata,
        )
        run_dir = Path(result["run_dir"])
        if replay_summary is not None:
            (run_dir / "offline_prediction_replay.json").write_text(
                json.dumps(replay_summary, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if policy_replay_summary is not None:
            (run_dir / "offline_policy_replay.json").write_text(
                json.dumps(policy_replay_summary, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if CHAPTER4_PLOTS_AVAILABLE and generate_png_figures is not None:
            _emit_progress("rendering_png", 0.82, "Rendering PNG figures")
            generate_png_figures(run_dir)
        archive_buffer = io.BytesIO()
        if naming_mode == "standard":
            default_archive_name = f"data_export_{datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')}_{datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d')}.zip"
        else:
            default_archive_name = f"{run_dir.name}.zip"
        final_name = archive_name or default_archive_name
        _emit_progress("archiving_zip", 0.94, "Packaging export archive")
        with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for path in sorted(run_dir.rglob("*")):
                if path.is_file():
                    relative_name = _archive_relative_path(path, run_dir, naming_mode)
                    archive.write(path, arcname=f"{run_dir.name}/{relative_name}")
        archive_buffer.seek(0)
        _emit_progress("ready", 1.0, "Export ready")
        return archive_buffer, final_name


def _resolve_tariff_state_for_ms(ts_ms: int, version: Dict[str, Any]) -> str:
    tz_name = str(version.get("timezone") or TARIFF_TIMEZONE)
    try:
        zone = ZoneInfo(tz_name)
    except Exception:
        zone = timezone.utc
    local_dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(zone)
    minute_of_day = (local_dt.hour * 60) + local_dt.minute
    if bool(version.get("weekends_offpeak", True)) and local_dt.weekday() >= 5:
        return "OFFPEAK"
    if bool(version.get("bank_holidays_offpeak", True)) and _is_uk_bank_holiday(local_dt):
        return "OFFPEAK"
    if _is_minute_in_window(minute_of_day, int(version.get("offpeak_start_min", 0)), int(version.get("offpeak_end_min", 0))):
        return "OFFPEAK"
    if _is_minute_in_window(minute_of_day, int(version.get("peak_start_min", 0)), int(version.get("peak_end_min", 0))):
        return "PEAK"
    return "MIDPEAK"


def _is_minute_in_window(minute_of_day: int, start_minute: int, end_minute: int) -> bool:
    minute = _normalize_minute_of_day(minute_of_day, 0)
    start = _normalize_minute_of_day(start_minute, 0)
    end = _normalize_minute_of_day(end_minute, 0)
    if start == end:
        return True
    if start < end:
        return start <= minute < end
    return minute >= start or minute < end


def _resolve_tariff_rate_for_state(state: str, version: Dict[str, Any]) -> float:
    key = str(state or "OFFPEAK").upper()
    if key == "PEAK":
        return _normalize_nonnegative_float(version.get("peak_rate_per_kwh"), DEFAULT_TARIFF_VERSION["peak_rate_per_kwh"])
    if key == "MIDPEAK":
        return _normalize_nonnegative_float(version.get("midpeak_rate_per_kwh"), DEFAULT_TARIFF_VERSION["midpeak_rate_per_kwh"])
    return _normalize_nonnegative_float(version.get("offpeak_rate_per_kwh"), DEFAULT_TARIFF_VERSION["offpeak_rate_per_kwh"])


def _bucket_ms(ts_ms: int, step_sec: int) -> int:
    step_ms = max(1, step_sec) * 1000
    return (ts_ms // step_ms) * step_ms


def _build_row(ts_ms: int) -> Dict[str, Optional[float]]:
    return {
        "ts": ts_ms,
        "label": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
        "actual_power_w": None,
        "actual_current_a": None,
        "actual_total_energy_wh": None,
        "actual_tariff_state": None,
        "actual_tariff_rate_per_kwh": None,
        "actual_incremental_energy_cost": None,
        "actual_accumulated_energy_cost": None,
        "actual_max_demand_kw": None,
        "actual_demand_charge_cost": None,
        "actual_total_cost": None,
        "actual_demand_interval_min": None,
        "pred_short_power_w": None,
        "pred_short_energy_wh": None,
        "pred_long_rf_power_w": None,
        "pred_long_rf_energy_wh": None,
        "pred_long_lstm_power_w": None,
        "pred_long_lstm_energy_wh": None,
    }


def _attach_actual_costs(rows: List[Dict[str, Optional[float]]], actual_points: List[Dict[str, float]], tariff_store: Dict[str, Any]) -> List[Dict[str, Optional[float]]]:
    if not rows:
        return rows

    sorted_points = sorted(
        (
            point for point in actual_points
            if point.get("ts") is not None
        ),
        key=lambda item: item["ts"],
    )
    if not sorted_points:
        return rows

    point_states: List[Dict[str, Any]] = []
    previous_ts = None
    previous_energy_wh = None
    accumulated_energy_cost = 0.0
    running_max_demand_kw = 0.0
    interval_buckets: Dict[Tuple[int, int], float] = {}

    for point in sorted_points:
        ts = int(point["ts"])
        power_w = _safe_float(point.get("power_w")) or 0.0
        energy_wh = _safe_float(point.get("energy_wh"))

        delta_energy_wh = None
        if previous_energy_wh is not None and energy_wh is not None:
            delta_energy_wh = energy_wh - previous_energy_wh
            if delta_energy_wh < 0:
                delta_energy_wh = None

        if delta_energy_wh is None and previous_ts is not None and ts > previous_ts:
            dt_hours = (ts - previous_ts) / 3600000.0
            if dt_hours > 0:
                delta_energy_wh = max(0.0, power_w * dt_hours)

        if delta_energy_wh is None or delta_energy_wh < 0:
            delta_energy_wh = 0.0

        version = _resolve_tariff_version_for_ms(tariff_store, ts)
        tariff_state = _resolve_tariff_state_for_ms(ts, version)
        tariff_rate_per_kwh = _resolve_tariff_rate_for_state(tariff_state, version)
        incremental_energy_cost = delta_energy_wh * (tariff_rate_per_kwh / 1000.0)
        accumulated_energy_cost += incremental_energy_cost

        demand_interval_min = _normalize_positive_int(version.get("demand_interval_min"), DEFAULT_TARIFF_VERSION["demand_interval_min"])
        interval_ms = demand_interval_min * 60000
        interval_hours = interval_ms / 3600000.0
        if delta_energy_wh > 0 and previous_ts is not None and ts > previous_ts:
            segment_start = previous_ts
            segment_end = ts
            segment_duration_ms = segment_end - segment_start
            while segment_start < segment_end:
                bucket_start = (segment_start // interval_ms) * interval_ms
                bucket_end = bucket_start + interval_ms
                overlap_end = min(segment_end, bucket_end)
                overlap_ms = overlap_end - segment_start
                overlap_energy_wh = delta_energy_wh * (overlap_ms / segment_duration_ms)
                bucket_key = (int(bucket_start), demand_interval_min)
                next_bucket_energy_wh = interval_buckets.get(bucket_key, 0.0) + overlap_energy_wh
                interval_buckets[bucket_key] = next_bucket_energy_wh
                bucket_demand_kw = next_bucket_energy_wh / interval_hours / 1000.0
                if bucket_demand_kw > running_max_demand_kw:
                    running_max_demand_kw = bucket_demand_kw
                segment_start = overlap_end

        demand_charge_rate = _normalize_nonnegative_float(version.get("demand_charge_per_kw"), DEFAULT_TARIFF_VERSION["demand_charge_per_kw"])
        demand_charge_cost = running_max_demand_kw * demand_charge_rate
        total_cost = accumulated_energy_cost + demand_charge_cost
        point_states.append({
            "ts": ts,
            "actual_tariff_state": tariff_state,
            "actual_tariff_rate_per_kwh": tariff_rate_per_kwh,
            "actual_incremental_energy_cost": incremental_energy_cost,
            "actual_accumulated_energy_cost": accumulated_energy_cost,
            "actual_max_demand_kw": running_max_demand_kw,
            "actual_demand_charge_cost": demand_charge_cost,
            "actual_total_cost": total_cost,
            "actual_demand_interval_min": demand_interval_min,
        })

        previous_ts = ts
        if energy_wh is not None:
            previous_energy_wh = energy_wh

    state_index = 0
    latest_state = None
    for row in rows:
        row_ts = _safe_float(row.get("ts"))
        if row_ts is None:
            continue
        while state_index < len(point_states) and point_states[state_index]["ts"] <= row_ts:
            latest_state = point_states[state_index]
            state_index += 1
        if latest_state is None:
            continue
        for key, value in latest_state.items():
            if key == "ts":
                continue
            row[key] = value

    return rows


def _parse_series(raw_series: str) -> Optional[set]:
    text = (raw_series or "").strip()
    if not text:
        return None
    return {item.strip().lower() for item in text.split(",") if item.strip()}


def _series_enabled(series_filter: Optional[set], name: str) -> bool:
    if series_filter is None:
        return True
    return name.lower() in series_filter


def _collect_history(
    start_ms: int,
    end_ms: int,
    step_sec: int,
    series_filter: Optional[set],
) -> Tuple[List[Dict[str, Optional[float]]], Dict[str, int], List[Dict[str, float]]]:
    rows: Dict[int, Dict[str, Optional[float]]] = {}
    stats = {"files_scanned": 0, "records_scanned": 0, "records_used": 0}
    actual_points: List[Dict[str, float]] = []

    for day in _iter_days(start_ms, end_ms):
        path = log_dir / f"{PLANT_IDENTIFIER}_telemetry_{day.isoformat()}.jsonl"
        if not path.exists():
            continue
        stats["files_scanned"] += 1
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    stats["records_scanned"] += 1
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        record = json.loads(text)
                    except Exception:
                        continue
                    topic = str(record.get("topic") or "")
                    payload = record.get("payload")
                    if not isinstance(payload, dict):
                        continue

                    if topic == TOPIC_TELEMETRY_NAME and _series_enabled(series_filter, "actual"):
                        system = payload.get("system") or {}
                        energy = payload.get("energy") or {}
                        ts_ms = (
                            _parse_epoch_ms(payload.get("timestamp"))
                            or _parse_epoch_ms(system.get("timestamp"))
                            or _parse_epoch_ms(record.get("logged_at_utc"))
                        )
                        if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                            continue
                        bucket = _bucket_ms(ts_ms, step_sec)
                        row = rows.setdefault(bucket, _build_row(bucket))
                        power_w = _safe_float(system.get("total_power_w"))
                        current_a = _safe_float(system.get("total_current_a"))
                        total_energy_wh = _safe_float(energy.get("total_energy_wh"))
                        row["actual_power_w"] = power_w
                        row["actual_current_a"] = current_a
                        row["actual_total_energy_wh"] = total_energy_wh
                        actual_points.append({
                            "ts": ts_ms,
                            "power_w": power_w if power_w is not None else 0.0,
                            "current_a": current_a if current_a is not None else 0.0,
                            "energy_wh": total_energy_wh if total_energy_wh is not None else None,
                        })
                        stats["records_used"] += 1
                        continue

                    if topic in (TOPIC_PREDICTION_NAME, TOPIC_PREDICTION_LONG_NAME, TOPIC_PREDICTION_LONG_LSTM_NAME):
                        if topic == TOPIC_PREDICTION_NAME and not _series_enabled(series_filter, "pred_short"):
                            continue
                        if topic == TOPIC_PREDICTION_LONG_NAME and not _series_enabled(series_filter, "pred_long_rf"):
                            continue
                        if topic == TOPIC_PREDICTION_LONG_LSTM_NAME and not _series_enabled(series_filter, "pred_long_lstm"):
                            continue
                        ts_base_ms = _parse_epoch_ms(payload.get("timestamp")) or _parse_epoch_ms(record.get("logged_at_utc"))
                        if ts_base_ms is None:
                            continue
                        horizon_sec = _safe_float(payload.get("horizon_sec")) or 0.0
                        ts_target_ms = ts_base_ms + int(horizon_sec * 1000.0)
                        if ts_target_ms < start_ms or ts_target_ms > end_ms:
                            continue
                        bucket = _bucket_ms(ts_target_ms, step_sec)
                        row = rows.setdefault(bucket, _build_row(bucket))
                        pred_power = _safe_float(payload.get("predicted_power_w"))
                        pred_energy = _safe_float(payload.get("predicted_total_energy_wh"))
                        if pred_energy is None:
                            pred_energy = _safe_float(payload.get("predicted_energy_wh"))
                        if topic == TOPIC_PREDICTION_NAME:
                            row["pred_short_power_w"] = pred_power
                            row["pred_short_energy_wh"] = pred_energy
                        elif topic == TOPIC_PREDICTION_LONG_NAME:
                            row["pred_long_rf_power_w"] = pred_power
                            row["pred_long_rf_energy_wh"] = pred_energy
                        else:
                            row["pred_long_lstm_power_w"] = pred_power
                            row["pred_long_lstm_energy_wh"] = pred_energy
                        stats["records_used"] += 1
        except Exception:
            continue

    out = [rows[key] for key in sorted(rows.keys())]
    return out, stats, actual_points


@app.get("/health")
def health():
    tariff_store = _load_tariff_store()
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "log_dir": str(log_dir),
            "tariff_config_path": str(tariff_config_path),
            "experiment_runs_path": str(experiment_runs_path),
            "topics": {
                "telemetry": TOPIC_TELEMETRY_NAME,
                "prediction": TOPIC_PREDICTION_NAME,
                "prediction_long": TOPIC_PREDICTION_LONG_NAME,
                "prediction_long_lstm": TOPIC_PREDICTION_LONG_LSTM_NAME,
            },
            "tariff": {
                "active": _resolve_tariff_version_for_ms(tariff_store, None),
                "version_count": len(tariff_store.get("versions") or []),
            },
        }
    )


@app.get("/api/tariff-config")
def get_tariff_config():
    at_ms = _parse_epoch_ms(request.args.get("at"))
    include_history = str(request.args.get("include_history", "")).strip().lower() in {"1", "true", "yes", "on"}
    tariff_store = _load_tariff_store()
    response = {
        "ok": True,
        "plant_id": PLANT_IDENTIFIER,
        "config_path": str(tariff_config_path),
        "active": _resolve_tariff_version_for_ms(tariff_store, at_ms),
        "version_count": len(tariff_store.get("versions") or []),
    }
    if include_history:
        response["versions"] = tariff_store.get("versions") or []
    return jsonify(response)


@app.put("/api/tariff-config")
@app.post("/api/tariff-config")
def save_tariff_config():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "json_body_required"}), 400

    tariff_store = _load_tariff_store()
    raw_version = payload.get("config") if isinstance(payload.get("config"), dict) else payload
    effective_from_ms = _parse_epoch_ms(raw_version.get("effective_from")) or int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    normalized_version = _normalize_tariff_version(raw_version, _iso_utc_from_ms(effective_from_ms))

    versions = [dict(item) for item in (tariff_store.get("versions") or [])]
    replaced = False
    for index, existing in enumerate(versions):
        existing_ms = _parse_epoch_ms(existing.get("effective_from")) or 0
        if existing_ms == effective_from_ms:
            versions[index] = normalized_version
            replaced = True
            break
    if not replaced:
        versions.append(normalized_version)

    updated_store = _write_tariff_store({
        "plant_id": PLANT_IDENTIFIER,
        "versions": versions,
    })
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "config_path": str(tariff_config_path),
            "active": _resolve_tariff_version_for_ms(updated_store, None),
            "saved": normalized_version,
            "version_count": len(updated_store.get("versions") or []),
        }
    )


def _run_synthetic_training_job(job_id: str, job_config: Dict[str, Any]) -> None:
    client = None
    deleted: List[str] = []
    try:
        source_mode = str(job_config.get("source_mode") or "generate").strip().lower()
        train_models = bool(job_config.get("train_models", True))
        history_from_ms = _parse_epoch_ms(job_config.get("history_from"))
        history_to_ms = _parse_epoch_ms(job_config.get("history_to"))
        estimated_records = (
            max(1, int((int(job_config["days"]) * 24 * 60 * 60) / max(1, int(job_config["step_sec"]))))
            if source_mode != "history"
            else 0
        )
        _set_synthetic_job_state(
            job_id=job_id,
            phase="starting",
            error="",
            started_at=_utc_iso(time.time()),
            finished_at="",
            progress_ratio=0.0,
            records_written=0,
            estimated_records=estimated_records,
            output_dir=str(job_config["output_dir"]),
            predictor_mode=str(job_config["predictor_mode"]),
            step_sec=int(job_config["step_sec"]),
            days=int(job_config["days"]),
            seed=int(job_config["seed"]),
            source_mode=source_mode,
            train_models=train_models,
            history_from=_iso_utc_from_ms(history_from_ms) if history_from_ms is not None else "",
            history_to=_iso_utc_from_ms(history_to_ms) if history_to_ms is not None else "",
            reuse_existing=bool(job_config.get("reuse_existing", False)),
            delete_saved_models=bool(job_config["delete_saved_models"]),
            overwrite=bool(job_config["overwrite"]),
            deleted_models=[],
            summary=None,
            predictor_commands_published=False,
            telemetry_files_found=0,
        )

        client = _connect_mqtt_client("history-api-synth")
        _publish_synthetic_job_status(client)

        if train_models and job_config["delete_saved_models"]:
            deleted = _delete_saved_models()
            _set_synthetic_job_state(deleted_models=deleted)
            _publish_synthetic_job_status(client)

        if source_mode == "history":
            if history_from_ms is None or history_to_ms is None or history_to_ms <= history_from_ms:
                raise RuntimeError("invalid_history_range")
            telemetry_files = _history_log_paths_for_range(history_from_ms, history_to_ms)
            telemetry_files_found = len(telemetry_files)
            if telemetry_files_found <= 0:
                raise RuntimeError(f"no_history_files_in_range:{_iso_utc_from_ms(history_from_ms)}:{_iso_utc_from_ms(history_to_ms)}")
            record_count = _count_history_telemetry_records(history_from_ms, history_to_ms)
            if record_count <= 0:
                raise RuntimeError(f"no_history_payloads_in_range:{_iso_utc_from_ms(history_from_ms)}:{_iso_utc_from_ms(history_to_ms)}")
            summary = {
                "plant_id": PLANT_IDENTIFIER,
                "synthetic_profile": "live_history_logs",
                "source_mode": "history",
                "output_dir": str(job_config["output_dir"]),
                "records_written": int(record_count),
                "step_sec": 0,
                "days": max(1, int((history_to_ms - history_from_ms) / (24 * 60 * 60 * 1000))),
                "start_utc": _iso_utc_from_ms(history_from_ms),
                "end_utc": _iso_utc_from_ms(history_to_ms),
            }
            _set_synthetic_job_state(
                phase="publishing_bootstrap",
                records_written=int(record_count),
                estimated_records=int(record_count),
                progress_ratio=1.0,
                summary=summary,
                telemetry_files_found=telemetry_files_found,
            )
            _publish_synthetic_job_status(client)
        elif job_config.get("reuse_existing", False):
            dataset_info = _synthetic_dataset_info(job_config["output_dir"])
            if dataset_info is None:
                raise RuntimeError(f"existing_synthetic_dataset_not_found:{job_config['output_dir']}")
            summary = dataset_info.get("summary") or {
                "plant_id": PLANT_IDENTIFIER,
                "synthetic_profile": dataset_info.get("synthetic_profile") or "pre_system_factory_baseline",
                "output_dir": str(job_config["output_dir"]),
                "records_written": int(dataset_info.get("record_count") or 0),
                "step_sec": int(dataset_info.get("step_sec") or job_config["step_sec"]),
                "days": int(dataset_info.get("days") or job_config["days"]),
                "start_utc": str(dataset_info.get("start_utc") or ""),
            }
            _set_synthetic_job_state(
                phase="reusing_existing",
                records_written=int(dataset_info.get("record_count") or 0),
                estimated_records=int(dataset_info.get("record_count") or 0),
                progress_ratio=1.0,
                summary=summary,
            )
            _publish_synthetic_job_status(client)
        else:
            def _on_progress(update: Dict[str, Any]) -> None:
                phase = str(update.get("phase") or "generating")
                state_updates = {
                    "phase": "generating" if phase != "generation_complete" else "publishing_bootstrap",
                    "records_written": int(update.get("records_written") or 0),
                    "estimated_records": int(update.get("estimated_records") or 0),
                    "progress_ratio": float(update.get("progress_ratio") or 0.0),
                    "output_dir": str(update.get("output_dir") or job_config["output_dir"]),
                }
                if update.get("day_key"):
                    state_updates["current_day"] = str(update["day_key"])
                if update.get("summary") is not None:
                    state_updates["summary"] = update["summary"]
                _set_synthetic_job_state(**state_updates)
                _publish_synthetic_job_status(client)

            summary = generate_dataset(
                plant_id=PLANT_IDENTIFIER,
                start_dt_utc=job_config["start_dt"],
                days=int(job_config["days"]),
                step_sec=int(job_config["step_sec"]),
                seed=int(job_config["seed"]),
                timezone_name=str(job_config["timezone_name"]),
                output_dir=job_config["output_dir"],
                policy_rotation=list(job_config["policy_items"]),
                overwrite=bool(job_config["overwrite"]),
                progress_callback=_on_progress,
            )

        record_count = max(1, int(summary.get("records_written") or 0))
        if source_mode != "history":
            telemetry_files_found = _count_synthetic_telemetry_files(job_config["output_dir"])
            if telemetry_files_found <= 0:
                raise RuntimeError(f"no_generated_telemetry_files:{job_config['output_dir']}")
        rf_payload = None
        lstm_payload = None
        predictor_commands_published = False
        if train_models:
            bootstrap_records = max(1000, min(200000, record_count))
            reset_model = bool(job_config["delete_saved_models"])
            rf_payload = {
                "PREDICTOR_MODE": job_config["predictor_mode"],
                "RESET_MODEL": reset_model,
                "RESET_BUFFERS": True,
                "LOGGER_OUTPUT_DIR": str(job_config["output_dir"]),
                "BOOTSTRAP_FROM_HISTORY": True,
                "BOOTSTRAP_FROM_HISTORY_ON_STARTUP": False,
                "BOOTSTRAP_HISTORY_MAX_RECORDS": bootstrap_records,
                "BOOTSTRAP_FROM_HISTORY_NOW": True,
            }
            lstm_payload = {
                "PREDICTOR_MODE": job_config["predictor_mode"],
                "RESET_MODEL": reset_model,
                "RESET_BUFFERS": True,
                "LOGGER_OUTPUT_DIR": str(job_config["output_dir"]),
                "BOOTSTRAP_FROM_HISTORY": True,
                "BOOTSTRAP_FROM_HISTORY_ON_STARTUP": False,
                "BOOTSTRAP_HISTORY_MAX_RECORDS": bootstrap_records,
                "BOOTSTRAP_FROM_HISTORY_NOW": True,
            }
            if source_mode != "history":
                rf_payload["TELEMETRY_SAMPLE_SEC"] = int(job_config["step_sec"])
                lstm_payload["TELEMETRY_SAMPLE_SEC"] = int(job_config["step_sec"])
            if history_from_ms is not None:
                rf_payload["BOOTSTRAP_RANGE_FROM"] = _iso_utc_from_ms(history_from_ms)
                lstm_payload["BOOTSTRAP_RANGE_FROM"] = _iso_utc_from_ms(history_from_ms)
            if history_to_ms is not None:
                rf_payload["BOOTSTRAP_RANGE_TO"] = _iso_utc_from_ms(history_to_ms)
                lstm_payload["BOOTSTRAP_RANGE_TO"] = _iso_utc_from_ms(history_to_ms)

            _set_synthetic_job_state(
                phase="publishing_bootstrap",
                summary=summary,
                progress_ratio=1.0,
                records_written=record_count,
                estimated_records=record_count,
                telemetry_files_found=telemetry_files_found,
            )
            _publish_synthetic_job_status(client)

            _publish_mqtt_json(client, TOPIC_PREDICTOR_LONG_CMD_NAME, rf_payload, retain=False)
            _publish_mqtt_json(client, TOPIC_PREDICTOR_LONG_LSTM_CMD_NAME, lstm_payload, retain=False)
            predictor_commands_published = True

        final_state = _set_synthetic_job_state(
            phase="complete",
            finished_at=_utc_iso(time.time()),
            predictor_commands_published=predictor_commands_published,
            summary=summary,
            deleted_models=deleted,
            models=_list_saved_models(),
            rf_command=rf_payload,
            lstm_command=lstm_payload,
        )
        _publish_synthetic_job_status(client, job=final_state)
    except Exception as exc:
        error_state = _set_synthetic_job_state(
            phase="error",
            error=str(exc),
            finished_at=_utc_iso(time.time()),
        )
        _publish_synthetic_job_status(client, job=error_state)
    finally:
        if client is not None:
            try:
                client.loop_stop()
            except Exception:
                pass
            try:
                client.disconnect()
            except Exception:
                pass


@app.get("/api/experiment-templates")
def get_experiment_templates():
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "templates": _experiment_templates(),
        }
    )


@app.get("/api/experiment-runs")
def list_experiment_runs():
    limit = _normalize_positive_int(request.args.get("limit"), 20)
    store = _load_experiment_store()
    runs = [_serialize_run(run) for run in (store.get("runs") or [])[: max(1, min(limit, 100))]]
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "path": str(experiment_runs_path),
            "count": len(runs),
            "runs": runs,
        }
    )


@app.post("/api/experiment-runs")
def create_experiment_run():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "json_body_required"}), 400
    template_id = str(payload.get("template_id") or "").strip()
    template = _template_map().get(template_id)
    if template is None:
        return jsonify({"ok": False, "error": f"unknown_template:{template_id}"}), 400
    try:
        config = _normalize_experiment_config(template_id, payload.get("config") if isinstance(payload.get("config"), dict) else payload)
        plan = _build_experiment_plan(template_id, config)
        offline_source = _resolve_offline_experiment_source(payload)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400

    now_iso = _iso_utc_from_ms(int(datetime.now(tz=timezone.utc).timestamp() * 1000))
    source_mode = offline_source["source_mode"] if offline_source is not None else "live"
    run = {
        "id": uuid.uuid4().hex,
        "template_id": template_id,
        "template_name": template.get("name"),
        "description": template.get("description"),
        "recommended_outputs": list(template.get("recommended_outputs") or []),
        "status": "completed" if offline_source is not None else "created",
        "config": config,
        "plan": [] if offline_source is not None else plan,
        "events": [{"type": "run_created", "ts": now_iso}],
        "created_at": now_iso,
        "updated_at": now_iso,
        "started_at": None,
        "finished_at": None,
        "current_step_index": 0,
        "source_mode": source_mode,
    }
    if offline_source is not None:
        run["started_at"] = _iso_utc_from_ms(int(offline_source["export_start_ms"]))
        run["finished_at"] = _iso_utc_from_ms(int(offline_source["export_end_ms"]))
        run["export_log_dir"] = str(offline_source["export_log_dir"])
        run["export_start_ms"] = int(offline_source["export_start_ms"])
        run["export_end_ms"] = int(offline_source["export_end_ms"])
        run["source_label"] = str(offline_source.get("source_label") or source_mode)
        if offline_source.get("source_summary") is not None:
            run["source_summary"] = offline_source["source_summary"]
        run["events"].append(
            {
                "type": "run_completed",
                "ts": now_iso,
                "detail": f"offline_export_source:{source_mode}",
            }
        )
    store = _load_experiment_store()
    runs = [run] + list(store.get("runs") or [])
    updated = _write_experiment_store({"plant_id": PLANT_IDENTIFIER, "runs": runs})
    saved = _find_run(updated, run["id"]) or run
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "run": _serialize_run(saved),
        }
    )


@app.get("/api/experiment-runs/<run_id>")
def get_experiment_run(run_id: str):
    store = _load_experiment_store()
    run = _find_run(store, run_id)
    if run is None:
        return jsonify({"ok": False, "error": "run_not_found"}), 404
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "run": _serialize_run(run),
        }
    )


@app.post("/api/experiment-runs/<run_id>/export-request")
def request_experiment_run_export(run_id: str):
    if not CHAPTER4_EXPORT_AVAILABLE or export_chapter4_dataset is None:
        return jsonify(
            {
                "ok": False,
                "error": "chapter4_export_unavailable",
                "detail": CHAPTER4_EXPORT_IMPORT_ERROR or "export_chapter4_data.py is not available on this deployment",
            }
        ), 503
    payload = request.get_json(silent=True)
    naming_mode = _normalize_export_naming_mode(
        request.args.get("naming")
        or ((payload or {}).get("naming") if isinstance(payload, dict) else None),
        "report",
    )
    run, job, should_start = _queue_experiment_export_job(run_id, naming_mode)
    if run is None:
        return jsonify({"ok": False, "error": "run_not_found"}), 404
    if job is None:
        return jsonify({"ok": False, "error": "run_not_exportable"}), 400
    if should_start:
        worker = threading.Thread(
            target=_run_experiment_export_job,
            args=(str(run_id), naming_mode),
            name=f"experiment-export-{str(run_id)[:8]}-{naming_mode}",
            daemon=True,
        )
        worker.start()
    return jsonify(
        {
            "ok": True,
            "accepted": True,
            "plant_id": PLANT_IDENTIFIER,
            "run": _serialize_run(run),
            "export_job": job,
        }
    ), 202


@app.post("/api/experiment-runs/<run_id>/event")
def post_experiment_run_event(run_id: str):
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "json_body_required"}), 400
    event_type = str(payload.get("type") or "").strip()
    if event_type not in {"run_started", "step_started", "step_completed", "run_completed", "run_failed", "run_cancelled"}:
        return jsonify({"ok": False, "error": f"invalid_event_type:{event_type}"}), 400
    step_id = str(payload.get("step_id") or "").strip() or None
    detail = payload.get("detail")

    store = _load_experiment_store()
    run = _find_run(store, run_id)
    if run is None:
        return jsonify({"ok": False, "error": "run_not_found"}), 404
    _record_run_event(run, event_type, step_id=step_id, detail=(None if detail is None else str(detail)))
    updated = _write_experiment_store(store)
    saved = _find_run(updated, run_id) or run
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "run": _serialize_run(saved),
        }
    )


@app.get("/api/admin/models")
def list_admin_models():
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "models_dir": str(models_dir),
            "resolved_model_dirs": {
                "rf": str(_resolve_model_dir("rf")),
                "lstm": str(_resolve_model_dir("lstm")),
            },
            "models": _list_saved_models(),
        }
    )


@app.delete("/api/admin/models")
@app.post("/api/admin/models/delete")
def delete_admin_models():
    payload = request.get_json(silent=True)
    model_type_raw = None
    if isinstance(payload, dict):
        model_type_raw = payload.get("model_type")
    elif request.args.get("model_type"):
        model_type_raw = request.args.get("model_type")
    model_type = str(model_type_raw or "").strip().lower()
    if model_type not in MODEL_FILE_GROUPS:
        model_type = None
    deleted = _delete_saved_models(model_type)
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "models_dir": str(models_dir),
            "resolved_model_dirs": {
                "rf": str(_resolve_model_dir("rf")),
                "lstm": str(_resolve_model_dir("lstm")),
            },
            "deleted": deleted,
            "models": _list_saved_models(),
        }
    )


@app.post("/api/admin/synthetic-training")
def synthetic_training_job():
    if not MQTT_AVAILABLE or mqtt is None:
        return jsonify({"ok": False, "error": "mqtt_unavailable", "detail": MQTT_IMPORT_ERROR or "paho-mqtt is not installed"}), 503

    payload = request.get_json(silent=True)
    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "json_body_required"}), 400
    source_mode = str(payload.get("source_mode") or "generate").strip().lower()
    if source_mode not in {"generate", "reuse", "history"}:
        source_mode = "generate"
    if source_mode in {"generate", "reuse"} and (not SYNTHETIC_DATA_AVAILABLE or generate_dataset is None or parse_start is None):
        return jsonify(
            {
                "ok": False,
                "error": "synthetic_training_unavailable",
                "detail": SYNTHETIC_DATA_IMPORT_ERROR or "generate_synthetic_industrial_telemetry.py is not available on this deployment",
            }
        ), 503
    existing_output_dir = _resolve_existing_synthetic_dir(payload.get("existing_output_dir"))
    reuse_existing = source_mode == "reuse" and existing_output_dir is not None
    if source_mode == "reuse" and not reuse_existing:
        return jsonify({"ok": False, "error": "invalid_existing_output_dir"}), 400

    days = _normalize_positive_int(payload.get("days"), 5)
    step_sec = _normalize_positive_int(payload.get("step_sec"), 5)
    try:
        seed = int(round(float(payload.get("seed", 42))))
    except Exception:
        seed = 42
    timezone_name = str(payload.get("timezone") or TARIFF_TIMEZONE or "Europe/London").strip() or "Europe/London"
    overwrite = bool(payload.get("overwrite", False))
    delete_saved_models = bool(payload.get("delete_saved_models", False))
    train_models = bool(payload.get("train_models", True))
    predictor_mode = _normalize_predictor_mode(payload.get("predictor_mode"), "TRAIN_ONLY")
    history_from_ms = _parse_epoch_ms(payload.get("history_from"))
    history_to_ms = _parse_epoch_ms(payload.get("history_to"))
    policy_rotation = payload.get("policy_rotation")
    if isinstance(policy_rotation, str):
        policy_items = [item.strip().upper() for item in policy_rotation.split(",") if item.strip()]
    elif isinstance(policy_rotation, list):
        policy_items = [str(item).strip().upper() for item in policy_rotation if str(item).strip()]
    else:
        policy_items = ["NO_ENERGY_MANAGEMENT"]
    policy_items = [item for item in policy_items if item in POLICY_OPTIONS]
    if not policy_items:
        policy_items = ["NO_ENERGY_MANAGEMENT"]

    start_dt = None
    if source_mode == "generate":
        try:
            start_dt = parse_start(payload.get("start"))
        except Exception as exc:
            return jsonify({"ok": False, "error": f"invalid_start:{exc}"}), 400
    elif source_mode == "history":
        if history_from_ms is None or history_to_ms is None or history_to_ms <= history_from_ms:
            return jsonify({"ok": False, "error": "invalid_history_range"}), 400

    current_job = _copy_synthetic_job_state()
    if _is_synthetic_job_active(current_job):
        return jsonify({"ok": False, "error": "synthetic_job_already_running", "job": current_job}), 409

    timestamp_slug = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if source_mode == "history":
        output_dir = log_dir.resolve()
    else:
        output_dir = existing_output_dir if reuse_existing else (synthetic_telemetry_output_dir / f"{PLANT_IDENTIFIER}_{timestamp_slug}_{days}d_{step_sec}s").resolve()
    if source_mode == "generate" and output_dir.exists() and not overwrite:
        return jsonify({"ok": False, "error": f"Refusing to overwrite existing file: {output_dir}"}), 409
    existing_dataset_info = _synthetic_dataset_info(output_dir) if reuse_existing else None
    if reuse_existing and existing_dataset_info is None:
        return jsonify({"ok": False, "error": f"invalid_existing_output_dir:{output_dir}"}), 400
    if existing_dataset_info:
        existing_summary = existing_dataset_info.get("summary") or {}
        days = int(existing_summary.get("days") or existing_dataset_info.get("days") or days)
        step_sec = int(existing_summary.get("step_sec") or existing_dataset_info.get("step_sec") or step_sec)
        try:
            seed = int(existing_summary.get("seed") or seed)
        except Exception:
            pass
        timezone_name = str(existing_summary.get("timezone") or timezone_name)

    job_id = uuid.uuid4().hex
    job_state = _set_synthetic_job_state(
        job_id=job_id,
        phase="queued",
        error="",
        queued_at=_utc_iso(time.time()),
        finished_at="",
        output_dir=str(output_dir),
        predictor_mode=predictor_mode,
        step_sec=step_sec,
        days=days,
        seed=seed,
        source_mode=source_mode,
        train_models=train_models,
        history_from=_iso_utc_from_ms(history_from_ms) if history_from_ms is not None else "",
        history_to=_iso_utc_from_ms(history_to_ms) if history_to_ms is not None else "",
        reuse_existing=reuse_existing,
        delete_saved_models=delete_saved_models,
        overwrite=overwrite,
        predictor_commands_published=False,
        records_written=0,
        estimated_records=(
            int(existing_dataset_info.get("record_count") or 0)
            if existing_dataset_info else (
                0 if source_mode == "history"
                else max(1, int((days * 24 * 60 * 60) / max(1, step_sec)))
            )
        ),
        progress_ratio=0.0,
    )

    worker = threading.Thread(
        target=_run_synthetic_training_job,
        args=(
            job_id,
            {
                "start_dt": start_dt,
                "days": days,
                "step_sec": step_sec,
                "seed": seed,
                "timezone_name": timezone_name,
                "output_dir": output_dir,
                "policy_items": policy_items,
                "overwrite": overwrite,
                "source_mode": source_mode,
                "history_from": history_from_ms,
                "history_to": history_to_ms,
                "reuse_existing": reuse_existing,
                "delete_saved_models": delete_saved_models,
                "train_models": train_models,
                "predictor_mode": predictor_mode,
            },
        ),
        name=f"synthetic-training-{job_id[:8]}",
        daemon=True,
    )
    worker.start()

    return jsonify(
        {
            "ok": True,
            "accepted": True,
            "plant_id": PLANT_IDENTIFIER,
            "job": job_state,
        }
    ), 202


@app.get("/api/admin/synthetic-training-status")
def get_synthetic_training_status():
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "job": _copy_synthetic_job_state(),
        }
    )


@app.get("/api/admin/synthetic-datasets")
def list_synthetic_datasets():
    limit = _normalize_positive_int(request.args.get("limit"), 50)
    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "root_dir": str(synthetic_telemetry_output_dir.resolve()),
            "datasets": _list_synthetic_datasets(limit=limit),
        }
    )


@app.get("/api/history")
def history():
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    default_from_ms = now_ms - (24 * 60 * 60 * 1000)
    start_ms = _parse_range_value(request.args.get("from"), default_from_ms)
    end_ms = _parse_range_value(request.args.get("to"), now_ms)
    if end_ms < start_ms:
        return jsonify({"ok": False, "error": "'to' must be >= 'from'"}), 400

    step_sec = int(_safe_float(request.args.get("step_sec")) or 30)
    step_sec = max(1, min(step_sec, 3600))

    max_points = int(_safe_float(request.args.get("max_points")) or 4000)
    max_points = max(100, min(max_points, 100000))

    power_min = _safe_float(request.args.get("power_min"))
    power_max = _safe_float(request.args.get("power_max"))
    if power_min is not None and power_max is not None and power_max < power_min:
        return jsonify({"ok": False, "error": "'power_max' must be >= 'power_min'"}), 400

    series_filter = _parse_series(request.args.get("series", ""))
    rows, stats, actual_points = _collect_history(start_ms, end_ms, step_sec, series_filter)
    tariff_store = _load_tariff_store()
    rows = _attach_actual_costs(rows, actual_points, tariff_store)

    if power_min is not None or power_max is not None:
        filtered = []
        for row in rows:
            power = _safe_float(row.get("actual_power_w"))
            if power is None:
                continue
            if power_min is not None and power < power_min:
                continue
            if power_max is not None and power > power_max:
                continue
            filtered.append(row)
        rows = filtered

    if len(rows) > max_points:
        stride = max(1, len(rows) // max_points)
        sampled = rows[::stride]
        if sampled and rows and sampled[-1]["ts"] != rows[-1]["ts"]:
            sampled.append(rows[-1])
        rows = sampled

    return jsonify(
        {
            "ok": True,
            "plant_id": PLANT_IDENTIFIER,
            "from": datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).isoformat(),
            "to": datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).isoformat(),
            "step_sec": step_sec,
            "count": len(rows),
            "rows": rows,
            "stats": stats,
            "tariff": {
                "active": _resolve_tariff_version_for_ms(tariff_store, end_ms),
                "version_count": len(tariff_store.get("versions") or []),
            },
        }
    )


@app.get("/api/chapter4-export")
def chapter4_export():
    if not CHAPTER4_EXPORT_AVAILABLE or export_chapter4_dataset is None:
        return jsonify(
            {
                "ok": False,
                "error": "chapter4_export_unavailable",
                "detail": CHAPTER4_EXPORT_IMPORT_ERROR or "export_chapter4_data.py is not available on this deployment",
            }
        ), 503

    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    default_from_ms = now_ms - (24 * 60 * 60 * 1000)
    start_ms = _parse_range_value(request.args.get("from"), default_from_ms)
    end_ms = _parse_range_value(request.args.get("to"), now_ms)
    if end_ms < start_ms:
        return jsonify({"ok": False, "error": "'to' must be >= 'from'"}), 400

    prediction_step_sec = int(_safe_float(request.args.get("prediction_step_sec")) or 5)
    prediction_step_sec = max(1, min(prediction_step_sec, 3600))

    max_lag_sec = int(_safe_float(request.args.get("max_lag_sec")) or 600)
    max_lag_sec = max(0, min(max_lag_sec, 24 * 60 * 60))
    naming_mode = str(request.args.get("naming") or "report").strip().lower()
    if naming_mode not in {"report", "standard"}:
        naming_mode = "report"

    try:
        archive_buffer, archive_name = _chapter4_export_buffer(
            start_ms,
            end_ms,
            prediction_step_sec=prediction_step_sec,
            max_lag_sec=max_lag_sec,
            naming_mode=naming_mode,
        )
        return send_file(
            archive_buffer,
            mimetype="application/zip",
            as_attachment=True,
            download_name=archive_name,
        )
    except FileNotFoundError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 404
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"ok": False, "error": f"chapter4_export_failed: {exc}"}), 500


@app.get("/api/experiment-runs/<run_id>/export")
def export_experiment_run(run_id: str):
    naming_mode = _normalize_export_naming_mode(request.args.get("naming"), "report")
    store = _load_experiment_store()
    run = _find_run(store, run_id)
    if run is None:
        return jsonify({"ok": False, "error": "run_not_found"}), 404
    serialized = _serialize_run(run)
    export_jobs = serialized.get("export_jobs") if isinstance(serialized.get("export_jobs"), dict) else {}
    job = export_jobs.get(naming_mode) if isinstance(export_jobs, dict) else None
    if not isinstance(job, dict) or not job.get("download_ready"):
        status = str((job or {}).get("status") or "idle").strip().lower() or "idle"
        error_text = str((job or {}).get("error") or "")
        return jsonify(
            {
                "ok": False,
                "error": "experiment_export_not_ready",
                "status": status,
                "detail": error_text,
                "run": serialized,
                "export_job": job or _normalize_export_job({}, naming_mode),
            }
        ), 409
    archive_name = str(job.get("archive_name") or "")
    if not archive_name:
        archive_name = f"experiment_{run.get('template_id')}_{run_id}.zip"
    archive_path = _cached_experiment_export_path(run_id, naming_mode, archive_name)
    if not archive_path.exists():
        _set_run_export_job(
            run_id,
            naming_mode,
            {
                "status": "error",
                "finished_at": _utc_iso(time.time()),
                "download_ready": False,
                "error": "cached_export_missing",
            },
        )
        return jsonify({"ok": False, "error": "cached_export_missing"}), 409
    return send_file(
        archive_path,
        mimetype="application/zip",
        as_attachment=True,
        download_name=archive_name,
    )


if __name__ == "__main__":
    print(f"[history-api] plant={PLANT_IDENTIFIER}")
    print(f"[history-api] log_dir={log_dir}")
    print(f"[history-api] listening on {HISTORY_API_HOST}:{HISTORY_API_PORT}")
    app.run(host=HISTORY_API_HOST, port=HISTORY_API_PORT)
