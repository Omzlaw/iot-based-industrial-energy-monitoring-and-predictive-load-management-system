#!/usr/bin/env python3
import json
import os
import signal
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt

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
TOPIC_TELEMETRY_NAME = f"dt/{PLANT_IDENTIFIER}/telemetry"
TOPIC_PREDICTION_NAME = f"dt/{PLANT_IDENTIFIER}/prediction"
TOPIC_PREDICTION_LONG_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long"
TOPIC_PREDICTION_LONG_LSTM_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long_lstm"
TOPICS_TO_LOG = [
    TOPIC_TELEMETRY_NAME,
    TOPIC_PREDICTION_NAME,
    TOPIC_PREDICTION_LONG_NAME,
    TOPIC_PREDICTION_LONG_LSTM_NAME,
]

_DEFAULT_LOG_DIR = os.path.join(os.path.dirname(__file__), "telemetry_logs")
LOGGER_OUTPUT_DIR = os.getenv("LOGGER_OUTPUT_DIR", _DEFAULT_LOG_DIR).strip() or _DEFAULT_LOG_DIR
try:
    LOGGER_AGGREGATION_SEC = int(float(os.getenv("LOGGER_AGGREGATION_SEC", "60").strip() or "60"))
except Exception:
    LOGGER_AGGREGATION_SEC = 60
LOGGER_AGGREGATION_SEC = max(1, LOGGER_AGGREGATION_SEC)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


LOGGER_EVENTS_ENABLED = _env_bool("LOGGER_EVENTS_ENABLED", True)

try:
    import certifi
    _CA_CERTS = certifi.where()
except Exception:
    _CA_CERTS = None

_log_dir = Path(LOGGER_OUTPUT_DIR)
_current_log_date = None
_current_log_file = None
_current_log_handle = None
_current_event_log_date = None
_current_event_log_file = None
_current_event_log_handle = None
_client = None
_topic_states = {}
_last_state = {
    "system": {},
    "process": {},
    "loads": {},
}


def _safe_float(value):
    try:
        number = float(value)
        if number != number:  # NaN
            return None
        return number
    except Exception:
        return None


def _round(value, places=4):
    if value is None:
        return None
    return round(float(value), places)


def _bucket_start_epoch(epoch_sec: int) -> int:
    return epoch_sec - (epoch_sec % LOGGER_AGGREGATION_SEC)


def _epoch_to_iso(epoch_sec: int) -> str:
    return datetime.fromtimestamp(epoch_sec, tz=timezone.utc).isoformat()


def _new_topic_state(bucket_start: int):
    return {
        "bucket_start": bucket_start,
        "count": 0,
        "last_payload": None,
        "metrics": {},
    }


def _update_metric(state: dict, key: str, value):
    number = _safe_float(value)
    if number is None:
        return
    metric = state["metrics"].setdefault(
        key,
        {"count": 0, "sum": 0.0, "min": None, "max": None, "last": None},
    )
    metric["count"] += 1
    metric["sum"] += number
    metric["last"] = number
    metric["min"] = number if metric["min"] is None else min(metric["min"], number)
    metric["max"] = number if metric["max"] is None else max(metric["max"], number)


def _metric_avg(state: dict, key: str):
    metric = state["metrics"].get(key)
    if not metric or metric["count"] <= 0:
        return None
    return metric["sum"] / metric["count"]


def _metric_last(state: dict, key: str):
    metric = state["metrics"].get(key)
    if not metric:
        return None
    return metric["last"]


def _metric_summary(state: dict, key: str):
    metric = state["metrics"].get(key)
    if not metric or metric["count"] <= 0:
        return None
    return {
        "avg": _round(metric["sum"] / metric["count"]),
        "min": _round(metric["min"]),
        "max": _round(metric["max"]),
        "last": _round(metric["last"]),
        "count": metric["count"],
    }


def _ensure_log_handle():
    global _current_log_date, _current_log_file, _current_log_handle
    now_utc = datetime.now(timezone.utc)
    log_date = now_utc.strftime("%Y-%m-%d")
    if _current_log_handle and _current_log_date == log_date:
        return now_utc

    if _current_log_handle:
        _current_log_handle.close()
        _current_log_handle = None

    _log_dir.mkdir(parents=True, exist_ok=True)
    _current_log_date = log_date
    _current_log_file = _log_dir / f"{PLANT_IDENTIFIER}_telemetry_{log_date}.jsonl"
    _current_log_handle = _current_log_file.open("a", encoding="utf-8")
    print(f"[telemetry-logger] writing: {_current_log_file}")
    return now_utc


def _ensure_event_log_handle():
    global _current_event_log_date, _current_event_log_file, _current_event_log_handle
    now_utc = datetime.now(timezone.utc)
    log_date = now_utc.strftime("%Y-%m-%d")
    if _current_event_log_handle and _current_event_log_date == log_date:
        return now_utc

    if _current_event_log_handle:
        _current_event_log_handle.close()
        _current_event_log_handle = None

    _log_dir.mkdir(parents=True, exist_ok=True)
    _current_event_log_date = log_date
    _current_event_log_file = _log_dir / f"{PLANT_IDENTIFIER}_events_{log_date}.jsonl"
    _current_event_log_handle = _current_event_log_file.open("a", encoding="utf-8")
    print(f"[telemetry-logger] writing events: {_current_event_log_file}")
    return now_utc


def _append_record(topic: str, payload):
    now_utc = _ensure_log_handle()
    record = {
        "logged_at_utc": now_utc.isoformat(),
        "topic": topic,
        "payload": payload,
    }
    _current_log_handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    _current_log_handle.flush()


def _append_event(event_type: str, payload: dict, topic: str = TOPIC_TELEMETRY_NAME):
    now_utc = _ensure_event_log_handle()
    record = {
        "logged_at_utc": now_utc.isoformat(),
        "topic": topic,
        "record_type": "event",
        "event_type": event_type,
        "payload": payload,
    }
    _current_event_log_handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    _current_event_log_handle.flush()


def _append_raw_record(topic: str, payload_raw: str):
    now_utc = _ensure_log_handle()
    record = {
        "logged_at_utc": now_utc.isoformat(),
        "topic": topic,
        "payload_raw": payload_raw,
        "parse_error": "invalid_json",
    }
    _current_log_handle.write(json.dumps(record, separators=(",", ":")) + "\n")
    _current_log_handle.flush()


def _track_change(section: str, key: str, current_value, epsilon: float = 0.0):
    previous_value = _last_state[section].get(key)
    if epsilon > 0.0:
        prev_num = _safe_float(previous_value)
        curr_num = _safe_float(current_value)
        if prev_num is not None and curr_num is not None:
            changed = abs(curr_num - prev_num) > epsilon
        else:
            changed = previous_value != current_value
    else:
        changed = previous_value != current_value
    _last_state[section][key] = current_value
    return changed, previous_value, current_value


def _log_critical_events(topic: str, payload: dict):
    if not LOGGER_EVENTS_ENABLED:
        return
    if topic != TOPIC_TELEMETRY_NAME:
        return

    system = payload.get("system", {}) if isinstance(payload.get("system"), dict) else {}
    process = payload.get("process", {}) if isinstance(payload.get("process"), dict) else {}
    loads = payload.get("loads", {}) if isinstance(payload.get("loads"), dict) else {}
    ts = payload.get("timestamp")

    system_watch = (
        "emergency_stop",
        "UNDERVOLTAGE_ACTIVE",
        "OVERVOLTAGE_ACTIVE",
    )
    for field in system_watch:
        if field not in system:
            continue
        changed, old_v, new_v = _track_change("system", field, system.get(field))
        if changed:
            _append_event(
                "system_state_changed",
                {
                    "timestamp": ts,
                    "field": field,
                    "old": old_v,
                    "new": new_v,
                },
                topic=topic,
            )

    process_watch = ("enabled", "state", "lock_active", "goal_reached", "cycle_count")
    for field in process_watch:
        if field not in process:
            continue
        changed, old_v, new_v = _track_change("process", field, process.get(field))
        if changed:
            _append_event(
                "process_state_changed",
                {
                    "timestamp": ts,
                    "field": field,
                    "old": old_v,
                    "new": new_v,
                },
                topic=topic,
            )

    load_watch = (
        ("on", 0.0),
        ("duty", 0.001),
        ("duty_applied", 0.001),
        ("fault_active", 0.0),
        ("fault_latched", 0.0),
        ("fault_code", 0.0),
        ("health", 0.0),
    )
    for load_name, load_data in loads.items():
        if not isinstance(load_data, dict):
            continue
        for field, epsilon in load_watch:
            if field not in load_data:
                continue
            state_key = f"{load_name}.{field}"
            changed, old_v, new_v = _track_change("loads", state_key, load_data.get(field), epsilon=epsilon)
            if changed:
                _append_event(
                    "load_state_changed",
                    {
                        "timestamp": ts,
                        "load": load_name,
                        "field": field,
                        "old": old_v,
                        "new": new_v,
                    },
                    topic=topic,
                )


def _update_topic_state(topic: str, state: dict, payload: dict):
    state["count"] += 1
    state["last_payload"] = payload

    if topic == TOPIC_TELEMETRY_NAME:
        system = payload.get("system", {}) if isinstance(payload.get("system"), dict) else {}
        energy = payload.get("energy", {}) if isinstance(payload.get("energy"), dict) else {}
        _update_metric(state, "total_power_w", system.get("total_power_w"))
        _update_metric(state, "total_current_a", system.get("total_current_a"))
        _update_metric(state, "supply_v", system.get("supply_v"))
        _update_metric(state, "total_energy_wh", energy.get("total_energy_wh"))
        return

    # Prediction topics
    _update_metric(state, "predicted_power_w", payload.get("predicted_power_w"))
    predicted_total_energy = payload.get("predicted_total_energy_wh")
    if predicted_total_energy is None:
        predicted_total_energy = payload.get("predicted_energy_wh")
    _update_metric(state, "predicted_total_energy_wh", predicted_total_energy)


def _build_telemetry_aggregate_payload(state: dict):
    bucket_start = state["bucket_start"]
    bucket_end = bucket_start + LOGGER_AGGREGATION_SEC
    last_payload = state.get("last_payload") or {}
    last_system = last_payload.get("system", {}) if isinstance(last_payload.get("system"), dict) else {}
    last_energy = last_payload.get("energy", {}) if isinstance(last_payload.get("energy"), dict) else {}

    avg_power = _metric_avg(state, "total_power_w")
    avg_current = _metric_avg(state, "total_current_a")
    avg_supply_v = _metric_avg(state, "supply_v")
    last_energy_wh = _metric_last(state, "total_energy_wh")

    if avg_power is None:
        avg_power = _safe_float(last_system.get("total_power_w"))
    if avg_current is None:
        avg_current = _safe_float(last_system.get("total_current_a"))
    if avg_supply_v is None:
        avg_supply_v = _safe_float(last_system.get("supply_v"))
    if last_energy_wh is None:
        last_energy_wh = _safe_float(last_energy.get("total_energy_wh"))

    system_payload = dict(last_system)
    system_payload.update(
        {
            "timestamp": _epoch_to_iso(bucket_end),
            "plant_id": str(last_system.get("plant_id", PLANT_IDENTIFIER)),
            "total_power_w": _round(avg_power, 4),
            "total_current_a": _round(avg_current, 4),
            "supply_v": _round(avg_supply_v, 4),
        }
    )

    energy_payload = dict(last_energy)
    energy_payload["total_energy_wh"] = _round(last_energy_wh, 4)

    output = {
        "schema_version": "1.0",
        "producer": "telemetry_logger_aggregate",
        "aggregation_sec": LOGGER_AGGREGATION_SEC,
        "bucket_start_utc": _epoch_to_iso(bucket_start),
        "bucket_end_utc": _epoch_to_iso(bucket_end),
        "sample_count": state["count"],
        "timestamp": _epoch_to_iso(bucket_end),
        "system": system_payload,
        "energy": energy_payload,
        "aggregation_stats": {
            "total_power_w": _metric_summary(state, "total_power_w"),
            "total_current_a": _metric_summary(state, "total_current_a"),
            "supply_v": _metric_summary(state, "supply_v"),
            "total_energy_wh": _metric_summary(state, "total_energy_wh"),
        },
    }

    for key in ("environment", "process", "loads", "evaluation", "prediction_context", "voltages"):
        value = last_payload.get(key)
        if isinstance(value, dict):
            output[key] = value

    return output


def _build_prediction_aggregate_payload(state: dict):
    bucket_start = state["bucket_start"]
    bucket_end = bucket_start + LOGGER_AGGREGATION_SEC
    last_payload = state.get("last_payload") or {}

    predicted_power_avg = _metric_avg(state, "predicted_power_w")
    if predicted_power_avg is None:
        predicted_power_avg = _safe_float(last_payload.get("predicted_power_w"))

    predicted_energy_avg = _metric_avg(state, "predicted_total_energy_wh")
    if predicted_energy_avg is None:
        predicted_energy_avg = _safe_float(last_payload.get("predicted_total_energy_wh"))
    if predicted_energy_avg is None:
        predicted_energy_avg = _safe_float(last_payload.get("predicted_energy_wh"))

    timestamp_value = last_payload.get("timestamp")
    if timestamp_value is None:
        timestamp_value = _epoch_to_iso(bucket_end)

    return {
        "schema_version": str(last_payload.get("schema_version", "1.0")),
        "producer": str(last_payload.get("producer", "predictor_aggregate")),
        "method": last_payload.get("method"),
        "plant_id": str(last_payload.get("plant_id", PLANT_IDENTIFIER)),
        "predictor_mode": last_payload.get("predictor_mode"),
        "model_ready": last_payload.get("model_ready"),
        "aggregation_sec": LOGGER_AGGREGATION_SEC,
        "bucket_start_utc": _epoch_to_iso(bucket_start),
        "bucket_end_utc": _epoch_to_iso(bucket_end),
        "sample_count": state["count"],
        "timestamp": timestamp_value,
        "horizon_sec": _safe_float(last_payload.get("horizon_sec")) or 0.0,
        "predicted_power_w": _round(predicted_power_avg, 4),
        "predicted_energy_wh": _round(predicted_energy_avg, 4),
        "predicted_total_energy_wh": _round(predicted_energy_avg, 4),
        "risk_level": last_payload.get("risk_level"),
        "peak_risk": last_payload.get("peak_risk"),
        "energy_budget_risk": last_payload.get("energy_budget_risk"),
        "aggregation_stats": {
            "predicted_power_w": _metric_summary(state, "predicted_power_w"),
            "predicted_total_energy_wh": _metric_summary(state, "predicted_total_energy_wh"),
        },
    }


def _flush_topic_state(topic: str, state: dict):
    if not state or state.get("count", 0) <= 0:
        return
    if topic == TOPIC_TELEMETRY_NAME:
        payload = _build_telemetry_aggregate_payload(state)
    else:
        payload = _build_prediction_aggregate_payload(state)
    _append_record(topic, payload)


def _flush_completed_buckets(current_bucket_start: int):
    stale_topics = [topic for topic, state in _topic_states.items() if state["bucket_start"] < current_bucket_start]
    for topic in stale_topics:
        _flush_topic_state(topic, _topic_states[topic])
        del _topic_states[topic]


def _flush_all_states():
    topics = list(_topic_states.keys())
    for topic in topics:
        _flush_topic_state(topic, _topic_states[topic])
        del _topic_states[topic]


def _shutdown(signum=None, frame=None):
    del signum, frame
    print("[telemetry-logger] shutting down...")
    try:
        _flush_all_states()
    except Exception as exc:
        print(f"[telemetry-logger] flush failed during shutdown: {exc}")
    try:
        if _client is not None:
            _client.loop_stop()
            _client.disconnect()
    except Exception:
        pass
    try:
        if _current_log_handle is not None:
            _current_log_handle.close()
    except Exception:
        pass
    try:
        if _current_event_log_handle is not None:
            _current_event_log_handle.close()
    except Exception:
        pass


def on_connect(client, userdata, flags, reason_code, properties):
    del userdata, flags, properties
    if reason_code == 0:
        print(f"[telemetry-logger] connected: topics={','.join(TOPICS_TO_LOG)}")
        # paho-mqtt expects a list of (topic, qos) pairs for multi-subscribe.
        client.subscribe([(topic, 0) for topic in TOPICS_TO_LOG])
    else:
        print(f"[telemetry-logger] connect failed: reason_code={reason_code}")


def on_message(client, userdata, msg):
    del client, userdata
    payload_raw = msg.payload.decode("utf-8", errors="replace")
    try:
        payload = json.loads(payload_raw)
        if not isinstance(payload, dict):
            return
    except Exception:
        _append_raw_record(msg.topic, payload_raw)
        return

    try:
        _log_critical_events(msg.topic, payload)

        now_sec = int(time.time())
        current_bucket_start = _bucket_start_epoch(now_sec)
        _flush_completed_buckets(current_bucket_start)

        state = _topic_states.get(msg.topic)
        if state is None:
            state = _new_topic_state(current_bucket_start)
            _topic_states[msg.topic] = state

        if state["bucket_start"] != current_bucket_start:
            _flush_topic_state(msg.topic, state)
            state = _new_topic_state(current_bucket_start)
            _topic_states[msg.topic] = state

        _update_topic_state(msg.topic, state, payload)
    except Exception as exc:
        print(f"[telemetry-logger] write failed: {exc}")


def main():
    global _client
    print("[telemetry-logger] starting...")
    print(f"[telemetry-logger] broker={MQTT_BROKER_HOST}:{MQTT_BROKER_PORT} topics={','.join(TOPICS_TO_LOG)}")
    print(f"[telemetry-logger] output_dir={_log_dir}")
    print(f"[telemetry-logger] aggregation_sec={LOGGER_AGGREGATION_SEC}")
    print(f"[telemetry-logger] events_enabled={LOGGER_EVENTS_ENABLED}")
    _log_dir.mkdir(parents=True, exist_ok=True)

    _client = mqtt.Client(
        client_id=f"telemetry-logger-{PLANT_IDENTIFIER}",
        protocol=mqtt.MQTTv5,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )

    if MQTT_USERNAME and MQTT_PASSWORD:
        _client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    else:
        print("[telemetry-logger] warning: HIVEMQ_USER/HIVEMQ_PASS not set")

    if _CA_CERTS:
        _client.tls_set(
            ca_certs=_CA_CERTS,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
    else:
        _client.tls_set_context(ssl.create_default_context())

    _client.on_connect = on_connect
    _client.on_message = on_message

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    _client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
    _client.loop_forever()


if __name__ == "__main__":
    main()
