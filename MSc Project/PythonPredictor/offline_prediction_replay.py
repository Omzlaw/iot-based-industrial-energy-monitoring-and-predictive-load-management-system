#!/usr/bin/env python3
import importlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

PLANT_IDENTIFIER = os.getenv("PLANT_IDENTIFIER", "factory1").strip() or "factory1"
MODEL_SERVICE_DIR_HINTS = {
    "rf": ("long", "long-rf", "predictor-long-rf", "rf"),
    "lstm": ("lstm", "long-lstm", "predictor-long-lstm"),
}


def _extract_logged_topic_payload(record: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    topic = str(record.get("topic") or "")
    payload = record.get("payload")
    if isinstance(payload, dict):
        return topic, payload
    if topic and isinstance(record.get("system"), dict):
        return topic, record
    return topic, None


def _parse_epoch_ms(value: Any) -> Optional[int]:
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
            iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
            try:
                dt = datetime.fromisoformat(iso_text)
            except Exception:
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
    if raw > 1e12:
        return int(raw)
    if raw > 946684800:
        return int(raw * 1000)
    return None


def _iter_days(start_ms: int, end_ms: int):
    start_day = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).date()
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


def _record_timestamp_ms(record: Dict[str, Any], payload: Dict[str, Any]) -> Optional[int]:
    system = payload.get("system") or {}
    return (
        _parse_epoch_ms(payload.get("timestamp"))
        or _parse_epoch_ms(system.get("timestamp"))
        or _parse_epoch_ms(record.get("logged_at_utc"))
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        return out if out == out else default
    except Exception:
        return default


def _candidate_model_dirs(model_type: str, plant_id: str, preferred_dir: Optional[Path]) -> Iterable[Path]:
    primary_name = f"{plant_id}_long_rf.pkl" if model_type == "rf" else f"{plant_id}_long_lstm.pt"
    seen = set()
    preferred = preferred_dir.expanduser().resolve() if preferred_dir is not None else None
    if preferred is not None:
        yield preferred
        seen.add(str(preferred))

    roots: List[Path] = []
    for base in [Path(__file__).resolve().parent, Path.cwd().resolve(), preferred.parent if preferred is not None else None]:
        if base is None:
            continue
        cursor = Path(base).resolve()
        for _ in range(5):
            if cursor.exists() and cursor.is_dir() and cursor not in roots:
                roots.append(cursor)
            parent = cursor.parent
            if parent == cursor:
                break
            cursor = parent

    direct_candidates: List[Path] = []
    for root in roots:
        for candidate in (
            root / "models",
            root / "shared" / "models",
            root / "current" / "models",
        ):
            if candidate not in direct_candidates:
                direct_candidates.append(candidate)
        for service_name in MODEL_SERVICE_DIR_HINTS.get(model_type, ()):
            for candidate in (
                root / service_name / "shared" / "models",
                root / service_name / "current" / "models",
                root / service_name / "models",
            ):
                if candidate not in direct_candidates:
                    direct_candidates.append(candidate)

    for candidate in direct_candidates:
        if str(candidate) in seen:
            continue
        seen.add(str(candidate))
        if (candidate / primary_name).exists():
            yield candidate.resolve()

    for root in roots:
        try:
            for match in root.rglob(primary_name):
                candidate = match.parent.resolve()
                if str(candidate) in seen:
                    continue
                seen.add(str(candidate))
                yield candidate
        except Exception:
            continue


@dataclass
class _ShortPredictorRunner:
    module: Any
    configured: bool = False

    def reset(self, sample_sec: float) -> None:
        cfg = {
            "RESET": True,
            "TELEMETRY_SAMPLE_SEC": max(0.1, float(sample_sec)),
            "PUBLISH_INTERVAL_SEC": 1,
            "MIN_SAMPLE_COUNT": 1,
        }
        self.module.apply_config(cfg)
        self.module.last_publish_time = 0.0
        self.configured = True

    def predict(self, payload: Dict[str, Any], sample_sec: float) -> Optional[Dict[str, Any]]:
        if not self.configured:
            self.reset(sample_sec)
        parsed = self.module._parse_telemetry_payload(payload)
        if not parsed:
            return None
        ema_power_value = self.module._update_power_buffers(parsed["total_power_w"], parsed["sample_ts"])
        smoothed_power_w = self.module._smoothed_power()
        predicted_power_w = self.module._forecast_power(self.module.FORECAST_HORIZON_SEC)
        system = parsed["system"]
        energy = parsed["energy"]
        process = parsed["process"]
        loads = parsed["loads"]
        energy_cap_goal_wh = float(
            system.get(
                "ENERGY_GOAL_VALUE_WH",
                system.get("MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH", self.module.MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH),
            )
            or 0.0
        )
        energy_goal_duration_sec = float(
            system.get(
                "ENERGY_GOAL_DURATION_SEC",
                (energy.get("budget", {}) or {}).get("window_duration_sec", 60 * 60.0),
            )
            or 60 * 60.0
        )
        energy_projection = self.module._compute_energy_projection(
            predicted_power_w,
            parsed["current_total_energy_wh"],
            parsed["current_energy_goal_wh"],
            energy_cap_goal_wh,
            self.module.FORECAST_HORIZON_SEC,
        )
        risk = self.module._determine_risk_levels(
            predicted_power_w,
            ema_power_value,
            energy_projection["energy_window_pressure"],
            energy_projection["energy_budget_risk"],
            energy_cap_goal_wh,
            parsed["current_energy_goal_wh"],
            energy_projection["projected_energy_goal_wh"],
            float(system.get("MAX_TOTAL_POWER_W", self.module.MAX_TOTAL_POWER_W) or self.module.MAX_TOTAL_POWER_W),
        )
        prediction_payload = self.module._build_prediction_payload(
            system,
            process,
            loads,
            parsed["env_temp"],
            parsed["env_humidity"],
            parsed["has_environment"],
            ema_power_value,
            smoothed_power_w,
            predicted_power_w,
            self.module.FORECAST_HORIZON_SEC,
            energy_projection,
            parsed["current_energy_goal_wh"],
            energy_cap_goal_wh,
            energy_goal_duration_sec,
            float(system.get("MAX_TOTAL_POWER_W", self.module.MAX_TOTAL_POWER_W) or self.module.MAX_TOTAL_POWER_W),
            risk["risk_level"],
            risk["peak_risk"],
            energy_projection["energy_budget_risk"],
        )
        predicted_power_w = _safe_float(prediction_payload.get("predicted_power_w"), 0.0) or 0.0
        predicted_window_energy_wh = _safe_float(prediction_payload.get("predicted_window_energy_wh"), 0.0) or 0.0
        ema_baseline_w = max(0.0, _safe_float(ema_power_value, 0.0) or 0.0)
        if ema_baseline_w > 0.0 and (predicted_power_w <= 0.0 or predicted_window_energy_wh <= 0.0):
            fallback_projection = self.module._compute_energy_projection(
                ema_baseline_w,
                parsed["current_total_energy_wh"],
                parsed["current_energy_goal_wh"],
                energy_cap_goal_wh,
                self.module.FORECAST_HORIZON_SEC,
            )
            prediction_payload["predicted_power_w"] = round(ema_baseline_w, 4)
            prediction_payload["predicted_energy_wh"] = round(fallback_projection["predicted_incremental_energy_wh"], 4)
            prediction_payload["predicted_incremental_energy_wh"] = round(fallback_projection["predicted_incremental_energy_wh"], 4)
            prediction_payload["predicted_total_energy_wh"] = round(fallback_projection["predicted_total_energy_wh"], 4)
            prediction_payload["predicted_window_energy_wh"] = round(fallback_projection["predicted_window_energy_wh"], 4)
            prediction_payload["projected_energy_window_wh"] = round(fallback_projection["projected_energy_goal_wh"], 4)
            prediction_payload["projected_energy_goal_wh"] = round(fallback_projection["projected_energy_goal_wh"], 4)
        return prediction_payload

@dataclass
class _RfPredictorRunner:
    module: Any
    model_dir: Path
    plant_id: str
    loaded: bool = False
    warning: str = ""
    status_message: str = ""
    last_error: str = ""

    def _configure_and_load(self, model_dir: Path) -> bool:
        self.model_dir = model_dir.resolve()
        meta_path = self.model_dir / f"{self.plant_id}_long_rf.meta.json"
        meta = {}
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        self.module.MODEL_DIR = str(self.model_dir)
        self.module.RF_MODEL_PATH = str(self.model_dir / f"{self.plant_id}_long_rf.pkl")
        self.module.RF_META_PATH = str(meta_path)
        self.module.RF_ANALYTICS_PATH = str(self.model_dir / f"{self.plant_id}_long_rf.analytics.json")
        self.module.apply_config(
            {
                "PREDICTOR_MODE": "EVAL_ONLY",
                "RESET_BUFFERS": True,
                "FORECAST_HORIZON_SEC": int(meta.get("forecast_horizon_sec", self.module.FORECAST_HORIZON_SEC)),
                "TELEMETRY_SAMPLE_SEC": float(meta.get("telemetry_sample_sec", self.module.TELEMETRY_SAMPLE_SEC)),
            }
        )
        self.module.pending_forecasts.clear()
        self.module.eval_errors.clear()
        self.module.last_status_error = ""
        self.module.model_meta_warning_fields = []
        self.loaded = bool(self.module.load_model_state())
        self.warning = ",".join(getattr(self.module, "model_meta_warning_fields", []) or [])
        self.status_message = str(getattr(self.module, "status_message", "") or "")
        self.last_error = str(getattr(self.module, "last_status_error", "") or "")
        return self.loaded

    def reset_and_load(self) -> bool:
        last_failure = ""
        for candidate in _candidate_model_dirs("rf", self.plant_id, self.model_dir):
            self.model_dir = candidate
            if self._configure_and_load(candidate):
                return True
            failure = str(getattr(self.module, "last_status_error", "") or getattr(self.module, "status_message", "") or "")
            if failure:
                last_failure = failure
        if last_failure and not self.last_error:
            self.last_error = last_failure
        return False

    def predict(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.loaded:
            return None
        return self.module._process_telemetry_payload(payload, client=None, publish=False, bootstrap=False)


@dataclass
class _LstmPredictorRunner:
    module: Any
    model_dir: Path
    plant_id: str
    loaded: bool = False
    warning: str = ""
    status_message: str = ""
    last_error: str = ""

    def _configure_and_load(self, model_dir: Path) -> bool:
        self.model_dir = model_dir.resolve()
        meta_path = self.model_dir / f"{self.plant_id}_long_lstm.meta.json"
        meta = {}
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                meta = {}
        self.module.MODEL_DIR = str(self.model_dir)
        self.module.LSTM_MODEL_PATH = str(self.model_dir / f"{self.plant_id}_long_lstm.pt")
        self.module.LSTM_X_SCALER_PATH = str(self.model_dir / f"{self.plant_id}_long_lstm_x_scaler.pkl")
        self.module.LSTM_Y_SCALER_PATH = str(self.model_dir / f"{self.plant_id}_long_lstm_y_scaler.pkl")
        self.module.LSTM_META_PATH = str(meta_path)
        self.module.LSTM_ANALYTICS_PATH = str(self.model_dir / f"{self.plant_id}_long_lstm.analytics.json")
        self.module.apply_config(
            {
                "PREDICTOR_MODE": "EVAL_ONLY",
                "RESET_BUFFERS": True,
                "FORECAST_HORIZON_SEC": int(meta.get("forecast_horizon_sec", self.module.FORECAST_HORIZON_SEC)),
                "TELEMETRY_SAMPLE_SEC": float(meta.get("telemetry_sample_sec", self.module.TELEMETRY_SAMPLE_SEC)),
                "SEQUENCE_LENGTH": int(meta.get("sequence_length", self.module.SEQUENCE_LENGTH)),
            }
        )
        self.module.pending_forecasts.clear()
        self.module.eval_errors.clear()
        self.module.last_status_error = ""
        self.module.model_meta_warning_fields = []
        self.loaded = bool(self.module.load_model_state())
        self.warning = ",".join(getattr(self.module, "model_meta_warning_fields", []) or [])
        self.status_message = str(getattr(self.module, "status_message", "") or "")
        self.last_error = str(getattr(self.module, "last_status_error", "") or "")
        return self.loaded

    def reset_and_load(self) -> bool:
        last_failure = ""
        for candidate in _candidate_model_dirs("lstm", self.plant_id, self.model_dir):
            self.model_dir = candidate
            if self._configure_and_load(candidate):
                return True
            failure = str(getattr(self.module, "last_status_error", "") or getattr(self.module, "status_message", "") or "")
            if failure:
                last_failure = failure
        if last_failure and not self.last_error:
            self.last_error = last_failure
        return False

    def predict(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not self.loaded:
            return None
        return self.module._process_telemetry_payload(payload, client=None, publish=False, bootstrap=False)


def _build_prediction_source(payload: Optional[Dict[str, Any]], method: str) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {
            "method": method,
            "model_ready": False,
            "fresh": False,
            "applied": False,
        }
    return {
        "method": method,
        "model_ready": bool(payload.get("model_ready", True)),
        "fresh": True,
        "applied": True,
        "predicted_power_w": payload.get("predicted_power_w"),
        "predicted_window_energy_wh": payload.get("predicted_window_energy_wh") or payload.get("projected_energy_window_wh"),
        "predicted_energy_wh": payload.get("predicted_energy_wh") or payload.get("predicted_incremental_energy_wh"),
        "predicted_total_energy_wh": payload.get("predicted_total_energy_wh"),
        "current_energy_window_wh": payload.get("current_energy_window_wh"),
        "projected_energy_window_wh": payload.get("projected_energy_window_wh") or payload.get("predicted_window_energy_wh"),
        "current_energy_goal_ratio": payload.get("current_energy_goal_ratio"),
        "projected_energy_goal_ratio": payload.get("projected_energy_goal_ratio"),
        "horizon_sec": payload.get("horizon_sec"),
        "risk_level": payload.get("risk_level"),
        "peak_risk": payload.get("peak_risk"),
        "energy_window_pressure": payload.get("energy_window_pressure"),
        "energy_budget_risk": payload.get("energy_budget_risk"),
    }


def _preferred_prediction_power(short_payload: Optional[Dict[str, Any]], rf_payload: Optional[Dict[str, Any]], lstm_payload: Optional[Dict[str, Any]]) -> Optional[float]:
    for payload in (rf_payload, lstm_payload, short_payload):
        if isinstance(payload, dict) and payload.get("predicted_power_w") is not None:
            return _safe_float(payload.get("predicted_power_w"), default=0.0)
    return None


def prepare_offline_prediction_log_dir(
    *,
    source_log_dir: Path,
    output_dir: Path,
    plant_id: str,
    start_ms: int,
    end_ms: int,
    rf_model_dir: Optional[Path] = None,
    lstm_model_dir: Optional[Path] = None,
    progress_callback=None,
) -> Dict[str, Any]:
    short_module = importlib.import_module("main")
    rf_module = importlib.import_module("long_term_predictor")
    lstm_module = importlib.import_module("lstm_long_term_predictor")

    short_runner = _ShortPredictorRunner(module=short_module)
    rf_runner = _RfPredictorRunner(
        module=rf_module,
        model_dir=Path(rf_model_dir or (Path(__file__).resolve().parent / "models")).resolve(),
        plant_id=plant_id,
    )
    lstm_runner = _LstmPredictorRunner(
        module=lstm_module,
        model_dir=Path(lstm_model_dir or (Path(__file__).resolve().parent / "models")).resolve(),
        plant_id=plant_id,
    )

    rf_loaded = rf_runner.reset_and_load()
    lstm_loaded = lstm_runner.reset_and_load()
    output_dir.mkdir(parents=True, exist_ok=True)
    telemetry_topic = f"dt/{plant_id}/telemetry"

    telemetry_records = 0
    ema_predictions = 0
    rf_predictions = 0
    lstm_predictions = 0
    ema_nonzero_predictions = 0
    ema_last_predicted_power_w = 0.0
    ema_max_predicted_power_w = 0.0
    ema_last_window_energy_wh = 0.0
    ema_max_window_energy_wh = 0.0
    first_sample_sec: Optional[float] = None
    previous_ts_ms: Optional[int] = None
    total_source_bytes = 0
    processed_source_bytes = 0

    for day in _iter_days(start_ms, end_ms):
        source_path = Path(source_log_dir) / f"{plant_id}_telemetry_{day.isoformat()}.jsonl"
        if source_path.exists():
            try:
                total_source_bytes += max(0, int(source_path.stat().st_size))
            except Exception:
                pass

    def _emit_progress(phase: str, ratio: float, **extra: Any) -> None:
        if progress_callback is None:
            return
        payload = {
            "phase": phase,
            "progress_ratio": max(0.0, min(1.0, float(ratio))),
            "telemetry_records": telemetry_records,
            "ema_predictions": ema_predictions,
            "rf_predictions": rf_predictions,
            "lstm_predictions": lstm_predictions,
            "total_source_bytes": total_source_bytes,
            "processed_source_bytes": processed_source_bytes,
        }
        payload.update(extra)
        try:
            progress_callback(payload)
        except Exception:
            pass

    _emit_progress(
        "loading_models",
        0.02,
        message="Loading saved models for offline prediction replay",
        rf_model_loaded=rf_loaded,
        lstm_model_loaded=lstm_loaded,
    )

    for day in _iter_days(start_ms, end_ms):
        source_path = Path(source_log_dir) / f"{plant_id}_telemetry_{day.isoformat()}.jsonl"
        if not source_path.exists():
            continue
        try:
            file_size = max(0, int(source_path.stat().st_size))
        except Exception:
            file_size = 0
        target_path = output_dir / source_path.name
        with source_path.open("r", encoding="utf-8") as src, target_path.open("w", encoding="utf-8") as dst:
            last_reported_records = telemetry_records
            for line in src:
                processed_source_bytes += len(line.encode("utf-8"))
                text = line.strip()
                if not text:
                    continue
                try:
                    record = json.loads(text)
                except Exception:
                    continue
                if not isinstance(record, dict):
                    continue
                topic, payload = _extract_logged_topic_payload(record)
                if topic != telemetry_topic or not isinstance(payload, dict):
                    continue
                ts_ms = _record_timestamp_ms(record, payload)
                if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                    continue
                telemetry_records += 1
                if first_sample_sec is None:
                    agg_sec = _safe_float(record.get("aggregation_sec"), default=0.0)
                    if agg_sec > 0:
                        first_sample_sec = agg_sec
                    elif previous_ts_ms is not None and ts_ms > previous_ts_ms:
                        first_sample_sec = max(0.1, (ts_ms - previous_ts_ms) / 1000.0)
                    else:
                        first_sample_sec = 5.0
                previous_ts_ms = ts_ms

                short_payload = short_runner.predict(payload, sample_sec=first_sample_sec or 5.0)
                rf_payload = rf_runner.predict(payload)
                lstm_payload = lstm_runner.predict(payload)
                if short_payload is not None:
                    ema_predictions += 1
                    ema_power_w = max(0.0, _safe_float(short_payload.get("predicted_power_w"), 0.0) or 0.0)
                    ema_window_energy_wh = max(0.0, _safe_float(short_payload.get("predicted_window_energy_wh"), 0.0) or 0.0)
                    ema_last_predicted_power_w = ema_power_w
                    ema_max_predicted_power_w = max(ema_max_predicted_power_w, ema_power_w)
                    ema_last_window_energy_wh = ema_window_energy_wh
                    ema_max_window_energy_wh = max(ema_max_window_energy_wh, ema_window_energy_wh)
                    if ema_power_w > 0.0 or ema_window_energy_wh > 0.0:
                        ema_nonzero_predictions += 1
                if rf_payload is not None:
                    rf_predictions += 1
                if lstm_payload is not None:
                    lstm_predictions += 1

                system = payload.setdefault("system", {}) if isinstance(payload, dict) else {}
                prediction_sources = system.setdefault("prediction_sources", {}) if isinstance(system, dict) else {}
                prediction_sources["short"] = _build_prediction_source(short_payload, "EMA + LinearTrend")
                prediction_sources["long_rf"] = _build_prediction_source(rf_payload, "RandomForestRegressor")
                prediction_sources["long_lstm"] = _build_prediction_source(lstm_payload, "LSTM")

                prediction_context = payload.setdefault("prediction_context", {}) if isinstance(payload, dict) else {}
                preferred_power = _preferred_prediction_power(short_payload, rf_payload, lstm_payload)
                if preferred_power is not None:
                    prediction_context["predicted_power_w"] = round(preferred_power, 4)
                payload["prediction_context"] = prediction_context

                if isinstance(record.get("payload"), dict):
                    record["payload"] = payload
                else:
                    record = payload
                dst.write(json.dumps(record, sort_keys=True) + "\n")
                if (
                    telemetry_records == 1
                    or telemetry_records - last_reported_records >= 250
                    or (file_size > 0 and processed_source_bytes >= total_source_bytes)
                ):
                    ratio = 0.05
                    if total_source_bytes > 0:
                        ratio = 0.05 + (min(processed_source_bytes, total_source_bytes) / total_source_bytes) * 0.90
                    _emit_progress(
                        "replaying_predictions",
                        ratio,
                        current_day=day.isoformat(),
                        message=f"Replaying predictions for {day.isoformat()}",
                        rf_model_loaded=rf_loaded,
                        lstm_model_loaded=lstm_loaded,
                    )
                    last_reported_records = telemetry_records

    _emit_progress(
        "completed",
        1.0,
        message="Offline prediction replay complete",
        rf_model_loaded=rf_loaded,
        lstm_model_loaded=lstm_loaded,
    )

    return {
        "source_log_dir": str(Path(source_log_dir).resolve()),
        "output_dir": str(output_dir.resolve()),
        "start_ms": int(start_ms),
        "end_ms": int(end_ms),
        "telemetry_records": telemetry_records,
        "ema_predictions": ema_predictions,
        "ema_nonzero_predictions": ema_nonzero_predictions,
        "ema_last_predicted_power_w": round(ema_last_predicted_power_w, 4),
        "ema_max_predicted_power_w": round(ema_max_predicted_power_w, 4),
        "ema_last_window_energy_wh": round(ema_last_window_energy_wh, 4),
        "ema_max_window_energy_wh": round(ema_max_window_energy_wh, 4),
        "rf_predictions": rf_predictions,
        "lstm_predictions": lstm_predictions,
        "rf_model_loaded": rf_loaded,
        "lstm_model_loaded": lstm_loaded,
        "rf_model_dir": str(rf_runner.model_dir),
        "lstm_model_dir": str(lstm_runner.model_dir),
        "rf_model_warning": rf_runner.warning,
        "lstm_model_warning": lstm_runner.warning,
        "rf_status_message": rf_runner.status_message,
        "rf_last_error": rf_runner.last_error,
        "lstm_status_message": lstm_runner.status_message,
        "lstm_last_error": lstm_runner.last_error,
    }
