#!/usr/bin/env python3
import io
import json
import os
import tempfile
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request, send_file
from flask_cors import CORS

try:
    from export_chapter4_data import export_chapter4_dataset
    CHAPTER4_EXPORT_AVAILABLE = True
    CHAPTER4_EXPORT_IMPORT_ERROR = ""
except Exception as exc:
    export_chapter4_dataset = None
    CHAPTER4_EXPORT_AVAILABLE = False
    CHAPTER4_EXPORT_IMPORT_ERROR = str(exc)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:
    pass

PLANT_IDENTIFIER = os.getenv("PLANT_IDENTIFIER", "factory1").strip() or "factory1"
LOGGER_OUTPUT_DIR = (os.getenv("LOGGER_OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "telemetry_logs")).strip()
                     or os.path.join(os.path.dirname(__file__), "telemetry_logs"))
HISTORY_API_HOST = os.getenv("HISTORY_API_HOST", "0.0.0.0").strip() or "0.0.0.0"
try:
    HISTORY_API_PORT = int(os.getenv("HISTORY_API_PORT", "8090"))
except (TypeError, ValueError):
    HISTORY_API_PORT = 8090
HISTORY_API_CORS_ORIGINS = os.getenv("HISTORY_API_CORS_ORIGINS", "*").strip() or "*"
TARIFF_CONFIG_PATH = (os.getenv("TARIFF_CONFIG_PATH", os.path.join(os.path.dirname(__file__), "tariff_config.json")).strip()
                      or os.path.join(os.path.dirname(__file__), "tariff_config.json"))
TARIFF_TIMEZONE = os.getenv("TARIFF_TIMEZONE", "Europe/London").strip() or "Europe/London"

TOPIC_TELEMETRY_NAME = f"dt/{PLANT_IDENTIFIER}/telemetry"
TOPIC_PREDICTION_NAME = f"dt/{PLANT_IDENTIFIER}/prediction"
TOPIC_PREDICTION_LONG_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long"
TOPIC_PREDICTION_LONG_LSTM_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long_lstm"

log_dir = Path(LOGGER_OUTPUT_DIR)
tariff_config_path = Path(TARIFF_CONFIG_PATH)
app = Flask(__name__)

if HISTORY_API_CORS_ORIGINS == "*":
    CORS(app)
else:
    cors_origins = [origin.strip() for origin in HISTORY_API_CORS_ORIGINS.split(",") if origin.strip()]
    CORS(app, resources={r"/api/*": {"origins": cors_origins}, r"/health": {"origins": cors_origins}})


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

    try:
        with tempfile.TemporaryDirectory(prefix="chapter4_export_") as temp_dir:
            result = export_chapter4_dataset(
                log_dir=log_dir,
                out_dir=temp_dir,
                plant_id=PLANT_IDENTIFIER,
                start_ms=start_ms,
                end_ms=end_ms,
                prediction_step_sec=prediction_step_sec,
                max_lag_sec=max_lag_sec,
            )
            run_dir = Path(result["run_dir"])
            archive_name = f"{run_dir.name}.zip"
            archive_buffer = io.BytesIO()
            with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
                for path in sorted(run_dir.rglob("*")):
                    if path.is_file():
                        archive.write(path, arcname=f"{run_dir.name}/{path.relative_to(run_dir)}")
            archive_buffer.seek(0)
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


if __name__ == "__main__":
    print(f"[history-api] plant={PLANT_IDENTIFIER}")
    print(f"[history-api] log_dir={log_dir}")
    print(f"[history-api] listening on {HISTORY_API_HOST}:{HISTORY_API_PORT}")
    app.run(host=HISTORY_API_HOST, port=HISTORY_API_PORT)
