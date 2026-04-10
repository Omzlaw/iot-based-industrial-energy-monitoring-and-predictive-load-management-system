#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
import shutil
from collections import defaultdict, deque
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
except Exception:
    pass


PLANT_IDENTIFIER = os.getenv("PLANT_IDENTIFIER", "factory1").strip() or "factory1"
DEFAULT_LOG_DIR = os.path.join(os.path.dirname(__file__), "telemetry_logs")
LOGGER_OUTPUT_DIR = os.getenv("LOGGER_OUTPUT_DIR", DEFAULT_LOG_DIR).strip() or DEFAULT_LOG_DIR
DEFAULT_OUT_DIR = os.path.join(os.path.dirname(__file__), "chapter4_exports")
TARIFF_CONFIG_PATH = (
    os.getenv("TARIFF_CONFIG_PATH", os.path.join(os.path.dirname(__file__), "tariff_config.json")).strip()
    or os.path.join(os.path.dirname(__file__), "tariff_config.json")
)
TARIFF_TIMEZONE = os.getenv("TARIFF_TIMEZONE", "Europe/London").strip() or "Europe/London"

TOPIC_TELEMETRY_NAME = f"dt/{PLANT_IDENTIFIER}/telemetry"
TOPIC_PREDICTION_NAME = f"dt/{PLANT_IDENTIFIER}/prediction"
TOPIC_PREDICTION_LONG_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long"
TOPIC_PREDICTION_LONG_LSTM_NAME = f"dt/{PLANT_IDENTIFIER}/prediction_long_lstm"
TOPIC_COMMAND_NAME = f"dt/{PLANT_IDENTIFIER}/cmd"
TOPIC_COMMAND_ACK_NAME = f"dt/{PLANT_IDENTIFIER}/cmd_ack"
TOPIC_PREDICTOR_CMD_NAME = f"dt/{PLANT_IDENTIFIER}/predictor_cmd"
TOPIC_PREDICTOR_LONG_CMD_NAME = f"dt/{PLANT_IDENTIFIER}/predictor_long_cmd"
TOPIC_PREDICTOR_LONG_LSTM_CMD_NAME = f"dt/{PLANT_IDENTIFIER}/predictor_long_lstm_cmd"
tariff_config_path = Path(TARIFF_CONFIG_PATH)
MODEL_DIR = Path(__file__).resolve().parent / "models"
RF_MODEL_DIR_ENV = os.getenv("LONG_RF_MODEL_DIR", "").strip()
LSTM_MODEL_DIR_ENV = os.getenv("LONG_LSTM_MODEL_DIR", "").strip()
MODEL_SERVICE_DIR_HINTS = {
    "rf": ("long", "long-rf", "predictor-long-rf", "rf"),
    "lstm": ("lstm", "long-lstm", "predictor-long-lstm"),
}


def _safe_float(value) -> Optional[float]:
    try:
        number = float(value)
        if number != number:
            return None
        return number
    except Exception:
        return None


def _safe_bool(value) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return None


def _extract_logged_topic_payload(record: dict) -> Tuple[str, Optional[dict]]:
    topic = str(record.get("topic") or "")
    payload = record.get("payload")
    if isinstance(payload, dict):
        return topic, payload
    if topic and isinstance(record.get("system"), dict):
        return topic, record
    return topic, None


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


def _normalize_minute_of_day(value, fallback: int = 0) -> int:
    try:
        number = int(round(float(value)))
    except Exception:
        return fallback
    return ((number % 1440) + 1440) % 1440


def _normalize_nonnegative_float(value, fallback: float = 0.0) -> float:
    try:
        number = float(value)
        if number != number:
            return fallback
        return max(0.0, number)
    except Exception:
        return fallback


def _normalize_positive_int(value, fallback: int = 1) -> int:
    try:
        number = int(round(float(value)))
    except Exception:
        return fallback
    return max(1, number)


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
    if raw > 946684800:
        return int(raw * 1000)
    return None


def _parse_time_arg(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    parsed = _parse_epoch_ms(value)
    if parsed is not None:
        return parsed
    try:
        dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _iso_utc(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


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

DEFAULT_ROLLING_WINDOW_MINUTES = [1, 5, 15, 30, 60, 180, 360, 1440, 10080, 43200]


def _parse_rolling_window_minutes(raw: Optional[str]) -> List[int]:
    if raw is None:
        return list(DEFAULT_ROLLING_WINDOW_MINUTES)
    values: List[int] = []
    for part in str(raw).split(","):
        text = str(part).strip()
        if not text:
            continue
        try:
            minutes = int(round(float(text)))
        except Exception:
            continue
        if minutes <= 0:
            continue
        values.append(minutes)
    unique = sorted({value for value in values if value > 0})
    return unique or list(DEFAULT_ROLLING_WINDOW_MINUTES)


ROLLING_WINDOW_MINUTES = _parse_rolling_window_minutes(
    os.getenv("ROLLING_WINDOW_MINUTES") or os.getenv("EXPORT_ROLLING_WINDOW_MINUTES")
)


def _merge_rolling_window_minutes(base_minutes: Sequence[int], extra_minutes: Sequence[int]) -> List[int]:
    merged = {int(minutes) for minutes in base_minutes if int(minutes) > 0}
    merged.update(int(minutes) for minutes in extra_minutes if int(minutes) > 0)
    return sorted(merged)


def _window_minutes_for_horizon(horizon_sec: Optional[float]) -> Optional[int]:
    horizon = _safe_float(horizon_sec)
    if horizon is None or horizon <= 0:
        return None
    minutes = int(round(horizon / 60.0))
    return minutes if minutes > 0 else None


def _prediction_window_minutes(prediction_rows: Sequence[dict]) -> List[int]:
    extra: List[int] = []
    for row in prediction_rows:
        for key in ("pred_short_horizon_sec", "pred_long_rf_horizon_sec", "pred_long_lstm_horizon_sec"):
            minutes = _window_minutes_for_horizon(row.get(key))
            if minutes is not None:
                extra.append(minutes)
    return sorted({value for value in extra if value > 0})


def _rolling_window_label(minutes: int) -> str:
    if minutes % 10080 == 0:
        return f"{minutes // 10080}w"
    if minutes % 1440 == 0:
        return f"{minutes // 1440}d"
    if minutes % 60 == 0:
        return f"{minutes // 60}h"
    return f"{minutes}min"


def _default_tariff_store() -> Dict[str, object]:
    return {
        "plant_id": PLANT_IDENTIFIER,
        "config_path": str(tariff_config_path),
        "versions": [dict(DEFAULT_TARIFF_VERSION)],
    }


def _bucket_ms(ts_ms: int, step_sec: int) -> int:
    step_ms = max(1, step_sec) * 1000
    return (ts_ms // step_ms) * step_ms


def _iter_days(start_ms: int, end_ms: int) -> Iterable[date]:
    start_day = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).date()
    end_day = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).date()
    current = start_day
    while current <= end_day:
        yield current
        current += timedelta(days=1)


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
    if new_year == 5:
        new_year_observed = 3
    elif new_year == 6:
        new_year_observed = 2
    out.add(f"{year:04d}-01-{new_year_observed:02d}")

    easter_month, easter_day = _compute_easter_sunday_utc(year)
    easter_dt = datetime(year, easter_month, easter_day, tzinfo=timezone.utc)
    out.add((easter_dt - timedelta(days=2)).date().isoformat())
    out.add((easter_dt + timedelta(days=1)).date().isoformat())

    out.add(f"{year:04d}-05-{_first_weekday_of_month_utc(year, 5, 0):02d}")
    out.add(f"{year:04d}-05-{_last_weekday_of_month_utc(year, 5, 0):02d}")
    out.add(f"{year:04d}-08-{_last_weekday_of_month_utc(year, 8, 0):02d}")

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


def _normalize_tariff_version(raw: dict, fallback_effective_from: str) -> dict:
    effective_ms = _parse_epoch_ms(raw.get("effective_from")) if isinstance(raw, dict) else None
    effective_from = _iso_utc(effective_ms) if effective_ms is not None else fallback_effective_from
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


def _load_tariff_store() -> dict:
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


def _resolve_tariff_version_for_ms(store: dict, ts_ms: Optional[int]) -> dict:
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


def _is_minute_in_window(minute_of_day: int, start_minute: int, end_minute: int) -> bool:
    minute = _normalize_minute_of_day(minute_of_day, 0)
    start = _normalize_minute_of_day(start_minute, 0)
    end = _normalize_minute_of_day(end_minute, 0)
    if start == end:
        return True
    if start < end:
        return start <= minute < end
    return minute >= start or minute < end


def _resolve_tariff_state_for_ms(ts_ms: int, version: dict) -> str:
    tz_name = str(version.get("timezone") or TARIFF_TIMEZONE)
    try:
        zone = ZoneInfo(tz_name)
    except Exception:
        zone = timezone.utc
    local_dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(zone)
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


def _resolve_tariff_rate_for_state(state: str, version: dict) -> float:
    key = str(state or "OFFPEAK").upper()
    if key == "PEAK":
        return _normalize_nonnegative_float(version.get("peak_rate_per_kwh"), DEFAULT_TARIFF_VERSION["peak_rate_per_kwh"])
    if key == "MIDPEAK":
        return _normalize_nonnegative_float(version.get("midpeak_rate_per_kwh"), DEFAULT_TARIFF_VERSION["midpeak_rate_per_kwh"])
    return _normalize_nonnegative_float(version.get("offpeak_rate_per_kwh"), DEFAULT_TARIFF_VERSION["offpeak_rate_per_kwh"])


def _available_log_dates(log_dir: Path, plant_id: str) -> List[date]:
    dates = []
    prefix = f"{plant_id}_telemetry_"
    for path in sorted(log_dir.glob(f"{plant_id}_telemetry_*.jsonl")):
        name = path.name
        if not name.startswith(prefix):
            continue
        suffix = name[len(prefix):-len(".jsonl")]
        try:
            dates.append(date.fromisoformat(suffix))
        except Exception:
            continue
    return dates


def _resolve_range(log_dir: Path, plant_id: str, start_ms: Optional[int], end_ms: Optional[int]) -> Tuple[int, int]:
    dates = _available_log_dates(log_dir, plant_id)
    if not dates:
        raise FileNotFoundError(f"no telemetry logs found in {log_dir}")

    if start_ms is None:
        start_ms = int(datetime.combine(min(dates), time.min, tzinfo=timezone.utc).timestamp() * 1000)
    if end_ms is None:
        end_ms = int(datetime.combine(max(dates), time.max, tzinfo=timezone.utc).timestamp() * 1000)
    if end_ms < start_ms:
        raise ValueError("--to must be greater than or equal to --from")
    return start_ms, end_ms


def _nested_value(mapping: dict, *keys):
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
        if current is None:
            return None
    return current


def _percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    if q <= 0:
        return min(values)
    if q >= 1:
        return max(values)
    ordered = sorted(values)
    idx = (len(ordered) - 1) * q
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


def _write_csv(path: Path, rows: List[dict], preferred_fieldnames: Optional[List[str]] = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if preferred_fieldnames:
        fieldnames = list(preferred_fieldnames)
        for row in rows:
            for key in row.keys():
                if key not in fieldnames:
                    fieldnames.append(key)
    else:
        keys = set()
        for row in rows:
            keys.update(row.keys())
        fieldnames = sorted(keys)

    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _csv_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def _attach_cost_fields(
    system_rows: List[dict],
    tariff_store: dict,
    rolling_window_minutes: Optional[Sequence[int]] = None,
) -> List[dict]:
    if not system_rows:
        return system_rows

    previous_ts = None
    previous_energy_wh = None
    accumulated_energy_cost = 0.0
    running_max_demand_kw = 0.0
    interval_buckets: Dict[Tuple[int, int], float] = {}
    window_minutes_list = list(rolling_window_minutes or ROLLING_WINDOW_MINUTES)
    rolling_specs = [(int(minutes), _rolling_window_label(int(minutes))) for minutes in window_minutes_list]
    rolling_segments = {minutes: deque() for minutes, _ in rolling_specs}
    rolling_energy_totals = {minutes: 0.0 for minutes, _ in rolling_specs}
    rolling_cost_totals = {minutes: 0.0 for minutes, _ in rolling_specs}

    for row in system_rows:
        ts = int(row["ts"])
        power_w = _safe_float(row.get("total_power_w")) or 0.0
        energy_wh = _safe_float(row.get("total_energy_wh"))

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
        cost_tariff_state = _resolve_tariff_state_for_ms(ts, version)
        cost_tariff_rate_per_kwh = _resolve_tariff_rate_for_state(cost_tariff_state, version)
        incremental_energy_cost = delta_energy_wh * (cost_tariff_rate_per_kwh / 1000.0)
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

        demand_charge_rate_per_kw = _normalize_nonnegative_float(
            version.get("demand_charge_per_kw"),
            DEFAULT_TARIFF_VERSION["demand_charge_per_kw"],
        )
        demand_charge_cost = running_max_demand_kw * demand_charge_rate_per_kw
        total_cost = accumulated_energy_cost + demand_charge_cost

        if previous_ts is not None and ts > previous_ts:
            segment = {
                "start_ms": int(previous_ts),
                "end_ms": int(ts),
                "energy_wh": float(delta_energy_wh),
                "cost": float(incremental_energy_cost),
            }
            for window_minutes, _label in rolling_specs:
                window_queue = rolling_segments[window_minutes]
                window_queue.append(segment)
                rolling_energy_totals[window_minutes] += float(delta_energy_wh)
                rolling_cost_totals[window_minutes] += float(incremental_energy_cost)

        for window_minutes, window_label in rolling_specs:
            window_queue = rolling_segments[window_minutes]
            window_start_ms = ts - (window_minutes * 60000)
            while window_queue and int(window_queue[0]["end_ms"]) <= window_start_ms:
                expired = window_queue.popleft()
                rolling_energy_totals[window_minutes] -= float(expired["energy_wh"])
                rolling_cost_totals[window_minutes] -= float(expired["cost"])

            window_energy_wh = float(rolling_energy_totals[window_minutes])
            window_cost = float(rolling_cost_totals[window_minutes])
            if window_queue:
                head = window_queue[0]
                head_start = int(head["start_ms"])
                head_end = int(head["end_ms"])
                if head_start < window_start_ms < head_end:
                    head_span_ms = max(1, head_end - head_start)
                    outside_ratio = (window_start_ms - head_start) / head_span_ms
                    window_energy_wh -= float(head["energy_wh"]) * outside_ratio
                    window_cost -= float(head["cost"]) * outside_ratio

            window_energy_wh = max(0.0, window_energy_wh)
            window_cost = max(0.0, window_cost)
            window_avg_power_w = window_energy_wh / (window_minutes / 60.0)
            row[f"rolling_energy_{window_label}_wh"] = window_energy_wh
            row[f"rolling_avg_power_{window_label}_w"] = window_avg_power_w
            row[f"rolling_cost_{window_label}"] = window_cost

        row["cost_tariff_state"] = cost_tariff_state
        row["cost_tariff_rate_per_kwh"] = cost_tariff_rate_per_kwh
        row["incremental_energy_cost"] = incremental_energy_cost
        row["accumulated_energy_cost"] = accumulated_energy_cost
        row["max_demand_kw"] = running_max_demand_kw
        row["demand_charge_rate_per_kw"] = demand_charge_rate_per_kw
        row["demand_charge_cost"] = demand_charge_cost
        row["total_cost"] = total_cost
        row["demand_interval_min"] = demand_interval_min

        previous_ts = ts
        if energy_wh is not None:
            previous_energy_wh = energy_wh

    return system_rows


def _prediction_row(ts_ms: int) -> Dict[str, Optional[float]]:
    return {
        "ts": ts_ms,
        "label_utc": _iso_utc(ts_ms),
        "actual_power_w": None,
        "actual_current_a": None,
        "actual_total_energy_wh": None,
        "pred_short_power_w": None,
        "pred_short_window_energy_wh": None,
        "pred_short_total_energy_wh": None,
        "pred_short_horizon_sec": None,
        "pred_long_rf_power_w": None,
        "pred_long_rf_window_energy_wh": None,
        "pred_long_rf_total_energy_wh": None,
        "pred_long_rf_horizon_sec": None,
        "pred_long_lstm_power_w": None,
        "pred_long_lstm_window_energy_wh": None,
        "pred_long_lstm_total_energy_wh": None,
        "pred_long_lstm_horizon_sec": None,
    }


def _resolve_fused_predicted_energy(system_row: dict) -> Optional[float]:
    fused_power = system_row.get("predictor_predicted_power_w")
    if fused_power is None:
        return None

    best_energy = None
    best_diff = None
    for power_key, energy_key in (
        ("short_predicted_power_w", "short_predicted_total_energy_wh"),
        ("rf_predicted_power_w", "rf_predicted_total_energy_wh"),
        ("lstm_predicted_power_w", "lstm_predicted_total_energy_wh"),
    ):
        power_w = system_row.get(power_key)
        energy_wh = system_row.get(energy_key)
        if power_w is None or energy_wh is None:
            continue
        diff = abs(power_w - fused_power)
        if best_diff is None or diff < best_diff:
            best_diff = diff
            best_energy = energy_wh
    return best_energy


def _derive_predicted_total_energy(
    actual_total_energy_wh: Optional[float],
    predicted_power_w: Optional[float],
    predicted_total_energy_wh: Optional[float],
    horizon_sec: Optional[float],
) -> Optional[float]:
    if predicted_total_energy_wh is not None:
        return predicted_total_energy_wh
    if actual_total_energy_wh is None or predicted_power_w is None:
        return None
    horizon = _safe_float(horizon_sec)
    if horizon is None or horizon <= 0:
        return None
    return float(actual_total_energy_wh) + (float(predicted_power_w) * float(horizon) / 3600.0)


def _derive_predicted_window_energy(predicted_power_w: Optional[float], horizon_sec: Optional[float]) -> Optional[float]:
    power_w = _safe_float(predicted_power_w)
    horizon = _safe_float(horizon_sec)
    if power_w is None or horizon is None or horizon <= 0:
        return None
    return float(power_w) * float(horizon) / 3600.0


def _derive_predicted_window_cost(predicted_window_energy_wh: Optional[float], tariff_rate_per_kwh: Optional[float]) -> Optional[float]:
    energy_wh = _safe_float(predicted_window_energy_wh)
    rate_per_kwh = _safe_float(tariff_rate_per_kwh)
    if energy_wh is None or rate_per_kwh is None:
        return None
    return float(energy_wh) * (float(rate_per_kwh) / 1000.0)


def _derive_window_avg_power_w(
    power_w: Optional[float],
    window_energy_wh: Optional[float],
    window_minutes: Optional[float],
) -> Optional[float]:
    candidate_power_w = _safe_float(power_w)
    if candidate_power_w is not None:
        return candidate_power_w
    energy_wh = _safe_float(window_energy_wh)
    minutes = _safe_float(window_minutes)
    if energy_wh is None or minutes is None or minutes <= 0:
        return None
    return float(energy_wh) / (float(minutes) / 60.0)


def _build_prediction_comparison_rows(
    prediction_rows: Sequence[dict],
    system_rows: Sequence[dict],
    prediction_step_sec: int,
    predicted_power_key: str,
    predicted_window_energy_key: str,
    predicted_total_energy_key: str,
    horizon_key: str,
) -> List[dict]:
    system_by_bucket: Dict[int, dict] = {}
    for row in system_rows:
        system_by_bucket[_bucket_ms(int(row["ts"]), prediction_step_sec)] = row

    output_rows: List[dict] = []
    for row in prediction_rows:
        system_row = system_by_bucket.get(int(row["ts"]))
        horizon_sec = row.get(horizon_key)
        window_minutes = _window_minutes_for_horizon(horizon_sec)
        window_label = _rolling_window_label(window_minutes) if window_minutes is not None else None
        predicted_window_energy_wh = _safe_float(row.get(predicted_window_energy_key))
        if predicted_window_energy_wh is None:
            predicted_window_energy_wh = _derive_predicted_window_energy(row.get(predicted_power_key), horizon_sec)
        tariff_rate_per_kwh = system_row.get("cost_tariff_rate_per_kwh") if system_row else None
        output_rows.append(
            {
                "ts": row["ts"],
                "label_utc": row["label_utc"],
                "horizon_sec": horizon_sec,
                "rolling_window_minutes": window_minutes,
                "rolling_window_label": window_label,
                "actual_power_w": row.get("actual_power_w"),
                "predicted_power_w": row.get(predicted_power_key),
                "actual_rolling_energy_wh": system_row.get(f"rolling_energy_{window_label}_wh") if system_row and window_label else None,
                "predicted_rolling_energy_wh": predicted_window_energy_wh,
                "actual_rolling_avg_power_w": system_row.get(f"rolling_avg_power_{window_label}_w") if system_row and window_label else None,
                "predicted_rolling_avg_power_w": _derive_window_avg_power_w(row.get(predicted_power_key), predicted_window_energy_wh, window_minutes),
                "cost_tariff_rate_per_kwh": tariff_rate_per_kwh,
                "cost_tariff_state": system_row.get("cost_tariff_state") if system_row else None,
                "actual_rolling_cost": system_row.get(f"rolling_cost_{window_label}") if system_row and window_label else None,
                "predicted_rolling_cost": _derive_predicted_window_cost(predicted_window_energy_wh, tariff_rate_per_kwh),
                "actual_cumulative_total_energy_wh": row.get("actual_total_energy_wh"),
                "predicted_cumulative_total_energy_wh": row.get(predicted_total_energy_key),
            }
        )
    if output_rows:
        samples: deque = deque()
        for row in output_rows:
            ts = int(row["ts"])
            actual_total_energy_wh = _safe_float(row.get("actual_cumulative_total_energy_wh"))
            window_minutes = _safe_float(row.get("rolling_window_minutes"))
            if actual_total_energy_wh is not None:
                samples.append({"ts": ts, "energy_wh": actual_total_energy_wh})
            if window_minutes is None or window_minutes <= 0:
                continue
            window_ms = int(window_minutes * 60000.0)
            while len(samples) >= 2 and int(samples[1]["ts"]) <= (ts - window_ms):
                samples.popleft()
            if actual_total_energy_wh is None or not samples:
                continue
            derived_actual_window_energy_wh: Optional[float] = None
            window_start_ms = ts - window_ms
            if len(samples) == 1:
                only = samples[0]
                if int(only["ts"]) >= window_start_ms:
                    derived_actual_window_energy_wh = max(0.0, actual_total_energy_wh)
            else:
                baseline = float(samples[0]["energy_wh"])
                if int(samples[0]["ts"]) < window_start_ms < int(samples[1]["ts"]):
                    a = samples[0]
                    b = samples[1]
                    span_ms = max(1, int(b["ts"]) - int(a["ts"]))
                    ratio = (window_start_ms - int(a["ts"])) / span_ms
                    baseline = float(a["energy_wh"]) + (float(b["energy_wh"]) - float(a["energy_wh"])) * ratio
                derived_actual_window_energy_wh = max(0.0, actual_total_energy_wh - baseline)
            if derived_actual_window_energy_wh is None:
                continue
            existing_actual_window_energy_wh = _safe_float(row.get("actual_rolling_energy_wh"))
            if existing_actual_window_energy_wh is None or (
                existing_actual_window_energy_wh <= 0.0 and derived_actual_window_energy_wh > 0.0
            ):
                row["actual_rolling_energy_wh"] = derived_actual_window_energy_wh
                row["actual_rolling_avg_power_w"] = derived_actual_window_energy_wh / (float(window_minutes) / 60.0)
    return output_rows


def _best_lag_sec(actual: List[Optional[float]], predicted: List[Optional[float]], step_sec: int, max_lag_sec: int) -> Tuple[Optional[int], int]:
    max_shift = max(0, int(max_lag_sec / max(1, step_sec)))
    best_shift = None
    best_rmse = None
    best_count = 0
    n = min(len(actual), len(predicted))

    for shift in range(-max_shift, max_shift + 1):
        errors = []
        for i in range(n):
            j = i + shift
            if j < 0 or j >= n:
                continue
            a = actual[i]
            p = predicted[j]
            if a is None or p is None:
                continue
            errors.append((a - p) ** 2)
        if len(errors) < 5:
            continue
        rmse = math.sqrt(sum(errors) / len(errors))
        if best_rmse is None or rmse < best_rmse:
            best_rmse = rmse
            best_shift = shift
            best_count = len(errors)

    if best_shift is None:
        return None, best_count
    return best_shift * step_sec, best_count


def _infer_metric_step_sec(rows: Sequence[dict], fallback_step_sec: int) -> int:
    timestamps = [int(row["ts"]) for row in rows if row.get("ts") is not None]
    if len(timestamps) < 2:
        return max(1, int(fallback_step_sec))
    timestamps.sort()
    deltas_sec = []
    previous_ts = timestamps[0]
    for ts in timestamps[1:]:
        delta_ms = int(ts) - int(previous_ts)
        previous_ts = ts
        if delta_ms > 0:
            deltas_sec.append(delta_ms / 1000.0)
    if not deltas_sec:
        return max(1, int(fallback_step_sec))
    deltas_sec.sort()
    middle = len(deltas_sec) // 2
    if len(deltas_sec) % 2 == 1:
        median_sec = deltas_sec[middle]
    else:
        median_sec = (deltas_sec[middle - 1] + deltas_sec[middle]) / 2.0
    return max(1, int(round(median_sec)))


def _summarize_horizon_sec(rows: Sequence[dict]) -> Optional[float]:
    values = sorted(
        _safe_float(row.get("horizon_sec"))
        for row in rows
        if _safe_float(row.get("horizon_sec")) is not None and _safe_float(row.get("horizon_sec")) > 0.0
    )
    if not values:
        return None
    middle = len(values) // 2
    if len(values) % 2 == 1:
        return float(values[middle])
    return float((values[middle - 1] + values[middle]) / 2.0)


def _compute_model_metrics(rows: List[dict], actual_col: str, pred_col: str, step_sec: int, max_lag_sec: int) -> dict:
    pairs = [(row[actual_col], row[pred_col]) for row in rows if row.get(actual_col) is not None and row.get(pred_col) is not None]
    sample_count = len(pairs)
    if not pairs:
        return {
            "sample_count": 0,
            "mae_wh": None,
            "mse_wh2": None,
            "rmse_wh": None,
            "r_squared": None,
            "lag_sec": None,
            "lag_eval_samples": 0,
            "metric_step_sec": max(1, int(step_sec)),
        }

    actual_values = [actual for actual, _pred in pairs]
    errors = [abs(actual - pred) for actual, pred in pairs]
    squared = [(actual - pred) ** 2 for actual, pred in pairs]
    actual_mean = sum(actual_values) / sample_count if sample_count else None
    total_sum_squares = (
        sum((actual - actual_mean) ** 2 for actual in actual_values)
        if actual_mean is not None
        else None
    )
    residual_sum_squares = sum(squared)
    mse = residual_sum_squares / sample_count
    r_squared = None
    if total_sum_squares is not None:
        if total_sum_squares > 0.0:
            r_squared = 1.0 - (residual_sum_squares / total_sum_squares)
        elif residual_sum_squares <= 0.0:
            r_squared = 1.0
    metric_step_sec = _infer_metric_step_sec(rows, step_sec)
    lag_sec, lag_samples = _best_lag_sec(
        [row.get(actual_col) for row in rows],
        [row[pred_col] for row in rows],
        metric_step_sec,
        max_lag_sec,
    )
    return {
        "sample_count": sample_count,
        "mae_wh": sum(errors) / sample_count,
        "mse_wh2": mse,
        "rmse_wh": math.sqrt(mse),
        "r_squared": r_squared,
        "lag_sec": lag_sec,
        "lag_eval_samples": lag_samples,
        "metric_step_sec": metric_step_sec,
    }


def _load_type_from_name(load_name: str) -> str:
    name = str(load_name or "").lower()
    if name.startswith("heater"):
        return "resistive"
    if name.startswith("motor"):
        return "motor_pwm"
    if name.startswith("lighting") or name.startswith("light"):
        return "lighting_pwm"
    return "other"


def _build_ts_load_summary(load_rows: List[dict]) -> Dict[int, dict]:
    summary = defaultdict(
        lambda: {
            "active_load_count": 0,
            "active_critical_count": 0,
            "active_essential_count": 0,
            "active_important_count": 0,
            "active_secondary_count": 0,
            "active_non_essential_count": 0,
            "total_duty_all": 0.0,
            "total_duty_on": 0.0,
            "total_current_a_sum": 0.0,
            "total_power_w_sum": 0.0,
        }
    )
    class_key_map = {
        "CRITICAL": "active_critical_count",
        "ESSENTIAL": "active_essential_count",
        "IMPORTANT": "active_important_count",
        "SECONDARY": "active_secondary_count",
        "NON_ESSENTIAL": "active_non_essential_count",
    }
    for row in load_rows:
        bucket = summary[row["ts"]]
        duty = row.get("duty")
        if duty is not None:
            bucket["total_duty_all"] += duty
        current_a = row.get("current_a")
        if current_a is not None:
            bucket["total_current_a_sum"] += current_a
        power_w = row.get("power_w")
        if power_w is not None:
            bucket["total_power_w_sum"] += power_w
        if row.get("on") is True:
            bucket["active_load_count"] += 1
            if duty is not None:
                bucket["total_duty_on"] += duty
            class_key = class_key_map.get(str(row.get("class") or "").upper())
            if class_key:
                bucket[class_key] += 1
    return summary


def _build_policy_summary_rows(system_rows: List[dict]) -> List[dict]:
    policy_stats = defaultdict(
        lambda: {
            "sample_count": 0,
            "peak_power_w": None,
            "energy_wh": 0.0,
            "observation_duration_sec": 0.0,
            "process_enabled_duration_sec": 0.0,
            "completed_cycle_delta": 0.0,
        }
    )

    prev_row = None
    for row in system_rows:
        policy = str(row.get("control_policy") or "UNKNOWN")
        stats = policy_stats[policy]
        stats["sample_count"] += 1
        power_w = row.get("total_power_w")
        if power_w is not None:
            stats["peak_power_w"] = power_w if stats["peak_power_w"] is None else max(stats["peak_power_w"], power_w)

        if prev_row is not None:
            dt_sec = max(0.0, (row["ts"] - prev_row["ts"]) / 1000.0)
            if policy == str(prev_row.get("control_policy") or "UNKNOWN"):
                stats["observation_duration_sec"] += dt_sec
                energy_now = row.get("total_energy_wh")
                energy_prev = prev_row.get("total_energy_wh")
                if energy_now is not None and energy_prev is not None:
                    delta_wh = energy_now - energy_prev
                    if delta_wh > 0.0:
                        stats["energy_wh"] += delta_wh
                if row.get("process_enabled") is True and prev_row.get("process_enabled") is True:
                    stats["process_enabled_duration_sec"] += dt_sec
                cycle_now = row.get("process_cycle_count")
                cycle_prev = prev_row.get("process_cycle_count")
                if cycle_now is not None and cycle_prev is not None:
                    cycle_delta = cycle_now - cycle_prev
                    if cycle_delta > 0.0:
                        stats["completed_cycle_delta"] += cycle_delta
        prev_row = row

    baseline = policy_stats.get("NO_ENERGY_MANAGEMENT")
    baseline_peak = baseline.get("peak_power_w") if baseline else None
    baseline_energy = baseline.get("energy_wh") if baseline else None

    order = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]
    seen = set()
    rows = []
    for policy in order + sorted(policy_stats.keys()):
        if policy in seen or policy not in policy_stats:
            continue
        seen.add(policy)
        stats = policy_stats[policy]
        peak = stats["peak_power_w"]
        energy = stats["energy_wh"]
        process_time_per_cycle = None
        if stats["completed_cycle_delta"] > 0:
            process_time_per_cycle = stats["process_enabled_duration_sec"] / stats["completed_cycle_delta"]
        peak_reduction_pct = None
        if baseline_peak is not None and baseline_peak > 0 and peak is not None:
            peak_reduction_pct = ((baseline_peak - peak) / baseline_peak) * 100.0
        energy_reduction_pct = None
        if baseline_energy is not None and baseline_energy > 0 and energy is not None:
            energy_reduction_pct = ((baseline_energy - energy) / baseline_energy) * 100.0
        rows.append(
            {
                "control_policy": policy,
                "sample_count": stats["sample_count"],
                "observation_duration_sec": stats["observation_duration_sec"],
                "peak_power_w": peak,
                "energy_wh": energy,
                "process_time_s": process_time_per_cycle,
                "completed_cycle_delta": stats["completed_cycle_delta"],
                "peak_reduction_pct": peak_reduction_pct,
                "energy_reduction_pct": energy_reduction_pct,
            }
        )
    return rows


def _field_delta(rows: Sequence[dict], key: str) -> Optional[float]:
    values = []
    for row in rows:
        value = _safe_float(row.get(key))
        if value is not None:
            values.append(value)
    if not values:
        return None
    return max(values) - min(values)


def _series_timing_row(
    stream_name: str,
    ts_values_ms: Sequence[int],
    *,
    fresh_values: Optional[Sequence[Optional[bool]]] = None,
    applied_values: Optional[Sequence[Optional[bool]]] = None,
) -> dict:
    cleaned = sorted(int(ts) for ts in ts_values_ms if ts is not None)
    intervals_sec = [
        max(0.0, (cleaned[index] - cleaned[index - 1]) / 1000.0)
        for index in range(1, len(cleaned))
        if cleaned[index] > cleaned[index - 1]
    ]
    median_interval_sec = _percentile(intervals_sec, 0.5) if intervals_sec else None
    approx_gap_count = 0
    if median_interval_sec is not None and median_interval_sec > 0.0:
        for interval_sec in intervals_sec:
            if interval_sec <= (median_interval_sec * 1.5):
                continue
            approx_gap_count += max(1, int(round(interval_sec / median_interval_sec)) - 1)

    row = {
        "stream": stream_name,
        "sample_count": len(cleaned),
        "start_utc": _iso_utc(cleaned[0]) if cleaned else None,
        "end_utc": _iso_utc(cleaned[-1]) if cleaned else None,
        "duration_sec": ((cleaned[-1] - cleaned[0]) / 1000.0) if len(cleaned) >= 2 else 0.0,
        "mean_interval_sec": mean(intervals_sec) if intervals_sec else None,
        "median_interval_sec": median_interval_sec,
        "p95_interval_sec": _percentile(intervals_sec, 0.95) if intervals_sec else None,
        "max_interval_sec": max(intervals_sec) if intervals_sec else None,
        "interval_std_sec": pstdev(intervals_sec) if len(intervals_sec) > 1 else 0.0 if intervals_sec else None,
        "approx_gap_count": approx_gap_count,
        "notes": "approx_gap_count is an interval-gap estimate, not true packet loss",
    }
    if fresh_values is not None:
        fresh_clean = [value for value in fresh_values if value is not None]
        row["fresh_true_ratio"] = (
            sum(1 for value in fresh_clean if value is True) / len(fresh_clean)
            if fresh_clean else None
        )
    if applied_values is not None:
        applied_clean = [value for value in applied_values if value is not None]
        row["applied_true_ratio"] = (
            sum(1 for value in applied_clean if value is True) / len(applied_clean)
            if applied_clean else None
        )
    return row


def _build_stream_timing_rows(system_rows: List[dict], prediction_rows: List[dict]) -> List[dict]:
    rows: List[dict] = []
    rows.append(_series_timing_row("telemetry", [int(row["ts"]) for row in system_rows]))
    rows.append(
        _series_timing_row(
            "prediction_short",
            [
                int(row["ts"])
                for row in prediction_rows
                if row.get("pred_short_power_w") is not None or row.get("pred_short_window_energy_wh") is not None
            ],
            fresh_values=[_safe_bool(row.get("short_fresh")) for row in system_rows],
            applied_values=[_safe_bool(row.get("short_applied")) for row in system_rows],
        )
    )
    rows.append(
        _series_timing_row(
            "prediction_long_rf",
            [
                int(row["ts"])
                for row in prediction_rows
                if row.get("pred_long_rf_power_w") is not None or row.get("pred_long_rf_window_energy_wh") is not None
            ],
            fresh_values=[_safe_bool(row.get("rf_fresh")) for row in system_rows],
            applied_values=[_safe_bool(row.get("rf_applied")) for row in system_rows],
        )
    )
    rows.append(
        _series_timing_row(
            "prediction_long_lstm",
            [
                int(row["ts"])
                for row in prediction_rows
                if row.get("pred_long_lstm_power_w") is not None or row.get("pred_long_lstm_window_energy_wh") is not None
            ],
            fresh_values=[_safe_bool(row.get("lstm_fresh")) for row in system_rows],
            applied_values=[_safe_bool(row.get("lstm_applied")) for row in system_rows],
        )
    )
    return rows


def _build_policy_control_action_rows(system_rows: List[dict]) -> List[dict]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in system_rows:
        grouped[str(row.get("control_policy") or "UNKNOWN")].append(row)

    order = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]
    rows: List[dict] = []
    seen = set()
    for policy in order + sorted(grouped.keys()):
        if policy not in grouped or policy in seen:
            continue
        seen.add(policy)
        policy_rows = grouped[policy]
        shedding_delta = _field_delta(policy_rows, "shedding_event_count")
        curtail_delta = _field_delta(policy_rows, "curtail_event_count")
        restore_delta = _field_delta(policy_rows, "restore_event_count")
        total_actions_delta = _field_delta(policy_rows, "total_control_actions")
        if (total_actions_delta is None or total_actions_delta <= 0.0) and any(
            value is not None for value in (shedding_delta, curtail_delta, restore_delta)
        ):
            total_actions_delta = sum(value or 0.0 for value in (shedding_delta, curtail_delta, restore_delta))
        rows.append(
            {
                "control_policy": policy,
                "sample_count": len(policy_rows),
                "observation_duration_sec": max(0.0, (policy_rows[-1]["ts"] - policy_rows[0]["ts"]) / 1000.0) if len(policy_rows) >= 2 else 0.0,
                "total_control_actions": total_actions_delta,
                "shedding_event_count": shedding_delta,
                "curtail_event_count": curtail_delta,
                "restore_event_count": restore_delta,
                "load_toggle_count": _field_delta(policy_rows, "load_toggle_count"),
                "mean_control_actions_per_min": mean([
                    value for value in (_safe_float(row.get("control_actions_per_min")) for row in policy_rows)
                    if value is not None
                ]) if any(_safe_float(row.get("control_actions_per_min")) is not None for row in policy_rows) else None,
                "max_control_actions_per_min": max([
                    value for value in (_safe_float(row.get("control_actions_per_min")) for row in policy_rows)
                    if value is not None
                ]) if any(_safe_float(row.get("control_actions_per_min")) is not None for row in policy_rows) else None,
            }
        )
    return rows


def _prediction_display_power_w(row: dict) -> Optional[float]:
    for key in (
        "predictor_predicted_power_w",
        "rf_predicted_power_w",
        "lstm_predicted_power_w",
        "short_predicted_power_w",
    ):
        value = _safe_float(row.get(key))
        if value is not None:
            return value
    return None


def _control_action_delta_row(row: dict, prev_row: Optional[dict]) -> dict:
    keys = (
        "total_control_actions",
        "shedding_event_count",
        "curtail_event_count",
        "restore_event_count",
        "load_toggle_count",
    )
    delta_row: Dict[str, float] = {}
    total_delta = 0.0
    for key in keys:
        current = _safe_float(row.get(key)) or 0.0
        previous = _safe_float(prev_row.get(key)) if prev_row is not None else 0.0
        delta = max(0.0, current - (previous or 0.0))
        delta_row[f"{key}_delta"] = delta
        if key != "total_control_actions":
            total_delta += delta
    if total_delta <= 0.0:
        total_delta = delta_row["total_control_actions_delta"]
    delta_row["control_action_delta"] = total_delta
    delta_row["control_action_active"] = total_delta > 0.0
    return delta_row


def _group_system_rows_by_policy(system_rows: Sequence[dict]) -> Dict[str, List[dict]]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in system_rows:
        grouped[str(row.get("control_policy") or "UNKNOWN")].append(row)
    for rows in grouped.values():
        rows.sort(key=lambda item: int(item["ts"]))
    return grouped


def _build_forecast_informed_control_response_rows(system_rows: List[dict]) -> List[dict]:
    grouped = _group_system_rows_by_policy(system_rows)
    rows: List[dict] = []
    order = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]
    seen = set()
    for policy in order + sorted(grouped.keys()):
        if policy in seen or policy not in grouped:
            continue
        seen.add(policy)
        policy_rows = grouped[policy]
        if not policy_rows:
            continue
        start_ts = int(policy_rows[0]["ts"])
        prev_row: Optional[dict] = None
        for row in policy_rows:
            delta_row = _control_action_delta_row(row, prev_row)
            rows.append(
                {
                    "ts": row["ts"],
                    "label_utc": row["label_utc"],
                    "control_policy": policy,
                    "elapsed_min": max(0.0, (int(row["ts"]) - start_ts) / 60000.0),
                    "actual_power_w": row.get("total_power_w"),
                    "predicted_power_w": _prediction_display_power_w(row),
                    "control_threshold_w": row.get("max_total_power_w"),
                    "control_action_delta": delta_row["control_action_delta"],
                    "control_action_active": delta_row["control_action_active"],
                    "shedding_event_delta": delta_row["shedding_event_count_delta"],
                    "curtail_event_delta": delta_row["curtail_event_count_delta"],
                    "restore_event_delta": delta_row["restore_event_count_delta"],
                    "load_toggle_delta": delta_row["load_toggle_count_delta"],
                    "predictor_risk_level": row.get("predictor_risk_level"),
                    "predictor_peak_risk": row.get("predictor_peak_risk"),
                }
            )
            prev_row = row
    return rows


def _select_reference_window_minutes(rolling_window_minutes: Sequence[int]) -> Optional[int]:
    cleaned = sorted({int(value) for value in rolling_window_minutes if int(value) > 0})
    if not cleaned:
        return None
    if 30 in cleaned:
        return 30
    for candidate in (15, 60, 5, 1):
        if candidate in cleaned:
            return candidate
    return cleaned[0]


def _build_policy_rolling_energy_comparison_rows(
    system_rows: List[dict],
    rolling_window_minutes: Sequence[int],
) -> List[dict]:
    reference_window = _select_reference_window_minutes(rolling_window_minutes)
    if reference_window is None:
        return []
    window_label = _rolling_window_label(int(reference_window))
    grouped = _group_system_rows_by_policy(system_rows)
    rows: List[dict] = []
    order = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]
    seen = set()
    for policy in order + sorted(grouped.keys()):
        if policy in seen or policy not in grouped:
            continue
        seen.add(policy)
        policy_rows = grouped[policy]
        if not policy_rows:
            continue
        start_ts = int(policy_rows[0]["ts"])
        for row in policy_rows:
            rows.append(
                {
                    "ts": row["ts"],
                    "label_utc": row["label_utc"],
                    "control_policy": policy,
                    "elapsed_min": max(0.0, (int(row["ts"]) - start_ts) / 60000.0),
                    "window_minutes": reference_window,
                    "window_label": window_label,
                    "rolling_energy_wh": row.get(f"rolling_energy_{window_label}_wh"),
                    "rolling_avg_power_w": row.get(f"rolling_avg_power_{window_label}_w"),
                }
            )
    return rows


def _build_before_after_demand_profile_rows(system_rows: List[dict]) -> List[dict]:
    grouped = _group_system_rows_by_policy(system_rows)
    included = [policy for policy in ("NO_ENERGY_MANAGEMENT", "HYBRID", "AI_PREFERRED") if policy in grouped]
    if not included and "RULE_ONLY" in grouped:
        included = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY"]
    rows: List[dict] = []
    for policy in included:
        policy_rows = grouped.get(policy) or []
        if not policy_rows:
            continue
        start_ts = int(policy_rows[0]["ts"])
        for row in policy_rows:
            rows.append(
                {
                    "ts": row["ts"],
                    "label_utc": row["label_utc"],
                    "control_policy": policy,
                    "elapsed_min": max(0.0, (int(row["ts"]) - start_ts) / 60000.0),
                    "total_power_w": row.get("total_power_w"),
                }
            )
    return rows


def _process_transfer_success(policy_rows: Sequence[dict]) -> bool:
    seen_transfer_1 = False
    seen_transfer_2 = False
    success = False
    for row in policy_rows:
        state = str(row.get("process_state") or "").upper()
        if state == "TRANSFER_1_TO_2":
            seen_transfer_1 = True
        elif state == "TRANSFER_2_TO_1":
            seen_transfer_2 = True
        elif state == "HEAT_TANK2" and seen_transfer_1:
            success = True
        elif state in {"HEAT_TANK1", "IDLE"} and seen_transfer_2:
            success = True
    return success


def _build_process_performance_rows(system_rows: List[dict], tank_rows: List[dict]) -> List[dict]:
    grouped_system = _group_system_rows_by_policy(system_rows)
    grouped_tanks: Dict[str, List[dict]] = defaultdict(list)
    for row in tank_rows:
        grouped_tanks[str(row.get("control_policy") or "UNKNOWN")].append(row)
    for rows in grouped_tanks.values():
        rows.sort(key=lambda item: int(item["ts"]))

    policy_summary = {str(row.get("control_policy")): row for row in _build_policy_summary_rows(system_rows)}
    rows: List[dict] = []
    order = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]
    seen = set()
    for policy in order + sorted(grouped_system.keys()):
        if policy in seen or policy not in grouped_system:
            continue
        seen.add(policy)
        policy_rows = grouped_system[policy]
        tank_policy_rows = grouped_tanks.get(policy) or []
        summary_row = policy_summary.get(policy) or {}
        target_temp_achieved = False
        for row in tank_policy_rows:
            tank1_temp = _safe_float(row.get("tank1_temp_c"))
            tank1_target = _safe_float(row.get("tank1_target_temp_c"))
            tank2_temp = _safe_float(row.get("tank2_temp_c"))
            tank2_target = _safe_float(row.get("tank2_target_temp_c"))
            if tank1_temp is not None and tank1_target is not None and tank1_temp >= tank1_target:
                target_temp_achieved = True
            if tank2_temp is not None and tank2_target is not None and tank2_temp >= tank2_target:
                target_temp_achieved = True
            if target_temp_achieved:
                break
        completed_cycles = _safe_float(summary_row.get("completed_cycle_delta")) or 0.0
        transfer_success = completed_cycles > 0.0 or _process_transfer_success(policy_rows)
        rows.append(
            {
                "control_policy": policy,
                "cycle_completed": completed_cycles > 0.0,
                "completed_cycle_count": completed_cycles,
                "process_time_s": summary_row.get("process_time_s"),
                "target_temperature_achieved": target_temp_achieved,
                "transfer_success": transfer_success,
            }
        )
    return rows


def _build_prediction_residual_rows(model_label: str, comparison_rows: List[dict]) -> List[dict]:
    rows: List[dict] = []
    for row in comparison_rows:
        actual_power = _safe_float(row.get("actual_rolling_avg_power_w"))
        predicted_power = _safe_float(row.get("predicted_rolling_avg_power_w"))
        actual_energy = _safe_float(row.get("actual_rolling_energy_wh"))
        predicted_energy = _safe_float(row.get("predicted_rolling_energy_wh"))
        if predicted_power is None and predicted_energy is None:
            continue
        rows.append(
            {
                "model": model_label,
                "ts": row.get("ts"),
                "label_utc": row.get("label_utc"),
                "rolling_window_minutes": row.get("rolling_window_minutes"),
                "actual_rolling_avg_power_w": actual_power,
                "predicted_rolling_avg_power_w": predicted_power,
                "power_residual_w": (predicted_power - actual_power) if predicted_power is not None and actual_power is not None else None,
                "actual_rolling_energy_wh": actual_energy,
                "predicted_rolling_energy_wh": predicted_energy,
                "energy_residual_wh": (predicted_energy - actual_energy) if predicted_energy is not None and actual_energy is not None else None,
            }
        )
    return rows


def _control_risk_active(row: dict) -> bool:
    if _safe_bool(row.get("peak_event")) is True:
        return True
    if _safe_bool(row.get("predictor_peak_risk")) is True:
        return True
    risk_level = str(row.get("predictor_risk_level") or "").upper()
    if risk_level == "HIGH":
        return True
    total_power_w = _safe_float(row.get("total_power_w"))
    max_total_power_w = _safe_float(row.get("max_total_power_w"))
    return bool(
        total_power_w is not None
        and max_total_power_w is not None
        and max_total_power_w > 0.0
        and total_power_w >= (max_total_power_w * 0.98)
    )


def _control_counter_snapshot(row: dict) -> Tuple[float, float, float, float]:
    return (
        _safe_float(row.get("total_control_actions")) or 0.0,
        _safe_float(row.get("shedding_event_count")) or 0.0,
        _safe_float(row.get("curtail_event_count")) or 0.0,
        _safe_float(row.get("restore_event_count")) or 0.0,
    )


def _control_counters_changed(row: dict, baseline: Tuple[float, float, float, float]) -> bool:
    current = _control_counter_snapshot(row)
    return any(current[index] > baseline[index] for index in range(len(baseline)))


def _build_control_response_metrics_rows(system_rows: List[dict]) -> List[dict]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in system_rows:
        grouped[str(row.get("control_policy") or "UNKNOWN")].append(row)

    rows: List[dict] = []
    order = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]
    seen = set()
    for policy in order + sorted(grouped.keys()):
        if policy not in grouped or policy in seen:
            continue
        seen.add(policy)
        policy_rows = sorted(grouped[policy], key=lambda item: item["ts"])
        action_delays_sec: List[float] = []
        clearance_delays_sec: List[float] = []
        risk_onset_count = 0
        previous_risk = False
        for index, row in enumerate(policy_rows):
            risk_active = _control_risk_active(row)
            if risk_active and not previous_risk:
                risk_onset_count += 1
                onset_ts = int(row["ts"])
                baseline = _control_counter_snapshot(row)
                for follow in policy_rows[index + 1:]:
                    if _control_counters_changed(follow, baseline):
                        action_delays_sec.append(max(0.0, (int(follow["ts"]) - onset_ts) / 1000.0))
                        break
                    if not _control_risk_active(follow):
                        break
                cap_w = _safe_float(row.get("max_total_power_w"))
                if cap_w is not None and cap_w > 0.0:
                    for follow in policy_rows[index:]:
                        total_power_w = _safe_float(follow.get("total_power_w"))
                        if total_power_w is not None and total_power_w <= cap_w:
                            clearance_delays_sec.append(max(0.0, (int(follow["ts"]) - onset_ts) / 1000.0))
                            break
            previous_risk = risk_active

        settling_values = [
            value for value in (_safe_float(row.get("last_peak_settling_time_sec")) for row in policy_rows)
            if value is not None and value > 0.0
        ]
        overshoot_values = [
            value for value in (_safe_float(row.get("max_overshoot_w")) for row in policy_rows)
            if value is not None
        ]
        rows.append(
            {
                "control_policy": policy,
                "sample_count": len(policy_rows),
                "risk_onset_count": risk_onset_count,
                "mean_first_control_action_delay_sec": mean(action_delays_sec) if action_delays_sec else None,
                "p95_first_control_action_delay_sec": _percentile(action_delays_sec, 0.95) if action_delays_sec else None,
                "mean_threshold_clearance_delay_sec": mean(clearance_delays_sec) if clearance_delays_sec else None,
                "p95_threshold_clearance_delay_sec": _percentile(clearance_delays_sec, 0.95) if clearance_delays_sec else None,
                "mean_peak_settling_time_sec": mean(settling_values) if settling_values else None,
                "p95_peak_settling_time_sec": _percentile(settling_values, 0.95) if settling_values else None,
                "max_recorded_overshoot_w": max(overshoot_values) if overshoot_values else None,
                "overshoot_event_count": _field_delta(policy_rows, "overshoot_event_count"),
            }
        )
    return rows


def _command_kind(payload: dict, topic: str) -> str:
    if topic == TOPIC_PREDICTOR_CMD_NAME:
        return "predictor_short_cmd"
    if topic == TOPIC_PREDICTOR_LONG_CMD_NAME:
        return "predictor_long_rf_cmd"
    if topic == TOPIC_PREDICTOR_LONG_LSTM_CMD_NAME:
        return "predictor_long_lstm_cmd"
    if not isinstance(payload, dict):
        return "unknown"
    if payload.get("device") is not None:
        return "device_set"
    if payload.get("control_policy") is not None:
        return "control_policy"
    if payload.get("predictor_mode") is not None or payload.get("PREDICTOR_MODE") is not None:
        return "predictor_mode"
    if payload.get("process") is not None:
        return "process"
    if payload.get("control") is not None:
        return "control_update"
    if payload.get("scene") is not None:
        return "scene"
    return "other"


def _command_key(payload: dict) -> str:
    try:
        return json.dumps(payload or {}, sort_keys=True, separators=(",", ":"))
    except Exception:
        return str(payload)


def _build_command_latency_rows(command_rows: List[dict], command_ack_rows: List[dict]) -> List[dict]:
    pending: Dict[str, deque] = defaultdict(deque)
    for row in command_rows:
        pending[_command_key(row.get("payload") or {})].append(row)

    rows: List[dict] = []
    for ack_row in command_ack_rows:
        payload = ack_row.get("payload") or {}
        ack_body = payload.get("ack") if isinstance(payload.get("ack"), dict) else {}
        cmd_body = payload.get("cmd") if isinstance(payload.get("cmd"), dict) else {}
        key = _command_key(cmd_body)
        command_row = pending[key].popleft() if pending.get(key) else None
        if command_row is None:
            continue
        latency_ms = max(0.0, float(ack_row["ts"]) - float(command_row["ts"]))
        rows.append(
            {
                "command_type": _command_kind(cmd_body, command_row.get("topic") or TOPIC_COMMAND_NAME),
                "command_label": cmd_body.get("device") or cmd_body.get("control_policy") or cmd_body.get("predictor_mode") or cmd_body.get("PREDICTOR_MODE") or "",
                "command_logged_at_utc": command_row.get("label_utc"),
                "ack_logged_at_utc": ack_row.get("label_utc"),
                "publish_to_ack_ms_approx": latency_ms,
                "ack_ok": _safe_bool(ack_body.get("ok")),
                "ack_msg": ack_body.get("msg"),
            }
        )
    return rows


def _build_command_latency_summary_rows(command_latency_rows: List[dict]) -> List[dict]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in command_latency_rows:
        grouped[str(row.get("command_type") or "unknown")].append(row)

    rows: List[dict] = []
    for command_type in sorted(grouped.keys()):
        items = grouped[command_type]
        latencies = [
            value for value in (_safe_float(row.get("publish_to_ack_ms_approx")) for row in items)
            if value is not None
        ]
        ack_flags = [value for value in (_safe_bool(row.get("ack_ok")) for row in items) if value is not None]
        rows.append(
            {
                "command_type": command_type,
                "count": len(items),
                "mean_publish_to_ack_ms_approx": mean(latencies) if latencies else None,
                "p50_publish_to_ack_ms_approx": _percentile(latencies, 0.5) if latencies else None,
                "p95_publish_to_ack_ms_approx": _percentile(latencies, 0.95) if latencies else None,
                "max_publish_to_ack_ms_approx": max(latencies) if latencies else None,
                "ack_ok_ratio": (sum(1 for flag in ack_flags if flag is True) / len(ack_flags)) if ack_flags else None,
                "notes": "Approximate broker-observed command-to-ack latency based on logger timestamps",
            }
        )
    return rows


def _build_step_timing_rows(run_metadata: Optional[Dict[str, object]]) -> List[dict]:
    if not isinstance(run_metadata, dict):
        return []
    rows: List[dict] = []
    for step in list(run_metadata.get("plan") or []):
        if not isinstance(step, dict):
            continue
        started_ms = _parse_epoch_ms(step.get("started_at"))
        completed_ms = _parse_epoch_ms(step.get("completed_at"))
        rows.append(
            {
                "step_id": step.get("id"),
                "action": step.get("action"),
                "label": step.get("label"),
                "status": step.get("status"),
                "planned_duration_sec": _safe_float(step.get("duration_sec")),
                "actual_duration_sec": ((completed_ms - started_ms) / 1000.0) if started_ms is not None and completed_ms is not None else None,
                "started_at": step.get("started_at"),
                "completed_at": step.get("completed_at"),
            }
        )
    return rows


def _build_run_timing_summary_rows(run_metadata: Optional[Dict[str, object]], step_rows: List[dict]) -> List[dict]:
    if not isinstance(run_metadata, dict):
        return []
    started_ms = _parse_epoch_ms(run_metadata.get("started_at"))
    finished_ms = _parse_epoch_ms(run_metadata.get("finished_at"))
    actual_step_durations = [
        value for value in (_safe_float(row.get("actual_duration_sec")) for row in step_rows)
        if value is not None
    ]
    return [
        {
            "run_id": run_metadata.get("id"),
            "template_id": run_metadata.get("template_id"),
            "status": run_metadata.get("status"),
            "started_at": run_metadata.get("started_at"),
            "finished_at": run_metadata.get("finished_at"),
            "total_duration_sec": ((finished_ms - started_ms) / 1000.0) if started_ms is not None and finished_ms is not None else None,
            "step_count": len(step_rows),
            "mean_step_duration_sec": mean(actual_step_durations) if actual_step_durations else None,
            "p95_step_duration_sec": _percentile(actual_step_durations, 0.95) if actual_step_durations else None,
        }
    ]


def _build_observability_notes(
    stream_timing_rows: List[dict],
    command_latency_rows: List[dict],
    step_rows: List[dict],
) -> str:
    telemetry_row = next((row for row in stream_timing_rows if str(row.get("stream")) == "telemetry"), None)
    telemetry_median = telemetry_row.get("median_interval_sec") if telemetry_row else None
    command_supported = bool(command_latency_rows)
    step_supported = bool(step_rows)
    lines = [
        "# IoT Observability Notes",
        "",
        "Measured directly:",
        "- Telemetry and predictor stream cadence, jitter, and approximate gap counts from logged timestamps.",
        "- Predictor freshness/applied ratios from telemetry `system.prediction_sources.*` state.",
        "- Control-action counts and response metrics from telemetry `evaluation.control` / `evaluation.stability` fields.",
    ]
    if command_supported:
        lines.append("- Approximate command-to-ack latency from logger-observed `cmd` and `cmd_ack` timestamps.")
    else:
        lines.append("- Command-to-ack latency is unavailable in this export because `cmd` / `cmd_ack` topics were not present in the source logs.")
    if step_supported:
        lines.append("- Experiment step timing from run orchestration events.")
    lines.extend(
        [
            "",
            "Not directly measurable from current logs:",
            "- True MQTT packet loss. `approx_gap_count` is only a cadence-gap estimate.",
            "- Exact frontend-to-broker-to-ESP32 latency and exact ESP32 receive timestamp.",
            "- Exact predictor inference latency unless predictor runtimes publish dedicated timing fields.",
            "",
            f"Observed telemetry median interval: {telemetry_median if telemetry_median is not None else 'n/a'} sec.",
        ]
    )
    return "\n".join(lines) + "\n"


def _build_scalability_notes(
    data: dict,
    stream_timing_rows: List[dict],
    rolling_window_minutes: Sequence[int],
) -> str:
    system_rows = data.get("system_rows") or []
    load_rows = data.get("load_rows") or []
    unique_loads = sorted({str(row.get("load_name") or "") for row in load_rows if row.get("load_name")})
    telemetry_row = next((row for row in stream_timing_rows if str(row.get("stream")) == "telemetry"), {})
    lines = [
        "# Scalability Considerations",
        "",
        f"- Telemetry samples exported: {len(system_rows)}",
        f"- Per-load samples exported: {len(load_rows)}",
        f"- Unique loads observed: {len(unique_loads)} ({', '.join(unique_loads) if unique_loads else 'n/a'})",
        f"- Rolling windows exported: {', '.join(str(int(value)) for value in rolling_window_minutes)} minute(s)",
        f"- Telemetry median interval: {telemetry_row.get('median_interval_sec', 'n/a')} sec",
        f"- Telemetry p95 interval: {telemetry_row.get('p95_interval_sec', 'n/a')} sec",
        "",
        "Engineering notes:",
        "- Export processing is linear in the number of JSONL records scanned for the selected run window.",
        "- Additional rolling windows and per-load exports increase CPU and CSV volume but do not change the underlying telemetry cadence.",
        "- If tighter latency analysis is required, keep command/ack logging enabled and avoid over-aggregating the logger cadence.",
    ]
    return "\n".join(lines) + "\n"


def _build_timing_diagram(
    stream_timing_rows: List[dict],
    command_latency_summary_rows: List[dict],
) -> str:
    telemetry_row = next((row for row in stream_timing_rows if str(row.get("stream")) == "telemetry"), {})
    rf_row = next((row for row in stream_timing_rows if str(row.get("stream")) == "prediction_long_rf"), {})
    lstm_row = next((row for row in stream_timing_rows if str(row.get("stream")) == "prediction_long_lstm"), {})
    short_row = next((row for row in stream_timing_rows if str(row.get("stream")) == "prediction_short"), {})
    all_ack_latencies = [
        _safe_float(row.get("p50_publish_to_ack_ms_approx"))
        for row in command_latency_summary_rows
        if _safe_float(row.get("p50_publish_to_ack_ms_approx")) is not None
    ]
    ack_note = f"Approx cmd->ack p50: {min(all_ack_latencies):.1f} ms" if all_ack_latencies else "Approx cmd->ack latency: unavailable"
    lines = [
        "sequenceDiagram",
        "    autonumber",
        "    participant UI as Frontend/Admin UI",
        "    participant MQTT as MQTT Broker",
        "    participant ESP as ESP32 Controller",
        "    participant PRED as Predictors (EMA/RF/LSTM)",
        "    participant LOG as Telemetry Logger / History API",
        f"    Note over UI,ESP: {ack_note}",
        f"    Note over ESP,LOG: Telemetry median interval ~ {telemetry_row.get('median_interval_sec', 'n/a')} s",
        f"    Note over PRED,LOG: EMA median ~ {short_row.get('median_interval_sec', 'n/a')} s; RF median ~ {rf_row.get('median_interval_sec', 'n/a')} s; LSTM median ~ {lstm_row.get('median_interval_sec', 'n/a')} s",
        "    UI->>MQTT: publish dt/<plant>/cmd",
        "    MQTT->>ESP: deliver command",
        "    ESP-->>MQTT: cmd_ack",
        "    ESP-->>MQTT: telemetry",
        "    PRED-->>MQTT: prediction topics",
        "    MQTT-->>LOG: persisted to JSONL",
        "    LOG-->>UI: export-ready artefacts",
    ]
    return "\n".join(lines) + "\n"


def _build_supply_voltage_summary(system_rows: List[dict], event_rows: List[dict]) -> List[dict]:
    supply_values = [row["supply_v"] for row in system_rows if row.get("supply_v") is not None]
    total_current_values = [row["total_current_a"] for row in system_rows if row.get("total_current_a") is not None]
    total_power_values = [row["total_power_w"] for row in system_rows if row.get("total_power_w") is not None]
    uv_events = sum(1 for row in event_rows if row.get("event_type") == "undervoltage")
    ov_events = sum(1 for row in event_rows if row.get("event_type") == "overvoltage")

    if not system_rows:
        return []

    latest = system_rows[-1]
    summary = {
        "start_utc": system_rows[0]["label_utc"],
        "end_utc": system_rows[-1]["label_utc"],
        "sample_count": len(system_rows),
        "mean_supply_v": mean(supply_values) if supply_values else None,
        "std_supply_v": pstdev(supply_values) if len(supply_values) > 1 else 0.0 if supply_values else None,
        "min_supply_v": min(supply_values) if supply_values else None,
        "p05_supply_v": _percentile(supply_values, 0.05),
        "p95_supply_v": _percentile(supply_values, 0.95),
        "max_supply_v": max(supply_values) if supply_values else None,
        "mean_total_current_a": mean(total_current_values) if total_current_values else None,
        "mean_total_power_w": mean(total_power_values) if total_power_values else None,
        "undervoltage_active_samples": sum(1 for row in system_rows if row.get("undervoltage_active") is True),
        "overvoltage_active_samples": sum(1 for row in system_rows if row.get("overvoltage_active") is True),
        "undervoltage_event_count": uv_events,
        "overvoltage_event_count": ov_events,
        "configured_uv_threshold_v": latest.get("undervoltage_threshold_v"),
        "configured_ov_threshold_v": latest.get("overvoltage_threshold_v"),
        "configured_uv_restore_margin_v": latest.get("undervoltage_restore_margin_v"),
        "configured_ov_restore_margin_v": latest.get("overvoltage_restore_margin_v"),
    }
    return [summary]


def _build_full_load_summary(load_rows: List[dict]) -> List[dict]:
    by_load = defaultdict(list)
    for row in load_rows:
        by_load[row["load_name"]].append(row)

    out = []
    for load_name in sorted(by_load.keys()):
        rows = by_load[load_name]
        max_duty = max((row.get("duty") or 0.0) for row in rows)
        if max_duty <= 0.0:
            continue
        full_load_rows = [
            row for row in rows
            if row.get("on") is True
            and row.get("duty") is not None
            and row.get("duty") >= (max_duty * 0.98)
        ]
        if not full_load_rows:
            continue
        voltage_values = [row["voltage_v"] for row in full_load_rows if row.get("voltage_v") is not None]
        current_values = [row["current_a"] for row in full_load_rows if row.get("current_a") is not None]
        power_values = [row["power_w"] for row in full_load_rows if row.get("power_w") is not None]
        out.append(
            {
                "load_name": load_name,
                "full_load_duty_basis": max_duty,
                "full_load_sample_count": len(full_load_rows),
                "mean_voltage_v": mean(voltage_values) if voltage_values else None,
                "mean_current_a": mean(current_values) if current_values else None,
                "mean_power_w": mean(power_values) if power_values else None,
                "max_voltage_v": max(voltage_values) if voltage_values else None,
                "max_current_a": max(current_values) if current_values else None,
                "max_power_w": max(power_values) if power_values else None,
                "first_sample_utc": full_load_rows[0]["label_utc"],
                "last_sample_utc": full_load_rows[-1]["label_utc"],
                "datasheet_voltage_v": "",
                "datasheet_current_a": "",
                "datasheet_power_w": "",
            }
        )
    return out


def _build_voltage_events(system_rows: List[dict]) -> List[dict]:
    events = []

    def close_event(event_type: str, start_row: dict, end_row: dict, min_v: Optional[float], max_v: Optional[float]):
        events.append(
            {
                "event_type": event_type,
                "start_ts": start_row["ts"],
                "start_utc": start_row["label_utc"],
                "end_ts": end_row["ts"],
                "end_utc": end_row["label_utc"],
                "duration_sec": max(0.0, (end_row["ts"] - start_row["ts"]) / 1000.0),
                "min_supply_v": min_v,
                "max_supply_v": max_v,
                "start_total_power_w": start_row.get("total_power_w"),
                "end_total_power_w": end_row.get("total_power_w"),
                "start_total_current_a": start_row.get("total_current_a"),
                "end_total_current_a": end_row.get("total_current_a"),
            }
        )

    for event_key, active_key in (("undervoltage", "undervoltage_active"), ("overvoltage", "overvoltage_active")):
        active = False
        start_row = None
        min_v = None
        max_v = None
        last_row = None
        for row in system_rows:
            state = row.get(active_key) is True
            supply_v = row.get("supply_v")
            if state and not active:
                active = True
                start_row = row
                min_v = supply_v
                max_v = supply_v
            if state and active:
                if supply_v is not None:
                    min_v = supply_v if min_v is None else min(min_v, supply_v)
                    max_v = supply_v if max_v is None else max(max_v, supply_v)
            if active and not state and start_row is not None and last_row is not None:
                close_event(event_key, start_row, last_row, min_v, max_v)
                active = False
                start_row = None
                min_v = None
                max_v = None
            last_row = row
        if active and start_row is not None and last_row is not None:
            close_event(event_key, start_row, last_row, min_v, max_v)

    return events


def _build_rolling_window_rows(system_rows: List[dict], rolling_window_minutes: Optional[Sequence[int]] = None) -> List[dict]:
    rows: List[dict] = []
    window_minutes_list = list(rolling_window_minutes or ROLLING_WINDOW_MINUTES)
    for row in system_rows:
        for window_minutes in window_minutes_list:
            window_label = _rolling_window_label(int(window_minutes))
            rows.append(
                {
                    "ts": row["ts"],
                    "label_utc": row["label_utc"],
                    "window_minutes": int(window_minutes),
                    "window_label": window_label,
                    "rolling_energy_wh": row.get(f"rolling_energy_{window_label}_wh"),
                    "rolling_avg_power_w": row.get(f"rolling_avg_power_{window_label}_w"),
                    "rolling_cost": row.get(f"rolling_cost_{window_label}"),
                    "total_energy_wh": row.get("total_energy_wh"),
                    "accumulated_energy_cost": row.get("accumulated_energy_cost"),
                    "total_cost": row.get("total_cost"),
                    "control_policy": row.get("control_policy"),
                    "process_state": row.get("process_state"),
                    "cost_tariff_state": row.get("cost_tariff_state"),
                }
            )
    return rows


def _build_rolling_and_cumulative_energy_rows(rolling_window_rows: Sequence[dict]) -> List[dict]:
    return [
        {
            "ts": row.get("ts"),
            "label_utc": row.get("label_utc"),
            "window_minutes": row.get("window_minutes"),
            "window_label": row.get("window_label"),
            "rolling_energy_wh": row.get("rolling_energy_wh"),
            "rolling_avg_power_w": row.get("rolling_avg_power_w"),
            "total_energy_wh": row.get("total_energy_wh"),
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
        }
        for row in rolling_window_rows
    ]


def _build_rolling_and_cumulative_cost_rows(rolling_window_rows: Sequence[dict]) -> List[dict]:
    return [
        {
            "ts": row.get("ts"),
            "label_utc": row.get("label_utc"),
            "window_minutes": row.get("window_minutes"),
            "window_label": row.get("window_label"),
            "rolling_cost": row.get("rolling_cost"),
            "accumulated_energy_cost": row.get("accumulated_energy_cost"),
            "total_cost": row.get("total_cost"),
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
            "cost_tariff_state": row.get("cost_tariff_state"),
        }
        for row in rolling_window_rows
    ]


def _attach_load_rolling_fields(load_rows: List[dict], rolling_window_minutes: Optional[Sequence[int]] = None) -> List[dict]:
    if not load_rows:
        return load_rows

    by_load: Dict[str, List[dict]] = defaultdict(list)
    for row in load_rows:
        by_load[str(row.get("load_name") or "")].append(row)

    for load_name, rows in by_load.items():
        rows.sort(key=lambda item: int(item.get("ts") or 0))
        previous_ts = None
        previous_power_w = None
        cumulative_energy_wh = 0.0
        window_minutes_list = list(rolling_window_minutes or ROLLING_WINDOW_MINUTES)
        rolling_specs = [(int(minutes), _rolling_window_label(int(minutes))) for minutes in window_minutes_list]
        rolling_segments = {minutes: deque() for minutes, _ in rolling_specs}
        rolling_energy_totals = {minutes: 0.0 for minutes, _ in rolling_specs}

        for row in rows:
            ts = int(row.get("ts") or 0)
            power_w = _safe_float(row.get("power_w")) or 0.0
            incremental_energy_wh = 0.0
            if previous_ts is not None and ts > previous_ts:
                dt_hours = (ts - previous_ts) / 3600000.0
                reference_power_w = power_w
                if previous_power_w is not None:
                    reference_power_w = (float(previous_power_w) + power_w) * 0.5
                incremental_energy_wh = max(0.0, reference_power_w * dt_hours)

            cumulative_energy_wh += incremental_energy_wh
            row["incremental_energy_wh"] = incremental_energy_wh
            row["cumulative_energy_wh"] = cumulative_energy_wh

            if previous_ts is not None and ts > previous_ts:
                segment = {
                    "start_ms": int(previous_ts),
                    "end_ms": int(ts),
                    "energy_wh": float(incremental_energy_wh),
                }
                for window_minutes, _ in rolling_specs:
                    rolling_segments[window_minutes].append(segment)
                    rolling_energy_totals[window_minutes] += float(incremental_energy_wh)

            for window_minutes, window_label in rolling_specs:
                window_queue = rolling_segments[window_minutes]
                window_start_ms = ts - (window_minutes * 60000)
                while window_queue and int(window_queue[0]["end_ms"]) <= window_start_ms:
                    expired = window_queue.popleft()
                    rolling_energy_totals[window_minutes] -= float(expired["energy_wh"])

                window_energy_wh = float(rolling_energy_totals[window_minutes])
                if window_queue:
                    head = window_queue[0]
                    head_start = int(head["start_ms"])
                    head_end = int(head["end_ms"])
                    if head_start < window_start_ms < head_end:
                        head_span_ms = max(1, head_end - head_start)
                        outside_ratio = (window_start_ms - head_start) / head_span_ms
                        window_energy_wh -= float(head["energy_wh"]) * outside_ratio

                window_energy_wh = max(0.0, window_energy_wh)
                row[f"rolling_energy_{window_label}_wh"] = window_energy_wh
                row[f"rolling_avg_power_{window_label}_w"] = window_energy_wh / (window_minutes / 60.0)

            previous_ts = ts
            previous_power_w = power_w

    return load_rows


def _build_per_load_rolling_rows(load_rows: List[dict], rolling_window_minutes: Optional[Sequence[int]] = None) -> List[dict]:
    rows: List[dict] = []
    window_minutes_list = list(rolling_window_minutes or ROLLING_WINDOW_MINUTES)
    for row in load_rows:
        for window_minutes in window_minutes_list:
            window_label = _rolling_window_label(int(window_minutes))
            rows.append(
                {
                    "ts": row.get("ts"),
                    "label_utc": row.get("label_utc"),
                    "load_name": row.get("load_name"),
                    "load_type": row.get("load_type"),
                    "class": row.get("class"),
                    "priority": row.get("priority"),
                    "window_minutes": int(window_minutes),
                    "window_label": window_label,
                    "power_w": row.get("power_w"),
                    "current_a": row.get("current_a"),
                    "incremental_energy_wh": row.get("incremental_energy_wh"),
                    "cumulative_energy_wh": row.get("cumulative_energy_wh"),
                    "rolling_energy_wh": row.get(f"rolling_energy_{window_label}_wh"),
                    "rolling_avg_power_w": row.get(f"rolling_avg_power_{window_label}_w"),
                    "on": row.get("on"),
                    "duty_applied": row.get("duty_applied"),
                }
            )
    return rows


def _collect_data(log_dir: Path, plant_id: str, start_ms: int, end_ms: int, prediction_step_sec: int) -> dict:
    system_rows: List[dict] = []
    env_rows: List[dict] = []
    tank_rows: List[dict] = []
    load_rows: List[dict] = []
    fig_4_15_rows: List[dict] = []
    prediction_rows: Dict[int, dict] = {}
    command_rows: List[dict] = []
    command_ack_rows: List[dict] = []
    used_files = 0
    used_records = 0

    for day in _iter_days(start_ms, end_ms):
        path = log_dir / f"{plant_id}_telemetry_{day.isoformat()}.jsonl"
        if not path.exists():
            continue
        used_files += 1
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
                if not isinstance(payload, dict):
                    continue

                if topic == TOPIC_TELEMETRY_NAME:
                    ts_ms = _parse_epoch_ms(payload.get("timestamp")) or _parse_epoch_ms(record.get("logged_at_utc"))
                    if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                        continue
                    used_records += 1
                    label_utc = _iso_utc(ts_ms)
                    system = payload.get("system") or {}
                    energy = payload.get("energy") or {}
                    environment = payload.get("environment") or {}
                    process = payload.get("process") or {}
                    loads = payload.get("loads") or {}
                    prediction_context = payload.get("prediction_context") or {}
                    control_eval = _nested_value(payload, "evaluation", "control") or {}
                    stability_eval = _nested_value(payload, "evaluation", "stability") or {}
                    prediction_sources = system.get("prediction_sources") or {}
                    pred_short = prediction_sources.get("short") or {}
                    pred_long_rf = prediction_sources.get("long_rf") or {}
                    pred_long_lstm = prediction_sources.get("long_lstm") or {}

                    system_row = {
                        "ts": ts_ms,
                        "label_utc": label_utc,
                        "mode": system.get("mode"),
                        "control_policy": system.get("control_policy"),
                        "predictor_mode": system.get("predictor_mode"),
                        "ai_source_active": system.get("ai_source_active"),
                        "ai_dynamic_process_source": system.get("ai_dynamic_process_source"),
                        "tariff_state": system.get("tariff_state"),
                        "peak_event": _safe_bool(system.get("peak_event")),
                        "supply_v": _safe_float(system.get("supply_v")),
                        "total_current_a": _safe_float(system.get("total_current_a")),
                        "total_power_w": _safe_float(system.get("total_power_w")),
                        "total_energy_wh": _safe_float(energy.get("total_energy_wh")),
                        "max_total_power_w": _safe_float(system.get("MAX_TOTAL_POWER_W")),
                        "energy_goal_value_wh": _safe_float(system.get("ENERGY_GOAL_VALUE_WH")),
                        "energy_goal_duration_sec": _safe_float(system.get("ENERGY_GOAL_DURATION_SEC")),
                        "undervoltage_active": _safe_bool(system.get("UNDERVOLTAGE_ACTIVE")),
                        "overvoltage_active": _safe_bool(system.get("OVERVOLTAGE_ACTIVE")),
                        "undervoltage_threshold_v": _safe_float(system.get("UNDERVOLTAGE_THRESHOLD_V")),
                        "overvoltage_threshold_v": _safe_float(system.get("OVERVOLTAGE_THRESHOLD_V")),
                        "undervoltage_trip_delay_ms": _safe_float(system.get("UNDERVOLTAGE_TRIP_DELAY_MS")),
                        "undervoltage_clear_delay_ms": _safe_float(system.get("UNDERVOLTAGE_CLEAR_DELAY_MS")),
                        "overvoltage_trip_delay_ms": _safe_float(system.get("OVERVOLTAGE_TRIP_DELAY_MS")),
                        "overvoltage_clear_delay_ms": _safe_float(system.get("OVERVOLTAGE_CLEAR_DELAY_MS")),
                        "undervoltage_restore_margin_v": _safe_float(system.get("UNDERVOLTAGE_RESTORE_MARGIN_V")),
                        "overvoltage_restore_margin_v": _safe_float(system.get("OVERVOLTAGE_RESTORE_MARGIN_V")),
                        "process_enabled": _safe_bool(process.get("enabled")),
                        "process_state": process.get("state"),
                        "process_elapsed_sec": _safe_float(process.get("elapsed_sec")),
                        "process_cycle_count": _safe_float(process.get("cycle_count")),
                        "predictor_peak_risk": _safe_bool(prediction_context.get("peak_risk")),
                        "predictor_risk_level": prediction_context.get("risk_level"),
                        "predictor_predicted_power_w": _safe_float(prediction_context.get("predicted_power_w")),
                        "ai_suggestion_count": _safe_float(prediction_context.get("ai_suggestion_count")),
                        "total_control_actions": _safe_float(control_eval.get("total_control_actions")),
                        "shedding_event_count": _safe_float(control_eval.get("shedding_event_count")),
                        "curtail_event_count": _safe_float(control_eval.get("curtail_event_count")),
                        "restore_event_count": _safe_float(control_eval.get("restore_event_count")),
                        "control_actions_per_min": _safe_float(control_eval.get("control_actions_per_min")),
                        "max_overshoot_w": _safe_float(stability_eval.get("max_overshoot_w")),
                        "overshoot_event_count": _safe_float(stability_eval.get("overshoot_event_count")),
                        "overshoot_energy_ws": _safe_float(stability_eval.get("overshoot_energy_ws")),
                        "load_toggle_count": _safe_float(stability_eval.get("load_toggle_count")),
                        "last_peak_settling_time_sec": _safe_float(stability_eval.get("last_peak_settling_time_sec")),
                        "short_fresh": _safe_bool(pred_short.get("fresh")),
                        "short_applied": _safe_bool(pred_short.get("applied")),
                        "short_predicted_power_w": _safe_float(pred_short.get("predicted_power_w")),
                        "short_predicted_window_energy_wh": _safe_float(pred_short.get("predicted_window_energy_wh")) or _safe_float(pred_short.get("projected_energy_window_wh")),
                        "short_predicted_total_energy_wh": _derive_predicted_total_energy(
                            _safe_float(energy.get("total_energy_wh")),
                            _safe_float(pred_short.get("predicted_power_w")),
                            _safe_float(pred_short.get("predicted_total_energy_wh")) or _safe_float(pred_short.get("predicted_energy_wh")),
                            _safe_float(pred_short.get("horizon_sec")),
                        ),
                        "short_horizon_sec": _safe_float(pred_short.get("horizon_sec")),
                        "rf_fresh": _safe_bool(pred_long_rf.get("fresh")),
                        "rf_applied": _safe_bool(pred_long_rf.get("applied")),
                        "rf_predicted_power_w": _safe_float(pred_long_rf.get("predicted_power_w")),
                        "rf_predicted_window_energy_wh": _safe_float(pred_long_rf.get("predicted_window_energy_wh")) or _safe_float(pred_long_rf.get("projected_energy_window_wh")),
                        "rf_predicted_total_energy_wh": _derive_predicted_total_energy(
                            _safe_float(energy.get("total_energy_wh")),
                            _safe_float(pred_long_rf.get("predicted_power_w")),
                            _safe_float(pred_long_rf.get("predicted_total_energy_wh")) or _safe_float(pred_long_rf.get("predicted_energy_wh")),
                            _safe_float(pred_long_rf.get("horizon_sec")),
                        ),
                        "rf_horizon_sec": _safe_float(pred_long_rf.get("horizon_sec")),
                        "lstm_fresh": _safe_bool(pred_long_lstm.get("fresh")),
                        "lstm_applied": _safe_bool(pred_long_lstm.get("applied")),
                        "lstm_predicted_power_w": _safe_float(pred_long_lstm.get("predicted_power_w")),
                        "lstm_predicted_window_energy_wh": _safe_float(pred_long_lstm.get("predicted_window_energy_wh")) or _safe_float(pred_long_lstm.get("projected_energy_window_wh")),
                        "lstm_predicted_total_energy_wh": _derive_predicted_total_energy(
                            _safe_float(energy.get("total_energy_wh")),
                            _safe_float(pred_long_lstm.get("predicted_power_w")),
                            _safe_float(pred_long_lstm.get("predicted_total_energy_wh")) or _safe_float(pred_long_lstm.get("predicted_energy_wh")),
                            _safe_float(pred_long_lstm.get("horizon_sec")),
                        ),
                        "lstm_horizon_sec": _safe_float(pred_long_lstm.get("horizon_sec")),
                    }
                    system_rows.append(system_row)

                    env_rows.append(
                        {
                            "ts": ts_ms,
                            "label_utc": label_utc,
                            "ambient_temp_c": _safe_float(environment.get("temperature_c")),
                            "ambient_humidity_pct": _safe_float(environment.get("humidity_pct")),
                            "process_state": process.get("state"),
                        }
                    )

                    tank1 = process.get("tank1") or {}
                    tank2 = process.get("tank2") or {}
                    tank_rows.append(
                        {
                            "ts": ts_ms,
                            "label_utc": label_utc,
                            "control_policy": system.get("control_policy"),
                            "process_state": process.get("state"),
                            "tank1_temp_c": _safe_float(_nested_value(tank1, "temperature_c") or process.get("tank1_temp_c")),
                            "tank2_temp_c": _safe_float(_nested_value(tank2, "temperature_c") or process.get("tank2_temp_c")),
                            "tank1_target_temp_c": _safe_float(_nested_value(tank1, "target_temp_c") or process.get("tank1_temp_target_c")),
                            "tank2_target_temp_c": _safe_float(_nested_value(tank2, "target_temp_c") or process.get("tank2_temp_target_c")),
                            "tank1_target_temp_base_c": _safe_float(process.get("tank1_temp_target_base_c")),
                            "tank2_target_temp_base_c": _safe_float(process.get("tank2_temp_target_base_c")),
                            "tank1_level_pct": _safe_float(_nested_value(tank1, "level_pct") or process.get("tank1_level_pct")),
                            "tank2_level_pct": _safe_float(_nested_value(tank2, "level_pct") or process.get("tank2_level_pct")),
                            "tank1_level_valid": _safe_bool(_nested_value(tank1, "level_valid") if "level_valid" in tank1 else process.get("tank1_level_valid")),
                            "tank2_level_valid": _safe_bool(_nested_value(tank2, "level_valid") if "level_valid" in tank2 else process.get("tank2_level_valid")),
                            "tank1_ultrasonic_distance_cm": _safe_float(_nested_value(tank1, "ultrasonic_distance_cm") or process.get("tank1_ultrasonic_distance_cm")),
                            "tank2_ultrasonic_distance_cm": _safe_float(_nested_value(tank2, "ultrasonic_distance_cm") or process.get("tank2_ultrasonic_distance_cm")),
                            "tank1_ultrasonic_raw_cm": _safe_float(_nested_value(tank1, "ultrasonic_raw_cm") or process.get("tank1_ultrasonic_raw_cm")),
                            "tank2_ultrasonic_raw_cm": _safe_float(_nested_value(tank2, "ultrasonic_raw_cm") or process.get("tank2_ultrasonic_raw_cm")),
                            "heater1_on": _safe_bool(_nested_value(loads, "heater1", "on")),
                            "heater2_on": _safe_bool(_nested_value(loads, "heater2", "on")),
                            "heater1_duty": _safe_float(_nested_value(loads, "heater1", "duty")),
                            "heater2_duty": _safe_float(_nested_value(loads, "heater2", "duty")),
                        }
                    )

                    fig_4_15_row = {
                        "ts": ts_ms,
                        "label_utc": label_utc,
                        "supply_v": system_row["supply_v"],
                        "total_current_a": system_row["total_current_a"],
                        "total_power_w": system_row["total_power_w"],
                        "undervoltage_active": system_row["undervoltage_active"],
                        "overvoltage_active": system_row["overvoltage_active"],
                        "control_policy": system_row["control_policy"],
                        "max_total_power_w": system_row["max_total_power_w"],
                        "total_control_actions": system_row["total_control_actions"],
                        "curtail_event_count": system_row["curtail_event_count"],
                        "shedding_event_count": system_row["shedding_event_count"],
                        "process_state": process.get("state"),
                    }

                    for load_name, load in loads.items():
                        if not isinstance(load, dict):
                            continue
                        load_row = {
                            "ts": ts_ms,
                            "label_utc": label_utc,
                            "load_name": load_name,
                            "load_type": _load_type_from_name(load_name),
                            "on": _safe_bool(load.get("on")),
                            "duty": _safe_float(load.get("duty")),
                            "duty_applied": _safe_float(load.get("duty_applied")),
                            "power_w": _safe_float(load.get("power_w")),
                            "current_a": _safe_float(load.get("current_a")),
                            "voltage_v": _safe_float(load.get("voltage_v")),
                            "priority": _safe_float(load.get("priority")),
                            "class": load.get("class"),
                            "override": _safe_bool(load.get("override")),
                            "health": load.get("health"),
                            "fault_code": load.get("fault_code"),
                            "fault_active": _safe_bool(load.get("fault_active")),
                            "fault_latched": _safe_bool(load.get("fault_latched")),
                            "fault_limit_a": _safe_float(load.get("fault_limit_a")),
                            "fault_trip_ms": _safe_float(load.get("fault_trip_ms")),
                            "fault_clear_ms": _safe_float(load.get("fault_clear_ms")),
                            "fault_restore_ms": _safe_float(load.get("fault_restore_ms")),
                            "inject_current_a": _safe_float(load.get("inject_current_a")),
                        }
                        load_rows.append(load_row)

                        fig_4_15_row[f"{load_name}_on"] = load_row["on"]
                        fig_4_15_row[f"{load_name}_duty"] = load_row["duty"]
                        fig_4_15_row[f"{load_name}_power_w"] = load_row["power_w"]
                    fig_4_15_rows.append(fig_4_15_row)

                    bucket = _bucket_ms(ts_ms, prediction_step_sec)
                    pred_row = prediction_rows.setdefault(bucket, _prediction_row(bucket))
                    pred_row["actual_power_w"] = system_row["total_power_w"]
                    pred_row["actual_current_a"] = system_row["total_current_a"]
                    pred_row["actual_total_energy_wh"] = system_row["total_energy_wh"]
                    if pred_row.get("pred_short_power_w") is None:
                        pred_row["pred_short_power_w"] = system_row.get("short_predicted_power_w")
                    if pred_row.get("pred_short_window_energy_wh") is None:
                        pred_row["pred_short_window_energy_wh"] = system_row.get("short_predicted_window_energy_wh")
                    if pred_row.get("pred_short_total_energy_wh") is None:
                        pred_row["pred_short_total_energy_wh"] = system_row.get("short_predicted_total_energy_wh")
                    if pred_row.get("pred_short_horizon_sec") is None:
                        pred_row["pred_short_horizon_sec"] = system_row.get("short_horizon_sec")
                    if pred_row.get("pred_long_rf_power_w") is None:
                        pred_row["pred_long_rf_power_w"] = system_row.get("rf_predicted_power_w")
                    if pred_row.get("pred_long_rf_window_energy_wh") is None:
                        pred_row["pred_long_rf_window_energy_wh"] = system_row.get("rf_predicted_window_energy_wh")
                    if pred_row.get("pred_long_rf_total_energy_wh") is None:
                        pred_row["pred_long_rf_total_energy_wh"] = system_row.get("rf_predicted_total_energy_wh")
                    if pred_row.get("pred_long_rf_horizon_sec") is None:
                        pred_row["pred_long_rf_horizon_sec"] = system_row.get("rf_horizon_sec")
                    if pred_row.get("pred_long_lstm_power_w") is None:
                        pred_row["pred_long_lstm_power_w"] = system_row.get("lstm_predicted_power_w")
                    if pred_row.get("pred_long_lstm_window_energy_wh") is None:
                        pred_row["pred_long_lstm_window_energy_wh"] = system_row.get("lstm_predicted_window_energy_wh")
                    if pred_row.get("pred_long_lstm_total_energy_wh") is None:
                        pred_row["pred_long_lstm_total_energy_wh"] = system_row.get("lstm_predicted_total_energy_wh")
                    if pred_row.get("pred_long_lstm_horizon_sec") is None:
                        pred_row["pred_long_lstm_horizon_sec"] = system_row.get("lstm_horizon_sec")
                    continue

                if topic in (TOPIC_PREDICTION_NAME, TOPIC_PREDICTION_LONG_NAME, TOPIC_PREDICTION_LONG_LSTM_NAME):
                    ts_base_ms = _parse_epoch_ms(payload.get("timestamp")) or _parse_epoch_ms(record.get("logged_at_utc"))
                    if ts_base_ms is None:
                        continue
                    horizon_sec = _safe_float(payload.get("horizon_sec")) or 0.0
                    target_ms = ts_base_ms + int(horizon_sec * 1000.0)
                    if target_ms < start_ms or target_ms > end_ms:
                        continue
                    used_records += 1
                    bucket = _bucket_ms(target_ms, prediction_step_sec)
                    pred_row = prediction_rows.setdefault(bucket, _prediction_row(bucket))
                    pred_power = _safe_float(payload.get("predicted_power_w"))
                    pred_window_energy = _safe_float(payload.get("predicted_window_energy_wh")) or _safe_float(payload.get("projected_energy_window_wh"))
                    pred_energy = _safe_float(payload.get("predicted_total_energy_wh"))
                    if pred_energy is None:
                        pred_energy = _safe_float(payload.get("predicted_energy_wh"))
                    if topic == TOPIC_PREDICTION_NAME:
                        pred_row["pred_short_power_w"] = pred_power
                        pred_row["pred_short_window_energy_wh"] = pred_window_energy
                        pred_row["pred_short_total_energy_wh"] = pred_energy
                        pred_row["pred_short_horizon_sec"] = horizon_sec
                    elif topic == TOPIC_PREDICTION_LONG_NAME:
                        pred_row["pred_long_rf_power_w"] = pred_power
                        pred_row["pred_long_rf_window_energy_wh"] = pred_window_energy
                        pred_row["pred_long_rf_total_energy_wh"] = pred_energy
                        pred_row["pred_long_rf_horizon_sec"] = horizon_sec
                    else:
                        pred_row["pred_long_lstm_power_w"] = pred_power
                        pred_row["pred_long_lstm_window_energy_wh"] = pred_window_energy
                        pred_row["pred_long_lstm_total_energy_wh"] = pred_energy
                        pred_row["pred_long_lstm_horizon_sec"] = horizon_sec
                    continue

                if topic == TOPIC_COMMAND_NAME:
                    ts_ms = _parse_epoch_ms(record.get("logged_at_utc")) or _parse_epoch_ms(payload.get("timestamp"))
                    if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                        continue
                    used_records += 1
                    command_rows.append(
                        {
                            "ts": ts_ms,
                            "label_utc": _iso_utc(ts_ms),
                            "topic": topic,
                            "payload": payload,
                            "logged_at_utc": record.get("logged_at_utc"),
                        }
                    )
                    continue

                if topic == TOPIC_COMMAND_ACK_NAME:
                    ts_ms = _parse_epoch_ms(record.get("logged_at_utc")) or _parse_epoch_ms(payload.get("timestamp"))
                    if ts_ms is None or ts_ms < start_ms or ts_ms > end_ms:
                        continue
                    used_records += 1
                    command_ack_rows.append(
                        {
                            "ts": ts_ms,
                            "label_utc": _iso_utc(ts_ms),
                            "topic": topic,
                            "payload": payload,
                            "logged_at_utc": record.get("logged_at_utc"),
                        }
                    )
                    continue

    system_rows.sort(key=lambda row: row["ts"])
    env_rows.sort(key=lambda row: row["ts"])
    tank_rows.sort(key=lambda row: row["ts"])
    load_rows.sort(key=lambda row: (row["load_name"], row["ts"]))
    fig_4_15_rows.sort(key=lambda row: row["ts"])
    prediction_row_list = [prediction_rows[key] for key in sorted(prediction_rows.keys())]
    command_rows.sort(key=lambda row: row["ts"])
    command_ack_rows.sort(key=lambda row: row["ts"])

    for row in prediction_row_list:
        row["pred_short_total_energy_wh"] = _derive_predicted_total_energy(
            row.get("actual_total_energy_wh"),
            row.get("pred_short_power_w"),
            row.get("pred_short_total_energy_wh"),
            row.get("pred_short_horizon_sec"),
        )
        row["pred_long_rf_total_energy_wh"] = _derive_predicted_total_energy(
            row.get("actual_total_energy_wh"),
            row.get("pred_long_rf_power_w"),
            row.get("pred_long_rf_total_energy_wh"),
            row.get("pred_long_rf_horizon_sec"),
        )
        row["pred_long_lstm_total_energy_wh"] = _derive_predicted_total_energy(
            row.get("actual_total_energy_wh"),
            row.get("pred_long_lstm_power_w"),
            row.get("pred_long_lstm_total_energy_wh"),
            row.get("pred_long_lstm_horizon_sec"),
        )

    prediction_by_bucket = {row["ts"]: row for row in prediction_row_list}
    for row in system_rows:
        pred_row = prediction_by_bucket.get(_bucket_ms(row["ts"], prediction_step_sec))
        if pred_row is None:
            row["short_predicted_total_energy_wh"] = None
            row["rf_predicted_total_energy_wh"] = None
            row["lstm_predicted_total_energy_wh"] = None
            row["predictor_predicted_total_energy_wh"] = None
            continue
        row["short_predicted_total_energy_wh"] = pred_row.get("pred_short_total_energy_wh")
        row["rf_predicted_total_energy_wh"] = pred_row.get("pred_long_rf_total_energy_wh")
        row["lstm_predicted_total_energy_wh"] = pred_row.get("pred_long_lstm_total_energy_wh")
        row["predictor_predicted_total_energy_wh"] = _resolve_fused_predicted_energy(row)

    export_rolling_window_minutes = _merge_rolling_window_minutes(
        ROLLING_WINDOW_MINUTES,
        _prediction_window_minutes(prediction_row_list),
    )
    load_rows = _attach_load_rolling_fields(load_rows, export_rolling_window_minutes)

    tariff_store = _load_tariff_store()
    _attach_cost_fields(system_rows, tariff_store, export_rolling_window_minutes)

    return {
        "system_rows": system_rows,
        "env_rows": env_rows,
        "tank_rows": tank_rows,
        "load_rows": load_rows,
        "fig_4_15_rows": fig_4_15_rows,
        "prediction_rows": prediction_row_list,
        "command_rows": command_rows,
        "command_ack_rows": command_ack_rows,
        "files_scanned": used_files,
        "records_used": used_records,
        "tariff_store": tariff_store,
        "rolling_window_minutes": export_rolling_window_minutes,
    }


def _export_chapter4_files(data: dict, out_dir: Path, prediction_step_sec: int, max_lag_sec: int) -> List[Tuple[str, Path]]:
    system_rows = data["system_rows"]
    env_rows = data["env_rows"]
    tank_rows = data["tank_rows"]
    load_rows = data["load_rows"]
    fig_4_15_rows = data["fig_4_15_rows"]
    prediction_rows = data["prediction_rows"]
    rolling_window_minutes = data.get("rolling_window_minutes") or ROLLING_WINDOW_MINUTES
    system_by_ts = {row["ts"]: row for row in system_rows}
    load_summary_by_ts = _build_ts_load_summary(load_rows)

    tank_level_rows = []
    for row in tank_rows:
        for tank_name in ("tank1", "tank2"):
            tank_level_rows.append(
                {
                    "ts": row["ts"],
                    "label_utc": row["label_utc"],
                    "tank_name": tank_name,
                    "ultrasonic_distance_cm": row.get(f"{tank_name}_ultrasonic_distance_cm"),
                    "ultrasonic_raw_cm": row.get(f"{tank_name}_ultrasonic_raw_cm"),
                    "level_pct": row.get(f"{tank_name}_level_pct"),
                    "level_valid": row.get(f"{tank_name}_level_valid"),
                    "manual_actual_level_pct": "",
                    "manual_actual_distance_cm": "",
                }
            )

    fig_4_7_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "total_power_w": row.get("total_power_w"),
            "total_current_a": row.get("total_current_a"),
            "total_energy_wh": row.get("total_energy_wh"),
            "cost_tariff_state": row.get("cost_tariff_state"),
            "cost_tariff_rate_per_kwh": row.get("cost_tariff_rate_per_kwh"),
            "incremental_energy_cost": row.get("incremental_energy_cost"),
            "accumulated_energy_cost": row.get("accumulated_energy_cost"),
            "max_demand_kw": row.get("max_demand_kw"),
            "demand_charge_cost": row.get("demand_charge_cost"),
            "total_cost": row.get("total_cost"),
            "demand_interval_min": row.get("demand_interval_min"),
            "supply_v": row.get("supply_v"),
            "tariff_state": row.get("tariff_state"),
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
        }
        for row in system_rows
    ]

    fig_4_1_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "load_name": row["load_name"],
            "voltage_v": row.get("voltage_v"),
            "current_a": row.get("current_a"),
            "duty": row.get("duty"),
            "power_w": row.get("power_w"),
            "on": row.get("on"),
        }
        for row in load_rows
    ]

    fig_4_2_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "supply_v": row.get("supply_v"),
            "total_current_a": row.get("total_current_a"),
            "total_power_w": row.get("total_power_w"),
            "undervoltage_active": row.get("undervoltage_active"),
            "overvoltage_active": row.get("overvoltage_active"),
        }
        for row in system_rows
    ]

    fig_4_3_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "process_state": row.get("process_state"),
            "tank1_temp_c": row.get("tank1_temp_c"),
            "tank2_temp_c": row.get("tank2_temp_c"),
            "tank1_target_temp_c": row.get("tank1_target_temp_c"),
            "tank2_target_temp_c": row.get("tank2_target_temp_c"),
            "tank1_target_temp_base_c": row.get("tank1_target_temp_base_c"),
            "tank2_target_temp_base_c": row.get("tank2_target_temp_base_c"),
            "heater1_on": row.get("heater1_on"),
            "heater2_on": row.get("heater2_on"),
            "heater1_duty": row.get("heater1_duty"),
            "heater2_duty": row.get("heater2_duty"),
        }
        for row in tank_rows
    ]

    fig_4_8_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "load_name": row["load_name"],
            "load_type": row.get("load_type"),
            "class": row.get("class"),
            "on": row.get("on"),
            "duty": row.get("duty"),
            "duty_applied": row.get("duty_applied"),
            "power_w": row.get("power_w"),
            "current_a": row.get("current_a"),
            "voltage_v": row.get("voltage_v"),
        }
        for row in load_rows
    ]

    fig_4_9_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "total_energy_wh": row.get("total_energy_wh"),
            "total_power_w": row.get("total_power_w"),
            "cost_tariff_state": row.get("cost_tariff_state"),
            "cost_tariff_rate_per_kwh": row.get("cost_tariff_rate_per_kwh"),
            "incremental_energy_cost": row.get("incremental_energy_cost"),
            "accumulated_energy_cost": row.get("accumulated_energy_cost"),
            "max_demand_kw": row.get("max_demand_kw"),
            "demand_charge_cost": row.get("demand_charge_cost"),
            "total_cost": row.get("total_cost"),
            "demand_interval_min": row.get("demand_interval_min"),
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
            "energy_goal_value_wh": row.get("energy_goal_value_wh"),
            "energy_goal_duration_sec": row.get("energy_goal_duration_sec"),
        }
        for row in system_rows
    ]

    fig_4_13_rows = []
    fig_4_16_rows = []
    for row in load_rows:
        sys = system_by_ts.get(row["ts"], {})
        shared = {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "control_policy": sys.get("control_policy"),
            "process_state": sys.get("process_state"),
            "total_power_w": sys.get("total_power_w"),
            "max_total_power_w": sys.get("max_total_power_w"),
            "total_control_actions": sys.get("total_control_actions"),
            "shedding_event_count": sys.get("shedding_event_count"),
            "curtail_event_count": sys.get("curtail_event_count"),
            "restore_event_count": sys.get("restore_event_count"),
            "load_name": row["load_name"],
            "load_type": row.get("load_type"),
            "class": row.get("class"),
            "priority": row.get("priority"),
            "on": row.get("on"),
            "duty": row.get("duty"),
            "duty_applied": row.get("duty_applied"),
            "power_w": row.get("power_w"),
        }
        fig_4_13_rows.append(shared)
        fig_4_16_rows.append(
            {
                **shared,
                "current_a": row.get("current_a"),
                "voltage_v": row.get("voltage_v"),
                "fault_active": row.get("fault_active"),
                "fault_latched": row.get("fault_latched"),
                "fault_code": row.get("fault_code"),
                "fault_limit_a": row.get("fault_limit_a"),
                "fault_trip_ms": row.get("fault_trip_ms"),
                "fault_clear_ms": row.get("fault_clear_ms"),
                "fault_restore_ms": row.get("fault_restore_ms"),
                "inject_current_a": row.get("inject_current_a"),
                "health": row.get("health"),
            }
        )

    fig_4_14_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
            "total_power_w": row.get("total_power_w"),
            "total_energy_wh": row.get("total_energy_wh"),
            "max_total_power_w": row.get("max_total_power_w"),
            "cost_tariff_state": row.get("cost_tariff_state"),
            "cost_tariff_rate_per_kwh": row.get("cost_tariff_rate_per_kwh"),
            "incremental_energy_cost": row.get("incremental_energy_cost"),
            "accumulated_energy_cost": row.get("accumulated_energy_cost"),
            "max_demand_kw": row.get("max_demand_kw"),
            "demand_charge_cost": row.get("demand_charge_cost"),
            "total_cost": row.get("total_cost"),
            "demand_interval_min": row.get("demand_interval_min"),
            "predictor_peak_risk": row.get("predictor_peak_risk"),
            "predictor_risk_level": row.get("predictor_risk_level"),
            "predictor_predicted_power_w": row.get("predictor_predicted_power_w"),
            "predictor_predicted_total_energy_wh": row.get("predictor_predicted_total_energy_wh"),
            "short_predicted_power_w": row.get("short_predicted_power_w"),
            "short_predicted_total_energy_wh": row.get("short_predicted_total_energy_wh"),
            "rf_predicted_power_w": row.get("rf_predicted_power_w"),
            "rf_predicted_total_energy_wh": row.get("rf_predicted_total_energy_wh"),
            "lstm_predicted_power_w": row.get("lstm_predicted_power_w"),
            "lstm_predicted_total_energy_wh": row.get("lstm_predicted_total_energy_wh"),
            "total_control_actions": row.get("total_control_actions"),
            "shedding_event_count": row.get("shedding_event_count"),
            "curtail_event_count": row.get("curtail_event_count"),
            "restore_event_count": row.get("restore_event_count"),
            "active_load_count": load_summary_by_ts.get(row["ts"], {}).get("active_load_count"),
            "active_critical_count": load_summary_by_ts.get(row["ts"], {}).get("active_critical_count"),
            "active_essential_count": load_summary_by_ts.get(row["ts"], {}).get("active_essential_count"),
            "active_important_count": load_summary_by_ts.get(row["ts"], {}).get("active_important_count"),
            "active_secondary_count": load_summary_by_ts.get(row["ts"], {}).get("active_secondary_count"),
            "active_non_essential_count": load_summary_by_ts.get(row["ts"], {}).get("active_non_essential_count"),
        }
        for row in system_rows
    ]

    table_4_1_rows = _build_full_load_summary(load_rows)
    event_rows = _build_voltage_events(system_rows)
    rolling_window_rows = _build_rolling_window_rows(system_rows, rolling_window_minutes)
    rolling_energy_rows = [
        {
            "ts": row.get("ts"),
            "label_utc": row.get("label_utc"),
            "window_minutes": row.get("window_minutes"),
            "window_label": row.get("window_label"),
            "rolling_energy_wh": row.get("rolling_energy_wh"),
            "rolling_avg_power_w": row.get("rolling_avg_power_w"),
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
            "cost_tariff_state": row.get("cost_tariff_state"),
        }
        for row in rolling_window_rows
    ]
    rolling_and_cumulative_energy_rows = _build_rolling_and_cumulative_energy_rows(rolling_window_rows)
    rolling_cost_rows = [
        {
            "ts": row.get("ts"),
            "label_utc": row.get("label_utc"),
            "window_minutes": row.get("window_minutes"),
            "window_label": row.get("window_label"),
            "rolling_cost": row.get("rolling_cost"),
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
            "cost_tariff_state": row.get("cost_tariff_state"),
        }
        for row in rolling_window_rows
    ]
    rolling_and_cumulative_cost_rows = _build_rolling_and_cumulative_cost_rows(rolling_window_rows)
    per_load_rolling_rows = _build_per_load_rolling_rows(load_rows, rolling_window_minutes)
    table_4_2_rows = _build_supply_voltage_summary(system_rows, event_rows)

    table_16_rows = _build_policy_summary_rows(system_rows)
    control_activity_rows = _build_policy_control_action_rows(system_rows)
    process_performance_rows = _build_process_performance_rows(system_rows, tank_rows)

    fig_4_10_rows = _build_prediction_comparison_rows(
        prediction_rows,
        system_rows,
        prediction_step_sec,
        "pred_short_power_w",
        "pred_short_window_energy_wh",
        "pred_short_total_energy_wh",
        "pred_short_horizon_sec",
    )
    fig_4_11_rows = _build_prediction_comparison_rows(
        prediction_rows,
        system_rows,
        prediction_step_sec,
        "pred_long_rf_power_w",
        "pred_long_rf_window_energy_wh",
        "pred_long_rf_total_energy_wh",
        "pred_long_rf_horizon_sec",
    )
    fig_4_12_rows = _build_prediction_comparison_rows(
        prediction_rows,
        system_rows,
        prediction_step_sec,
        "pred_long_lstm_power_w",
        "pred_long_lstm_window_energy_wh",
        "pred_long_lstm_total_energy_wh",
        "pred_long_lstm_horizon_sec",
    )

    table_4_3_rows = []
    for label, rows in (
        ("EMA", fig_4_10_rows),
        ("RF", fig_4_11_rows),
        ("LSTM", fig_4_12_rows),
    ):
        metrics = _compute_model_metrics(rows, "actual_rolling_energy_wh", "predicted_rolling_energy_wh", prediction_step_sec, max_lag_sec)
        horizon_sec = _summarize_horizon_sec(rows)
        table_4_3_rows.append(
            {
                "model": label,
                "target": "rolling_energy_wh",
                "comparison_group": "short_horizon_baseline" if label == "EMA" else "long_horizon_model",
                "horizon_sec": horizon_sec,
                "horizon_minutes": (horizon_sec / 60.0) if horizon_sec is not None else None,
                "sample_count": metrics["sample_count"],
                "mae_wh": metrics["mae_wh"],
                "mse_wh2": metrics["mse_wh2"],
                "rmse_wh": metrics["rmse_wh"],
                "r_squared": metrics["r_squared"],
                "lag_sec": metrics["lag_sec"],
                "lag_eval_samples": metrics["lag_eval_samples"],
                "lag_step_sec": metrics["metric_step_sec"],
            }
        )
    model_metrics_by_label = {str(row.get("model")): row for row in table_4_3_rows}
    ema_metrics_rows = [model_metrics_by_label["EMA"]] if "EMA" in model_metrics_by_label else []
    rf_metrics_rows = [model_metrics_by_label["RF"]] if "RF" in model_metrics_by_label else []
    lstm_metrics_rows = [model_metrics_by_label["LSTM"]] if "LSTM" in model_metrics_by_label else []
    prediction_residual_rows = (
        _build_prediction_residual_rows("EMA", fig_4_10_rows)
        + _build_prediction_residual_rows("RF", fig_4_11_rows)
        + _build_prediction_residual_rows("LSTM", fig_4_12_rows)
    )

    fig_4_18_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "control_policy": row.get("control_policy"),
            "total_power_w": row.get("total_power_w"),
            "total_energy_wh": row.get("total_energy_wh"),
            "accumulated_energy_cost": row.get("accumulated_energy_cost"),
            "demand_charge_cost": row.get("demand_charge_cost"),
            "total_cost": row.get("total_cost"),
            "process_state": row.get("process_state"),
        }
        for row in system_rows
    ]

    fig_4_19_rows = [
        {
            "control_policy": row.get("control_policy"),
            "peak_power_w": row.get("peak_power_w"),
            "peak_reduction_pct": row.get("peak_reduction_pct"),
        }
        for row in table_16_rows
    ]

    fig_4_20_rows = [
        {
            "control_policy": row.get("control_policy"),
            "energy_wh": row.get("energy_wh"),
            "energy_reduction_pct": row.get("energy_reduction_pct"),
        }
        for row in table_16_rows
    ]
    forecast_control_rows = _build_forecast_informed_control_response_rows(system_rows)
    rolling_energy_policy_rows = _build_policy_rolling_energy_comparison_rows(system_rows, rolling_window_minutes)
    before_after_demand_rows = _build_before_after_demand_profile_rows(system_rows)

    fig_4_21_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "control_policy": row.get("control_policy"),
            "predictor_mode": row.get("predictor_mode"),
            "ai_source_active": row.get("ai_source_active"),
            "ai_dynamic_process_source": row.get("ai_dynamic_process_source"),
            "process_enabled": row.get("process_enabled"),
            "process_state": row.get("process_state"),
            "process_cycle_count": row.get("process_cycle_count"),
            "total_power_w": row.get("total_power_w"),
            "total_current_a": row.get("total_current_a"),
            "total_energy_wh": row.get("total_energy_wh"),
            "cost_tariff_state": row.get("cost_tariff_state"),
            "cost_tariff_rate_per_kwh": row.get("cost_tariff_rate_per_kwh"),
            "incremental_energy_cost": row.get("incremental_energy_cost"),
            "accumulated_energy_cost": row.get("accumulated_energy_cost"),
            "max_demand_kw": row.get("max_demand_kw"),
            "demand_charge_cost": row.get("demand_charge_cost"),
            "total_cost": row.get("total_cost"),
            "demand_interval_min": row.get("demand_interval_min"),
            "supply_v": row.get("supply_v"),
            "predictor_peak_risk": row.get("predictor_peak_risk"),
            "predictor_risk_level": row.get("predictor_risk_level"),
            "predictor_predicted_power_w": row.get("predictor_predicted_power_w"),
            "predictor_predicted_total_energy_wh": row.get("predictor_predicted_total_energy_wh"),
            "short_predicted_power_w": row.get("short_predicted_power_w"),
            "short_predicted_total_energy_wh": row.get("short_predicted_total_energy_wh"),
            "rf_predicted_power_w": row.get("rf_predicted_power_w"),
            "rf_predicted_total_energy_wh": row.get("rf_predicted_total_energy_wh"),
            "lstm_predicted_power_w": row.get("lstm_predicted_power_w"),
            "lstm_predicted_total_energy_wh": row.get("lstm_predicted_total_energy_wh"),
            "short_fresh": row.get("short_fresh"),
            "rf_fresh": row.get("rf_fresh"),
            "lstm_fresh": row.get("lstm_fresh"),
            "undervoltage_active": row.get("undervoltage_active"),
            "overvoltage_active": row.get("overvoltage_active"),
            "total_control_actions": row.get("total_control_actions"),
            "active_load_count": load_summary_by_ts.get(row["ts"], {}).get("active_load_count"),
            "active_critical_count": load_summary_by_ts.get(row["ts"], {}).get("active_critical_count"),
            "active_essential_count": load_summary_by_ts.get(row["ts"], {}).get("active_essential_count"),
            "active_important_count": load_summary_by_ts.get(row["ts"], {}).get("active_important_count"),
            "active_secondary_count": load_summary_by_ts.get(row["ts"], {}).get("active_secondary_count"),
            "active_non_essential_count": load_summary_by_ts.get(row["ts"], {}).get("active_non_essential_count"),
        }
        for row in system_rows
    ]

    accumulated_cost_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "control_policy": row.get("control_policy"),
            "process_state": row.get("process_state"),
            "total_energy_wh": row.get("total_energy_wh"),
            "cost_tariff_state": row.get("cost_tariff_state"),
            "cost_tariff_rate_per_kwh": row.get("cost_tariff_rate_per_kwh"),
            "incremental_energy_cost": row.get("incremental_energy_cost"),
            "accumulated_energy_cost": row.get("accumulated_energy_cost"),
            "max_demand_kw": row.get("max_demand_kw"),
            "demand_charge_rate_per_kw": row.get("demand_charge_rate_per_kw"),
            "demand_charge_cost": row.get("demand_charge_cost"),
            "total_cost": row.get("total_cost"),
            "demand_interval_min": row.get("demand_interval_min"),
        }
        for row in system_rows
    ]

    exports = [
        ("table_4_1_per_load_full_load.csv", table_4_1_rows),
        ("table_4_2_supply_voltage_behaviour.csv", table_4_2_rows),
        ("table_4_3_model_performance_metrics.csv", table_4_3_rows),
        ("ema_model_performance_metrics.csv", ema_metrics_rows),
        ("rf_model_performance_metrics.csv", rf_metrics_rows),
        ("lstm_model_performance_metrics.csv", lstm_metrics_rows),
        ("table_16_policy_performance_comparison.csv", table_16_rows),
        ("control_activity_metrics_table.csv", control_activity_rows),
        ("process_performance_table.csv", process_performance_rows),
        ("policy_performance_summary_table.csv", table_16_rows),
        ("figure_4_1_load_voltage_vs_load_current.csv", fig_4_1_rows),
        ("figure_4_2_supply_voltage_vs_total_current.csv", fig_4_2_rows),
        ("figure_4_3_temperature_vs_time_heating.csv", fig_4_3_rows),
        ("figure_4_4_ambient_temp_humidity_vs_time.csv", env_rows),
        ("figure_4_5_ultrasonic_vs_level.csv", tank_level_rows),
        ("figure_4_6_power_vs_time_per_load.csv", load_rows),
        ("figure_4_7_total_system_power.csv", fig_4_7_rows),
        ("figure_4_8_power_profiles_by_load_type.csv", fig_4_8_rows),
        ("figure_4_9_cumulative_energy_over_time.csv", fig_4_9_rows),
        ("figure_4_10_ema_prediction_vs_actual.csv", fig_4_10_rows),
        ("figure_4_11_rf_prediction_vs_actual.csv", fig_4_11_rows),
        ("figure_4_12_lstm_prediction_vs_actual.csv", fig_4_12_rows),
        ("figure_4_13_load_states_priority_curtailment.csv", fig_4_13_rows),
        ("figure_4_14_total_demand_with_control_response.csv", fig_4_14_rows),
        ("figure_4_15_voltage_drop_with_control_response.csv", fig_4_15_rows),
        ("figure_4_16_per_load_current_protection_response.csv", fig_4_16_rows),
        ("figure_4_18_power_profiles_under_policies.csv", fig_4_18_rows),
        ("figure_4_19_peak_demand_comparison.csv", fig_4_19_rows),
        ("figure_4_20_energy_consumption_comparison.csv", fig_4_20_rows),
        ("figure_4_21_integrated_system_operation_timeline.csv", fig_4_21_rows),
        ("forecast_informed_control_response.csv", forecast_control_rows),
        ("rolling_energy_consumption_comparison.csv", rolling_energy_policy_rows),
        ("before_vs_after_demand_profile.csv", before_after_demand_rows),
        ("prediction_residuals.csv", prediction_residual_rows),
        ("accumulated_cost_over_time.csv", accumulated_cost_rows),
        ("rolling_window_energy_cost.csv", rolling_window_rows),
        ("rolling_energy_consumption.csv", rolling_energy_rows),
        ("rolling_cost_consumption.csv", rolling_cost_rows),
        ("rolling_and_cumulative_energy.csv", rolling_and_cumulative_energy_rows),
        ("rolling_and_cumulative_cost.csv", rolling_and_cumulative_cost_rows),
        ("per_load_rolling_energy.csv", per_load_rolling_rows),
        ("voltage_protection_events.csv", event_rows),
        ("system_timeseries_master.csv", system_rows),
        ("tank_timeseries_master.csv", tank_rows),
        ("load_timeseries_master.csv", load_rows),
        ("prediction_timeseries_master.csv", prediction_rows),
    ]

    written = []
    for filename, rows in exports:
        path = out_dir / filename
        _write_csv(path, rows)
        written.append((filename, path))
    return written


def _build_run_name(plant_id: str, start_ms: int, end_ms: int) -> str:
    start_text = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    end_text = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{plant_id}_{start_text}_{end_text}"


def _copy_model_analytics(run_dir: Path) -> List[Path]:
    written: List[Path] = []
    analytics_dir = run_dir / "model_analytics"
    analytics_dir.mkdir(parents=True, exist_ok=True)
    source_dirs: List[Path] = []
    for raw_dir in (RF_MODEL_DIR_ENV, LSTM_MODEL_DIR_ENV):
        if not raw_dir:
            continue
        candidate = Path(raw_dir).expanduser().resolve()
        if candidate.exists() and candidate not in source_dirs:
            source_dirs.append(candidate)
    root_candidates = [Path(__file__).resolve().parent, Path.cwd().resolve()]
    for root in list(root_candidates):
        for parent in (root.parent, root.parent.parent):
            if parent.exists() and parent not in root_candidates:
                root_candidates.append(parent)
    for model_type, hints in MODEL_SERVICE_DIR_HINTS.items():
        for root in root_candidates:
            for service_name in hints:
                for candidate in (
                    root / service_name / "shared" / "models",
                    root / service_name / "current" / "models",
                    root / service_name / "models",
                ):
                    if candidate.exists() and candidate not in source_dirs:
                        source_dirs.append(candidate)
    if MODEL_DIR.exists() and MODEL_DIR not in source_dirs:
        source_dirs.append(MODEL_DIR)
    copied_names = set()
    for source_dir in source_dirs:
        for source in sorted(source_dir.glob("*.analytics.json")):
            if source.name in copied_names:
                continue
            target = analytics_dir / source.name
            shutil.copy2(source, target)
            copied_names.add(source.name)
            written.append(target)
    return written


def _write_operational_exports(run_dir: Path, data: dict, run_metadata: Optional[Dict[str, object]]) -> List[Path]:
    written: List[Path] = []
    stream_timing_rows = _build_stream_timing_rows(data.get("system_rows") or [], data.get("prediction_rows") or [])
    policy_control_action_rows = _build_policy_control_action_rows(data.get("system_rows") or [])
    control_response_rows = _build_control_response_metrics_rows(data.get("system_rows") or [])
    step_timing_rows = _build_step_timing_rows(run_metadata)
    run_timing_summary_rows = _build_run_timing_summary_rows(run_metadata, step_timing_rows)
    command_latency_rows = _build_command_latency_rows(data.get("command_rows") or [], data.get("command_ack_rows") or [])
    command_latency_summary_rows = _build_command_latency_summary_rows(command_latency_rows)

    csv_exports = [
        ("telemetry_stream_timing.csv", stream_timing_rows),
        ("policy_control_action_comparison.csv", policy_control_action_rows),
        ("control_response_metrics.csv", control_response_rows),
        ("experiment_step_timing.csv", step_timing_rows),
        ("experiment_run_timing_summary.csv", run_timing_summary_rows),
        ("command_ack_latency.csv", command_latency_rows),
        ("command_ack_latency_summary.csv", command_latency_summary_rows),
    ]
    for filename, rows in csv_exports:
        path = run_dir / filename
        _write_csv(path, rows)
        written.append(path)

    notes_path = run_dir / "iot_observability_notes.md"
    notes_path.write_text(
        _build_observability_notes(stream_timing_rows, command_latency_rows, step_timing_rows),
        encoding="utf-8",
    )
    written.append(notes_path)

    scalability_path = run_dir / "scalability_considerations.md"
    scalability_path.write_text(
        _build_scalability_notes(data, stream_timing_rows, data.get("rolling_window_minutes") or ROLLING_WINDOW_MINUTES),
        encoding="utf-8",
    )
    written.append(scalability_path)

    timing_diagram_path = run_dir / "data_flow_timing_diagram.mmd"
    timing_diagram_path.write_text(
        _build_timing_diagram(stream_timing_rows, command_latency_summary_rows),
        encoding="utf-8",
    )
    written.append(timing_diagram_path)

    if isinstance(run_metadata, dict):
        run_metadata_path = run_dir / "experiment_run.json"
        run_metadata_path.write_text(json.dumps(run_metadata, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        written.append(run_metadata_path)

    return written


def export_chapter4_dataset(
    *,
    log_dir,
    out_dir,
    plant_id: str = PLANT_IDENTIFIER,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    prediction_step_sec: int = 5,
    max_lag_sec: int = 600,
    run_metadata: Optional[Dict[str, object]] = None,
) -> dict:
    log_dir_path = Path(log_dir)
    out_dir_path = Path(out_dir)
    start_ms, end_ms = _resolve_range(log_dir_path, plant_id, start_ms, end_ms)
    data = _collect_data(log_dir_path, plant_id, start_ms, end_ms, prediction_step_sec)
    run_dir = out_dir_path / _build_run_name(plant_id, start_ms, end_ms)
    written = _export_chapter4_files(data, run_dir, prediction_step_sec, max_lag_sec)
    auxiliary_written = _write_operational_exports(run_dir, data, run_metadata)
    analytics_written = _copy_model_analytics(run_dir)
    row_counts = {path.name: _csv_row_count(path) for _, path in written}
    for path in auxiliary_written:
        if path.suffix.lower() == ".csv":
            row_counts[path.name] = _csv_row_count(path)

    summary_path = run_dir / "export_summary.txt"
    summary_lines = [
        f"plant_id={plant_id}",
        f"log_dir={log_dir_path}",
        f"from_utc={_iso_utc(start_ms)}",
        f"to_utc={_iso_utc(end_ms)}",
        f"files_scanned={data['files_scanned']}",
        f"records_used={data['records_used']}",
        "",
        "written_files:",
    ]
    summary_lines.extend(f"- {path.name} rows={row_counts.get(path.name, 0)}" for _, path in written)
    if auxiliary_written:
        summary_lines.extend(["", "auxiliary_files:"])
        for path in auxiliary_written:
            if path.suffix.lower() == ".csv":
                summary_lines.append(f"- {path.name} rows={row_counts.get(path.name, 0)}")
            else:
                summary_lines.append(f"- {path.name}")
    if analytics_written:
        summary_lines.extend(["", "model_analytics:"])
        summary_lines.extend(f"- {path.relative_to(run_dir)}" for path in analytics_written)
    empty_files = [name for name, count in row_counts.items() if count == 0]
    if empty_files:
        summary_lines.extend(["", "empty_files:"])
        summary_lines.extend(f"- {name}" for name in empty_files)
    summary_path.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    return {
        "plant_id": plant_id,
        "log_dir": log_dir_path,
        "out_dir": out_dir_path,
        "run_dir": run_dir,
        "from_ms": start_ms,
        "to_ms": end_ms,
        "files_scanned": data["files_scanned"],
        "records_used": data["records_used"],
        "written": written,
        "auxiliary_written": auxiliary_written,
        "analytics_written": analytics_written,
        "row_counts": row_counts,
        "summary_path": summary_path,
    }


def main():
    parser = argparse.ArgumentParser(description="Export Chapter 4 report datasets from telemetry JSONL logs.")
    parser.add_argument("--from", dest="start_time", help="UTC start time (ISO8601, YYYY-MM-DD, or epoch)")
    parser.add_argument("--to", dest="end_time", help="UTC end time (ISO8601, YYYY-MM-DD, or epoch)")
    parser.add_argument("--log-dir", default=LOGGER_OUTPUT_DIR, help="Telemetry log directory")
    parser.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help="Base output directory for exported CSVs")
    parser.add_argument("--plant-id", default=PLANT_IDENTIFIER, help="Plant identifier used in log filenames")
    parser.add_argument("--prediction-step-sec", type=int, default=5, help="Bucketing step for prediction-vs-actual exports")
    parser.add_argument("--max-lag-sec", type=int, default=600, help="Maximum absolute lag window used in model metrics")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    if not log_dir.exists():
        raise SystemExit(f"log directory does not exist: {log_dir}")

    start_ms = _parse_time_arg(args.start_time)
    end_ms = _parse_time_arg(args.end_time)
    try:
        start_ms, end_ms = _resolve_range(log_dir, args.plant_id, start_ms, end_ms)
    except Exception as exc:
        raise SystemExit(str(exc))

    result = export_chapter4_dataset(
        log_dir=log_dir,
        out_dir=args.out_dir,
        plant_id=args.plant_id,
        start_ms=start_ms,
        end_ms=end_ms,
        prediction_step_sec=args.prediction_step_sec,
        max_lag_sec=args.max_lag_sec,
    )

    print(f"[chapter4-export] wrote {len(result['written'])} CSV files to {result['run_dir']}")
    print(f"[chapter4-export] files_scanned={result['files_scanned']} records_used={result['records_used']}")
    print(f"[chapter4-export] summary={result['summary_path']}")


if __name__ == "__main__":
    main()
