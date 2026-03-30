#!/usr/bin/env python3
import argparse
import csv
import json
import math
import os
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import mean, pstdev
from typing import Dict, Iterable, List, Optional, Tuple
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
tariff_config_path = Path(TARIFF_CONFIG_PATH)


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


def _attach_cost_fields(system_rows: List[dict], tariff_store: dict) -> List[dict]:
    if not system_rows:
        return system_rows

    previous_ts = None
    previous_energy_wh = None
    accumulated_energy_cost = 0.0
    running_max_demand_kw = 0.0
    interval_buckets: Dict[Tuple[int, int], float] = {}

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
        "pred_short_total_energy_wh": None,
        "pred_long_rf_power_w": None,
        "pred_long_rf_total_energy_wh": None,
        "pred_long_lstm_power_w": None,
        "pred_long_lstm_total_energy_wh": None,
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


def _compute_model_metrics(rows: List[dict], pred_col: str, step_sec: int, max_lag_sec: int) -> dict:
    pairs = [(row["actual_power_w"], row[pred_col]) for row in rows if row["actual_power_w"] is not None and row[pred_col] is not None]
    sample_count = len(pairs)
    if not pairs:
        return {
            "sample_count": 0,
            "mae_w": None,
            "rmse_w": None,
            "lag_sec": None,
            "lag_eval_samples": 0,
        }

    errors = [abs(actual - pred) for actual, pred in pairs]
    squared = [(actual - pred) ** 2 for actual, pred in pairs]
    lag_sec, lag_samples = _best_lag_sec(
        [row["actual_power_w"] for row in rows],
        [row[pred_col] for row in rows],
        step_sec,
        max_lag_sec,
    )
    return {
        "sample_count": sample_count,
        "mae_w": sum(errors) / sample_count,
        "rmse_w": math.sqrt(sum(squared) / sample_count),
        "lag_sec": lag_sec,
        "lag_eval_samples": lag_samples,
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


def _collect_data(log_dir: Path, plant_id: str, start_ms: int, end_ms: int, prediction_step_sec: int) -> dict:
    system_rows: List[dict] = []
    env_rows: List[dict] = []
    tank_rows: List[dict] = []
    load_rows: List[dict] = []
    fig_4_15_rows: List[dict] = []
    prediction_rows: Dict[int, dict] = {}
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
                topic = str(record.get("topic") or "")
                payload = record.get("payload")
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
                        "rf_fresh": _safe_bool(pred_long_rf.get("fresh")),
                        "rf_applied": _safe_bool(pred_long_rf.get("applied")),
                        "rf_predicted_power_w": _safe_float(pred_long_rf.get("predicted_power_w")),
                        "lstm_fresh": _safe_bool(pred_long_lstm.get("fresh")),
                        "lstm_applied": _safe_bool(pred_long_lstm.get("applied")),
                        "lstm_predicted_power_w": _safe_float(pred_long_lstm.get("predicted_power_w")),
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
                    pred_energy = _safe_float(payload.get("predicted_total_energy_wh"))
                    if pred_energy is None:
                        pred_energy = _safe_float(payload.get("predicted_energy_wh"))
                    if topic == TOPIC_PREDICTION_NAME:
                        pred_row["pred_short_power_w"] = pred_power
                        pred_row["pred_short_total_energy_wh"] = pred_energy
                    elif topic == TOPIC_PREDICTION_LONG_NAME:
                        pred_row["pred_long_rf_power_w"] = pred_power
                        pred_row["pred_long_rf_total_energy_wh"] = pred_energy
                    else:
                        pred_row["pred_long_lstm_power_w"] = pred_power
                        pred_row["pred_long_lstm_total_energy_wh"] = pred_energy

    system_rows.sort(key=lambda row: row["ts"])
    env_rows.sort(key=lambda row: row["ts"])
    tank_rows.sort(key=lambda row: row["ts"])
    load_rows.sort(key=lambda row: (row["load_name"], row["ts"]))
    fig_4_15_rows.sort(key=lambda row: row["ts"])
    prediction_row_list = [prediction_rows[key] for key in sorted(prediction_rows.keys())]

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

    tariff_store = _load_tariff_store()
    _attach_cost_fields(system_rows, tariff_store)

    return {
        "system_rows": system_rows,
        "env_rows": env_rows,
        "tank_rows": tank_rows,
        "load_rows": load_rows,
        "fig_4_15_rows": fig_4_15_rows,
        "prediction_rows": prediction_row_list,
        "files_scanned": used_files,
        "records_used": used_records,
        "tariff_store": tariff_store,
    }


def _export_chapter4_files(data: dict, out_dir: Path, prediction_step_sec: int, max_lag_sec: int) -> List[Tuple[str, Path]]:
    system_rows = data["system_rows"]
    env_rows = data["env_rows"]
    tank_rows = data["tank_rows"]
    load_rows = data["load_rows"]
    fig_4_15_rows = data["fig_4_15_rows"]
    prediction_rows = data["prediction_rows"]
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
    table_4_2_rows = _build_supply_voltage_summary(system_rows, event_rows)

    table_4_3_rows = []
    for label, pred_col in (
        ("EMA", "pred_short_power_w"),
        ("RF", "pred_long_rf_power_w"),
        ("LSTM", "pred_long_lstm_power_w"),
    ):
        metrics = _compute_model_metrics(prediction_rows, pred_col, prediction_step_sec, max_lag_sec)
        table_4_3_rows.append(
            {
                "model": label,
                "target": "total_power_w",
                "sample_count": metrics["sample_count"],
                "mae_w": metrics["mae_w"],
                "rmse_w": metrics["rmse_w"],
                "lag_sec": metrics["lag_sec"],
                "lag_eval_samples": metrics["lag_eval_samples"],
            }
        )

    table_16_rows = _build_policy_summary_rows(system_rows)

    fig_4_10_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "actual_power_w": row.get("actual_power_w"),
            "predicted_power_w": row.get("pred_short_power_w"),
            "predicted_total_energy_wh": row.get("pred_short_total_energy_wh"),
        }
        for row in prediction_rows
    ]
    fig_4_11_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "actual_power_w": row.get("actual_power_w"),
            "predicted_power_w": row.get("pred_long_rf_power_w"),
            "predicted_total_energy_wh": row.get("pred_long_rf_total_energy_wh"),
        }
        for row in prediction_rows
    ]
    fig_4_12_rows = [
        {
            "ts": row["ts"],
            "label_utc": row["label_utc"],
            "actual_power_w": row.get("actual_power_w"),
            "predicted_power_w": row.get("pred_long_lstm_power_w"),
            "predicted_total_energy_wh": row.get("pred_long_lstm_total_energy_wh"),
        }
        for row in prediction_rows
    ]

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
        ("table_16_policy_performance_comparison.csv", table_16_rows),
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
        ("accumulated_cost_over_time.csv", accumulated_cost_rows),
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


def export_chapter4_dataset(
    *,
    log_dir,
    out_dir,
    plant_id: str = PLANT_IDENTIFIER,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    prediction_step_sec: int = 5,
    max_lag_sec: int = 600,
) -> dict:
    log_dir_path = Path(log_dir)
    out_dir_path = Path(out_dir)
    start_ms, end_ms = _resolve_range(log_dir_path, plant_id, start_ms, end_ms)
    data = _collect_data(log_dir_path, plant_id, start_ms, end_ms, prediction_step_sec)
    run_dir = out_dir_path / _build_run_name(plant_id, start_ms, end_ms)
    written = _export_chapter4_files(data, run_dir, prediction_step_sec, max_lag_sec)
    row_counts = {path.name: _csv_row_count(path) for _, path in written}

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
