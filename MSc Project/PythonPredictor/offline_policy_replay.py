#!/usr/bin/env python3
import copy
import json
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from offline_prediction_replay import prepare_offline_prediction_log_dir


LOAD_DEFAULTS = {
    "motor1": {"class": "CRITICAL", "priority": 1, "supports_duty": True, "rated_power_w": 3.8},
    "motor2": {"class": "CRITICAL", "priority": 1, "supports_duty": True, "rated_power_w": 3.8},
    "heater1": {"class": "ESSENTIAL", "priority": 2, "supports_duty": False, "rated_power_w": 6.8},
    "heater2": {"class": "ESSENTIAL", "priority": 2, "supports_duty": False, "rated_power_w": 6.8},
    "lighting1": {"class": "IMPORTANT", "priority": 3, "supports_duty": True, "rated_power_w": 2.6},
    "lighting2": {"class": "SECONDARY", "priority": 4, "supports_duty": True, "rated_power_w": 1.8},
}
CLASS_RANK = {
    "CRITICAL": 0,
    "ESSENTIAL": 1,
    "IMPORTANT": 2,
    "SECONDARY": 3,
    "NON_ESSENTIAL": 4,
}
POLICY_ORDER = ["NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"]


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        out = float(value)
        if out != out:
            return default
        return out
    except Exception:
        return default


def _safe_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value or "").strip().lower()
    return text in {"1", "true", "yes", "on"}


def _clamp(value: Any, low: float, high: float) -> float:
    v = _safe_float(value, low)
    if v is None:
        v = low
    return max(low, min(high, float(v)))


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


def _extract_logged_topic_payload(record: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    topic = str(record.get("topic") or "")
    payload = record.get("payload")
    if isinstance(payload, dict):
        return topic, payload
    if topic and isinstance(record.get("system"), dict):
        return topic, record
    return topic, None


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


def _iso_utc_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _history_step_ms(log_dir: Path, plant_id: str, start_ms: int, end_ms: int) -> int:
    previous_ts = None
    for _record, _payload, ts_ms in _iter_source_records(log_dir, plant_id, start_ms, end_ms):
        if previous_ts is not None and ts_ms > previous_ts:
            return max(1000, ts_ms - previous_ts)
        previous_ts = ts_ms
    return 60000


def _baseline_peak_power_w(log_dir: Path, plant_id: str, start_ms: int, end_ms: int) -> float:
    peak_power_w = 0.0
    for _record, payload, _ts_ms in _iter_source_records(log_dir, plant_id, start_ms, end_ms):
        system = payload.get("system") or {}
        peak_power_w = max(peak_power_w, max(0.0, _safe_float(system.get("total_power_w"), 0.0) or 0.0))
    return peak_power_w


def _iter_source_records(log_dir: Path, plant_id: str, start_ms: int, end_ms: int):
    telemetry_topic = f"dt/{plant_id}/telemetry"
    for day in _iter_days(start_ms, end_ms):
        path = Path(log_dir) / f"{plant_id}_telemetry_{day.isoformat()}.jsonl"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
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
                yield record, payload, ts_ms


@dataclass
class PolicyReplayState:
    policy: str
    total_energy_wh: float = 0.0
    goal_window_consumed_wh: float = 0.0
    goal_window_start_ms: int = 0
    goal_window_reset_count: int = 0
    goal_window_reset_reason: str = "startup"
    total_control_actions: int = 0
    shedding_event_count: int = 0
    curtail_event_count: int = 0
    restore_event_count: int = 0
    overload_event_count: int = 0
    previous_ts_ms: Optional[int] = None
    previous_total_power_w: Optional[float] = None


@dataclass
class LoadState:
    name: str
    on: bool
    duty: float
    base_on: bool
    base_duty: float
    base_power_w: float
    base_current_a: float
    voltage_v: float
    priority: int
    load_class: str
    supports_duty: bool
    rated_power_w: float


@dataclass
class RiskSnapshot:
    source_label: str
    predicted_power_w: float
    current_goal_ratio: float
    projected_goal_ratio: float
    peak_risk: bool
    energy_budget_risk: bool
    risk_level: str


def _fallback_load_defaults(load_name: str) -> Dict[str, Any]:
    return LOAD_DEFAULTS.get(load_name, {"class": "NON_ESSENTIAL", "priority": 5, "supports_duty": True, "rated_power_w": 1.0})


def _load_states_from_payload(loads_payload: Dict[str, Any]) -> Dict[str, LoadState]:
    states: Dict[str, LoadState] = {}
    for name, raw in (loads_payload or {}).items():
        if not isinstance(raw, dict):
            continue
        defaults = _fallback_load_defaults(name)
        duty = _safe_float(raw.get("duty_applied"), None)
        if duty is None:
            duty = _safe_float(raw.get("duty"), None)
        if duty is None:
            duty = _safe_float(raw.get("duty_end"), 0.0)
        duty = _clamp(duty or 0.0, 0.0, 1.0)
        on = _safe_bool(raw.get("on")) and duty > 0.0
        base_power = max(0.0, _safe_float(raw.get("power_w"), 0.0) or 0.0)
        base_current = max(0.0, _safe_float(raw.get("current_a"), 0.0) or 0.0)
        if base_power <= 0.0 and on:
            base_power = float(defaults["rated_power_w"])
        voltage_v = max(0.1, _safe_float(raw.get("voltage_v"), 24.0) or 24.0)
        if base_current <= 0.0 and base_power > 0.0:
            base_current = base_power / voltage_v
        supports_duty = bool(raw.get("supports_duty")) if raw.get("supports_duty") is not None else bool(defaults["supports_duty"])
        load_class = str(raw.get("class") or defaults["class"] or "NON_ESSENTIAL").strip().upper()
        priority = int(round(_safe_float(raw.get("priority"), defaults["priority"]) or defaults["priority"]))
        states[name] = LoadState(
            name=name,
            on=on,
            duty=duty if on else 0.0,
            base_on=on,
            base_duty=duty if on else 0.0,
            base_power_w=base_power,
            base_current_a=base_current,
            voltage_v=voltage_v,
            priority=priority,
            load_class=load_class,
            supports_duty=supports_duty,
            rated_power_w=float(defaults["rated_power_w"]),
        )
    return states


def _refresh_load_measurements(states: Dict[str, LoadState]) -> Tuple[float, float]:
    total_power_w = 0.0
    total_current_a = 0.0
    for state in states.values():
        if (not state.on) or state.duty <= 0.0:
            power_w = 0.0
            current_a = 0.0
            state.on = False
            state.duty = 0.0
        elif state.supports_duty:
            if state.base_duty > 0.0 and state.base_power_w > 0.0:
                scale = state.duty / max(0.01, state.base_duty)
                power_w = max(0.0, state.base_power_w * scale)
            else:
                power_w = max(0.0, state.rated_power_w * state.duty)
            if state.base_duty > 0.0 and state.base_current_a > 0.0:
                current_a = max(0.0, state.base_current_a * (state.duty / max(0.01, state.base_duty)))
            else:
                current_a = max(0.0, power_w / max(0.1, state.voltage_v))
        else:
            power_w = max(0.0, state.base_power_w or state.rated_power_w)
            current_a = max(0.0, state.base_current_a or (power_w / max(0.1, state.voltage_v)))
            state.duty = 1.0
        total_power_w += power_w
        total_current_a += current_a
    return total_power_w, total_current_a


def _active_curtail_candidates(states: Dict[str, LoadState]) -> List[LoadState]:
    return sorted(
        [state for state in states.values() if state.on and state.duty > 0.0],
        key=lambda state: (
            int(state.priority),
            CLASS_RANK.get(state.load_class, 5),
            1 if state.supports_duty else 0,
            state.name,
        ),
        reverse=True,
    )


def _record_action(runtime: PolicyReplayState, action_type: str) -> None:
    runtime.total_control_actions += 1
    if action_type == "shed":
        runtime.shedding_event_count += 1
    elif action_type == "curtail":
        runtime.curtail_event_count += 1
    elif action_type == "restore":
        runtime.restore_event_count += 1


def _cap_load_duty(state: LoadState, duty_cap: float, runtime: PolicyReplayState) -> bool:
    if (not state.on) or state.duty <= 0.0:
        return False
    if not state.supports_duty:
        return False
    next_duty = _clamp(duty_cap, 0.0, 1.0)
    if next_duty >= state.duty - 1e-6:
        return False
    state.duty = next_duty
    if state.duty <= 0.0:
        state.on = False
    _record_action(runtime, "curtail")
    return True


def _shed_load(state: LoadState, runtime: PolicyReplayState) -> bool:
    if (not state.on) or state.duty <= 0.0:
        return False
    if state.load_class == "CRITICAL":
        return False
    state.on = False
    state.duty = 0.0
    _record_action(runtime, "shed")
    return True


def _apply_rule_overload(
    states: Dict[str, LoadState],
    runtime: PolicyReplayState,
    max_total_power_w: float,
    *,
    target_power_w: Optional[float] = None,
) -> bool:
    changed = False
    target_limit_w = max(0.1, _safe_float(target_power_w, max_total_power_w) or max_total_power_w)
    total_power_w, _ = _refresh_load_measurements(states)
    if total_power_w <= target_limit_w:
        return False
    runtime.overload_event_count += 1
    for state in _active_curtail_candidates(states):
        if total_power_w <= target_limit_w:
            break
        if state.supports_duty and state.load_class != "CRITICAL":
            min_target = 0.35 if state.priority >= 3 else 0.5
            if state.duty > min_target + 1e-6:
                state.duty = max(min_target, state.duty - 0.25)
                _record_action(runtime, "curtail")
                changed = True
        elif state.load_class != "CRITICAL":
            changed = _shed_load(state, runtime) or changed
        total_power_w, _ = _refresh_load_measurements(states)
    return changed


def _apply_rule_risk(states: Dict[str, LoadState], runtime: PolicyReplayState, risk_level: str) -> bool:
    target_duty = 0.35 if str(risk_level).upper() in {"HIGH", "MEDIUM"} else 0.5
    for state in _active_curtail_candidates(states):
        if state.load_class == "CRITICAL":
            continue
        if state.supports_duty and state.duty > target_duty + 1e-6:
            state.duty = target_duty
            _record_action(runtime, "curtail")
            return True
        if str(risk_level).upper() == "HIGH" and _shed_load(state, runtime):
            return True
    return False


def _apply_ai_adjustments(states: Dict[str, LoadState], runtime: PolicyReplayState, risk: RiskSnapshot) -> bool:
    changed = False
    severity = str(risk.risk_level or "LOW").upper()
    caps = []
    if severity in {"HIGH", "MEDIUM"} or risk.energy_budget_risk or risk.peak_risk:
        caps.extend([("lighting2", 0.15 if severity == "HIGH" else 0.30), ("lighting1", 0.50 if severity == "HIGH" else 0.65)])
    for load_name, duty_cap in caps:
        state = states.get(load_name)
        if state is not None:
            changed = _cap_load_duty(state, duty_cap, runtime) or changed
    if severity == "HIGH" or risk.projected_goal_ratio >= 1.03:
        heater2 = states.get("heater2")
        if heater2 is not None:
            changed = _shed_load(heater2, runtime) or changed
    return changed


def _apply_ai_preferred_adjustments(states: Dict[str, LoadState], runtime: PolicyReplayState, risk: RiskSnapshot) -> bool:
    changed = _apply_ai_adjustments(states, runtime, risk)
    severity = str(risk.risk_level or "LOW").upper()
    if severity in {"HIGH", "MEDIUM"} or risk.projected_goal_ratio >= 0.95:
        lighting2 = states.get("lighting2")
        if lighting2 is not None:
            changed = _cap_load_duty(lighting2, 0.10 if severity == "HIGH" else 0.20, runtime) or changed
        lighting1 = states.get("lighting1")
        if lighting1 is not None:
            changed = _cap_load_duty(lighting1, 0.35 if severity == "HIGH" else 0.50, runtime) or changed
    if severity == "HIGH" or risk.projected_goal_ratio >= 1.00:
        heater2 = states.get("heater2")
        if heater2 is not None:
            changed = _shed_load(heater2, runtime) or changed
    if risk.projected_goal_ratio >= 1.08:
        heater1 = states.get("heater1")
        if heater1 is not None:
            changed = _shed_load(heater1, runtime) or changed
    return changed


def _select_prediction_source(system: Dict[str, Any]) -> RiskSnapshot:
    sources = (system or {}).get("prediction_sources") or {}
    candidates = [
        ("LONG_RF", sources.get("long_rf") or {}),
        ("LONG_LSTM", sources.get("long_lstm") or {}),
        ("SHORT", sources.get("short") or {}),
    ]
    chosen_label = "NONE"
    chosen = {}
    for label, payload in candidates:
        if isinstance(payload, dict) and _safe_float(payload.get("predicted_power_w"), None) is not None:
            chosen_label = label
            chosen = payload
            break
    if not chosen:
        for label, payload in candidates:
            if isinstance(payload, dict) and _safe_bool(payload.get("model_ready")):
                chosen_label = label
                chosen = payload
                break
    predicted_power_w = _safe_float(chosen.get("predicted_power_w"), 0.0) or 0.0
    current_goal_ratio = _safe_float(chosen.get("current_energy_goal_ratio"), 0.0) or 0.0
    projected_goal_ratio = _safe_float(chosen.get("projected_energy_goal_ratio"), current_goal_ratio) or 0.0
    peak_risk = _safe_bool(chosen.get("peak_risk"))
    energy_budget_risk = _safe_bool(chosen.get("energy_budget_risk"))
    risk_level = str(chosen.get("risk_level") or "LOW").strip().upper() or "LOW"
    return RiskSnapshot(
        source_label=chosen_label,
        predicted_power_w=predicted_power_w,
        current_goal_ratio=current_goal_ratio,
        projected_goal_ratio=projected_goal_ratio,
        peak_risk=peak_risk,
        energy_budget_risk=energy_budget_risk,
        risk_level=risk_level,
    )


def _ensure_goal_window(runtime: PolicyReplayState, ts_ms: int, duration_sec: float) -> None:
    if runtime.goal_window_start_ms <= 0:
        runtime.goal_window_start_ms = ts_ms
        return
    duration_ms = max(1000, int(max(1.0, duration_sec) * 1000.0))
    while ts_ms - runtime.goal_window_start_ms >= duration_ms:
        runtime.goal_window_start_ms += duration_ms
        runtime.goal_window_consumed_wh = 0.0
        runtime.goal_window_reset_count += 1
        runtime.goal_window_reset_reason = "elapsed"


def _goal_window_status(runtime: PolicyReplayState, ts_ms: int, goal_wh: float, duration_sec: float) -> Dict[str, float]:
    duration_ms = max(1000, int(max(1.0, duration_sec) * 1000.0))
    elapsed_ms = max(0, ts_ms - runtime.goal_window_start_ms)
    remaining_ms = max(0, duration_ms - elapsed_ms)
    ratio_used = (runtime.goal_window_consumed_wh / goal_wh) if goal_wh > 0 else 0.0
    return {
        "elapsed_sec": elapsed_ms / 1000.0,
        "remaining_sec": remaining_ms / 1000.0,
        "ratio_used": ratio_used,
        "remaining_ratio": max(0.0, 1.0 - ratio_used) if goal_wh > 0 else 0.0,
        "remaining_wh": max(0.0, goal_wh - runtime.goal_window_consumed_wh),
    }


def _apply_policy_to_record(
    payload: Dict[str, Any],
    runtime: PolicyReplayState,
    policy: str,
    shifted_ts_ms: int,
) -> Dict[str, Any]:
    payload = copy.deepcopy(payload)
    system = payload.setdefault("system", {}) if isinstance(payload, dict) else {}
    energy = payload.setdefault("energy", {}) if isinstance(payload, dict) else {}
    process = payload.setdefault("process", {}) if isinstance(payload, dict) else {}
    evaluation = payload.setdefault("evaluation", {}) if isinstance(payload, dict) else {}
    loads_payload = payload.setdefault("loads", {}) if isinstance(payload, dict) else {}
    prediction_context = payload.setdefault("prediction_context", {}) if isinstance(payload, dict) else {}

    max_total_power_w = max(0.1, _safe_float(system.get("MAX_TOTAL_POWER_W"), 0.0) or 0.0)
    goal_wh = max(
        0.0,
        _safe_float(system.get("ENERGY_GOAL_VALUE_WH"), None)
        or _safe_float((energy.get("budget") or {}).get("window_budget_wh"), 0.0)
        or 0.0,
    )
    goal_duration_sec = max(
        60.0,
        _safe_float(system.get("ENERGY_GOAL_DURATION_SEC"), None)
        or _safe_float((energy.get("budget") or {}).get("window_duration_sec"), 8 * 3600.0)
        or (8 * 3600.0),
    )
    _ensure_goal_window(runtime, shifted_ts_ms, goal_duration_sec)

    states = _load_states_from_payload(loads_payload)
    total_power_w, total_current_a = _refresh_load_measurements(states)

    risk = _select_prediction_source(system)
    current_goal_ratio = (runtime.goal_window_consumed_wh / goal_wh) if goal_wh > 0 else 0.0
    projected_goal_ratio = max(risk.projected_goal_ratio, current_goal_ratio)
    energy_budget_risk = risk.energy_budget_risk or (current_goal_ratio >= 0.95) or (projected_goal_ratio >= 1.0)
    peak_risk = risk.peak_risk or (risk.predicted_power_w >= max_total_power_w * 0.98)
    risk_level = str(risk.risk_level or "LOW").upper()
    if projected_goal_ratio >= 1.02 or risk.predicted_power_w >= max_total_power_w * 1.02:
        risk_level = "HIGH"
    elif risk_level == "LOW" and (peak_risk or current_goal_ratio >= 0.95 or projected_goal_ratio >= 0.95):
        risk_level = "MEDIUM"
    risk = RiskSnapshot(
        source_label=risk.source_label,
        predicted_power_w=risk.predicted_power_w,
        current_goal_ratio=current_goal_ratio,
        projected_goal_ratio=projected_goal_ratio,
        peak_risk=peak_risk,
        energy_budget_risk=energy_budget_risk,
        risk_level=risk_level,
    )

    if policy != "NO_ENERGY_MANAGEMENT":
        _apply_rule_overload(states, runtime, max_total_power_w)
        total_power_w, total_current_a = _refresh_load_measurements(states)
        effective_risk = risk.peak_risk or risk.energy_budget_risk
        rule_trigger = effective_risk or risk.risk_level in {"MEDIUM", "HIGH"}
        hybrid_target_w = max_total_power_w * (0.96 if (risk.energy_budget_risk or risk.projected_goal_ratio >= 0.98) else 1.0)
        ai_target_w = max_total_power_w * (0.88 if (risk.energy_budget_risk or risk.projected_goal_ratio >= 1.0) else 0.94)
        if policy == "RULE_ONLY":
            if rule_trigger:
                _apply_rule_risk(states, runtime, risk.risk_level)
                total_power_w, total_current_a = _refresh_load_measurements(states)
            if total_power_w > max_total_power_w:
                _apply_rule_overload(states, runtime, max_total_power_w)
                total_power_w, total_current_a = _refresh_load_measurements(states)
        elif policy == "HYBRID":
            ai_soft_optimized = False
            if risk.energy_budget_risk or risk.projected_goal_ratio >= 0.95 or risk.risk_level in {"MEDIUM", "HIGH"}:
                ai_soft_optimized = _apply_ai_adjustments(states, runtime, risk)
                total_power_w, total_current_a = _refresh_load_measurements(states)
            if risk.peak_risk or total_power_w > hybrid_target_w or (risk.risk_level == "HIGH" and not ai_soft_optimized):
                _apply_rule_risk(states, runtime, risk.risk_level)
                total_power_w, total_current_a = _refresh_load_measurements(states)
            if total_power_w > hybrid_target_w:
                _apply_rule_overload(states, runtime, max_total_power_w, target_power_w=hybrid_target_w)
                total_power_w, total_current_a = _refresh_load_measurements(states)
        elif policy == "AI_PREFERRED":
            if effective_risk or risk.projected_goal_ratio >= 0.92:
                ai_applied = _apply_ai_preferred_adjustments(states, runtime, risk)
                total_power_w, total_current_a = _refresh_load_measurements(states)
                if total_power_w > ai_target_w:
                    _apply_rule_overload(states, runtime, max_total_power_w, target_power_w=ai_target_w)
                    total_power_w, total_current_a = _refresh_load_measurements(states)
                elif (not ai_applied) and risk.risk_level == "HIGH":
                    _apply_rule_risk(states, runtime, risk.risk_level)
                    total_power_w, total_current_a = _refresh_load_measurements(states)

    if runtime.previous_ts_ms is not None and shifted_ts_ms > runtime.previous_ts_ms:
        dt_hours = (shifted_ts_ms - runtime.previous_ts_ms) / 3600000.0
        reference_power_w = total_power_w
        if runtime.previous_total_power_w is not None:
            reference_power_w = (runtime.previous_total_power_w + total_power_w) * 0.5
        delta_wh = max(0.0, reference_power_w * dt_hours)
    else:
        delta_wh = 0.0
    runtime.total_energy_wh += delta_wh
    runtime.goal_window_consumed_wh += delta_wh
    runtime.previous_ts_ms = shifted_ts_ms
    runtime.previous_total_power_w = total_power_w

    goal_window = _goal_window_status(runtime, shifted_ts_ms, goal_wh, goal_duration_sec)

    payload["timestamp"] = _iso_utc_from_ms(shifted_ts_ms)
    system["timestamp"] = shifted_ts_ms / 1000.0
    system["control_policy"] = policy
    system["total_power_w"] = round(total_power_w, 4)
    system["total_current_a"] = round(total_current_a, 4)
    system["ai_dynamic_process"] = policy in {"HYBRID", "AI_PREFERRED"}
    system["ai_source_active"] = risk.source_label if policy in {"HYBRID", "AI_PREFERRED"} else "NONE"
    system["ai_dynamic_process_source"] = risk.source_label if (policy == "AI_PREFERRED" or (policy == "HYBRID" and (risk.peak_risk or risk.energy_budget_risk))) else "NONE"
    system["ENERGY_GOAL_VALUE_WH"] = round(goal_wh, 4)
    system["ENERGY_GOAL_DURATION_SEC"] = int(goal_duration_sec)
    system["ENERGY_GOAL_WINDOW_ELAPSED_SEC"] = round(goal_window["elapsed_sec"], 4)
    system["ENERGY_GOAL_WINDOW_REMAINING_SEC"] = round(goal_window["remaining_sec"], 4)
    system["ENERGY_GOAL_WINDOW_RATIO_USED"] = round(goal_window["ratio_used"], 6)
    system["ENERGY_GOAL_WINDOW_REMAINING_RATIO"] = round(goal_window["remaining_ratio"], 6)
    system["predictor_peak_risk"] = bool(risk.peak_risk)
    system["predictor_risk_level"] = risk.risk_level

    prediction_context["peak_risk"] = bool(risk.peak_risk)
    prediction_context["risk_level"] = risk.risk_level
    prediction_context["predicted_power_w"] = round(risk.predicted_power_w, 4)
    prediction_context["ai_suggestion_count"] = runtime.curtail_event_count + runtime.shedding_event_count

    energy["total_energy_wh"] = round(runtime.total_energy_wh, 4)
    budget = energy.setdefault("budget", {}) if isinstance(energy, dict) else {}
    budget["window_budget_wh"] = round(goal_wh, 4)
    budget["window_duration_sec"] = int(goal_duration_sec)
    budget["consumed_window_wh"] = round(runtime.goal_window_consumed_wh, 4)
    budget["remaining_window_wh"] = round(goal_window["remaining_wh"], 4)
    budget["ratio_used"] = round(goal_window["ratio_used"], 6)
    budget["remaining_ratio"] = round(goal_window["remaining_ratio"], 6)
    budget["elapsed_sec"] = round(goal_window["elapsed_sec"], 4)
    budget["remaining_sec"] = round(goal_window["remaining_sec"], 4)
    budget["window_started_utc"] = _iso_utc_from_ms(runtime.goal_window_start_ms)
    budget["window_ends_utc"] = _iso_utc_from_ms(runtime.goal_window_start_ms + int(goal_duration_sec * 1000.0))
    budget["reset_count"] = runtime.goal_window_reset_count
    budget["reset_reason"] = runtime.goal_window_reset_reason

    control_eval = evaluation.setdefault("control", {}) if isinstance(evaluation, dict) else {}
    control_eval["total_control_actions"] = runtime.total_control_actions
    control_eval["shedding_event_count"] = runtime.shedding_event_count
    control_eval["curtail_event_count"] = runtime.curtail_event_count
    control_eval["restore_event_count"] = runtime.restore_event_count
    control_eval["control_actions_per_min"] = 0.0
    stability_eval = evaluation.setdefault("stability", {}) if isinstance(evaluation, dict) else {}
    stability_eval["load_toggle_count"] = runtime.total_control_actions
    stability_eval["overshoot_event_count"] = runtime.overload_event_count

    for name, state in states.items():
        load = loads_payload.setdefault(name, {}) if isinstance(loads_payload, dict) else {}
        power_w = 0.0
        current_a = 0.0
        if state.on and state.duty > 0.0:
            if state.supports_duty:
                if state.base_duty > 0.0 and state.base_power_w > 0.0:
                    power_w = max(0.0, state.base_power_w * (state.duty / max(0.01, state.base_duty)))
                else:
                    power_w = max(0.0, state.rated_power_w * state.duty)
                if state.base_duty > 0.0 and state.base_current_a > 0.0:
                    current_a = max(0.0, state.base_current_a * (state.duty / max(0.01, state.base_duty)))
                else:
                    current_a = max(0.0, power_w / max(0.1, state.voltage_v))
            else:
                power_w = max(0.0, state.base_power_w or state.rated_power_w)
                current_a = max(0.0, state.base_current_a or (power_w / max(0.1, state.voltage_v)))
        load["on"] = bool(state.on)
        load["duty"] = round(state.duty if state.on else 0.0, 4)
        load["duty_applied"] = round(state.duty if state.on else 0.0, 4)
        load["power_w"] = round(power_w, 4)
        load["current_a"] = round(current_a, 4)
        load["voltage_v"] = round(state.voltage_v, 4)
        load["priority"] = state.priority
        load["class"] = state.load_class

    process["enabled"] = _safe_bool(process.get("enabled"))
    process["state"] = process.get("state") or ("IDLE" if not _safe_bool(process.get("enabled")) else "PROCESS")
    return payload


def _write_record(output_dir: Path, plant_id: str, record: Dict[str, Any], payload: Dict[str, Any], ts_ms: int, file_handles: Dict[date, Any]) -> None:
    day = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).date()
    handle = file_handles.get(day)
    if handle is None:
        path = output_dir / f"{plant_id}_telemetry_{day.isoformat()}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a", encoding="utf-8")
        file_handles[day] = handle
    if isinstance(record.get("payload"), dict):
        next_record = copy.deepcopy(record)
        next_record["payload"] = payload
        next_record["logged_at_utc"] = _iso_utc_from_ms(ts_ms)
    else:
        next_record = payload
    handle.write(json.dumps(next_record, sort_keys=True) + "\n")


def prepare_offline_policy_log_dir(
    *,
    source_log_dir: Path,
    output_dir: Path,
    plant_id: str,
    start_ms: int,
    end_ms: int,
    policies: Optional[List[str]] = None,
    max_total_power_w: Optional[float] = None,
    energy_goal_wh: Optional[float] = None,
    rf_model_dir: Optional[Path] = None,
    lstm_model_dir: Optional[Path] = None,
    progress_callback=None,
) -> Dict[str, Any]:
    selected_policies = [str(item).strip().upper() for item in (policies or POLICY_ORDER) if str(item).strip()]
    selected_policies = [item for item in POLICY_ORDER if item in selected_policies] + [item for item in selected_policies if item not in POLICY_ORDER]
    if not selected_policies:
        selected_policies = list(POLICY_ORDER)

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    source_log_dir = Path(source_log_dir).resolve()

    with tempfile.TemporaryDirectory(prefix="offline_policy_prediction_") as temp_dir:
        predicted_dir = Path(temp_dir) / "predicted"
        def _emit_progress(phase: str, ratio: float, **extra: Any) -> None:
            if progress_callback is None:
                return
            payload = {
                "phase": phase,
                "progress_ratio": max(0.0, min(1.0, float(ratio))),
                "selected_policies": list(selected_policies),
            }
            payload.update(extra)
            try:
                progress_callback(payload)
            except Exception:
                pass

        prediction_summary = prepare_offline_prediction_log_dir(
            source_log_dir=source_log_dir,
            output_dir=predicted_dir,
            plant_id=plant_id,
            start_ms=start_ms,
            end_ms=end_ms,
            rf_model_dir=rf_model_dir,
            lstm_model_dir=lstm_model_dir,
            progress_callback=lambda update: _emit_progress(
                "prediction_replay",
                0.05 + (float(update.get("progress_ratio") or 0.0) * 0.45),
                message=str(update.get("message") or "Replaying predictions"),
                prediction_progress=update,
            ),
        )
        baseline_peak_power_w = _baseline_peak_power_w(predicted_dir, plant_id, start_ms, end_ms)
        resolved_max_total_power_w = _safe_float(max_total_power_w, None)
        if resolved_max_total_power_w is None and baseline_peak_power_w > 0.0:
            resolved_max_total_power_w = round(baseline_peak_power_w * 0.85, 4)
        resolved_energy_goal_wh = _safe_float(energy_goal_wh, None)

        sample_step_ms = _history_step_ms(predicted_dir, plant_id, start_ms, end_ms)
        source_span_ms = max(sample_step_ms, end_ms - start_ms + sample_step_ms)
        file_handles: Dict[date, Any] = {}
        policy_summaries: List[Dict[str, Any]] = []
        replay_start_ms: Optional[int] = None
        replay_end_ms: Optional[int] = None

        try:
            for policy_index, policy in enumerate(selected_policies):
                runtime = PolicyReplayState(policy=policy)
                segment_offset_ms = policy_index * source_span_ms
                segment_records = 0
                segment_peak_power_w = 0.0
                segment_start_ms: Optional[int] = None
                segment_end_ms: Optional[int] = None
                total_segments = max(1, len(selected_policies))
                last_reported_records = 0
                for record, payload, ts_ms in _iter_source_records(predicted_dir, plant_id, start_ms, end_ms):
                    shifted_ts_ms = start_ms + segment_offset_ms + (ts_ms - start_ms)
                    if segment_start_ms is None:
                        segment_start_ms = shifted_ts_ms
                        runtime.goal_window_start_ms = shifted_ts_ms
                    segment_end_ms = shifted_ts_ms
                    if resolved_max_total_power_w is not None:
                        system = payload.setdefault("system", {}) if isinstance(payload, dict) else {}
                        system["MAX_TOTAL_POWER_W"] = float(resolved_max_total_power_w)
                    if resolved_energy_goal_wh is not None:
                        system = payload.setdefault("system", {}) if isinstance(payload, dict) else {}
                        energy = payload.setdefault("energy", {}) if isinstance(payload, dict) else {}
                        budget = energy.setdefault("budget", {}) if isinstance(energy, dict) else {}
                        system["ENERGY_GOAL_VALUE_WH"] = float(resolved_energy_goal_wh)
                        budget["window_budget_wh"] = float(resolved_energy_goal_wh)
                    next_payload = _apply_policy_to_record(payload, runtime, policy, shifted_ts_ms)
                    _write_record(output_dir, plant_id, record, next_payload, shifted_ts_ms, file_handles)
                    segment_records += 1
                    total_power_w = _safe_float((next_payload.get("system") or {}).get("total_power_w"), 0.0) or 0.0
                    segment_peak_power_w = max(segment_peak_power_w, total_power_w)
                    replay_start_ms = shifted_ts_ms if replay_start_ms is None else min(replay_start_ms, shifted_ts_ms)
                    replay_end_ms = shifted_ts_ms if replay_end_ms is None else max(replay_end_ms, shifted_ts_ms)
                    if segment_records == 1 or segment_records - last_reported_records >= 250:
                        segment_ratio = min(1.0, segment_records / max(1, prediction_summary.get("telemetry_records") or 1))
                        overall_ratio = 0.55 + (((policy_index + segment_ratio) / total_segments) * 0.35)
                        _emit_progress(
                            "policy_replay",
                            overall_ratio,
                            current_policy=policy,
                            policy_index=policy_index + 1,
                            policy_count=total_segments,
                            message=f"Applying {policy} policy to replayed telemetry",
                            segment_records=segment_records,
                        )
                        last_reported_records = segment_records
                policy_summaries.append(
                    {
                        "control_policy": policy,
                        "record_count": segment_records,
                        "start_ms": segment_start_ms,
                        "end_ms": segment_end_ms,
                        "peak_power_w": round(segment_peak_power_w, 4),
                        "energy_wh": round(runtime.total_energy_wh, 4),
                        "total_control_actions": runtime.total_control_actions,
                        "shed_count": runtime.shedding_event_count,
                        "curtail_count": runtime.curtail_event_count,
                        "restore_count": runtime.restore_event_count,
                        "overload_count": runtime.overload_event_count,
                    }
                )
                _emit_progress(
                    "policy_segment_complete",
                    0.55 + (((policy_index + 1) / max(1, len(selected_policies))) * 0.35),
                    current_policy=policy,
                    policy_index=policy_index + 1,
                    policy_count=max(1, len(selected_policies)),
                    message=f"Completed replay for {policy}",
                    segment_records=segment_records,
                )
        finally:
            for handle in file_handles.values():
                try:
                    handle.close()
                except Exception:
                    pass

    if progress_callback is not None:
        try:
            progress_callback({
                "phase": "completed",
                "progress_ratio": 1.0,
                "message": "Offline policy replay complete",
                "selected_policies": list(selected_policies),
            })
        except Exception:
            pass

    return {
        "source_log_dir": str(source_log_dir),
        "output_dir": str(output_dir),
        "start_ms": int(start_ms),
        "end_ms": int(end_ms),
        "replay_start_ms": int(replay_start_ms if replay_start_ms is not None else start_ms),
        "replay_end_ms": int(replay_end_ms if replay_end_ms is not None else end_ms),
        "policies": selected_policies,
        "baseline_peak_power_w": round(baseline_peak_power_w, 4),
        "resolved_max_total_power_w": resolved_max_total_power_w,
        "resolved_energy_goal_wh": resolved_energy_goal_wh,
        "policy_summaries": policy_summaries,
        "prediction_replay": prediction_summary,
    }
