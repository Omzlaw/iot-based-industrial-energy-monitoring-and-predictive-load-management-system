#!/usr/bin/env python3
import argparse
import json
import math
import random
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


LOAD_SPECS = {
    "motor1": {"class": "CRITICAL", "priority": 1, "rated_power_w": 3.8, "pf": 0.92, "supports_duty": True},
    "motor2": {"class": "CRITICAL", "priority": 1, "rated_power_w": 3.8, "pf": 0.92, "supports_duty": True},
    "heater1": {"class": "ESSENTIAL", "priority": 2, "rated_power_w": 6.8, "pf": 1.0, "supports_duty": False},
    "heater2": {"class": "ESSENTIAL", "priority": 2, "rated_power_w": 6.8, "pf": 1.0, "supports_duty": False},
    "lighting1": {"class": "IMPORTANT", "priority": 3, "rated_power_w": 2.6, "pf": 0.98, "supports_duty": True},
    "lighting2": {"class": "SECONDARY", "priority": 4, "rated_power_w": 1.8, "pf": 0.98, "supports_duty": True},
}

POLICY_ROTATION = ["NO_ENERGY_MANAGEMENT"]
POLICY_PROFILES = {
    "NO_ENERGY_MANAGEMENT": {"max_total_power_w": 18.5, "energy_goal_wh": 95.0, "energy_goal_duration_min": 480},
    "RULE_ONLY": {"max_total_power_w": 12.5, "energy_goal_wh": 82.0, "energy_goal_duration_min": 480},
    "HYBRID": {"max_total_power_w": 11.5, "energy_goal_wh": 76.0, "energy_goal_duration_min": 480},
    "AI_PREFERRED": {"max_total_power_w": 10.8, "energy_goal_wh": 72.0, "energy_goal_duration_min": 480},
}

OFFPEAK_RATE = 0.12
MIDPEAK_RATE = 0.18
PEAK_RATE = 0.28


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def reset_goal_window(runtime: "PlantRuntime", *, start_ts: float, reason: str, goal_wh: float, duration_sec: float, power_cap_w: float, increment_counter: bool) -> None:
    runtime.goal_window_started_ts = float(start_ts)
    runtime.goal_window_consumed_wh = 0.0
    runtime.per_load_goal_window_wh = {name: 0.0 for name in LOAD_SPECS}
    runtime.goal_window_reset_reason = str(reason)
    runtime.goal_window_config_wh = float(goal_wh)
    runtime.goal_window_config_duration_sec = float(duration_sec)
    runtime.goal_window_config_power_cap_w = float(power_cap_w)
    if increment_counter:
        runtime.goal_window_reset_count += 1


def average(values: List[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def cycle_fraction(sample_ts: float, period_sec: float, phase_offset: float = 0.0) -> float:
    if period_sec <= 0.0:
        return 0.0
    return ((float(sample_ts) / float(period_sec)) + float(phase_offset)) % 1.0


def cycle_wave(sample_ts: float, period_sec: float, phase_offset: float = 0.0) -> float:
    if period_sec <= 0.0:
        return 0.5
    return 0.5 + (0.5 * math.sin((((float(sample_ts) / float(period_sec)) + float(phase_offset)) * math.tau)))


def thermostatic_on_state(previous_on: bool, current_temp_c: float, target_temp_c: float, *, engage_delta_c: float, disengage_delta_c: float) -> bool:
    if current_temp_c <= (target_temp_c - engage_delta_c):
        return True
    if current_temp_c >= (target_temp_c - disengage_delta_c):
        return False
    return bool(previous_on)


def internal_simulation_step_sec(step_sec: int) -> int:
    if step_sec <= 5:
        return step_sec
    if step_sec % 5 == 0:
        return 5
    return 1


def local_zone(name: str):
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(name)
    except Exception:
        return timezone.utc


def tariff_state_and_rate(local_dt: datetime) -> Tuple[str, float]:
    minute_of_day = local_dt.hour * 60 + local_dt.minute
    if local_dt.weekday() >= 5 or minute_of_day < 7 * 60:
        return "OFFPEAK", OFFPEAK_RATE
    if 16 * 60 <= minute_of_day < 20 * 60:
        return "PEAK", PEAK_RATE
    return "MIDPEAK", MIDPEAK_RATE


def shift_schedule(local_dt: datetime) -> Dict[str, object]:
    minute = local_dt.hour * 60 + local_dt.minute
    weekday = local_dt.weekday()
    weekend = weekday >= 5

    if weekend:
        if 8 * 60 <= minute < 9 * 60:
            return {"shift_type": "Day", "production_mode": "Setup", "process_enabled": False, "utilization_pct": 28.0}
        if 9 * 60 <= minute < 14 * 60:
            return {"shift_type": "Day", "production_mode": "Production", "process_enabled": True, "utilization_pct": 58.0}
        if 14 * 60 <= minute < 16 * 60:
            return {"shift_type": "Day", "production_mode": "Maintenance", "process_enabled": False, "utilization_pct": 18.0}
        return {"shift_type": "Night" if minute < 6 * 60 or minute >= 18 * 60 else "Day", "production_mode": "Idle", "process_enabled": False, "utilization_pct": 10.0}

    if 6 * 60 <= minute < 6 * 60 + 20:
        return {"shift_type": "Day", "production_mode": "Setup", "process_enabled": False, "utilization_pct": 42.0}
    if 6 * 60 + 20 <= minute < 10 * 60:
        return {"shift_type": "Day", "production_mode": "Production", "process_enabled": True, "utilization_pct": 78.0}
    if 10 * 60 <= minute < 10 * 60 + 15:
        return {"shift_type": "Day", "production_mode": "Idle", "process_enabled": False, "utilization_pct": 16.0}
    if 10 * 60 + 15 <= minute < 13 * 60:
        return {"shift_type": "Day", "production_mode": "Production", "process_enabled": True, "utilization_pct": 82.0}
    if 13 * 60 <= minute < 13 * 60 + 30:
        return {"shift_type": "Day", "production_mode": "Idle", "process_enabled": False, "utilization_pct": 14.0}
    if 13 * 60 + 30 <= minute < 17 * 60 + 45:
        return {"shift_type": "Day", "production_mode": "Production", "process_enabled": True, "utilization_pct": 84.0}
    if 17 * 60 + 45 <= minute < 18 * 60:
        return {"shift_type": "Day", "production_mode": "Setup", "process_enabled": False, "utilization_pct": 24.0}
    if 18 * 60 <= minute < 18 * 60 + 15:
        return {"shift_type": "Night", "production_mode": "Setup", "process_enabled": False, "utilization_pct": 34.0}
    if 18 * 60 + 15 <= minute < 22 * 60:
        return {"shift_type": "Night", "production_mode": "Production", "process_enabled": True, "utilization_pct": 70.0}
    if 22 * 60 <= minute < 22 * 60 + 15:
        return {"shift_type": "Night", "production_mode": "Idle", "process_enabled": False, "utilization_pct": 12.0}
    if minute >= 22 * 60 + 15 or minute < 60:
        return {"shift_type": "Night", "production_mode": "Production", "process_enabled": True, "utilization_pct": 74.0}
    if 60 <= minute < 90:
        return {"shift_type": "Night", "production_mode": "Maintenance", "process_enabled": False, "utilization_pct": 18.0}
    if 90 <= minute < 5 * 60 + 45:
        return {"shift_type": "Night", "production_mode": "Production", "process_enabled": True, "utilization_pct": 72.0}
    return {"shift_type": "Night", "production_mode": "Setup", "process_enabled": False, "utilization_pct": 18.0}


def cosine_window_strength(minute_of_day: float, center_minute: float, half_width_min: float) -> float:
    if half_width_min <= 0.0:
        return 0.0
    direct = abs(float(minute_of_day) - float(center_minute))
    distance = min(direct, 1440.0 - direct)
    if distance >= half_width_min:
        return 0.0
    return 0.5 + (0.5 * math.cos(math.pi * (distance / half_width_min)))


def day_dynamic_profile(day_index: int, seed: int) -> Dict[str, float]:
    rng = random.Random(((int(seed) + 1) * 1_000_003) + ((int(day_index) + 11) * 97_087))
    return {
        "ambient_offset_c": rng.uniform(-2.4, 2.8),
        "humidity_offset_pct": rng.uniform(-6.0, 6.5),
        "utilization_scale": rng.uniform(0.84, 1.18),
        "utilization_bias_pct": rng.uniform(-8.5, 9.0),
        "transfer_rate_scale": rng.uniform(0.86, 1.18),
        "heat_rate_scale": rng.uniform(0.88, 1.14),
        "cool_ratio_scale": rng.uniform(0.88, 1.16),
        "target_temp_bias_c": rng.uniform(-1.6, 2.1),
        "level_bias_pct": rng.uniform(-2.0, 2.0),
        "goal_cycle_bias": float(rng.choice([-1, 0, 0, 1])),
        "lighting_bias": rng.uniform(-0.10, 0.12),
        "motor_scale_bias": rng.uniform(-0.08, 0.11),
        "heater_scale_bias": rng.uniform(-0.06, 0.09),
        "lighting_scale_bias": rng.uniform(-0.10, 0.08),
        "surge_center_min": float(rng.randint(7 * 60, 21 * 60)),
        "surge_half_width_min": float(rng.randint(25, 95)),
        "surge_gain": rng.uniform(0.08, 0.28),
        "slump_center_min": float(rng.randint(8 * 60, 22 * 60)),
        "slump_half_width_min": float(rng.randint(35, 120)),
        "slump_depth": rng.uniform(0.10, 0.34),
        "micro_stop_period_sec": float(rng.randint(1400, 5200)),
        "micro_stop_phase": rng.random(),
        "micro_stop_depth": rng.uniform(0.08, 0.24),
        "campaign_period_days": rng.uniform(3.5, 8.5),
        "campaign_phase": rng.random(),
        "motor_wave_period_sec": float(rng.randint(420, 2100)),
        "motor_wave_phase": rng.random(),
        "heater_wave_period_sec": float(rng.randint(600, 2400)),
        "heater_wave_phase": rng.random(),
        "lighting_wave_period_sec": float(rng.randint(540, 1800)),
        "lighting_wave_phase": rng.random(),
        "background_load_bias_w": rng.uniform(-0.12, 0.48),
        "background_wave_period_sec": float(rng.randint(900, 3600)),
        "background_wave_phase": rng.random(),
    }


def intraday_activity_profile(local_dt: datetime, sample_ts: float, day_profile: Dict[str, float]) -> Dict[str, float]:
    minute_of_day = float(local_dt.hour * 60 + local_dt.minute) + (float(local_dt.second) / 60.0)
    surge_strength = cosine_window_strength(minute_of_day, float(day_profile["surge_center_min"]), float(day_profile["surge_half_width_min"]))
    slump_strength = cosine_window_strength(minute_of_day, float(day_profile["slump_center_min"]), float(day_profile["slump_half_width_min"]))
    micro_wave = cycle_wave(sample_ts, float(day_profile["micro_stop_period_sec"]), float(day_profile["micro_stop_phase"]))
    micro_stop_strength = clamp((micro_wave - 0.82) / 0.18, 0.0, 1.0)
    day_position = float(local_dt.toordinal()) + (minute_of_day / 1440.0)
    campaign_wave = math.sin(((day_position / max(float(day_profile["campaign_period_days"]), 1.0)) + float(day_profile["campaign_phase"])) * math.tau)
    activity_multiplier = clamp(
        1.0
        + (float(day_profile["surge_gain"]) * surge_strength)
        - (float(day_profile["slump_depth"]) * slump_strength)
        - (float(day_profile["micro_stop_depth"]) * micro_stop_strength)
        + (0.10 * campaign_wave),
        0.46,
        1.45,
    )
    lighting_multiplier = clamp(1.0 + (0.18 * surge_strength) - (0.12 * slump_strength) + (0.05 * campaign_wave), 0.70, 1.30)
    process_multiplier = clamp(1.0 + (0.14 * surge_strength) - (0.08 * slump_strength) - (0.10 * micro_stop_strength), 0.76, 1.24)
    background_multiplier = clamp(1.0 + (0.20 * surge_strength) + (0.10 * slump_strength) + (0.06 * campaign_wave), 0.78, 1.42)
    return {
        "surge_strength": surge_strength,
        "slump_strength": slump_strength,
        "micro_stop_strength": micro_stop_strength,
        "campaign_wave": campaign_wave,
        "activity_multiplier": activity_multiplier,
        "lighting_multiplier": lighting_multiplier,
        "process_multiplier": process_multiplier,
        "background_multiplier": background_multiplier,
    }


@dataclass
class PlantRuntime:
    total_energy_wh: float = 0.0
    tank1_temp_c: float = 28.0
    tank2_temp_c: float = 27.5
    tank1_level_pct: float = 78.0
    tank2_level_pct: float = 34.0
    process_state: str = "IDLE"
    process_enabled: bool = False
    cycle_count: int = 0
    goal_cycles: int = 10
    process_elapsed_sec: float = 0.0
    process_start_ts: float = 0.0
    last_duration_sec: float = 0.0
    avg_duration_sec: float = 0.0
    duration_count: int = 0
    last_process_end_ts: float = 0.0
    shed_events: int = 0
    curtail_events: int = 0
    restore_events: int = 0
    overload_events: int = 0
    load_toggle_count: int = 0
    total_control_actions: int = 0
    max_overshoot_w: float = 0.0
    last_load_state: Dict[str, Tuple[bool, float]] = field(default_factory=dict)
    energy_window: deque = field(default_factory=deque)
    goal_window_started_ts: float = 0.0
    goal_window_consumed_wh: float = 0.0
    goal_window_reset_count: int = 0
    goal_window_reset_reason: str = "startup"
    goal_window_config_wh: float = 0.0
    goal_window_config_duration_sec: float = 0.0
    goal_window_config_power_cap_w: float = 0.0
    per_load_goal_window_wh: Dict[str, float] = field(default_factory=lambda: {name: 0.0 for name in LOAD_SPECS})
    manual_override_until: float = 0.0


def process_phase_loads(
    runtime: PlantRuntime,
    *,
    sample_ts: float,
    utilization_pct: float,
    target_t1: float,
    target_t2: float,
) -> Dict[str, Dict[str, float]]:
    loads = {name: {"on": False, "duty": 0.0} for name in LOAD_SPECS}
    if not runtime.process_enabled:
        return loads

    if runtime.process_state == "HEAT_TANK1":
        previous_on = bool(runtime.last_load_state.get("heater1", (False, 0.0))[0])
        heater_on = thermostatic_on_state(
            previous_on,
            runtime.tank1_temp_c,
            target_t1,
            engage_delta_c=1.25,
            disengage_delta_c=0.18,
        )
        loads["heater1"] = {"on": heater_on, "duty": 1.0 if heater_on else 0.0}
    elif runtime.process_state == "TRANSFER_1_TO_2":
        base_duty = clamp(0.58 + (utilization_pct / 190.0), 0.56, 0.98)
        pulse = 0.84 + (0.22 * cycle_wave(sample_ts, 55.0, 0.11))
        pause_gate = 0.0 if cycle_fraction(sample_ts, 210.0, 0.17) >= 0.93 else 1.0
        duty = clamp(base_duty * pulse * pause_gate, 0.0, 1.0)
        if duty > 0.0:
            duty = clamp(duty, 0.42, 1.0)
        loads["motor1"] = {"on": duty > 0.0, "duty": duty}
    elif runtime.process_state == "HEAT_TANK2":
        previous_on = bool(runtime.last_load_state.get("heater2", (False, 0.0))[0])
        heater_on = thermostatic_on_state(
            previous_on,
            runtime.tank2_temp_c,
            target_t2,
            engage_delta_c=1.15,
            disengage_delta_c=0.16,
        )
        loads["heater2"] = {"on": heater_on, "duty": 1.0 if heater_on else 0.0}
    elif runtime.process_state == "TRANSFER_2_TO_1":
        base_duty = clamp(0.56 + (utilization_pct / 195.0), 0.54, 0.96)
        pulse = 0.82 + (0.24 * cycle_wave(sample_ts, 48.0, 0.63))
        pause_gate = 0.0 if cycle_fraction(sample_ts, 235.0, 0.41) >= 0.94 else 1.0
        duty = clamp(base_duty * pulse * pause_gate, 0.0, 1.0)
        if duty > 0.0:
            duty = clamp(duty, 0.40, 1.0)
        loads["motor2"] = {"on": duty > 0.0, "duty": duty}
    return loads


def update_process(
    runtime: PlantRuntime,
    *,
    process_enabled: bool,
    sample_ts: float,
    step_sec: int,
    ambient_temp_c: float,
    shift_type: str,
    utilization_pct: float,
    process_profile: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    process_profile = process_profile or {}
    level_bias_pct = float(process_profile.get("level_bias_pct", 0.0))
    low_level = clamp(22.0 + level_bias_pct, 18.0, 28.0)
    high_level = clamp(82.0 + level_bias_pct, 76.0, 86.0)
    height_cm = clamp(50.0 + (0.4 * level_bias_pct), 46.0, 54.0)
    transfer_rate_pct_per_sec = (0.16 + (utilization_pct / 1000.0)) * float(process_profile.get("transfer_rate_scale", 1.0))
    heat_rate_c_per_sec = (0.026 + (0.006 if shift_type == "Night" else 0.0)) * float(process_profile.get("heat_rate_scale", 1.0))
    cool_ratio = 0.0016 * float(process_profile.get("cool_ratio_scale", 1.0))

    goal_cycle_bias = int(round(float(process_profile.get("goal_cycle_bias", 0.0))))
    if shift_type == "Day":
        runtime.goal_cycles = int(clamp(12 + goal_cycle_bias, 8, 15))
    elif shift_type == "Night":
        runtime.goal_cycles = int(clamp(10 + goal_cycle_bias, 7, 14))

    target_bias_c = float(process_profile.get("target_temp_bias_c", 0.0))
    target_t1 = min(60.0, 45.0 + runtime.cycle_count + target_bias_c)
    target_t2 = min(62.0, 47.0 + runtime.cycle_count + target_bias_c)

    if process_enabled and not runtime.process_enabled:
        runtime.process_enabled = True
        runtime.process_state = "HEAT_TANK1"
        runtime.process_start_ts = sample_ts
        runtime.process_elapsed_sec = 0.0
    elif not process_enabled and runtime.process_enabled:
        runtime.process_enabled = False
        runtime.process_state = "IDLE"
        runtime.process_elapsed_sec = 0.0
        runtime.last_process_end_ts = sample_ts
        runtime.cycle_count = 0

    phase_loads = process_phase_loads(
        runtime,
        sample_ts=sample_ts,
        utilization_pct=utilization_pct,
        target_t1=target_t1,
        target_t2=target_t2,
    )

    if runtime.process_enabled:
        runtime.process_elapsed_sec = max(0.0, sample_ts - runtime.process_start_ts)
        if runtime.process_state == "HEAT_TANK1":
            if phase_loads["heater1"]["on"]:
                runtime.tank1_temp_c += heat_rate_c_per_sec * step_sec
            if runtime.tank1_temp_c >= target_t1 - 0.25:
                runtime.process_state = "TRANSFER_1_TO_2"
                runtime.process_elapsed_sec = 0.0
                runtime.process_start_ts = sample_ts
        elif runtime.process_state == "TRANSFER_1_TO_2":
            motor_duty = float(phase_loads["motor1"]["duty"])
            moved = 0.0
            if motor_duty > 0.0:
                moved = transfer_rate_pct_per_sec * step_sec * clamp(0.35 + (0.75 * motor_duty), 0.0, 1.10)
            runtime.tank1_level_pct = max(low_level, runtime.tank1_level_pct - moved)
            runtime.tank2_level_pct = min(high_level, runtime.tank2_level_pct + moved)
            if runtime.tank1_level_pct <= low_level + 0.35 or runtime.tank2_level_pct >= high_level - 0.35:
                runtime.process_state = "HEAT_TANK2"
                runtime.process_elapsed_sec = 0.0
                runtime.process_start_ts = sample_ts
        elif runtime.process_state == "HEAT_TANK2":
            if phase_loads["heater2"]["on"]:
                runtime.tank2_temp_c += heat_rate_c_per_sec * step_sec
            if runtime.tank2_temp_c >= target_t2 - 0.25:
                runtime.process_state = "TRANSFER_2_TO_1"
                runtime.process_elapsed_sec = 0.0
                runtime.process_start_ts = sample_ts
        elif runtime.process_state == "TRANSFER_2_TO_1":
            motor_duty = float(phase_loads["motor2"]["duty"])
            moved = 0.0
            if motor_duty > 0.0:
                moved = transfer_rate_pct_per_sec * step_sec * clamp(0.35 + (0.75 * motor_duty), 0.0, 1.10)
            runtime.tank2_level_pct = max(low_level, runtime.tank2_level_pct - moved)
            runtime.tank1_level_pct = min(high_level, runtime.tank1_level_pct + moved)
            if runtime.tank2_level_pct <= low_level + 0.35 or runtime.tank1_level_pct >= high_level - 0.35:
                runtime.process_state = "HEAT_TANK1"
                runtime.cycle_count += 1
                duration = max(0.0, sample_ts - runtime.process_start_ts)
                runtime.last_duration_sec = duration
                runtime.duration_count += 1
                if runtime.duration_count == 1:
                    runtime.avg_duration_sec = duration
                else:
                    runtime.avg_duration_sec += (duration - runtime.avg_duration_sec) / runtime.duration_count
                runtime.process_elapsed_sec = 0.0
                runtime.process_start_ts = sample_ts
    else:
        runtime.tank1_level_pct = clamp(runtime.tank1_level_pct, low_level, high_level)
        runtime.tank2_level_pct = clamp(runtime.tank2_level_pct, low_level, high_level)

    runtime.tank1_temp_c += (ambient_temp_c - runtime.tank1_temp_c) * cool_ratio * step_sec
    runtime.tank2_temp_c += (ambient_temp_c - runtime.tank2_temp_c) * cool_ratio * step_sec

    return {
        "target_t1": target_t1,
        "target_t2": target_t2,
        "height_cm": height_cm,
        "low_level_pct": low_level,
        "high_level_pct": high_level,
        "phase_loads": phase_loads,
    }


def lighting_demand(
    name: str,
    production_mode: str,
    shift_type: str,
    tariff_state: str,
    sample_ts: float,
    dynamic_profile: Optional[Dict[str, float]] = None,
) -> Dict[str, float]:
    dynamic_profile = dynamic_profile or {}
    lighting_bias = float(dynamic_profile.get("lighting_bias", 0.0))
    lighting_multiplier = float(dynamic_profile.get("lighting_multiplier", 1.0))
    if name == "lighting1":
        if production_mode == "Production":
            base = 0.92
        elif production_mode == "Setup":
            base = 0.74
        elif production_mode == "Maintenance":
            base = 0.56
        else:
            base = 0.18 if shift_type == "Day" else 0.12
        occupancy = 0.92 + (0.10 * cycle_wave(sample_ts, 900.0, 0.19))
        if production_mode == "Idle":
            occupancy *= 1.0 if cycle_fraction(sample_ts, 1800.0, 0.07) < 0.16 else 0.52
    else:
        if production_mode == "Production":
            base = 0.78
        elif production_mode == "Setup":
            base = 0.42
        elif production_mode == "Maintenance":
            base = 0.24
        else:
            base = 0.05 if shift_type == "Day" else 0.03
        occupancy = 0.88 + (0.16 * cycle_wave(sample_ts, 720.0, 0.61))
        if production_mode == "Idle":
            occupancy *= 1.0 if cycle_fraction(sample_ts, 2400.0, 0.31) < 0.08 else 0.35
    if shift_type == "Night":
        base += 0.06 if name == "lighting1" else 0.04
    if tariff_state == "PEAK" and name == "lighting2":
        base *= 0.72
    base = clamp(base + (lighting_bias * (1.0 if name == "lighting1" else 0.85)), 0.0, 1.0)
    duty = clamp(base * occupancy * lighting_multiplier, 0.0, 1.0)
    return {"on": duty > 0.05, "duty": duty if duty > 0.05 else 0.0}


def support_activity_loads(
    runtime: PlantRuntime,
    production_mode: str,
    shift_type: str,
    utilization_pct: float,
    sample_ts: float,
) -> Dict[str, Dict[str, float]]:
    loads = {name: {"on": False, "duty": 0.0} for name in LOAD_SPECS}
    if runtime.process_enabled:
        return loads

    support_scale = clamp(0.22 + (utilization_pct / 185.0), 0.18, 0.62)

    if production_mode == "Setup":
        standby_t1 = 36.0 + (1.0 if shift_type == "Night" else 0.0)
        standby_t2 = 38.0 + (1.0 if shift_type == "Night" else 0.0)
        heater1_previous = bool(runtime.last_load_state.get("heater1", (False, 0.0))[0])
        heater2_previous = bool(runtime.last_load_state.get("heater2", (False, 0.0))[0])
        heater1_on = thermostatic_on_state(
            heater1_previous,
            runtime.tank1_temp_c,
            standby_t1,
            engage_delta_c=1.9,
            disengage_delta_c=0.35,
        )
        heater2_on = thermostatic_on_state(
            heater2_previous,
            runtime.tank2_temp_c,
            standby_t2,
            engage_delta_c=1.9,
            disengage_delta_c=0.35,
        )
        if heater1_on:
            loads["heater1"] = {"on": True, "duty": 1.0}
        if heater2_on:
            loads["heater2"] = {"on": True, "duty": 1.0}

        priming_phase = cycle_fraction(sample_ts, 1800.0, 0.13)
        priming_duty = clamp(
            0.24 + (0.20 * cycle_wave(sample_ts, 240.0, 0.09)) + (0.22 * support_scale),
            0.24,
            0.55,
        )
        if priming_phase < 0.14:
            loads["motor1"] = {"on": True, "duty": priming_duty}
        elif priming_phase < 0.28:
            loads["motor2"] = {"on": True, "duty": priming_duty}

    elif production_mode == "Maintenance":
        standby_t1 = 31.5 + (0.5 if shift_type == "Night" else 0.0)
        standby_t2 = 32.5 + (0.5 if shift_type == "Night" else 0.0)
        heater1_previous = bool(runtime.last_load_state.get("heater1", (False, 0.0))[0])
        heater2_previous = bool(runtime.last_load_state.get("heater2", (False, 0.0))[0])
        heater1_on = thermostatic_on_state(
            heater1_previous,
            runtime.tank1_temp_c,
            standby_t1,
            engage_delta_c=2.2,
            disengage_delta_c=0.45,
        )
        heater2_on = thermostatic_on_state(
            heater2_previous,
            runtime.tank2_temp_c,
            standby_t2,
            engage_delta_c=2.2,
            disengage_delta_c=0.45,
        )
        heater_gate_1 = cycle_fraction(sample_ts, 3600.0, 0.17) < 0.20
        heater_gate_2 = cycle_fraction(sample_ts, 3600.0, 0.61) < 0.20
        if heater1_on and heater_gate_1:
            loads["heater1"] = {"on": True, "duty": 1.0}
        if heater2_on and heater_gate_2:
            loads["heater2"] = {"on": True, "duty": 1.0}

        maintenance_phase = cycle_fraction(sample_ts, 2700.0, 0.31)
        maintenance_duty = clamp(
            0.22 + (0.16 * cycle_wave(sample_ts, 210.0, 0.48)) + (0.16 * support_scale),
            0.20,
            0.44,
        )
        if maintenance_phase < 0.10:
            loads["motor1"] = {"on": True, "duty": maintenance_duty}
        elif maintenance_phase < 0.20:
            loads["motor2"] = {"on": True, "duty": maintenance_duty}

    elif production_mode == "Idle" and shift_type == "Night":
        patrol_phase = cycle_fraction(sample_ts, 5400.0, 0.44)
        patrol_duty = clamp(0.16 + (0.08 * cycle_wave(sample_ts, 300.0, 0.49)), 0.14, 0.28)
        if patrol_phase < 0.03:
            loads["motor2"] = {"on": True, "duty": patrol_duty}

    return loads


def desired_load_state(
    runtime: PlantRuntime,
    production_mode: str,
    shift_type: str,
    utilization_pct: float,
    tariff_state: str,
    sample_ts: float,
    process_loads: Optional[Dict[str, Dict[str, float]]] = None,
    dynamic_profile: Optional[Dict[str, float]] = None,
) -> Dict[str, Dict[str, float]]:
    loads = {
        name: {"on": False, "duty": 0.0}
        for name in LOAD_SPECS
    }

    if production_mode in {"Setup", "Production", "Maintenance"}:
        loads["lighting1"] = lighting_demand("lighting1", production_mode, shift_type, tariff_state, sample_ts, dynamic_profile=dynamic_profile)
    if production_mode in {"Setup", "Production", "Maintenance", "Idle"}:
        loads["lighting2"] = lighting_demand("lighting2", production_mode, shift_type, tariff_state, sample_ts, dynamic_profile=dynamic_profile)

    for name, state in support_activity_loads(runtime, production_mode, shift_type, utilization_pct, sample_ts).items():
        duty = clamp(float(state.get("duty", 0.0) or 0.0), 0.0, 1.0)
        if bool(state.get("on", False)) or duty > 0.0:
            loads[name] = {"on": bool(state.get("on", False)), "duty": duty}

    for name, state in (process_loads or {}).items():
        duty = clamp(float(state.get("duty", 0.0) or 0.0), 0.0, 1.0)
        if bool(state.get("on", False)) or duty > 0.0:
            loads[name] = {"on": bool(state.get("on", False)), "duty": duty}

    return loads


def apply_dynamic_power_scaling(
    load_powers: Dict[str, float],
    *,
    sample_ts: float,
    day_profile: Dict[str, float],
    activity_profile: Dict[str, float],
) -> Dict[str, float]:
    scaled: Dict[str, float] = {}
    for name, power in load_powers.items():
        if power <= 0.0:
            scaled[name] = 0.0
            continue
        if name.startswith("motor"):
            wave = (2.0 * cycle_wave(sample_ts, float(day_profile["motor_wave_period_sec"]), float(day_profile["motor_wave_phase"]) + (0.18 if name.endswith("2") else 0.0))) - 1.0
            scale = 1.0 + float(day_profile["motor_scale_bias"]) + (0.11 * wave) + (0.10 * (float(activity_profile["activity_multiplier"]) - 1.0))
            lower, upper = 0.74, 1.28
        elif name.startswith("heater"):
            wave = (2.0 * cycle_wave(sample_ts, float(day_profile["heater_wave_period_sec"]), float(day_profile["heater_wave_phase"]) + (0.21 if name.endswith("2") else 0.0))) - 1.0
            scale = 1.0 + float(day_profile["heater_scale_bias"]) + (0.08 * wave) + (0.07 * (float(activity_profile["process_multiplier"]) - 1.0))
            lower, upper = 0.78, 1.24
        else:
            wave = (2.0 * cycle_wave(sample_ts, float(day_profile["lighting_wave_period_sec"]), float(day_profile["lighting_wave_phase"]) + (0.27 if name.endswith("2") else 0.0))) - 1.0
            scale = 1.0 + float(day_profile["lighting_scale_bias"]) + (0.06 * wave) + (0.09 * (float(activity_profile["lighting_multiplier"]) - 1.0))
            lower, upper = 0.70, 1.18
        scaled[name] = max(0.0, power * clamp(scale, lower, upper))
    return scaled


def facility_base_power_w(
    *,
    sample_ts: float,
    ambient_temp_c: float,
    production_mode: str,
    tariff_state: str,
    day_profile: Dict[str, float],
    activity_profile: Dict[str, float],
) -> float:
    if production_mode == "Production":
        base = 0.90
    elif production_mode == "Setup":
        base = 0.58
    elif production_mode == "Maintenance":
        base = 0.66
    else:
        base = 0.24
    hvac = max(0.0, ambient_temp_c - 22.0) * 0.16
    compressor_wave = cycle_wave(sample_ts, float(day_profile["background_wave_period_sec"]), float(day_profile["background_wave_phase"]))
    compressor = 0.18 + (0.42 * compressor_wave)
    ventilation = 0.12 + (0.18 * float(activity_profile["background_multiplier"]))
    tariff_trim = 0.92 if tariff_state == "PEAK" and production_mode != "Production" else 1.0
    power = (base + hvac + compressor + ventilation + float(day_profile["background_load_bias_w"])) * tariff_trim
    return clamp(power, 0.10, 3.20)


def estimate_load_powers(loads: Dict[str, Dict[str, float]], ambient_temp_c: float, rng: random.Random) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for name, state in loads.items():
        spec = LOAD_SPECS[name]
        duty = clamp(float(state.get("duty", 0.0) or 0.0), 0.0, 1.0)
        on = bool(state.get("on", False)) and duty > 0.0
        rated = float(spec["rated_power_w"])
        if not on:
            out[name] = 0.0
            continue
        if name.startswith("heater"):
            temp_factor = clamp(1.0 - max(0.0, ambient_temp_c - 24.0) * 0.012, 0.86, 1.03)
            power = rated * temp_factor
        elif name.startswith("motor"):
            load_factor = clamp(0.36 + (0.64 * duty), 0.24, 1.04)
            power = rated * load_factor
        else:
            power = rated * clamp(0.18 + (0.82 * duty), 0.0, 1.0)
        out[name] = max(0.0, power + rng.uniform(-0.08, 0.08))
    return out


def apply_policy_management(
    runtime: PlantRuntime,
    desired: Dict[str, Dict[str, float]],
    powers: Dict[str, float],
    *,
    policy: str,
    max_total_power_w: float,
    tariff_state: str,
    process_state: str,
    manual_override: bool,
) -> Tuple[Dict[str, Dict[str, float]], Dict[str, float]]:
    managed = {name: dict(values) for name, values in desired.items()}
    events = {"shed": 0, "curtail": 0, "restore": 0, "overload": 0, "actions": 0}

    def current_total() -> float:
        return sum(estimate_load_powers(managed, 24.0, random.Random(0)).values())

    if manual_override:
        return managed, events

    if policy in {"HYBRID", "AI_PREFERRED"} and tariff_state == "PEAK":
        for device, duty_cap in (("lighting2", 0.30), ("lighting1", 0.65)):
            if managed[device]["on"] and managed[device]["duty"] > duty_cap:
                managed[device]["duty"] = duty_cap
                events["curtail"] += 1
                events["actions"] += 1

    if policy == "AI_PREFERRED" and process_state in {"HEAT_TANK1", "HEAT_TANK2"}:
        for device, duty_cap in (("lighting2", 0.15), ("lighting1", 0.50)):
            if managed[device]["on"] and managed[device]["duty"] > duty_cap:
                managed[device]["duty"] = duty_cap
                events["curtail"] += 1
                events["actions"] += 1

    total_power = sum(powers.values())
    if policy == "NO_ENERGY_MANAGEMENT":
        return managed, events

    if total_power > max_total_power_w:
        events["overload"] += 1
        shed_order = ["lighting2", "lighting1", "motor2", "motor1"]
        for device in shed_order:
            if total_power <= max_total_power_w:
                break
            if not managed[device]["on"]:
                continue
            before = managed[device]["duty"]
            if device.startswith("lighting"):
                managed[device]["duty"] = max(0.0, before - 0.35)
                managed[device]["on"] = managed[device]["duty"] > 0.05
            else:
                managed[device]["duty"] = max(0.0, before - 0.20)
                managed[device]["on"] = managed[device]["duty"] > 0.10
            if not managed[device]["on"] and before > 0.0:
                events["shed"] += 1
            elif managed[device]["duty"] < before:
                events["curtail"] += 1
            events["actions"] += 1
            total_power = current_total()
    return managed, events


def build_record(
    plant_id: str,
    dt_utc: datetime,
    step_sec: int,
    runtime: PlantRuntime,
    schedule: Dict[str, object],
    policy: str,
    max_total_power_w: float,
    energy_goal_wh: float,
    energy_goal_duration_min: float,
    ambient_temp_c: float,
    humidity_pct: float,
    tariff_state: str,
    tariff_rate_per_kwh: float,
    loads: Dict[str, Dict[str, float]],
    load_powers: Dict[str, float],
    process_meta: Dict[str, float],
    total_power_w: float,
    total_current_a: float,
    supply_v: float,
    budget_wh: float,
    rng: random.Random,
    sample_count: int = 1,
    bucket_start_dt: Optional[datetime] = None,
    bucket_end_dt: Optional[datetime] = None,
    aggregate_metrics: Optional[Dict[str, float]] = None,
) -> Dict[str, object]:
    bucket_start_dt = bucket_start_dt or dt_utc
    bucket_end_dt = bucket_end_dt or (bucket_start_dt + timedelta(seconds=step_sec))
    aggregate_metrics = aggregate_metrics or {}

    avg_total_power_w = float(aggregate_metrics.get("total_power_w", total_power_w))
    avg_total_current_a = float(aggregate_metrics.get("total_current_a", total_current_a))
    avg_supply_v = float(aggregate_metrics.get("supply_v", supply_v))
    avg_ambient_temp_c = float(aggregate_metrics.get("ambient_temp_c", ambient_temp_c))
    avg_humidity_pct = float(aggregate_metrics.get("humidity_pct", humidity_pct))
    avg_tank1_level_pct = float(aggregate_metrics.get("tank1_level_pct", runtime.tank1_level_pct))
    avg_tank2_level_pct = float(aggregate_metrics.get("tank2_level_pct", runtime.tank2_level_pct))
    avg_tank1_temp_c = float(aggregate_metrics.get("tank1_temp_c", runtime.tank1_temp_c))
    avg_tank2_temp_c = float(aggregate_metrics.get("tank2_temp_c", runtime.tank2_temp_c))
    load_payload = {}
    for name, state in loads.items():
        spec = LOAD_SPECS[name]
        power_w = float(aggregate_metrics.get(f"load_{name}_power_w_avg", load_powers.get(name, 0.0)) or 0.0)
        current_a = float(aggregate_metrics.get(f"load_{name}_current_a_avg", (power_w / max(avg_supply_v, 1.0))) or 0.0)
        on_ratio = float(aggregate_metrics.get(f"load_{name}_on_ratio", 1.0 if bool(state["on"]) else 0.0) or 0.0)
        duty_applied_avg = float(aggregate_metrics.get(f"load_{name}_duty_applied_avg", state["duty"]) or 0.0)
        distance_cm = process_meta["height_cm"] * (1.0 - (avg_tank1_level_pct / 100.0))
        if name == "heater2":
            distance_cm = process_meta["height_cm"] * (1.0 - (avg_tank2_level_pct / 100.0))
        load_payload[name] = {
            "on": bool(state["on"]),
            "duty_end": round(float(state["duty"]), 4),
            "duty": round(float(duty_applied_avg), 4),
            "duty_applied": round(float(duty_applied_avg), 4),
            "duty_applied_avg": round(float(duty_applied_avg), 4),
            "on_ratio": round(float(on_ratio), 4),
            "class": spec["class"],
            "priority": spec["priority"],
            "power_w": round(power_w, 4),
            "current_a": round(current_a, 4),
            "energy_goal_window_wh": round(float(runtime.per_load_goal_window_wh.get(name, 0.0)), 4),
            "voltage_v": round(avg_supply_v, 4),
            "temperature_c": round(avg_ambient_temp_c + rng.uniform(1.0, 5.0), 3),
            "fault_active": False,
            "fault_latched": False,
            "fault_code": "NONE",
            "fault_limit_a": 2.0,
            "inject_current_a": 0.0,
            "override": False,
        }

    tank1_distance = float(aggregate_metrics.get("tank1_ultrasonic_distance_cm", process_meta["height_cm"] * (1.0 - (avg_tank1_level_pct / 100.0))))
    tank2_distance = float(aggregate_metrics.get("tank2_ultrasonic_distance_cm", process_meta["height_cm"] * (1.0 - (avg_tank2_level_pct / 100.0))))
    tank1_raw_cm = float(aggregate_metrics.get("tank1_ultrasonic_raw_cm", tank1_distance + rng.uniform(-0.6, 0.6)))
    tank2_raw_cm = float(aggregate_metrics.get("tank2_ultrasonic_raw_cm", tank2_distance + rng.uniform(-0.6, 0.6)))
    balance_delta_level_pct = float(aggregate_metrics.get("balance_delta_level_pct", abs(avg_tank1_level_pct - avg_tank2_level_pct)))
    goal_window_duration_sec = float(energy_goal_duration_min) * 60.0
    goal_window_start_dt = datetime.fromtimestamp(runtime.goal_window_started_ts or bucket_end_dt.timestamp(), tz=timezone.utc)
    goal_window_end_dt = goal_window_start_dt + timedelta(seconds=goal_window_duration_sec)
    goal_window_remaining_sec = max(0.0, (goal_window_end_dt - bucket_end_dt).total_seconds())
    goal_window_remaining_ratio = clamp(goal_window_remaining_sec / max(goal_window_duration_sec, 1.0), 0.0, 1.0)

    return {
        "schema_version": "1.0",
        "topic": f"dt/{plant_id}/telemetry",
        "aggregation_sec": step_sec,
        "sample_count": sample_count,
        "bucket_start_utc": iso_utc(bucket_start_dt),
        "bucket_end_utc": iso_utc(bucket_end_dt),
        "timestamp": iso_utc(bucket_end_dt),
        "system": {
            "timestamp": iso_utc(bucket_end_dt),
            "plant_id": plant_id,
            "mode": "PROCESS" if runtime.process_enabled else "IDLE",
            "control_policy": policy,
            "manual_override": runtime.manual_override_until > bucket_end_dt.timestamp(),
            "tariff_state": tariff_state,
            "peak_event": tariff_state == "PEAK" or avg_total_power_w >= (max_total_power_w * 0.92),
            "supply_v": round(avg_supply_v, 4),
            "supply_v_raw": round(avg_supply_v + rng.uniform(-0.03, 0.03), 4),
            "total_power_w": round(avg_total_power_w, 4),
            "total_current_a": round(avg_total_current_a, 4),
            "MAX_TOTAL_POWER_W": round(max_total_power_w, 4),
            "MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH": round(energy_goal_wh, 4),
            "ENERGY_GOAL_VALUE_WH": round(energy_goal_wh, 4),
            "ENERGY_GOAL_DURATION_MIN": round(energy_goal_duration_min, 4),
            "ENERGY_GOAL_DURATION_SEC": int(energy_goal_duration_min * 60.0),
            "ENERGY_GOAL_WINDOW_STARTED_AT_UTC": iso_utc(goal_window_start_dt),
            "ENERGY_GOAL_WINDOW_ENDS_AT_UTC": iso_utc(goal_window_end_dt),
            "ENERGY_GOAL_WINDOW_ELAPSED_SEC": round(max(0.0, goal_window_duration_sec - goal_window_remaining_sec), 4),
            "ENERGY_GOAL_WINDOW_REMAINING_SEC": round(goal_window_remaining_sec, 4),
            "ENERGY_GOAL_WINDOW_REMAINING_RATIO": round(goal_window_remaining_ratio, 6),
            "ENERGY_GOAL_WINDOW_RESET_COUNT": int(runtime.goal_window_reset_count),
            "ENERGY_GOAL_WINDOW_RESET_REASON": runtime.goal_window_reset_reason,
            "UNDERVOLTAGE_THRESHOLD_V": 10.5,
            "UNDERVOLTAGE_TRIP_DELAY_MS": 500,
            "UNDERVOLTAGE_CLEAR_DELAY_MS": 500,
            "UNDERVOLTAGE_RESTORE_MARGIN_V": 0.3,
            "UNDERVOLTAGE_ACTIVE": False,
            "OVERVOLTAGE_THRESHOLD_V": 13.2,
            "OVERVOLTAGE_TRIP_DELAY_MS": 500,
            "OVERVOLTAGE_CLEAR_DELAY_MS": 500,
            "OVERVOLTAGE_RESTORE_MARGIN_V": 0.3,
            "OVERVOLTAGE_ACTIVE": False,
        },
        "energy": {
            "total_energy_wh": round(runtime.total_energy_wh, 4),
            "budget": {
                "goal_window_wh": round(energy_goal_wh, 4),
                "window_duration_sec": int(goal_window_duration_sec),
                "window_duration_min": round(energy_goal_duration_min, 4),
                "consumed_window_wh": round(budget_wh, 4),
                "remaining_window_wh": round(max(0.0, energy_goal_wh - budget_wh), 4),
                "used_ratio_window": round(clamp((budget_wh / max(energy_goal_wh, 1e-9)) if energy_goal_wh > 0.0 else 0.0, 0.0, 1.0), 6),
                "started_at_utc": iso_utc(goal_window_start_dt),
                "ends_at_utc": iso_utc(goal_window_end_dt),
                "elapsed_sec": round(max(0.0, goal_window_duration_sec - goal_window_remaining_sec), 4),
                "remaining_sec": round(goal_window_remaining_sec, 4),
                "remaining_ratio": round(goal_window_remaining_ratio, 6),
                "reset_count": int(runtime.goal_window_reset_count),
                "reset_reason": runtime.goal_window_reset_reason,
            },
        },
        "environment": {
            "temperature_c": round(avg_ambient_temp_c, 4),
            "humidity_pct": round(avg_humidity_pct, 4),
        },
        "process": {
            "enabled": runtime.process_enabled,
            "lock_active": runtime.process_enabled,
            "state": runtime.process_state,
            "goal_cycles": runtime.goal_cycles,
            "cycle_count": runtime.cycle_count,
            "elapsed_sec": round(runtime.process_elapsed_sec, 4),
            "tank1_height_cm": process_meta["height_cm"],
            "tank2_height_cm": process_meta["height_cm"],
            "tank1_low_level_pct": process_meta["low_level_pct"],
            "tank2_low_level_pct": process_meta["low_level_pct"],
            "tank1_high_level_pct": process_meta["high_level_pct"],
            "tank2_high_level_pct": process_meta["high_level_pct"],
            "tank1_level_pct": round(avg_tank1_level_pct, 4),
            "tank2_level_pct": round(avg_tank2_level_pct, 4),
            "tank1_temp_c": round(avg_tank1_temp_c, 4),
            "tank2_temp_c": round(avg_tank2_temp_c, 4),
            "tank1_ultrasonic_distance_cm": round(tank1_distance, 4),
            "tank2_ultrasonic_distance_cm": round(tank2_distance, 4),
            "tank1_ultrasonic_raw_cm": round(tank1_raw_cm, 4),
            "tank2_ultrasonic_raw_cm": round(tank2_raw_cm, 4),
            "tank1": {
                "target_level_pct": process_meta["high_level_pct"],
                "target_temp_c": round(process_meta["target_t1"], 4),
                "level_pct": round(avg_tank1_level_pct, 4),
                "temperature_c": round(avg_tank1_temp_c, 4),
                "height_cm": process_meta["height_cm"],
                "low_level_pct": process_meta["low_level_pct"],
                "high_level_pct": process_meta["high_level_pct"],
                "ultrasonic_distance_cm": round(tank1_distance, 4),
                "ultrasonic_raw_cm": round(tank1_raw_cm, 4),
            },
            "tank2": {
                "target_level_pct": process_meta["high_level_pct"],
                "target_temp_c": round(process_meta["target_t2"], 4),
                "level_pct": round(avg_tank2_level_pct, 4),
                "temperature_c": round(avg_tank2_temp_c, 4),
                "height_cm": process_meta["height_cm"],
                "low_level_pct": process_meta["low_level_pct"],
                "high_level_pct": process_meta["high_level_pct"],
                "ultrasonic_distance_cm": round(tank2_distance, 4),
                "ultrasonic_raw_cm": round(tank2_raw_cm, 4),
            },
            "kpi": {
                "balance_delta_level_pct": round(balance_delta_level_pct, 4),
            },
        },
        "loads": load_payload,
        "evaluation": {
            "control": {
                "total_control_actions": runtime.total_control_actions,
                "shedding_event_count": runtime.shed_events,
                "curtail_event_count": runtime.curtail_events,
                "restore_event_count": runtime.restore_events,
                "control_actions_per_min": round((runtime.total_control_actions * 60.0) / max((bucket_end_dt.timestamp() - runtime.goal_window_started_ts), 60.0), 4),
            },
            "stability": {
                "max_overshoot_w": round(runtime.max_overshoot_w, 4),
                "overload_event_count": runtime.overload_events,
                "overshoot_energy_ws": 0.0,
                "load_toggle_count": runtime.load_toggle_count,
                "last_peak_settling_time_sec": 0.0,
            },
            "prediction": {
                "prediction_age_ms": 0.0,
                "apply_delay_sec": 0.0,
                "horizon_sec": 0.0,
            },
        },
    }


def simulate_step(
    *,
    plant_id: str,
    start_local_date: date,
    runtime: PlantRuntime,
    zone,
    dt_utc: datetime,
    step_sec: int,
    policy_rotation: List[str],
    seed: int,
    rng: random.Random,
) -> Dict[str, object]:
    local_dt = dt_utc.astimezone(zone)
    day_index = (local_dt.date() - start_local_date).days
    policy = policy_rotation[day_index % len(policy_rotation)]
    profile = POLICY_PROFILES[policy]
    schedule = shift_schedule(local_dt)
    schedule = dict(schedule)
    tariff_state, tariff_rate = tariff_state_and_rate(local_dt)
    day_profile = day_dynamic_profile(day_index, seed)
    activity_profile = intraday_activity_profile(local_dt, dt_utc.timestamp(), day_profile)

    daily_wave = math.sin(((local_dt.hour * 60 + local_dt.minute) / 1440.0) * math.tau)
    weekly_wave = math.sin(((local_dt.weekday() + local_dt.hour / 24.0) / 7.0) * math.tau)
    campaign_wave = float(activity_profile["campaign_wave"])
    ambient_temp_c = (
        24.0
        + 4.5 * daily_wave
        + 1.3 * weekly_wave
        + float(day_profile["ambient_offset_c"])
        + (0.9 * campaign_wave)
        + (0.8 * float(activity_profile["surge_strength"]))
        - (0.6 * float(activity_profile["slump_strength"]))
        + rng.uniform(-0.35, 0.35)
    )
    humidity_pct = clamp(
        56.0
        - (7.5 * daily_wave)
        + float(day_profile["humidity_offset_pct"])
        - (1.1 * float(day_profile["ambient_offset_c"]))
        - (2.0 * float(activity_profile["surge_strength"]))
        + (3.8 * float(activity_profile["slump_strength"]))
        + rng.uniform(-2.2, 2.2),
        32.0,
        84.0,
    )
    utilization_base = float(schedule["utilization_pct"])
    utilization_variation = (5.0 * cycle_wave(dt_utc.timestamp(), 2700.0, day_index * 0.17)) - 2.5
    utilization_variation += (3.0 * cycle_wave(dt_utc.timestamp(), 840.0, 0.23)) - 1.5
    utilization_pct = (
        (utilization_base * float(day_profile["utilization_scale"]) * float(activity_profile["activity_multiplier"]))
        + float(day_profile["utilization_bias_pct"])
        + utilization_variation
        + (4.0 * campaign_wave)
        + rng.uniform(-2.4, 2.4)
    )
    utilization_pct = clamp(utilization_pct, 6.0, 97.0)
    if str(schedule["production_mode"]) == "Production" and cycle_fraction(dt_utc.timestamp(), 3600.0, day_index * 0.09) >= 0.975:
        utilization_pct = max(18.0, utilization_pct * 0.55)
    if str(schedule["production_mode"]) == "Production" and float(activity_profile["micro_stop_strength"]) > 0.82:
        utilization_pct = max(10.0, utilization_pct * (0.42 + (0.35 * (1.0 - float(activity_profile["micro_stop_strength"])))))
    schedule["utilization_pct"] = round(utilization_pct, 4)

    manual_override_probability = 1.0 - math.pow(1.0 - 0.00035, step_sec / 5.0)
    if rng.random() < manual_override_probability:
        runtime.manual_override_until = dt_utc.timestamp() + rng.randint(600, 1800)
    manual_override = runtime.manual_override_until > dt_utc.timestamp()

    process_meta = update_process(
        runtime,
        process_enabled=bool(schedule["process_enabled"]),
        sample_ts=dt_utc.timestamp(),
        step_sec=step_sec,
        ambient_temp_c=ambient_temp_c,
        shift_type=str(schedule["shift_type"]),
        utilization_pct=utilization_pct,
        process_profile={
            "transfer_rate_scale": float(day_profile["transfer_rate_scale"]) * float(activity_profile["process_multiplier"]),
            "heat_rate_scale": float(day_profile["heat_rate_scale"]) * clamp(0.95 + (0.12 * float(activity_profile["surge_strength"])) - (0.06 * float(activity_profile["slump_strength"])), 0.82, 1.20),
            "cool_ratio_scale": float(day_profile["cool_ratio_scale"]),
            "target_temp_bias_c": float(day_profile["target_temp_bias_c"]) + (0.5 * campaign_wave),
            "level_bias_pct": float(day_profile["level_bias_pct"]),
            "goal_cycle_bias": float(day_profile["goal_cycle_bias"]),
        },
    )

    desired = desired_load_state(
        runtime,
        str(schedule["production_mode"]),
        str(schedule["shift_type"]),
        utilization_pct,
        tariff_state,
        dt_utc.timestamp(),
        process_loads=process_meta.get("phase_loads"),
        dynamic_profile={
            "lighting_bias": float(day_profile["lighting_bias"]),
            "lighting_multiplier": float(activity_profile["lighting_multiplier"]),
        },
    )
    baseline_powers = apply_dynamic_power_scaling(
        estimate_load_powers(desired, ambient_temp_c, rng),
        sample_ts=dt_utc.timestamp(),
        day_profile=day_profile,
        activity_profile=activity_profile,
    )
    managed, policy_events = apply_policy_management(
        runtime,
        desired,
        baseline_powers,
        policy=policy,
        max_total_power_w=float(profile["max_total_power_w"]),
        tariff_state=tariff_state,
        process_state=runtime.process_state,
        manual_override=manual_override,
    )
    load_powers = apply_dynamic_power_scaling(
        estimate_load_powers(managed, ambient_temp_c, rng),
        sample_ts=dt_utc.timestamp(),
        day_profile=day_profile,
        activity_profile=activity_profile,
    )

    for name, state in managed.items():
        previous = runtime.last_load_state.get(name, (False, 0.0))
        now_state = (bool(state["on"]), round(float(state["duty"]), 4))
        if previous != now_state:
            runtime.load_toggle_count += 1
        if previous[0] and not now_state[0]:
            runtime.shed_events += 1
        elif previous[1] > now_state[1]:
            runtime.curtail_events += 1
        elif previous[1] < now_state[1]:
            runtime.restore_events += 1
        runtime.last_load_state[name] = now_state

    runtime.total_control_actions += int(policy_events["actions"])
    runtime.overload_events += int(policy_events["overload"])

    total_power_w = sum(load_powers.values()) + facility_base_power_w(
        sample_ts=dt_utc.timestamp(),
        ambient_temp_c=ambient_temp_c,
        production_mode=str(schedule["production_mode"]),
        tariff_state=tariff_state,
        day_profile=day_profile,
        activity_profile=activity_profile,
    )
    total_current_a = total_power_w / 12.0
    supply_v = clamp(12.18 - (total_current_a * 0.11) + rng.uniform(-0.03, 0.03), 11.2, 12.35)
    total_current_a = total_power_w / max(supply_v, 1.0)
    overshoot = max(0.0, total_power_w - float(profile["max_total_power_w"]))
    runtime.max_overshoot_w = max(runtime.max_overshoot_w, overshoot)
    runtime.total_energy_wh += total_power_w * (step_sec / 3600.0)

    increment_wh = total_power_w * (step_sec / 3600.0)
    horizon_sec = int(float(profile["energy_goal_duration_min"]) * 60.0)
    goal_wh = float(profile["energy_goal_wh"])
    power_cap_w = float(profile["max_total_power_w"])
    sample_ts = dt_utc.timestamp()
    if runtime.goal_window_started_ts <= 0.0:
        reset_goal_window(
            runtime,
            start_ts=sample_ts,
            reason="startup",
            goal_wh=goal_wh,
            duration_sec=horizon_sec,
            power_cap_w=power_cap_w,
            increment_counter=False,
        )
    elif (
        not math.isclose(runtime.goal_window_config_wh, goal_wh, abs_tol=1e-9)
        or not math.isclose(runtime.goal_window_config_duration_sec, horizon_sec, abs_tol=1e-9)
        or not math.isclose(runtime.goal_window_config_power_cap_w, power_cap_w, abs_tol=1e-9)
    ):
        reset_goal_window(
            runtime,
            start_ts=sample_ts,
            reason="config_changed",
            goal_wh=goal_wh,
            duration_sec=horizon_sec,
            power_cap_w=power_cap_w,
            increment_counter=True,
        )
    while (sample_ts - runtime.goal_window_started_ts) >= horizon_sec:
        reset_goal_window(
            runtime,
            start_ts=runtime.goal_window_started_ts + horizon_sec,
            reason="window_completed",
            goal_wh=goal_wh,
            duration_sec=horizon_sec,
            power_cap_w=power_cap_w,
            increment_counter=True,
        )
    runtime.goal_window_consumed_wh += increment_wh
    for load_name, load_power_w in load_powers.items():
        runtime.per_load_goal_window_wh[load_name] = float(runtime.per_load_goal_window_wh.get(load_name, 0.0) + (float(load_power_w) * (step_sec / 3600.0)))

    runtime.energy_window.append((dt_utc.timestamp(), increment_wh))
    while runtime.energy_window and (dt_utc.timestamp() - runtime.energy_window[0][0]) > horizon_sec:
        runtime.energy_window.popleft()
    budget_wh = runtime.goal_window_consumed_wh

    tank1_distance = process_meta["height_cm"] * (1.0 - (runtime.tank1_level_pct / 100.0))
    tank2_distance = process_meta["height_cm"] * (1.0 - (runtime.tank2_level_pct / 100.0))
    tank1_raw_cm = tank1_distance + rng.uniform(-0.6, 0.6)
    tank2_raw_cm = tank2_distance + rng.uniform(-0.6, 0.6)

    return {
        "plant_id": plant_id,
        "dt_utc": dt_utc,
        "policy": policy,
        "profile": profile,
        "schedule": schedule,
        "tariff_state": tariff_state,
        "tariff_rate": tariff_rate,
        "ambient_temp_c": ambient_temp_c,
        "humidity_pct": humidity_pct,
        "loads": managed,
        "load_powers": load_powers,
        "process_meta": process_meta,
        "total_power_w": total_power_w,
        "total_current_a": total_current_a,
        "supply_v": supply_v,
        "budget_wh": budget_wh,
        "tank1_level_pct": runtime.tank1_level_pct,
        "tank2_level_pct": runtime.tank2_level_pct,
        "tank1_temp_c": runtime.tank1_temp_c,
        "tank2_temp_c": runtime.tank2_temp_c,
        "tank1_ultrasonic_distance_cm": tank1_distance,
        "tank2_ultrasonic_distance_cm": tank2_distance,
        "tank1_ultrasonic_raw_cm": tank1_raw_cm,
        "tank2_ultrasonic_raw_cm": tank2_raw_cm,
        "balance_delta_level_pct": abs(runtime.tank1_level_pct - runtime.tank2_level_pct),
        "day_profile": day_profile,
        "activity_profile": activity_profile,
    }


def iter_records(
    *,
    plant_id: str,
    start_dt_utc: datetime,
    days: int,
    step_sec: int,
    policy_rotation: List[str],
    timezone_name: str,
    seed: int,
) -> Iterable[Tuple[datetime, Dict[str, object]]]:
    rng = random.Random(seed)
    runtime = PlantRuntime()
    zone = local_zone(timezone_name)
    start_local_date = start_dt_utc.astimezone(zone).date()
    total_steps = int((days * 24 * 3600) / step_sec)
    sample_step_sec = internal_simulation_step_sec(step_sec)

    for index in range(total_steps):
        bucket_start_dt = start_dt_utc + timedelta(seconds=index * step_sec)
        bucket_end_dt = bucket_start_dt + timedelta(seconds=step_sec)
        sample_points: List[Dict[str, object]] = []
        elapsed_sec = 0
        while elapsed_sec < step_sec:
            current_step_sec = min(sample_step_sec, step_sec - elapsed_sec)
            sample_dt = bucket_start_dt + timedelta(seconds=elapsed_sec)
            sample_points.append(
                simulate_step(
                    plant_id=plant_id,
                    start_local_date=start_local_date,
                    runtime=runtime,
                    zone=zone,
                    dt_utc=sample_dt,
                    step_sec=current_step_sec,
                    policy_rotation=policy_rotation,
                    seed=seed,
                    rng=rng,
                )
            )
            elapsed_sec += current_step_sec

        last_sample = sample_points[-1]
        aggregate_metrics = {
            "total_power_w": average([float(point["total_power_w"]) for point in sample_points]),
            "total_current_a": average([float(point["total_current_a"]) for point in sample_points]),
            "supply_v": average([float(point["supply_v"]) for point in sample_points]),
            "ambient_temp_c": average([float(point["ambient_temp_c"]) for point in sample_points]),
            "humidity_pct": average([float(point["humidity_pct"]) for point in sample_points]),
            "tank1_level_pct": average([float(point["tank1_level_pct"]) for point in sample_points]),
            "tank2_level_pct": average([float(point["tank2_level_pct"]) for point in sample_points]),
            "tank1_temp_c": average([float(point["tank1_temp_c"]) for point in sample_points]),
            "tank2_temp_c": average([float(point["tank2_temp_c"]) for point in sample_points]),
            "tank1_ultrasonic_distance_cm": average([float(point["tank1_ultrasonic_distance_cm"]) for point in sample_points]),
            "tank2_ultrasonic_distance_cm": average([float(point["tank2_ultrasonic_distance_cm"]) for point in sample_points]),
            "tank1_ultrasonic_raw_cm": average([float(point["tank1_ultrasonic_raw_cm"]) for point in sample_points]),
            "tank2_ultrasonic_raw_cm": average([float(point["tank2_ultrasonic_raw_cm"]) for point in sample_points]),
            "balance_delta_level_pct": average([float(point["balance_delta_level_pct"]) for point in sample_points]),
        }
        for load_name in LOAD_SPECS:
            aggregate_metrics[f"load_{load_name}_on_ratio"] = average([
                1.0 if bool((point.get("loads", {}) or {}).get(load_name, {}).get("on", False)) else 0.0
                for point in sample_points
            ])
            aggregate_metrics[f"load_{load_name}_duty_applied_avg"] = average([
                float(((point.get("loads", {}) or {}).get(load_name, {}) or {}).get("duty", 0.0))
                for point in sample_points
            ])
            aggregate_metrics[f"load_{load_name}_power_w_avg"] = average([
                float((point.get("load_powers", {}) or {}).get(load_name, 0.0))
                for point in sample_points
            ])
            aggregate_metrics[f"load_{load_name}_current_a_avg"] = average([
                float((point.get("load_powers", {}) or {}).get(load_name, 0.0)) / max(float(point.get("supply_v", 12.0) or 12.0), 1.0)
                for point in sample_points
            ])

        yield bucket_start_dt, build_record(
            plant_id=plant_id,
            dt_utc=bucket_end_dt,
            step_sec=step_sec,
            runtime=runtime,
            schedule=last_sample["schedule"],
            policy=str(last_sample["policy"]),
            max_total_power_w=float(last_sample["profile"]["max_total_power_w"]),
            energy_goal_wh=float(last_sample["profile"]["energy_goal_wh"]),
            energy_goal_duration_min=float(last_sample["profile"]["energy_goal_duration_min"]),
            ambient_temp_c=float(last_sample["ambient_temp_c"]),
            humidity_pct=float(last_sample["humidity_pct"]),
            tariff_state=str(last_sample["tariff_state"]),
            tariff_rate_per_kwh=float(last_sample["tariff_rate"]),
            loads=last_sample["loads"],
            load_powers=last_sample["load_powers"],
            process_meta=last_sample["process_meta"],
            total_power_w=float(last_sample["total_power_w"]),
            total_current_a=float(last_sample["total_current_a"]),
            supply_v=float(last_sample["supply_v"]),
            budget_wh=float(last_sample["budget_wh"]),
            rng=rng,
            sample_count=len(sample_points),
            bucket_start_dt=bucket_start_dt,
            bucket_end_dt=bucket_end_dt,
            aggregate_metrics=aggregate_metrics,
        )


def generate_dataset(
    *,
    plant_id: str,
    start_dt_utc: datetime,
    days: int,
    step_sec: int,
    seed: int,
    timezone_name: str,
    output_dir: Path,
    policy_rotation: List[str],
    overwrite: bool = False,
    progress_callback: Optional[Callable[[Dict[str, object]], None]] = None,
) -> Dict[str, object]:
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    handles: Dict[str, object] = {}
    record_count = 0
    estimated_records = max(1, int((days * 24 * 60 * 60) / max(1, int(step_sec))))
    progress_every = max(1, min(1000, estimated_records // 200 if estimated_records >= 200 else 25))
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "generating",
                "records_written": 0,
                "estimated_records": estimated_records,
                "progress_ratio": 0.0,
                "output_dir": str(output_dir),
            }
        )
    try:
        for dt_utc, record in iter_records(
            plant_id=plant_id,
            start_dt_utc=start_dt_utc,
            days=days,
            step_sec=step_sec,
            policy_rotation=policy_rotation,
            timezone_name=timezone_name,
            seed=seed,
        ):
            day_key = dt_utc.date().isoformat()
            file_path = output_dir / f"{plant_id}_telemetry_{day_key}.jsonl"
            if file_path.exists() and not overwrite and day_key not in handles:
                raise FileExistsError(f"Refusing to overwrite existing file: {file_path}")
            if day_key not in handles:
                mode = "w" if overwrite else "a"
                handles[day_key] = file_path.open(mode, encoding="utf-8")
            handles[day_key].write(json.dumps(record, sort_keys=True) + "\n")
            record_count += 1
            if progress_callback is not None:
                if record_count == 1 or record_count % progress_every == 0 or record_count >= estimated_records:
                    progress_callback(
                        {
                            "phase": "generating",
                            "records_written": record_count,
                            "estimated_records": estimated_records,
                            "progress_ratio": round(min(1.0, record_count / estimated_records), 4),
                            "day_key": day_key,
                            "output_dir": str(output_dir),
                        }
                    )
    finally:
        for handle in handles.values():
            handle.close()

    summary = {
        "plant_id": plant_id,
        "synthetic_profile": "dynamic_factory_baseline_v2",
        "energy_management_active": False,
        "start_utc": iso_utc(start_dt_utc),
        "days": days,
        "step_sec": step_sec,
        "seed": seed,
        "timezone": timezone_name,
        "policy_rotation": policy_rotation,
        "output_dir": str(output_dir),
        "records_written": record_count,
        "profile_features": [
            "day_level_regime_variation",
            "intraday_surge_and_slump_windows",
            "micro_stop_events",
            "dynamic_process_transfer_and_heating_rates",
            "background_facility_base_load",
        ],
        "recommended_env": {
            "LOGGER_OUTPUT_DIR": str(output_dir),
            "BOOTSTRAP_FROM_HISTORY": "1",
            "LONG_TERM_TELEMETRY_SAMPLE_SEC": str(step_sec),
            "LONG_TERM_LSTM_TELEMETRY_SAMPLE_SEC": str(step_sec),
        },
    }
    (output_dir / "synthetic_dataset_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if progress_callback is not None:
        progress_callback(
            {
                "phase": "generation_complete",
                "records_written": record_count,
                "estimated_records": estimated_records,
                "progress_ratio": 1.0,
                "summary": summary,
                "output_dir": str(output_dir),
            }
        )
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic telemetry logs aligned to the current predictor schema.")
    parser.add_argument("--plant-id", default="factory1")
    parser.add_argument("--start", default=None, help="UTC start timestamp, e.g. 2026-04-01T00:00:00Z")
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--step-sec", type=int, default=5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timezone", default="Europe/London")
    parser.add_argument("--output-dir", default=str(Path(__file__).resolve().parent / "telemetry_logs_synthetic"))
    parser.add_argument("--policy-rotation", default="NO_ENERGY_MANAGEMENT")
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def parse_start(value: Optional[str]) -> datetime:
    if not value:
        return datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def main() -> int:
    args = parse_args()
    if args.days <= 0:
        raise SystemExit("--days must be > 0")
    if args.step_sec <= 0:
        raise SystemExit("--step-sec must be > 0")

    start_dt = parse_start(args.start)
    policies = [item.strip().upper() for item in str(args.policy_rotation).split(",") if item.strip()]
    policies = [item for item in policies if item in POLICY_PROFILES]
    if not policies:
        policies = list(POLICY_ROTATION)
    try:
        summary = generate_dataset(
            plant_id=args.plant_id,
            start_dt_utc=start_dt,
            days=args.days,
            step_sec=args.step_sec,
            seed=args.seed,
            timezone_name=args.timezone,
            output_dir=Path(args.output_dir),
            policy_rotation=policies,
            overwrite=args.overwrite,
        )
    except FileExistsError as exc:
        raise SystemExit(f"{exc}. Use --overwrite.")

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
