#!/usr/bin/env python3
"""Canonical payload contract for long-horizon predictor inputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Sequence, Tuple


LOAD_NAMES: Tuple[str, ...] = (
    "motor1",
    "motor2",
    "heater1",
    "heater2",
    "lighting1",
    "lighting2",
)


@dataclass(frozen=True)
class PayloadRequirement:
    name: str
    alternatives: Tuple[Tuple[str, ...], ...]


CORE_REQUIREMENTS: Tuple[PayloadRequirement, ...] = (
    PayloadRequirement("timestamp", (("timestamp",), ("system", "timestamp"))),
    PayloadRequirement("system.total_power_w", (("system", "total_power_w"),)),
    PayloadRequirement("system.supply_v", (("system", "supply_v"),)),
    PayloadRequirement("environment.temperature_c", (("environment", "temperature_c"),)),
    PayloadRequirement("environment.humidity_pct", (("environment", "humidity_pct"),)),
    PayloadRequirement("energy.total_energy_wh", (("energy", "total_energy_wh"),)),
    PayloadRequirement(
        "energy.budget.consumed_window_wh",
        (("energy", "budget", "consumed_window_wh"), ("current_energy_window_wh",)),
    ),
    PayloadRequirement("process.enabled", (("process", "enabled"),)),
    PayloadRequirement("process.state", (("process", "state"),)),
    PayloadRequirement("process.cycle_count", (("process", "cycle_count"),)),
    PayloadRequirement(
        "process.tank1.level_pct",
        (("process", "tank1", "level_pct"), ("process", "tank1_level_pct")),
    ),
    PayloadRequirement(
        "process.tank2.level_pct",
        (("process", "tank2", "level_pct"), ("process", "tank2_level_pct")),
    ),
    PayloadRequirement(
        "process.tank1.temperature_c",
        (("process", "tank1", "temperature_c"), ("process", "tank1_temp_c")),
    ),
    PayloadRequirement(
        "process.tank2.temperature_c",
        (("process", "tank2", "temperature_c"), ("process", "tank2_temp_c")),
    ),
)


def _load_requirements() -> List[PayloadRequirement]:
    requirements = list(CORE_REQUIREMENTS)
    for load_name in LOAD_NAMES:
        requirements.extend(
            [
                PayloadRequirement(
                    f"loads.{load_name}.class",
                    (( "loads", load_name, "class"),),
                ),
                PayloadRequirement(
                    f"loads.{load_name}.on_ratio",
                    (( "loads", load_name, "on_ratio"), ("loads", load_name, "on")),
                ),
                PayloadRequirement(
                    f"loads.{load_name}.fault_active",
                    (( "loads", load_name, "fault_active"),),
                ),
                PayloadRequirement(
                    f"loads.{load_name}.fault_latched",
                    (( "loads", load_name, "fault_latched"),),
                ),
            ]
        )
    return requirements


LONG_MODEL_PAYLOAD_REQUIREMENTS: Tuple[PayloadRequirement, ...] = tuple(_load_requirements())


def _path_exists(payload: Dict[str, Any], path: Sequence[str]) -> bool:
    current: Any = payload
    for part in path:
        if not isinstance(current, dict) or part not in current:
            return False
        current = current.get(part)
    return current is not None


def find_missing_payload_requirements(payload: Dict[str, Any]) -> List[str]:
    missing: List[str] = []
    for requirement in LONG_MODEL_PAYLOAD_REQUIREMENTS:
        if not any(_path_exists(payload, alt) for alt in requirement.alternatives):
            missing.append(requirement.name)
    return missing


def payload_contract_coverage(payload: Dict[str, Any]) -> Dict[str, Any]:
    total = len(LONG_MODEL_PAYLOAD_REQUIREMENTS)
    missing = find_missing_payload_requirements(payload)
    return {
        "total_requirements": total,
        "present_requirements": total - len(missing),
        "missing_requirements": missing,
        "coverage_ratio": ((total - len(missing)) / total) if total > 0 else 1.0,
    }


def iter_requirement_names() -> Iterable[str]:
    for requirement in LONG_MODEL_PAYLOAD_REQUIREMENTS:
        yield requirement.name
