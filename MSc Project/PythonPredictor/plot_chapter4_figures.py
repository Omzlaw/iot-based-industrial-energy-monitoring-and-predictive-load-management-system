#!/usr/bin/env python3
"""Generate organized PNG figures from a Chapter 4 export directory."""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import matplotlib
import numpy as np

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt


DEFAULT_EXPORT_ROOT = Path(__file__).resolve().parent / "chapter4_exports"
DEFAULT_MODEL_DIR = Path(__file__).resolve().parent / "models"


def _safe_float(value: object) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"none", "nan"}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _safe_bool(value: object) -> Optional[bool]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _parse_time(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def _load_csv(path: Path) -> List[dict]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = [dict(row) for row in reader]
    for row in rows:
        row["_dt"] = _parse_time(row.get("label_utc", ""))
        row["_ts"] = _safe_float(row.get("ts"))
    rows.sort(key=lambda item: (
        item["_dt"] if item["_dt"] is not None else datetime.min.replace(tzinfo=timezone.utc),
        item.get("load_name", ""),
        item.get("tank_name", ""),
    ))
    return rows


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _finish_time_axis(ax, title: str, ylabel: str, xlabel: str = "Time (UTC)") -> None:
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.set_xlabel(xlabel)
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
    ax.tick_params(axis="x", rotation=20)


def _save_fig(fig: plt.Figure, path: Path) -> None:
    _ensure_parent(path)
    fig.tight_layout()
    fig.savefig(path, dpi=220, bbox_inches="tight")
    plt.close(fig)


def _axes_list(axes) -> List[plt.Axes]:
    return list(axes.flat) if hasattr(axes, "flat") else [axes]


def _group_rows(rows: Sequence[dict], key: str) -> Dict[str, List[dict]]:
    grouped: Dict[str, List[dict]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get(key, ""))].append(row)
    return dict(grouped)


def _find_analytics_file(export_dir: Path, suffix: str) -> Optional[Path]:
    export_match = sorted((export_dir / "model_analytics").glob(f"*{suffix}"))
    if export_match:
        return export_match[0]
    model_match = sorted(DEFAULT_MODEL_DIR.glob(f"*{suffix}"))
    if model_match:
        return model_match[0]
    return None


def _load_json(path: Optional[Path]) -> Optional[dict]:
    if path is None or not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _time_and_values(rows: Sequence[dict], key: str) -> tuple[list, list]:
    xs, ys = [], []
    for row in rows:
        dt = row.get("_dt")
        value = _safe_float(row.get(key))
        if dt is None or value is None:
            continue
        xs.append(dt)
        ys.append(value)
    return xs, ys


def _smooth_time_series(xs: Sequence[datetime], ys: Sequence[float], *, window_points: Optional[int] = None, dense_factor: int = 4) -> tuple[list, list]:
    if len(xs) < 4 or len(ys) < 4:
        return list(xs), list(ys)

    x_num = np.asarray(mdates.date2num(list(xs)), dtype=float)
    y_arr = np.asarray(list(ys), dtype=float)
    if window_points is None:
        target = max(5, min(31, len(y_arr) // 9 if len(y_arr) >= 9 else 5))
        window_points = target + 1 if target % 2 == 0 else target
    if window_points >= len(y_arr):
        window_points = len(y_arr) if len(y_arr) % 2 == 1 else len(y_arr) - 1
    if window_points < 3:
        return list(xs), list(ys)

    kernel = np.hanning(window_points)
    if not np.isfinite(kernel).all() or float(kernel.sum()) <= 0.0:
        kernel = np.ones(window_points, dtype=float)
    kernel = kernel / float(kernel.sum())
    pad = window_points // 2
    padded = np.pad(y_arr, (pad, pad), mode="edge")
    smooth_y = np.convolve(padded, kernel, mode="valid")

    dense_count = min(600, max(len(y_arr) * max(2, dense_factor), 160))
    dense_x = np.linspace(float(x_num[0]), float(x_num[-1]), dense_count)
    dense_y = np.interp(dense_x, x_num, smooth_y)
    return list(mdates.num2date(dense_x)), dense_y.tolist()


def _plot_scatter_with_smooth(
    ax,
    xs: Sequence[datetime],
    ys: Sequence[float],
    *,
    label: str,
    color: str,
    scatter_size: float = 10.0,
    scatter_alpha: float = 0.28,
    linewidth: float = 2.1,
) -> None:
    if not xs or not ys:
        return
    ax.scatter(xs, ys, s=scatter_size, alpha=scatter_alpha, color=color, edgecolors="none")
    smooth_xs, smooth_ys = _smooth_time_series(xs, ys)
    ax.plot(smooth_xs, smooth_ys, label=label, color=color, linewidth=linewidth)


def _x_and_values(rows: Sequence[dict], x_key: str, y_key: str) -> tuple[list, list]:
    xs, ys = [], []
    for row in rows:
        x_val = _safe_float(row.get(x_key))
        y_val = _safe_float(row.get(y_key))
        if x_val is None or y_val is None:
            continue
        xs.append(x_val)
        ys.append(y_val)
    return xs, ys


def _time_and_bool(rows: Sequence[dict], key: str) -> tuple[list, list]:
    xs, ys = [], []
    for row in rows:
        dt = row.get("_dt")
        value = _safe_bool(row.get(key))
        if dt is None or value is None:
            continue
        xs.append(dt)
        ys.append(1.0 if value else 0.0)
    return xs, ys


def _choose_representative_loads(rows: Sequence[dict], max_loads: int = 4) -> List[str]:
    best_by_class: Dict[str, tuple[int, str]] = {}
    fallback: Dict[str, tuple[int, int]] = {}
    for row in rows:
        name = str(row.get("load_name", ""))
        if not name:
            continue
        prio = int(_safe_float(row.get("priority")) or 99)
        weight = int(_safe_float(row.get("priority")) or 99)
        load_class = str(row.get("class", "UNKNOWN"))
        current_best = best_by_class.get(load_class)
        if current_best is None or prio < current_best[0]:
            best_by_class[load_class] = (prio, name)
        current_fb = fallback.get(name)
        if current_fb is None or prio < current_fb[0]:
            fallback[name] = (prio, weight)

    chosen: List[str] = [name for _, name in sorted(best_by_class.values(), key=lambda item: item[0])]
    if len(chosen) >= max_loads:
        return chosen[:max_loads]

    for name, _ in sorted(fallback.items(), key=lambda item: item[1][0]):
        if name not in chosen:
            chosen.append(name)
        if len(chosen) >= max_loads:
            break
    return chosen[:max_loads]


def _plot_figure_4_1(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_1_load_voltage_vs_load_current.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "load_name")
    fig, ax = plt.subplots(figsize=(10, 6))
    for load_name, load_rows in sorted(grouped.items()):
        xs, ys = [], []
        for row in load_rows:
            current_a = _safe_float(row.get("current_a"))
            voltage_v = _safe_float(row.get("voltage_v"))
            if current_a is None or voltage_v is None:
                continue
            xs.append(current_a)
            ys.append(voltage_v)
        if xs:
            ax.scatter(xs, ys, s=18, alpha=0.75, label=load_name)
    ax.set_title("Figure 4.1 - Load Voltage vs Load Current")
    ax.set_xlabel("Load Current (A)")
    ax.set_ylabel("Load Voltage (V)")
    ax.grid(True, alpha=0.3)
    if ax.has_data():
        ax.legend(loc="best", fontsize=8)
    path = out_dir / "figure_4_1_load_voltage_vs_load_current.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_2(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_2_supply_voltage_vs_total_current.csv")
    if not rows:
        return None
    fig, ax1 = plt.subplots(figsize=(11, 6))
    xs, supply_v = _time_and_values(rows, "supply_v")
    _, total_current = _time_and_values(rows, "total_current_a")
    if xs:
        ax1.plot(xs, supply_v, color="#1f77b4", label="Supply Voltage")
    ax1.set_ylabel("Supply Voltage (V)", color="#1f77b4")
    ax1.tick_params(axis="y", labelcolor="#1f77b4")
    ax2 = ax1.twinx()
    if xs:
        ax2.plot(xs, total_current, color="#d62728", label="Total Current")
    ax2.set_ylabel("Total Current (A)", color="#d62728")
    ax2.tick_params(axis="y", labelcolor="#d62728")
    _finish_time_axis(ax1, "Figure 4.2 - Supply Voltage and Total Current vs Time", "Supply Voltage (V)")
    path = out_dir / "figure_4_2_supply_voltage_vs_total_current.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_3(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_3_temperature_vs_time_heating.csv")
    if not rows:
        return None
    fig, ax1 = plt.subplots(figsize=(11, 6))
    xs, tank1 = _time_and_values(rows, "tank1_temp_c")
    _, tank2 = _time_and_values(rows, "tank2_temp_c")
    _, tank1_target = _time_and_values(rows, "tank1_target_temp_c")
    _, tank2_target = _time_and_values(rows, "tank2_target_temp_c")
    if xs:
        ax1.plot(xs, tank1, label="Tank 1 Temp", color="#d62728")
        ax1.plot(xs, tank2, label="Tank 2 Temp", color="#1f77b4")
        if tank1_target:
            ax1.plot(xs[: len(tank1_target)], tank1_target, "--", color="#ff9896", label="Tank 1 Target")
        if tank2_target:
            ax1.plot(xs[: len(tank2_target)], tank2_target, "--", color="#9ecae1", label="Tank 2 Target")
    ax2 = ax1.twinx()
    _, heater1_on = _time_and_bool(rows, "heater1_on")
    _, heater2_on = _time_and_bool(rows, "heater2_on")
    if xs:
        if heater1_on:
            ax2.step(xs[: len(heater1_on)], heater1_on, where="post", alpha=0.35, label="Heater 1 On", color="#ff7f0e")
        if heater2_on:
            ax2.step(xs[: len(heater2_on)], heater2_on, where="post", alpha=0.35, label="Heater 2 On", color="#2ca02c")
    ax2.set_ylim(-0.05, 1.05)
    ax2.set_ylabel("Heater State")
    _finish_time_axis(ax1, "Figure 4.3 - Temperature Response During Heating", "Temperature (C)")
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    if handles1 or handles2:
        ax1.legend(handles1 + handles2, labels1 + labels2, loc="best", fontsize=8)
    path = out_dir / "figure_4_3_temperature_vs_time_heating.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_4(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_4_ambient_temp_humidity_vs_time.csv")
    if not rows:
        return None
    fig, ax1 = plt.subplots(figsize=(11, 6))
    xs, temp = _time_and_values(rows, "temperature_c")
    _, hum = _time_and_values(rows, "humidity_pct")
    if xs:
        ax1.plot(xs, temp, color="#d62728", label="Ambient Temperature")
    ax1.set_ylabel("Ambient Temperature (C)", color="#d62728")
    ax1.tick_params(axis="y", labelcolor="#d62728")
    ax2 = ax1.twinx()
    if xs:
        ax2.plot(xs, hum, color="#1f77b4", label="Ambient Humidity")
    ax2.set_ylabel("Humidity (%)", color="#1f77b4")
    ax2.tick_params(axis="y", labelcolor="#1f77b4")
    _finish_time_axis(ax1, "Figure 4.4 - Ambient Temperature and Humidity vs Time", "Ambient Temperature (C)")
    path = out_dir / "figure_4_4_ambient_temp_humidity_vs_time.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_5(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_5_ultrasonic_vs_level.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "tank_name")
    fig, axes = plt.subplots(len(grouped), 1, figsize=(11, 4 * max(1, len(grouped))), sharex=True)
    axes = _axes_list(axes)
    for ax, (tank_name, tank_rows) in zip(axes, sorted(grouped.items())):
        xs, distance = _time_and_values(tank_rows, "ultrasonic_distance_cm")
        _, level_pct = _time_and_values(tank_rows, "level_pct")
        if xs:
            ax.plot(xs, distance, color="#1f77b4", label=f"{tank_name} Distance")
        ax.set_ylabel("Distance (cm)")
        ax2 = ax.twinx()
        if xs:
            ax2.plot(xs[: len(level_pct)], level_pct, color="#d62728", label=f"{tank_name} Level")
        ax2.set_ylabel("Level (%)")
        ax.set_title(f"Figure 4.5 - Ultrasonic vs Level ({tank_name})")
        ax.grid(True, alpha=0.3)
        handles1, labels1 = ax.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        if handles1 or handles2:
            ax.legend(handles1 + handles2, labels1 + labels2, loc="best", fontsize=8)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
    axes[-1].tick_params(axis="x", rotation=20)
    axes[-1].set_xlabel("Time (UTC)")
    path = out_dir / "figure_4_5_ultrasonic_vs_level.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_6(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_6_power_vs_time_per_load.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "load_name")
    load_names = sorted(grouped.keys())
    cols = 2
    rows_count = math.ceil(len(load_names) / cols)
    fig, axes = plt.subplots(rows_count, cols, figsize=(14, max(4, rows_count * 3.5)), sharex=True)
    axes_flat = list(getattr(axes, "flat", [axes]))
    for ax, load_name in zip(axes_flat, load_names):
        load_rows = grouped[load_name]
        xs, power = _time_and_values(load_rows, "power_w")
        _, duty_applied = _time_and_values(load_rows, "duty_applied")
        _, on_state = _time_and_bool(load_rows, "on")
        if xs:
            ax.plot(xs, power, color="#1f77b4", label="Power")
        ax.set_title(load_name)
        ax.set_ylabel("Power (W)")
        ax.grid(True, alpha=0.3)
        ax2 = ax.twinx()
        if xs and duty_applied:
            ax2.plot(xs[: len(duty_applied)], duty_applied, color="#ff7f0e", alpha=0.8, label="Duty Applied")
        if xs and on_state:
            ax2.step(xs[: len(on_state)], on_state, where="post", color="#2ca02c", alpha=0.45, label="On")
        ax2.set_ylim(-0.05, 1.05)
        ax2.set_ylabel("Duty / On")
        handles1, labels1 = ax.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        if handles1 or handles2:
            ax.legend(handles1 + handles2, labels1 + labels2, loc="best", fontsize=7)
    for ax in axes_flat[len(load_names):]:
        ax.remove()
    for ax in axes_flat[: len(load_names)]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.tick_params(axis="x", rotation=20)
    fig.suptitle("Figure 4.6 - Real-Time Power Consumption of Individual Loads", y=1.02)
    path = out_dir / "figure_4_6_power_vs_time_per_load.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_7(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_7_total_system_power.csv")
    if not rows:
        return None
    fig, ax = plt.subplots(figsize=(11, 6))
    xs, power = _time_and_values(rows, "total_power_w")
    if xs:
        ax.plot(xs, power, color="#1f77b4")
    _finish_time_axis(ax, "Figure 4.7 - Total System Power", "Total Power (W)")
    path = out_dir / "figure_4_7_total_system_power.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_8(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_8_power_profiles_by_load_type.csv")
    if not rows:
        return None
    by_type_ts: Dict[str, Dict[datetime, float]] = defaultdict(lambda: defaultdict(float))
    for row in rows:
        dt = row.get("_dt")
        load_type = str(row.get("load_type", "unknown"))
        power_w = _safe_float(row.get("power_w"))
        if dt is None or power_w is None:
            continue
        by_type_ts[load_type][dt] += power_w
    fig, ax = plt.subplots(figsize=(11, 6))
    for load_type, values in sorted(by_type_ts.items()):
        xs = sorted(values.keys())
        ys = [values[x] for x in xs]
        if xs:
            ax.plot(xs, ys, label=load_type)
    _finish_time_axis(ax, "Figure 4.8 - Power Profiles by Load Type", "Power (W)")
    if ax.has_data():
        ax.legend(loc="best", fontsize=8)
    path = out_dir / "figure_4_8_power_profiles_by_load_type.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_9(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_9_cumulative_energy_over_time.csv")
    if not rows:
        return None
    fig, ax1 = plt.subplots(figsize=(11, 6))
    xs, energy = _time_and_values(rows, "total_energy_wh")
    _, total_cost = _time_and_values(rows, "total_cost")
    if xs:
        ax1.plot(xs, energy, color="#1f77b4", label="Total Energy")
    ax1.set_ylabel("Energy (Wh)", color="#1f77b4")
    ax1.tick_params(axis="y", labelcolor="#1f77b4")
    ax2 = ax1.twinx()
    if xs and total_cost:
        ax2.plot(xs[: len(total_cost)], total_cost, color="#d62728", label="Total Cost")
    ax2.set_ylabel("Cost", color="#d62728")
    ax2.tick_params(axis="y", labelcolor="#d62728")
    _finish_time_axis(ax1, "Figure 4.9 - Cumulative Energy Over Time", "Energy (Wh)")
    path = out_dir / "figure_4_9_cumulative_energy_over_time.png"
    _save_fig(fig, path)
    return path.name


def _plot_prediction_vs_actual(export_dir: Path, out_dir: Path, csv_name: str, png_name: str, title: str) -> Optional[str]:
    rows = _load_csv(export_dir / csv_name)
    if not rows:
        return None
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(11, 10), sharex=True, height_ratios=[2, 1.35, 1.2])
    xs, actual = _time_and_values(rows, "actual_rolling_avg_power_w")
    pred_xs, predicted = _time_and_values(rows, "predicted_rolling_avg_power_w")
    if xs:
        _plot_scatter_with_smooth(ax1, xs, actual, label="Actual Rolling Avg Power", color="#1f77b4", scatter_size=12.0)
    if pred_xs and predicted:
        _plot_scatter_with_smooth(ax1, pred_xs, predicted, label="Predicted Rolling Avg Power", color="#d62728", scatter_size=12.0)
    _finish_time_axis(ax1, title, "Average Power (W)", "")
    if ax1.has_data():
        ax1.legend(loc="best")

    energy_actual_xs, actual_rolling_energy = _time_and_values(rows, "actual_rolling_energy_wh")
    pred_energy_xs, predicted_rolling_energy = _time_and_values(rows, "predicted_rolling_energy_wh")
    if energy_actual_xs and actual_rolling_energy:
        _plot_scatter_with_smooth(ax2, energy_actual_xs, actual_rolling_energy, label="Actual Rolling Energy", color="#2ca02c")
    if pred_energy_xs and predicted_rolling_energy:
        _plot_scatter_with_smooth(ax2, pred_energy_xs, predicted_rolling_energy, label="Predicted Rolling Energy", color="#9467bd")
    _finish_time_axis(ax2, "", "Energy (Wh)", "")
    if ax2.has_data():
        ax2.legend(loc="best")

    cost_actual_xs, actual_rolling_cost = _time_and_values(rows, "actual_rolling_cost")
    pred_cost_xs, predicted_rolling_cost = _time_and_values(rows, "predicted_rolling_cost")
    if cost_actual_xs and actual_rolling_cost:
        _plot_scatter_with_smooth(ax3, cost_actual_xs, actual_rolling_cost, label="Actual Rolling Cost", color="#8c564b")
    if pred_cost_xs and predicted_rolling_cost:
        _plot_scatter_with_smooth(ax3, pred_cost_xs, predicted_rolling_cost, label="Predicted Rolling Cost", color="#e377c2")
    _finish_time_axis(ax3, "", "Cost")
    if ax3.has_data():
        ax3.legend(loc="best")
    path = out_dir / png_name
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_13_14_combined(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows_13 = _load_csv(export_dir / "figure_4_13_load_states_priority_curtailment.csv")
    rows_14 = _load_csv(export_dir / "figure_4_14_total_demand_with_control_response.csv")
    if not rows_13 or not rows_14:
        return None
    selected_loads = _choose_representative_loads(rows_13)
    grouped_13 = _group_rows(rows_13, "load_name")

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9), sharex=True, height_ratios=[2, 2])
    xs, total_power = _time_and_values(rows_14, "total_power_w")
    _, max_power = _time_and_values(rows_14, "max_total_power_w")
    _, predicted = _time_and_values(rows_14, "predictor_predicted_power_w")
    if xs:
        ax1.plot(xs, total_power, color="#1f77b4", label="Total Power")
    if xs and max_power:
        ax1.plot(xs[: len(max_power)], max_power, "--", color="#d62728", label="Max Threshold")
    if xs and predicted:
        ax1.plot(xs[: len(predicted)], predicted, ":", color="#9467bd", label="Predicted Power")
    ax1.set_ylabel("Power (W)")
    ax1.set_title("Combined Figure 4.13 / 4.14 - Demand Response and Priority Curtailment")
    ax1.grid(True, alpha=0.3)
    if ax1.has_data():
        ax1.legend(loc="best", fontsize=8)

    for load_name in selected_loads:
        load_rows = grouped_13.get(load_name, [])
        load_xs, duty_applied = _time_and_values(load_rows, "duty_applied")
        if load_xs and duty_applied:
            ax2.step(load_xs, duty_applied, where="post", label=f"{load_name} duty", linewidth=1.6)
            continue
        load_xs, on_state = _time_and_bool(load_rows, "on")
        if load_xs:
            ax2.step(load_xs, on_state, where="post", label=f"{load_name} on", linewidth=1.6)
    ax2.set_ylabel("Duty / On")
    ax2.set_ylim(-0.05, 1.05)
    ax2.grid(True, alpha=0.3)
    ax2.set_xlabel("Time (UTC)")
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
    ax2.tick_params(axis="x", rotation=20)
    if ax2.has_data():
        ax2.legend(loc="best", fontsize=8, ncol=2)
    path = out_dir / "figure_4_13_4_14_combined.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_15(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_15_voltage_drop_with_control_response.csv")
    if not rows:
        return None
    fig, ax1 = plt.subplots(figsize=(11, 6))
    xs, supply_v = _time_and_values(rows, "supply_v")
    _, total_power = _time_and_values(rows, "total_power_w")
    _, uv_active = _time_and_bool(rows, "undervoltage_active")
    _, ov_active = _time_and_bool(rows, "overvoltage_active")
    if xs:
        ax1.plot(xs, supply_v, color="#1f77b4", label="Supply Voltage")
    ax1.set_ylabel("Supply Voltage (V)")
    ax2 = ax1.twinx()
    if xs and total_power:
        ax2.plot(xs[: len(total_power)], total_power, color="#d62728", label="Total Power")
    if xs and uv_active:
        ax2.step(xs[: len(uv_active)], uv_active, where="post", color="#ff7f0e", alpha=0.4, label="UV Active")
    if xs and ov_active:
        ax2.step(xs[: len(ov_active)], ov_active, where="post", color="#2ca02c", alpha=0.4, label="OV Active")
    ax2.set_ylabel("Power / Protection State")
    _finish_time_axis(ax1, "Figure 4.15 - Voltage Profile with Protection Response", "Supply Voltage (V)")
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    if handles1 or handles2:
        ax1.legend(handles1 + handles2, labels1 + labels2, loc="best", fontsize=8)
    path = out_dir / "figure_4_15_voltage_drop_with_control_response.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_16(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_16_per_load_current_protection_response.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "load_name")
    load_names = sorted(grouped.keys())
    cols = 2
    rows_count = math.ceil(len(load_names) / cols)
    fig, axes = plt.subplots(rows_count, cols, figsize=(14, max(4, rows_count * 3.5)), sharex=True)
    axes_flat = list(getattr(axes, "flat", [axes]))
    for ax, load_name in zip(axes_flat, load_names):
        load_rows = grouped[load_name]
        xs, current_a = _time_and_values(load_rows, "current_a")
        _, fault_limit = _time_and_values(load_rows, "fault_limit_a")
        _, fault_active = _time_and_bool(load_rows, "fault_active")
        if xs:
            ax.plot(xs, current_a, color="#1f77b4", label="Current")
        if xs and fault_limit:
            ax.plot(xs[: len(fault_limit)], fault_limit, "--", color="#d62728", label="Fault Limit")
        ax.set_title(load_name)
        ax.set_ylabel("Current (A)")
        ax.grid(True, alpha=0.3)
        ax2 = ax.twinx()
        if xs and fault_active:
            ax2.step(xs[: len(fault_active)], fault_active, where="post", color="#ff7f0e", alpha=0.4, label="Fault Active")
        ax2.set_ylim(-0.05, 1.05)
        handles1, labels1 = ax.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        if handles1 or handles2:
            ax.legend(handles1 + handles2, labels1 + labels2, loc="best", fontsize=7)
    for ax in axes_flat[len(load_names):]:
        ax.remove()
    for ax in axes_flat[: len(load_names)]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))
        ax.tick_params(axis="x", rotation=20)
    fig.suptitle("Figure 4.16 - Per-Load Current and Protection Response", y=1.02)
    path = out_dir / "figure_4_16_per_load_current_protection_response.png"
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_18(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_18_power_profiles_under_policies.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "control_policy")
    policies = [name for name in sorted(grouped.keys()) if name]
    if not policies:
        return None
    fig, axes = plt.subplots(len(policies), 1, figsize=(11, max(4, len(policies) * 3)), sharex=True)
    axes = _axes_list(axes)
    for ax, policy in zip(axes, policies):
        xs, power = _time_and_values(grouped[policy], "total_power_w")
        if xs:
            ax.plot(xs, power, label=policy)
        ax.set_ylabel("Power (W)")
        ax.set_title(policy)
        ax.grid(True, alpha=0.3)
        if ax.has_data():
            ax.legend(loc="best", fontsize=8)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
    axes[-1].tick_params(axis="x", rotation=20)
    axes[-1].set_xlabel("Time (UTC)")
    path = out_dir / "figure_4_18_power_profiles_under_policies.png"
    _save_fig(fig, path)
    return path.name


def _plot_bar_chart(export_dir: Path, out_dir: Path, csv_name: str, png_name: str, title: str, value_key: str, ylabel: str) -> Optional[str]:
    rows = _load_csv(export_dir / csv_name)
    if not rows:
        return None
    labels, values = [], []
    for row in rows:
        label = str(row.get("control_policy", "")).strip()
        value = _safe_float(row.get(value_key))
        if not label or value is None:
            continue
        labels.append(label)
        values.append(value)
    if not labels:
        return None
    fig, ax = plt.subplots(figsize=(9, 5))
    bars = ax.bar(labels, values, color=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"][: len(labels)])
    ax.set_title(title)
    ax.set_ylabel(ylabel)
    ax.grid(True, axis="y", alpha=0.3)
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2.0, value, f"{value:.2f}", ha="center", va="bottom", fontsize=8)
    path = out_dir / png_name
    _save_fig(fig, path)
    return path.name


def _plot_figure_4_21(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "figure_4_21_integrated_system_operation_timeline.csv")
    if not rows:
        return None
    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True, height_ratios=[2, 2, 1.6])
    xs, total_power = _time_and_values(rows, "total_power_w")
    _, supply_v = _time_and_values(rows, "supply_v")
    _, pred_power = _time_and_values(rows, "predictor_predicted_power_w")
    _, active_loads = _time_and_values(rows, "active_load_count")
    _, uv = _time_and_bool(rows, "undervoltage_active")
    _, ov = _time_and_bool(rows, "overvoltage_active")
    if xs:
        axes[0].plot(xs, total_power, label="Total Power", color="#1f77b4")
        axes[0].plot(xs[: len(supply_v)], supply_v, label="Supply Voltage", color="#d62728")
        axes[0].set_ylabel("Power / Voltage")
        axes[0].legend(loc="best", fontsize=8)
    axes[0].grid(True, alpha=0.3)
    if xs and pred_power:
        axes[1].plot(xs[: len(pred_power)], pred_power, label="Predicted Power", color="#9467bd")
    if xs and uv:
        axes[1].step(xs[: len(uv)], uv, where="post", label="UV Active", color="#ff7f0e", alpha=0.45)
    if xs and ov:
        axes[1].step(xs[: len(ov)], ov, where="post", label="OV Active", color="#2ca02c", alpha=0.45)
    axes[1].set_ylabel("Prediction / Protection")
    axes[1].grid(True, alpha=0.3)
    if axes[1].has_data():
        axes[1].legend(loc="best", fontsize=8)
    if xs and active_loads:
        axes[2].step(xs[: len(active_loads)], active_loads, where="post", color="#1f77b4", label="Active Loads")
    axes[2].set_ylabel("Count")
    axes[2].grid(True, alpha=0.3)
    if axes[2].has_data():
        axes[2].legend(loc="best", fontsize=8)
    axes[2].set_xlabel("Time (UTC)")
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%d/%m %H:%M"))
    axes[2].tick_params(axis="x", rotation=20)
    fig.suptitle("Figure 4.21 - Integrated System Operation Timeline", y=1.02)
    path = out_dir / "figure_4_21_integrated_system_operation_timeline.png"
    _save_fig(fig, path)
    return path.name


def _plot_accumulated_cost(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "accumulated_cost_over_time.csv")
    if not rows:
        return None
    fig, ax = plt.subplots(figsize=(11, 6))
    xs, energy_cost = _time_and_values(rows, "accumulated_energy_cost")
    _, demand_charge = _time_and_values(rows, "demand_charge_cost")
    _, total_cost = _time_and_values(rows, "total_cost")
    if xs and energy_cost:
        ax.plot(xs[: len(energy_cost)], energy_cost, label="Accumulated Energy Cost", color="#1f77b4")
    if xs and demand_charge:
        ax.plot(xs[: len(demand_charge)], demand_charge, label="Demand Charge", color="#ff7f0e")
    if xs and total_cost:
        ax.plot(xs[: len(total_cost)], total_cost, label="Total Cost", color="#d62728", linewidth=2.0)
    _finish_time_axis(ax, "Accumulated Cost Over Time", "Cost")
    if ax.has_data():
        ax.legend(loc="best", fontsize=8)
    path = out_dir / "accumulated_cost_over_time.png"
    _save_fig(fig, path)
    return path.name


def _plot_rolling_window_energy_cost(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "rolling_window_energy_cost.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "window_label")
    fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
    plotted = False
    for window_label, window_rows in sorted(grouped.items(), key=lambda item: (_safe_float(item[1][0].get("window_minutes")) or 0.0, item[0])):
        xs_energy, ys_energy = _time_and_values(window_rows, "rolling_energy_wh")
        _, ys_cost = _time_and_values(window_rows, "rolling_cost")
        if xs_energy and ys_energy:
            axes[0].plot(xs_energy, ys_energy, linewidth=1.5, label=window_label)
            plotted = True
        if xs_energy and ys_cost:
            axes[1].plot(xs_energy, ys_cost, linewidth=1.5, label=window_label)
            plotted = True
    if not plotted:
        plt.close(fig)
        return None
    _finish_time_axis(axes[0], "Rolling-Window Energy Consumption", "Energy (Wh)", xlabel="")
    _finish_time_axis(axes[1], "Rolling-Window Energy Cost", "Cost", xlabel="Time (UTC)")
    axes[0].legend(loc="best", fontsize=8, ncol=2)
    axes[1].legend(loc="best", fontsize=8, ncol=2)
    path = out_dir / "rolling_window_energy_cost.png"
    _save_fig(fig, path)
    return path.name


def _plot_rolling_energy_consumption(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "rolling_energy_consumption.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "window_label")
    fig, ax = plt.subplots(figsize=(12, 6))
    plotted = False
    for window_label, window_rows in sorted(grouped.items(), key=lambda item: (_safe_float(item[1][0].get("window_minutes")) or 0.0, item[0])):
        xs, ys = _time_and_values(window_rows, "rolling_energy_wh")
        if xs and ys:
            ax.plot(xs, ys, linewidth=1.5, label=window_label)
            plotted = True
    if not plotted:
        plt.close(fig)
        return None
    _finish_time_axis(ax, "Rolling Energy Consumption", "Energy (Wh)")
    ax.legend(loc="best", fontsize=8, ncol=2)
    path = out_dir / "rolling_energy_consumption.png"
    _save_fig(fig, path)
    return path.name


def _plot_rolling_cost_consumption(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "rolling_cost_consumption.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "window_label")
    fig, ax = plt.subplots(figsize=(12, 6))
    plotted = False
    for window_label, window_rows in sorted(grouped.items(), key=lambda item: (_safe_float(item[1][0].get("window_minutes")) or 0.0, item[0])):
        xs, ys = _time_and_values(window_rows, "rolling_cost")
        if xs and ys:
            ax.plot(xs, ys, linewidth=1.5, label=window_label)
            plotted = True
    if not plotted:
        plt.close(fig)
        return None
    _finish_time_axis(ax, "Rolling Cost Consumption", "Cost")
    ax.legend(loc="best", fontsize=8, ncol=2)
    path = out_dir / "rolling_cost_consumption.png"
    _save_fig(fig, path)
    return path.name


def _first_row_per_timestamp(rows: Sequence[dict]) -> List[dict]:
    seen = set()
    unique: List[dict] = []
    for row in rows:
        ts = row.get("ts")
        if ts in seen:
            continue
        seen.add(ts)
        unique.append(row)
    return unique


def _plot_rolling_and_cumulative_energy(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "rolling_and_cumulative_energy.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "window_label")
    cumulative_rows = _first_row_per_timestamp(rows)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True, height_ratios=[1.6, 1.0])
    plotted = False
    for window_label, window_rows in sorted(grouped.items(), key=lambda item: (_safe_float(item[1][0].get("window_minutes")) or 0.0, item[0])):
        xs, ys = _time_and_values(window_rows, "rolling_energy_wh")
        if xs and ys:
            ax1.plot(xs, ys, linewidth=1.5, label=window_label)
            plotted = True
    xs_total, ys_total = _time_and_values(cumulative_rows, "total_energy_wh")
    if xs_total and ys_total:
        ax2.plot(xs_total, ys_total, color="#1f77b4", linewidth=2.0, label="Cumulative Energy")
        plotted = True
    if not plotted:
        plt.close(fig)
        return None
    _finish_time_axis(ax1, "Rolling Energy Consumption", "Energy (Wh)", "")
    _finish_time_axis(ax2, "Cumulative Energy Consumption", "Energy (Wh)")
    if ax1.has_data():
        ax1.legend(loc="best", fontsize=8, ncol=2)
    if ax2.has_data():
        ax2.legend(loc="best", fontsize=8)
    path = out_dir / "rolling_and_cumulative_energy.png"
    _save_fig(fig, path)
    return path.name


def _plot_rolling_and_cumulative_cost(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "rolling_and_cumulative_cost.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "window_label")
    cumulative_rows = _first_row_per_timestamp(rows)
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True, height_ratios=[1.6, 1.0])
    plotted = False
    for window_label, window_rows in sorted(grouped.items(), key=lambda item: (_safe_float(item[1][0].get("window_minutes")) or 0.0, item[0])):
        xs, ys = _time_and_values(window_rows, "rolling_cost")
        if xs and ys:
            ax1.plot(xs, ys, linewidth=1.5, label=window_label)
            plotted = True
    xs_total, ys_total = _time_and_values(cumulative_rows, "total_cost")
    xs_energy_cost, ys_energy_cost = _time_and_values(cumulative_rows, "accumulated_energy_cost")
    if xs_total and ys_total:
        ax2.plot(xs_total, ys_total, color="#d62728", linewidth=2.0, label="Cumulative Total Cost")
        plotted = True
    if xs_energy_cost and ys_energy_cost:
        ax2.plot(xs_energy_cost, ys_energy_cost, color="#ff9896", linewidth=1.6, linestyle="--", label="Cumulative Energy Cost")
        plotted = True
    if not plotted:
        plt.close(fig)
        return None
    _finish_time_axis(ax1, "Rolling Cost Consumption", "Cost", "")
    _finish_time_axis(ax2, "Cumulative Cost", "Cost")
    if ax1.has_data():
        ax1.legend(loc="best", fontsize=8, ncol=2)
    if ax2.has_data():
        ax2.legend(loc="best", fontsize=8)
    path = out_dir / "rolling_and_cumulative_cost.png"
    _save_fig(fig, path)
    return path.name


def _plot_per_load_rolling_energy(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "per_load_rolling_energy.csv")
    if not rows:
        return None
    available_labels = sorted({str(row.get("window_label") or "") for row in rows if str(row.get("window_label") or "")})
    preferred_label = "30min" if "30min" in available_labels else (available_labels[0] if available_labels else "")
    filtered = [row for row in rows if str(row.get("window_label") or "") == preferred_label]
    if not filtered:
        return None
    grouped = _group_rows(filtered, "load_name")
    fig, ax = plt.subplots(figsize=(12, 6))
    plotted = False
    for load_name, load_rows in sorted(grouped.items()):
        xs, ys = _time_and_values(load_rows, "rolling_energy_wh")
        if xs and ys:
            ax.plot(xs, ys, linewidth=1.5, label=load_name)
            plotted = True
    if not plotted:
        plt.close(fig)
        return None
    label_title = preferred_label.replace("min", " min").replace("h", " h").replace("d", " d").replace("w", " w")
    _finish_time_axis(ax, f"Per-Load Rolling Energy Consumption ({label_title})", "Energy (Wh)")
    ax.legend(loc="best", fontsize=8, ncol=2)
    path = out_dir / "per_load_rolling_energy.png"
    _save_fig(fig, path)
    return path.name


def _plot_heatmap_matrix(labels: List[str], matrix: np.ndarray, title: str, path: Path) -> str:
    fig, ax = plt.subplots(figsize=(max(7, len(labels) * 0.8), max(6, len(labels) * 0.6)))
    image = ax.imshow(matrix, cmap="coolwarm", vmin=-1.0, vmax=1.0)
    ax.set_title(title)
    ax.set_xticks(range(len(labels)))
    ax.set_yticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=45, ha="right", fontsize=8)
    ax.set_yticklabels(labels, fontsize=8)
    for i in range(len(labels)):
        for j in range(len(labels)):
            ax.text(j, i, f"{matrix[i, j]:.2f}", ha="center", va="center", fontsize=6, color="black")
    fig.colorbar(image, ax=ax, fraction=0.046, pad=0.04, label="Correlation")
    _save_fig(fig, path)
    return path.name


def _plot_model_correlation_heatmap(export_dir: Path, out_dir: Path, suffix: str, png_name: str, title: str) -> Optional[str]:
    analytics = _load_json(_find_analytics_file(export_dir, suffix))
    corr_payload = (analytics or {}).get("correlation_heatmap", {}) if isinstance(analytics, dict) else {}
    raw_labels = corr_payload.get("labels") if isinstance(corr_payload, dict) else None
    raw_matrix = corr_payload.get("matrix") if isinstance(corr_payload, dict) else None
    if not isinstance(raw_labels, list) or not isinstance(raw_matrix, list):
        return None
    try:
        labels = [str(item) for item in raw_labels]
        matrix = np.asarray(raw_matrix, dtype=float)
    except Exception:
        return None
    if matrix.ndim != 2 or matrix.shape[0] != matrix.shape[1] or matrix.shape[0] != len(labels):
        return None
    matrix = np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)
    return _plot_heatmap_matrix(labels, matrix, title, out_dir / png_name)


def _plot_core_correlation_heatmap(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "system_timeseries_master.csv")
    candidate_fields = [
        "total_power_w",
        "total_current_a",
        "supply_v",
        "total_energy_wh",
        "max_total_power_w",
        "process_elapsed_sec",
        "process_cycle_count",
        "predictor_predicted_power_w",
        "short_predicted_power_w",
        "rf_predicted_power_w",
        "lstm_predicted_power_w",
        "accumulated_energy_cost",
        "total_cost",
        "shedding_event_count",
        "curtail_event_count",
        "load_toggle_count",
    ]
    selected_fields: List[str] = []
    for field in candidate_fields:
        values = np.array([_safe_float(row.get(field)) for row in rows], dtype=object)
        finite_values = np.array([float(v) for v in values if v is not None], dtype=float)
        if finite_values.size < 12:
            continue
        if float(np.std(finite_values)) <= 0.0:
            continue
        selected_fields.append(field)
        if len(selected_fields) >= 10:
            break
    if len(selected_fields) < 2:
        return None
    matrix_rows: List[List[float]] = []
    for row in rows:
        values = [_safe_float(row.get(field)) for field in selected_fields]
        if any(value is None for value in values):
            continue
        matrix_rows.append([float(value) for value in values])
    if len(matrix_rows) < 8:
        return None
    matrix = np.corrcoef(np.asarray(matrix_rows, dtype=float), rowvar=False)
    matrix = np.nan_to_num(matrix, nan=0.0, posinf=0.0, neginf=0.0)
    return _plot_heatmap_matrix(selected_fields, matrix, "Correlation Heatmap - Core System Metrics", out_dir / "correlation_heatmap.png")


def _plot_ai_feature_importance(export_dir: Path, out_dir: Path) -> Optional[str]:
    rf_analytics = _load_json(_find_analytics_file(export_dir, "_long_rf.analytics.json"))
    lstm_analytics = _load_json(_find_analytics_file(export_dir, "_long_lstm.analytics.json"))
    payloads = []
    if isinstance(rf_analytics, dict) and rf_analytics.get("top_features"):
        payloads.append(("Random Forest", rf_analytics))
    if isinstance(lstm_analytics, dict) and lstm_analytics.get("top_features"):
        payloads.append(("LSTM", lstm_analytics))
    if not payloads:
        return None

    fig, axes = plt.subplots(len(payloads), 1, figsize=(12, max(4.5, len(payloads) * 4.5)))
    axes_list = _axes_list(axes)
    for ax, (title, payload) in zip(axes_list, payloads):
        top_features = list(payload.get("top_features", []))[:12]
        if not top_features:
            continue
        labels = [str(item.get("feature", "unknown")) for item in top_features][::-1]
        values = [float(item.get("importance", 0.0) or 0.0) for item in top_features][::-1]
        ax.barh(labels, values, color="#1f77b4" if title == "Random Forest" else "#d62728")
        ax.set_title(f"{title} Feature Importance")
        ax.set_xlabel("Relative Importance")
        ax.grid(True, axis="x", alpha=0.3)
        for index, value in enumerate(values):
            ax.text(value, index, f" {value:.3f}", va="center", fontsize=8)
    fig.suptitle("AI Model Feature Importance", y=1.01)
    path = out_dir / "ai_model_feature_importance.png"
    _save_fig(fig, path)
    return path.name


def _plot_single_model_metrics(export_dir: Path, out_dir: Path, csv_name: str, png_name: str, title: str) -> Optional[str]:
    rows = _load_csv(export_dir / csv_name)
    if not rows:
        return None
    row = rows[0]
    mae = _safe_float(row.get("mae_wh"))
    mse = _safe_float(row.get("mse_wh2"))
    rmse = _safe_float(row.get("rmse_wh"))
    error_unit = "Wh"
    mse_unit = "Wh^2"
    if mae is None and mse is None and rmse is None:
        mae = _safe_float(row.get("mae_w"))
        mse = _safe_float(row.get("mse_w2"))
        rmse = _safe_float(row.get("rmse_w"))
        error_unit = "W"
        mse_unit = "W^2"
    r_squared = _safe_float(row.get("r_squared"))
    lag_sec = _safe_float(row.get("lag_sec"))
    sample_count = _safe_float(row.get("sample_count"))
    horizon_sec = _safe_float(row.get("horizon_sec"))
    lag_step_sec = _safe_float(row.get("lag_step_sec"))
    comparison_group = str(row.get("comparison_group") or "").strip()
    if mae is None and mse is None and rmse is None and r_squared is None and lag_sec is None:
        return None

    labels: List[str] = []
    values: List[float] = []
    colors: List[str] = []
    if mae is not None:
        labels.append(f"MAE ({error_unit})")
        values.append(mae)
        colors.append("#1f77b4")
    if mse is not None:
        labels.append(f"MSE ({mse_unit})")
        values.append(mse)
        colors.append("#ff7f0e")
    if rmse is not None:
        labels.append(f"RMSE ({error_unit})")
        values.append(rmse)
        colors.append("#d62728")
    if r_squared is not None:
        labels.append("R^2")
        values.append(r_squared)
        colors.append("#9467bd")
    if lag_sec is not None:
        labels.append("Lag (s)")
        values.append(abs(lag_sec))
        colors.append("#2ca02c")
    if not values:
        return None

    fig, ax = plt.subplots(figsize=(8, 4.8))
    bars = ax.bar(labels, values, color=colors)
    ax.set_title(title)
    ax.set_ylabel("Metric Value")
    ax.grid(True, axis="y", alpha=0.3)
    subtitle = []
    if sample_count is not None:
        subtitle.append(f"samples={int(sample_count)}")
    if horizon_sec is not None and horizon_sec > 0.0:
        subtitle.append(f"horizon={int(round(horizon_sec / 60.0))}m")
    if comparison_group:
        subtitle.append(comparison_group)
    raw_lag = row.get("lag_sec")
    if raw_lag not in (None, ""):
        subtitle.append(f"lag_signed={raw_lag}")
    if lag_step_sec is not None and lag_step_sec > 0.0:
        subtitle.append(f"lag_step={int(round(lag_step_sec))}s")
    if subtitle:
        ax.text(0.99, 0.98, "  ".join(subtitle), transform=ax.transAxes, ha="right", va="top", fontsize=8)
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2.0, value, f"{value:.3f}", ha="center", va="bottom", fontsize=8)
    path = out_dir / png_name
    _save_fig(fig, path)
    return path.name


def _plot_policy_control_actions(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "policy_control_action_comparison.csv")
    if not rows:
        return None
    labels = [str(row.get("control_policy", "")).strip() for row in rows if str(row.get("control_policy", "")).strip()]
    if not labels:
        return None
    shed = [_safe_float(row.get("shedding_event_count")) or 0.0 for row in rows]
    curtail = [_safe_float(row.get("curtail_event_count")) or 0.0 for row in rows]
    restore = [_safe_float(row.get("restore_event_count")) or 0.0 for row in rows]
    total = [_safe_float(row.get("total_control_actions")) or 0.0 for row in rows]
    x = np.arange(len(labels))
    width = 0.2
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.bar(x - 1.5 * width, shed, width, label="Shed", color="#1f77b4")
    ax.bar(x - 0.5 * width, curtail, width, label="Curtail", color="#ff7f0e")
    ax.bar(x + 0.5 * width, restore, width, label="Restore", color="#2ca02c")
    ax.bar(x + 1.5 * width, total, width, label="Total Actions", color="#d62728")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15)
    ax.set_ylabel("Count")
    ax.set_title("Policy Control-Action Comparison")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    path = out_dir / "policy_control_action_comparison.png"
    _save_fig(fig, path)
    return path.name


def _plot_forecast_informed_control_response(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "forecast_informed_control_response.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "control_policy")
    order = [policy for policy in ("NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED") if policy in grouped]
    if not order:
        order = sorted(grouped.keys())
    if not order:
        return None
    fig, axes = plt.subplots(len(order), 1, figsize=(12, max(4.8, 3.1 * len(order))), sharex=False)
    axes_list = _axes_list(axes)
    for ax, policy in zip(axes_list, order):
        policy_rows = grouped.get(policy) or []
        xs_actual, actual = _x_and_values(policy_rows, "elapsed_min", "actual_power_w")
        xs_pred, predicted = _x_and_values(policy_rows, "elapsed_min", "predicted_power_w")
        xs_threshold, threshold = _x_and_values(policy_rows, "elapsed_min", "control_threshold_w")
        xs_action, action = _x_and_values(policy_rows, "elapsed_min", "control_action_delta")
        if xs_actual:
            ax.plot(xs_actual, actual, label="Actual Demand", color="#1f77b4", linewidth=1.7)
        if xs_pred:
            ax.plot(xs_pred, predicted, label="Predicted Demand", color="#d62728", linewidth=1.5)
        if xs_threshold:
            ax.plot(xs_threshold, threshold, label="Threshold", color="#2ca02c", linestyle="--", linewidth=1.3)
        ax2 = ax.twinx()
        if xs_action:
            ax2.step(xs_action, action, where="post", label="Control Action", color="#9467bd", alpha=0.45)
        ax.set_title(f"Forecast-Informed Control Response - {policy}")
        ax.set_ylabel("Power (W)")
        ax2.set_ylabel("Action Delta")
        ax.set_xlabel("Elapsed Time (min)")
        ax.grid(True, alpha=0.3)
        handles, labels = ax.get_legend_handles_labels()
        handles2, labels2 = ax2.get_legend_handles_labels()
        if handles or handles2:
            ax.legend(handles + handles2, labels + labels2, loc="upper right", fontsize=8)
    path = out_dir / "forecast_informed_control_response.png"
    _save_fig(fig, path)
    return path.name


def _plot_rolling_energy_comparison(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "rolling_energy_consumption_comparison.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "control_policy")
    fig, ax = plt.subplots(figsize=(11.5, 6))
    plotted = False
    for policy in ("NO_ENERGY_MANAGEMENT", "RULE_ONLY", "HYBRID", "AI_PREFERRED"):
        policy_rows = grouped.get(policy) or []
        xs, ys = _x_and_values(policy_rows, "elapsed_min", "rolling_energy_wh")
        if xs and ys:
            ax.plot(xs, ys, linewidth=1.8, label=policy)
            plotted = True
    if not plotted:
        plt.close(fig)
        return None
    window_label = str(rows[0].get("window_label") or "")
    ax.set_title(f"Rolling Energy Consumption Comparison ({window_label})" if window_label else "Rolling Energy Consumption Comparison")
    ax.set_xlabel("Elapsed Time (min)")
    ax.set_ylabel("Energy (Wh)")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    path = out_dir / "rolling_energy_consumption_comparison.png"
    _save_fig(fig, path)
    return path.name


def _plot_before_vs_after_demand_profile(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "before_vs_after_demand_profile.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "control_policy")
    fig, ax = plt.subplots(figsize=(11.5, 6))
    plotted = False
    for policy in ("NO_ENERGY_MANAGEMENT", "HYBRID", "AI_PREFERRED", "RULE_ONLY"):
        policy_rows = grouped.get(policy) or []
        xs, ys = _x_and_values(policy_rows, "elapsed_min", "total_power_w")
        if xs and ys:
            ax.plot(xs, ys, linewidth=1.8, label=policy)
            plotted = True
    if not plotted:
        plt.close(fig)
        return None
    ax.set_title("Before vs After Demand Profile")
    ax.set_xlabel("Elapsed Time (min)")
    ax.set_ylabel("Power (W)")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    path = out_dir / "before_vs_after_demand_profile.png"
    _save_fig(fig, path)
    return path.name


def _plot_prediction_residuals(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "prediction_residuals.csv")
    if not rows:
        return None
    grouped = _group_rows(rows, "model")
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(11, 8), sharex=False)
    plotted = False
    for model, color in (("EMA", "#1f77b4"), ("RF", "#ff7f0e"), ("LSTM", "#2ca02c")):
        model_rows = grouped.get(model) or []
        power_errors = [_safe_float(row.get("power_residual_w")) for row in model_rows]
        power_errors = [value for value in power_errors if value is not None]
        energy_errors = [_safe_float(row.get("energy_residual_wh")) for row in model_rows]
        energy_errors = [value for value in energy_errors if value is not None]
        if power_errors:
            ax1.hist(power_errors, bins=min(30, max(8, len(power_errors) // 4)), alpha=0.4, label=model, color=color)
            plotted = True
        if energy_errors:
            ax2.hist(energy_errors, bins=min(30, max(8, len(energy_errors) // 4)), alpha=0.4, label=model, color=color)
            plotted = True
    if not plotted:
        plt.close(fig)
        return None
    ax1.set_title("Prediction Residual Distribution - Rolling Avg Power")
    ax1.set_xlabel("Residual (W)")
    ax1.set_ylabel("Count")
    ax1.grid(True, alpha=0.3)
    ax2.set_title("Prediction Residual Distribution - Rolling Energy")
    ax2.set_xlabel("Residual (Wh)")
    ax2.set_ylabel("Count")
    ax2.grid(True, alpha=0.3)
    if ax1.has_data():
        ax1.legend(loc="best", fontsize=8)
    if ax2.has_data():
        ax2.legend(loc="best", fontsize=8)
    path = out_dir / "prediction_residuals.png"
    _save_fig(fig, path)
    return path.name


def _plot_stream_timing_overview(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "telemetry_stream_timing.csv")
    if not rows:
        return None
    labels = [str(row.get("stream", "")).strip() for row in rows if str(row.get("stream", "")).strip()]
    if not labels:
        return None
    median_vals = [_safe_float(row.get("median_interval_sec")) or 0.0 for row in rows]
    p95_vals = [_safe_float(row.get("p95_interval_sec")) or 0.0 for row in rows]
    gap_vals = [_safe_float(row.get("approx_gap_count")) or 0.0 for row in rows]
    x = np.arange(len(labels))
    width = 0.35
    fig, axes = plt.subplots(2, 1, figsize=(11, 8), sharex=True, height_ratios=[2.0, 1.2])
    axes[0].bar(x - width / 2.0, median_vals, width, label="Median Interval", color="#1f77b4")
    axes[0].bar(x + width / 2.0, p95_vals, width, label="P95 Interval", color="#ff7f0e")
    axes[0].set_ylabel("Seconds")
    axes[0].set_title("Telemetry and Prediction Stream Timing")
    axes[0].grid(True, axis="y", alpha=0.3)
    axes[0].legend(loc="best", fontsize=8)
    axes[1].bar(x, gap_vals, width=0.55, color="#d62728")
    axes[1].set_ylabel("Approx Gaps")
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(labels, rotation=15)
    axes[1].grid(True, axis="y", alpha=0.3)
    path = out_dir / "telemetry_stream_timing.png"
    _save_fig(fig, path)
    return path.name


def _plot_control_response_metrics(export_dir: Path, out_dir: Path) -> Optional[str]:
    rows = _load_csv(export_dir / "control_response_metrics.csv")
    if not rows:
        return None
    labels = [str(row.get("control_policy", "")).strip() for row in rows if str(row.get("control_policy", "")).strip()]
    if not labels:
        return None
    action_delay = [_safe_float(row.get("mean_first_control_action_delay_sec")) or 0.0 for row in rows]
    settling = [_safe_float(row.get("mean_peak_settling_time_sec")) or 0.0 for row in rows]
    clearance = [_safe_float(row.get("mean_threshold_clearance_delay_sec")) or 0.0 for row in rows]
    x = np.arange(len(labels))
    width = 0.25
    fig, ax = plt.subplots(figsize=(11, 5.5))
    ax.bar(x - width, action_delay, width, label="First Action Delay", color="#1f77b4")
    ax.bar(x, clearance, width, label="Threshold Clearance", color="#ff7f0e")
    ax.bar(x + width, settling, width, label="Peak Settling", color="#2ca02c")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15)
    ax.set_ylabel("Seconds")
    ax.set_title("Control Response Metrics")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    path = out_dir / "control_response_metrics.png"
    _save_fig(fig, path)
    return path.name


PLOTTERS: Sequence[Callable[[Path, Path], Optional[str]]] = (
    lambda export_dir, out_dir: _plot_model_correlation_heatmap(
        export_dir,
        out_dir,
        "_long_rf.analytics.json",
        "rf_correlation_heatmap.png",
        "Correlation Heatmap - RF Training Features",
    ),
    lambda export_dir, out_dir: _plot_model_correlation_heatmap(
        export_dir,
        out_dir,
        "_long_lstm.analytics.json",
        "lstm_correlation_heatmap.png",
        "Correlation Heatmap - LSTM Training Features",
    ),
    lambda export_dir, out_dir: _plot_single_model_metrics(
        export_dir,
        out_dir,
        "ema_model_performance_metrics.csv",
        "ema_model_performance_metrics.png",
        "EMA Model Performance",
    ),
    lambda export_dir, out_dir: _plot_single_model_metrics(
        export_dir,
        out_dir,
        "rf_model_performance_metrics.csv",
        "rf_model_performance_metrics.png",
        "RF Model Performance",
    ),
    lambda export_dir, out_dir: _plot_single_model_metrics(
        export_dir,
        out_dir,
        "lstm_model_performance_metrics.csv",
        "lstm_model_performance_metrics.png",
        "LSTM Model Performance",
    ),
    _plot_policy_control_actions,
    _plot_stream_timing_overview,
    _plot_control_response_metrics,
    _plot_core_correlation_heatmap,
    _plot_ai_feature_importance,
    _plot_figure_4_1,
    _plot_figure_4_2,
    _plot_figure_4_3,
    _plot_figure_4_4,
    _plot_figure_4_5,
    _plot_figure_4_6,
    _plot_figure_4_7,
    _plot_figure_4_8,
    _plot_figure_4_9,
    lambda export_dir, out_dir: _plot_prediction_vs_actual(
        export_dir,
        out_dir,
        "figure_4_10_ema_prediction_vs_actual.csv",
        "figure_4_10_ema_prediction_vs_actual.png",
        "Figure 4.10 - EMA Prediction vs Actual Demand",
    ),
    lambda export_dir, out_dir: _plot_prediction_vs_actual(
        export_dir,
        out_dir,
        "figure_4_11_rf_prediction_vs_actual.csv",
        "figure_4_11_rf_prediction_vs_actual.png",
        "Figure 4.11 - Random Forest Prediction vs Actual Demand",
    ),
    lambda export_dir, out_dir: _plot_prediction_vs_actual(
        export_dir,
        out_dir,
        "figure_4_12_lstm_prediction_vs_actual.csv",
        "figure_4_12_lstm_prediction_vs_actual.png",
        "Figure 4.12 - LSTM Prediction vs Actual Demand",
    ),
    _plot_figure_4_13_14_combined,
    _plot_figure_4_15,
    _plot_figure_4_16,
    _plot_figure_4_18,
    lambda export_dir, out_dir: _plot_bar_chart(
        export_dir,
        out_dir,
        "figure_4_19_peak_demand_comparison.csv",
        "figure_4_19_peak_demand_comparison.png",
        "Figure 4.19 - Peak Demand Comparison",
        "peak_power_w",
        "Peak Power (W)",
    ),
    lambda export_dir, out_dir: _plot_bar_chart(
        export_dir,
        out_dir,
        "figure_4_20_energy_consumption_comparison.csv",
        "figure_4_20_energy_consumption_comparison.png",
        "Figure 4.20 - Energy Consumption Comparison",
        "energy_wh",
        "Energy (Wh)",
    ),
    _plot_figure_4_21,
    _plot_accumulated_cost,
    _plot_rolling_window_energy_cost,
    _plot_rolling_energy_consumption,
    _plot_rolling_cost_consumption,
    _plot_rolling_and_cumulative_energy,
    _plot_rolling_and_cumulative_cost,
    _plot_per_load_rolling_energy,
    _plot_policy_control_actions,
    _plot_forecast_informed_control_response,
    _plot_rolling_energy_comparison,
    _plot_before_vs_after_demand_profile,
    _plot_prediction_residuals,
)


def _latest_export_dir(root: Path) -> Optional[Path]:
    if not root.exists():
        return None
    candidates = [path for path in root.iterdir() if path.is_dir()]
    if not candidates:
        return None
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate PNG figures from a Chapter 4 export folder")
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Chapter 4 export directory. Defaults to the newest directory under PythonPredictor/chapter4_exports.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory for PNG files. Defaults to <input>/png_figures.",
    )
    return parser.parse_args()


def generate_png_figures(export_dir: Path, output_dir: Optional[Path] = None) -> dict:
    export_dir = Path(export_dir)
    output_dir = Path(output_dir) if output_dir is not None else (export_dir / "png_figures")
    output_dir.mkdir(parents=True, exist_ok=True)

    written: List[str] = []
    for plotter in PLOTTERS:
        filename = plotter(export_dir, output_dir)
        if filename:
            written.append(filename)

    summary_path = output_dir / "plot_summary.txt"
    with summary_path.open("w", encoding="utf-8") as handle:
        handle.write(f"export_dir: {export_dir}\n")
        handle.write(f"png_dir: {output_dir}\n")
        handle.write("files:\n")
        for name in written:
            handle.write(f"- {name}\n")

    return {
        "export_dir": export_dir,
        "png_dir": output_dir,
        "written": written,
        "summary_path": summary_path,
    }


def main() -> int:
    args = _parse_args()
    export_dir = args.input or _latest_export_dir(DEFAULT_EXPORT_ROOT)
    if export_dir is None or not export_dir.exists():
        print("No Chapter 4 export directory found.")
        return 1

    result = generate_png_figures(export_dir, args.output)
    print(f"Generated {len(result['written'])} PNG file(s) in {result['png_dir']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
