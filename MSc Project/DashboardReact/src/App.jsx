import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  applyAck,
  applyPrediction,
  applyTelemetry,
  buildTopicSet,
  createDemoTick,
  createInitialState,
  createTwinClient,
} from './lib/mqttTwin';

const PLANT_ID = import.meta.env.VITE_PLANT_ID || 'factory1';
const BROKER_URL = import.meta.env.VITE_MQTT_URL || 'wss://2d7641080ba74f7a91be1b429b265933.s1.eu.hivemq.cloud:8884/mqtt';
const USERNAME = import.meta.env.VITE_MQTT_USERNAME || '';
const PASSWORD = import.meta.env.VITE_MQTT_PASSWORD || '';
const HISTORY_API_URL = import.meta.env.VITE_HISTORY_API_URL || '/history-api';
const CURRENCY_CODE = String(import.meta.env.VITE_CURRENCY_CODE || 'USD').toUpperCase();
const UK_TIMEZONE = 'Europe/London';
const parseHourSetting = (raw, fallback) => {
  const n = Number(raw);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, Math.min(23, Math.round(n)));
};
const USE_UK_TARIFF_TOU = !['0', 'false', 'no', 'off'].includes(
  String(import.meta.env.VITE_TARIFF_USE_UK_TOU ?? 'true').trim().toLowerCase()
);
const UK_OFFPEAK_START_HOUR = parseHourSetting(import.meta.env.VITE_TARIFF_UK_OFFPEAK_START_HOUR, 0);
const UK_OFFPEAK_END_HOUR = parseHourSetting(import.meta.env.VITE_TARIFF_UK_OFFPEAK_END_HOUR, 7);
const UK_PEAK_START_HOUR = parseHourSetting(import.meta.env.VITE_TARIFF_UK_PEAK_START_HOUR, 16);
const UK_PEAK_END_HOUR = parseHourSetting(import.meta.env.VITE_TARIFF_UK_PEAK_END_HOUR, 20);
const DEFAULT_TARIFF_RATE_PER_KWH = {
  OFFPEAK: Number.isFinite(Number(import.meta.env.VITE_TARIFF_OFFPEAK_PER_KWH))
    ? Math.max(0, Number(import.meta.env.VITE_TARIFF_OFFPEAK_PER_KWH))
    : 0.12,
  MIDPEAK: Number.isFinite(Number(import.meta.env.VITE_TARIFF_MIDPEAK_PER_KWH))
    ? Math.max(0, Number(import.meta.env.VITE_TARIFF_MIDPEAK_PER_KWH))
    : 0.18,
  PEAK: Number.isFinite(Number(import.meta.env.VITE_TARIFF_PEAK_PER_KWH))
    ? Math.max(0, Number(import.meta.env.VITE_TARIFF_PEAK_PER_KWH))
    : 0.28,
};
const DEFAULT_DEMAND_CHARGE_PER_KW = Number.isFinite(Number(import.meta.env.VITE_TARIFF_DEMAND_CHARGE_PER_KW))
  ? Math.max(0, Number(import.meta.env.VITE_TARIFF_DEMAND_CHARGE_PER_KW))
  : 0;
const DEFAULT_DEMAND_INTERVAL_MIN = Number.isFinite(Number(import.meta.env.VITE_TARIFF_DEMAND_INTERVAL_MIN))
  ? Math.max(1, Math.round(Number(import.meta.env.VITE_TARIFF_DEMAND_INTERVAL_MIN)))
  : 30;
const DEFAULT_TARIFF_CONFIG = {
  useDashboardTou: USE_UK_TARIFF_TOU,
  offpeakStartMin: UK_OFFPEAK_START_HOUR * 60,
  offpeakEndMin: UK_OFFPEAK_END_HOUR * 60,
  peakStartMin: UK_PEAK_START_HOUR * 60,
  peakEndMin: UK_PEAK_END_HOUR * 60,
  offpeakRatePerKwh: DEFAULT_TARIFF_RATE_PER_KWH.OFFPEAK,
  midpeakRatePerKwh: DEFAULT_TARIFF_RATE_PER_KWH.MIDPEAK,
  peakRatePerKwh: DEFAULT_TARIFF_RATE_PER_KWH.PEAK,
  demandChargePerKw: DEFAULT_DEMAND_CHARGE_PER_KW,
  demandIntervalMin: DEFAULT_DEMAND_INTERVAL_MIN,
};

const POLICY_OPTIONS = ['RULE_ONLY', 'HYBRID', 'AI_PREFERRED', 'NO_ENERGY_MANAGEMENT'];
const LOAD_CLASS_OPTIONS = ['CRITICAL', 'ESSENTIAL', 'IMPORTANT', 'SECONDARY', 'NON_ESSENTIAL'];
const PREDICTOR_MODE_OPTIONS = ['TRAIN_ONLY', 'EVAL_ONLY', 'ONLINE', 'OFFLINE'];
const PREDICTION_SERIES = [
  { key: 'short', name: 'Short EMA', color: '#ffd166' },
  { key: 'long_rf', name: 'Long RF', color: '#ee6c4d' },
  { key: 'long_lstm', name: 'Long LSTM', color: '#72efdd' },
];
const ULTRASONIC_BLIND_SPOT_CM = 2;
const aiDynamicReasonText = (reasonRaw) => {
  const reason = String(reasonRaw || '').toLowerCase();
  if (!reason) return '';
  if (reason === 'source_short_not_supported') return 'Short-term prediction is not used for dynamic process control.';
  if (reason === 'predictor_not_online') return 'At least one long-term predictor must be in ONLINE mode.';
  if (reason === 'predictor_not_trained') return 'No ready long-term model is available yet.';
  if (reason === 'prediction_stale') return 'No fresh long-term prediction stream is available.';
  if (reason === 'no_suggestions') return 'Long-term predictors have no suggestions yet.';
  if (reason === 'no_process_plan') return 'No process-aware plan is available yet.';
  if (reason === 'disabled') return 'Dynamic process is currently disabled.';
  if (reason === 'ready') return 'Dynamic process is ready.';
  return reason.replace(/_/g, ' ');
};

const fx = (v, decimals = 2) => (Number.isFinite(Number(v)) ? Number(v).toFixed(decimals) : '-');
const f2 = (v) => fx(v, 2);
const trimTrailingZeros = (text) => String(text).replace(/(\.\d*?[1-9])0+$/u, '$1').replace(/\.0+$/u, '').replace(/^-0$/u, '0');
const formatAutoDecimals = (value, mode = 'axis') => {
  const n = Number(value);
  if (!Number.isFinite(n)) return '-';
  const abs = Math.abs(n);
  let decimals = 2;
  if (mode === 'axis') {
    if (abs >= 100) decimals = 0;
    else if (abs >= 10) decimals = 1;
    else if (abs >= 1) decimals = 2;
    else if (abs >= 0.1) decimals = 3;
    else decimals = 4;
  } else {
    if (abs >= 100) decimals = 1;
    else if (abs >= 10) decimals = 2;
    else if (abs >= 1) decimals = 3;
    else if (abs >= 0.1) decimals = 4;
    else decimals = 5;
  }
  return trimTrailingZeros(n.toFixed(decimals));
};
const collectSeriesValues = (rows, keys) => {
  const values = [];
  for (const row of rows || []) {
    for (const key of keys || []) {
      const n = Number(row?.[key]);
      if (Number.isFinite(n)) values.push(n);
    }
  }
  return values;
};
const MINOR_CURRENCY_UNITS = {
  GBP: 'p',
  USD: 'c',
  EUR: 'c',
  CAD: 'c',
  AUD: 'c',
  NZD: 'c',
};
const DEFAULT_SCALE_SPEC = { factor: 1, unit: '', kind: 'plain' };
const chooseScaleSpec = (kind, values, currencyCode = CURRENCY_CODE) => {
  const maxAbs = Math.max(0, ...(values || []).map((value) => Math.abs(Number(value) || 0)));
  if (kind === 'power') {
    if (maxAbs >= 1_000_000) return { factor: 1 / 1_000_000, unit: 'MW', kind };
    if (maxAbs >= 1_000) return { factor: 1 / 1_000, unit: 'kW', kind };
    if (maxAbs >= 1) return { factor: 1, unit: 'W', kind };
    if (maxAbs >= 0.001) return { factor: 1_000, unit: 'mW', kind };
    return { factor: 1_000_000, unit: 'uW', kind };
  }
  if (kind === 'energy') {
    if (maxAbs >= 1_000_000) return { factor: 1 / 1_000_000, unit: 'MWh', kind };
    if (maxAbs >= 1_000) return { factor: 1 / 1_000, unit: 'kWh', kind };
    if (maxAbs >= 1) return { factor: 1, unit: 'Wh', kind };
    if (maxAbs >= 0.001) return { factor: 1_000, unit: 'mWh', kind };
    return { factor: 1_000_000, unit: 'uWh', kind };
  }
  if (kind === 'current') {
    if (maxAbs >= 1_000) return { factor: 1 / 1_000, unit: 'kA', kind };
    if (maxAbs >= 1) return { factor: 1, unit: 'A', kind };
    if (maxAbs >= 0.001) return { factor: 1_000, unit: 'mA', kind };
    return { factor: 1_000_000, unit: 'uA', kind };
  }
  if (kind === 'cost') {
    if (maxAbs >= 1_000_000) return { factor: 1 / 1_000_000, unit: `M${currencyCode}`, kind };
    if (maxAbs >= 1_000) return { factor: 1 / 1_000, unit: `k${currencyCode}`, kind };
    if (maxAbs >= 1) return { factor: 1, unit: currencyCode, kind };
    const minorUnit = MINOR_CURRENCY_UNITS[String(currencyCode || '').toUpperCase()];
    if (minorUnit) return { factor: 100, unit: minorUnit, kind };
    if (maxAbs >= 0.001) return { factor: 1_000, unit: `m${currencyCode}`, kind };
    return { factor: 1_000_000, unit: `u${currencyCode}`, kind };
  }
  return DEFAULT_SCALE_SPEC;
};
const scaleValue = (value, spec = DEFAULT_SCALE_SPEC) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return NaN;
  return n * Number(spec?.factor || 1);
};
const makeAxisTickFormatter = (spec = DEFAULT_SCALE_SPEC) => (value) => formatAutoDecimals(scaleValue(value, spec), 'axis');
const makeTooltipFormatter = (specByKey = {}) => (value, name, item) => {
  const spec = specByKey?.[item?.dataKey] || DEFAULT_SCALE_SPEC;
  const unit = spec?.unit ? ` ${spec.unit}` : '';
  return [`${formatAutoDecimals(scaleValue(value, spec), 'tooltip')}${unit}`, name];
};
const formatScaledValueOnly = (value, kind, { scaleSpec, fallback = '-' } = {}) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  const spec = scaleSpec || chooseScaleSpec(kind, [n]);
  return formatAutoDecimals(scaleValue(n, spec), 'tooltip');
};
const formatScaledMetric = (value, kind, { scaleSpec, unitSuffix = '', fallback = '-' } = {}) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return { value: fallback, unit: '' };
  const spec = scaleSpec || chooseScaleSpec(kind, [n]);
  return {
    value: formatAutoDecimals(scaleValue(n, spec), 'tooltip'),
    unit: `${spec.unit || ''}${unitSuffix}`,
  };
};
const normalizeTariffState = (value) => {
  const state = String(value || '').toUpperCase().replace(/[^A-Z]/g, '');
  if (state === 'PEAK') return 'PEAK';
  if (state === 'MIDPEAK' || state === 'MID') return 'MIDPEAK';
  return 'OFFPEAK';
};
const UK_CLOCK_PARTS_FORMATTER = new Intl.DateTimeFormat('en-GB', {
  timeZone: UK_TIMEZONE,
  year: 'numeric',
  month: '2-digit',
  day: '2-digit',
  weekday: 'short',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
});
const UK_WEEKDAY_INDEX = {
  SUN: 0,
  MON: 1,
  TUE: 2,
  WED: 3,
  THU: 4,
  FRI: 5,
  SAT: 6,
};
const UK_BANK_HOLIDAY_CACHE = new Map();
const pad2 = (v) => String(v).padStart(2, '0');
const normalizeMinuteOfDay = (value, fallback = 0) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return ((Math.round(n) % 1440) + 1440) % 1440;
};
const formatMinuteOfDay = (minuteOfDay) => {
  const total = normalizeMinuteOfDay(minuteOfDay, 0);
  const hours = Math.floor(total / 60);
  const minutes = total % 60;
  return `${pad2(hours)}:${pad2(minutes)}`;
};
const parseTimeInputToMinutes = (value, fallback) => {
  const text = String(value || '').trim();
  if (!/^\d{2}:\d{2}$/.test(text)) return fallback;
  const [hoursRaw, minutesRaw] = text.split(':');
  const hours = Number(hoursRaw);
  const minutes = Number(minutesRaw);
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return fallback;
  return normalizeMinuteOfDay((hours * 60) + minutes, fallback);
};
const normalizeTariffRate = (value, fallback) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(0, n);
};
const normalizeDemandIntervalMin = (value, fallback) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  return Math.max(1, Math.round(n));
};
const normalizeTariffConfig = (raw = {}) => ({
  useDashboardTou: typeof raw.useDashboardTou === 'boolean'
    ? raw.useDashboardTou
    : DEFAULT_TARIFF_CONFIG.useDashboardTou,
  offpeakStartMin: normalizeMinuteOfDay(raw.offpeakStartMin, DEFAULT_TARIFF_CONFIG.offpeakStartMin),
  offpeakEndMin: normalizeMinuteOfDay(raw.offpeakEndMin, DEFAULT_TARIFF_CONFIG.offpeakEndMin),
  peakStartMin: normalizeMinuteOfDay(raw.peakStartMin, DEFAULT_TARIFF_CONFIG.peakStartMin),
  peakEndMin: normalizeMinuteOfDay(raw.peakEndMin, DEFAULT_TARIFF_CONFIG.peakEndMin),
  offpeakRatePerKwh: normalizeTariffRate(raw.offpeakRatePerKwh, DEFAULT_TARIFF_CONFIG.offpeakRatePerKwh),
  midpeakRatePerKwh: normalizeTariffRate(raw.midpeakRatePerKwh, DEFAULT_TARIFF_CONFIG.midpeakRatePerKwh),
  peakRatePerKwh: normalizeTariffRate(raw.peakRatePerKwh, DEFAULT_TARIFF_CONFIG.peakRatePerKwh),
  demandChargePerKw: normalizeTariffRate(raw.demandChargePerKw, DEFAULT_TARIFF_CONFIG.demandChargePerKw),
  demandIntervalMin: normalizeDemandIntervalMin(raw.demandIntervalMin, DEFAULT_TARIFF_CONFIG.demandIntervalMin),
});
const dateKeyYmd = (year, month, day) => `${year}-${pad2(month)}-${pad2(day)}`;
const dayOfWeekUtc = (year, month, day) => new Date(Date.UTC(year, month - 1, day)).getUTCDay();
const daysInMonthUtc = (year, month) => new Date(Date.UTC(year, month, 0)).getUTCDate();
const addDaysUtc = (year, month, day, delta) => {
  const dt = new Date(Date.UTC(year, month - 1, day + delta));
  return { year: dt.getUTCFullYear(), month: dt.getUTCMonth() + 1, day: dt.getUTCDate() };
};
const firstWeekdayOfMonthUtc = (year, month, weekday) => {
  for (let day = 1; day <= 7; day += 1) {
    if (dayOfWeekUtc(year, month, day) === weekday) return day;
  }
  return 1;
};
const lastWeekdayOfMonthUtc = (year, month, weekday) => {
  for (let day = daysInMonthUtc(year, month); day >= 1; day -= 1) {
    if (dayOfWeekUtc(year, month, day) === weekday) return day;
  }
  return daysInMonthUtc(year, month);
};
const computeEasterSundayUtc = (year) => {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return { year, month, day };
};
const buildUkBankHolidaySet = (year) => {
  const out = new Set();

  // New Year's Day (with substitute day).
  const newYearDow = dayOfWeekUtc(year, 1, 1);
  let newYearObserved = 1;
  if (newYearDow === 6) newYearObserved = 3;
  else if (newYearDow === 0) newYearObserved = 2;
  out.add(dateKeyYmd(year, 1, newYearObserved));

  // Good Friday + Easter Monday.
  const easter = computeEasterSundayUtc(year);
  const goodFriday = addDaysUtc(easter.year, easter.month, easter.day, -2);
  const easterMonday = addDaysUtc(easter.year, easter.month, easter.day, 1);
  out.add(dateKeyYmd(goodFriday.year, goodFriday.month, goodFriday.day));
  out.add(dateKeyYmd(easterMonday.year, easterMonday.month, easterMonday.day));

  // Early May bank holiday (first Monday in May).
  out.add(dateKeyYmd(year, 5, firstWeekdayOfMonthUtc(year, 5, 1)));
  // Spring bank holiday (last Monday in May).
  out.add(dateKeyYmd(year, 5, lastWeekdayOfMonthUtc(year, 5, 1)));
  // Summer bank holiday (last Monday in August).
  out.add(dateKeyYmd(year, 8, lastWeekdayOfMonthUtc(year, 8, 1)));

  // Christmas Day + Boxing Day (with substitute days).
  const christmasDow = dayOfWeekUtc(year, 12, 25);
  let christmasObserved = 25;
  if (christmasDow === 6 || christmasDow === 0) christmasObserved = 27;
  out.add(dateKeyYmd(year, 12, christmasObserved));

  const boxingDow = dayOfWeekUtc(year, 12, 26);
  let boxingObserved = 26;
  if (boxingDow === 6 || boxingDow === 0) boxingObserved = 28;
  out.add(dateKeyYmd(year, 12, boxingObserved));

  return out;
};
const isUkBankHoliday = (year, month, day) => {
  if (!UK_BANK_HOLIDAY_CACHE.has(year)) {
    UK_BANK_HOLIDAY_CACHE.set(year, buildUkBankHolidaySet(year));
  }
  return UK_BANK_HOLIDAY_CACHE.get(year).has(dateKeyYmd(year, month, day));
};
const isMinuteInWindow = (minuteOfDay, startMinute, endMinute) => {
  const minute = ((minuteOfDay % 1440) + 1440) % 1440;
  const start = ((startMinute % 1440) + 1440) % 1440;
  const end = ((endMinute % 1440) + 1440) % 1440;
  if (start === end) return true;
  if (start < end) return minute >= start && minute < end;
  return minute >= start || minute < end;
};
const getUkClockParts = (tsMs = Date.now()) => {
  const bag = {};
  for (const part of UK_CLOCK_PARTS_FORMATTER.formatToParts(new Date(tsMs))) {
    if (part.type !== 'literal') bag[part.type] = part.value;
  }
  const weekday = UK_WEEKDAY_INDEX[String(bag.weekday || '').slice(0, 3).toUpperCase()];
  const year = Number(bag.year);
  const month = Number(bag.month);
  const day = Number(bag.day);
  const hour = Number(bag.hour);
  const minute = Number(bag.minute);
  if (![year, month, day, hour, minute].every(Number.isFinite) || weekday === undefined) return null;
  return { year, month, day, hour, minute, weekday };
};
const resolveUkTouTariffState = (tsMs = Date.now(), tariffConfig = DEFAULT_TARIFF_CONFIG) => {
  const uk = getUkClockParts(tsMs);
  if (!uk) return 'OFFPEAK';
  const minuteOfDay = (uk.hour * 60) + uk.minute;
  const isWeekend = uk.weekday === 0 || uk.weekday === 6;
  if (isWeekend || isUkBankHoliday(uk.year, uk.month, uk.day)) return 'OFFPEAK';
  if (isMinuteInWindow(minuteOfDay, tariffConfig.offpeakStartMin, tariffConfig.offpeakEndMin)) return 'OFFPEAK';
  if (isMinuteInWindow(minuteOfDay, tariffConfig.peakStartMin, tariffConfig.peakEndMin)) return 'PEAK';
  return 'MIDPEAK';
};
const formatMinuteWindow = (startMinute, endMinute) => `${formatMinuteOfDay(startMinute)}-${formatMinuteOfDay(endMinute)}`;
const resolveTariffRatePerKwh = (fallbackTariffState, tariffConfig = DEFAULT_TARIFF_CONFIG) => {
  const state = normalizeTariffState(fallbackTariffState);
  if (state === 'PEAK') return tariffConfig.peakRatePerKwh;
  if (state === 'MIDPEAK') return tariffConfig.midpeakRatePerKwh;
  return tariffConfig.offpeakRatePerKwh;
};
const buildAccumulatedCostSeries = (rows, {
  powerKey,
  energyKey,
  tsKey = 'ts',
}, tariffConfig = DEFAULT_TARIFF_CONFIG) => {
  let previousTs = NaN;
  let previousPowerW = NaN;
  let previousEnergyWh = NaN;
  let accumulatedEnergyCost = 0;
  let runningMaxDemandKw = 0;
  const intervalMs = Math.max(1, normalizeDemandIntervalMin(tariffConfig.demandIntervalMin, DEFAULT_TARIFF_CONFIG.demandIntervalMin)) * 60000;
  const intervalHours = intervalMs / 3600000;
  const intervalEnergyWh = new Map();

  return (rows || []).map((row) => {
    const ts = Number(row?.[tsKey]);
    const powerW = Number(row?.[powerKey]);
    const energyWh = Number(row?.[energyKey]);
    const tariffState = resolveUkTouTariffState(Number.isFinite(ts) ? ts : Date.now(), tariffConfig);
    const tariffRatePerKwh = resolveTariffRatePerKwh(tariffState, tariffConfig);

    let deltaEnergyWh = NaN;
    if (Number.isFinite(energyWh) && Number.isFinite(previousEnergyWh)) {
      deltaEnergyWh = energyWh - previousEnergyWh;
      if (deltaEnergyWh < 0) deltaEnergyWh = NaN;
    }
    if (!Number.isFinite(deltaEnergyWh) && Number.isFinite(ts) && Number.isFinite(previousTs) && ts > previousTs) {
      const dtHours = (ts - previousTs) / 3600000;
      const referencePowerW = Number.isFinite(previousPowerW) && Number.isFinite(powerW)
        ? (previousPowerW + powerW) * 0.5
        : (Number.isFinite(powerW) ? powerW : previousPowerW);
      if (Number.isFinite(referencePowerW) && referencePowerW >= 0) {
        deltaEnergyWh = referencePowerW * dtHours;
      }
    }
    if (!Number.isFinite(deltaEnergyWh) || deltaEnergyWh < 0) deltaEnergyWh = 0;

    const incrementalEnergyCost = deltaEnergyWh * (tariffRatePerKwh / 1000);
    accumulatedEnergyCost += incrementalEnergyCost;

    if (deltaEnergyWh > 0 && Number.isFinite(ts) && Number.isFinite(previousTs) && ts > previousTs) {
      let segmentStart = previousTs;
      const segmentEnd = ts;
      const segmentDurationMs = segmentEnd - segmentStart;
      while (segmentStart < segmentEnd) {
        const bucketStart = Math.floor(segmentStart / intervalMs) * intervalMs;
        const bucketEnd = bucketStart + intervalMs;
        const overlapEnd = Math.min(segmentEnd, bucketEnd);
        const overlapMs = overlapEnd - segmentStart;
        const overlapEnergyWh = deltaEnergyWh * (overlapMs / segmentDurationMs);
        const nextBucketEnergyWh = (intervalEnergyWh.get(bucketStart) || 0) + overlapEnergyWh;
        intervalEnergyWh.set(bucketStart, nextBucketEnergyWh);
        const bucketDemandKw = nextBucketEnergyWh / intervalHours / 1000;
        if (bucketDemandKw > runningMaxDemandKw) runningMaxDemandKw = bucketDemandKw;
        segmentStart = overlapEnd;
      }
    }

    const demandChargeCost = runningMaxDemandKw * tariffConfig.demandChargePerKw;
    const totalCost = accumulatedEnergyCost + demandChargeCost;

    if (Number.isFinite(ts)) previousTs = ts;
    if (Number.isFinite(powerW)) previousPowerW = powerW;
    if (Number.isFinite(energyWh)) previousEnergyWh = energyWh;

    return {
      ...row,
      tariff_state_calc: tariffState,
      tariff_rate_per_kwh_calc: tariffRatePerKwh,
      demand_interval_min: tariffConfig.demandIntervalMin,
      incremental_energy_cost: incrementalEnergyCost,
      accumulated_energy_cost: accumulatedEnergyCost,
      max_demand_kw: runningMaxDemandKw,
      demand_charge_cost: demandChargeCost,
      incremental_cost: incrementalEnergyCost,
      accumulated_cost: totalCost,
      total_cost: totalCost,
    };
  });
};
const normalizePredictionAgeMs = (rawValue) => {
  const value = Number(rawValue);
  if (!Number.isFinite(value)) return NaN;
  if (value < 0) return 0;
  // If backend accidentally sends epoch timestamp instead of age, convert to age.
  if (value > 1e12) return Math.max(0, Date.now() - value);
  if (value > 946684800 && value < 4102444800) return Math.max(0, Date.now() - value * 1000);
  return value;
};
const liveText = (value, unit, decimals = 2) => {
  if (value === undefined || value === null || value === '') return '-';
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) return '-';
    const fixed = decimals === 0 ? Math.round(value).toString() : value.toFixed(decimals);
    return unit ? `${fixed} ${unit}` : fixed;
  }
  const asText = String(value);
  return unit ? `${asText} ${unit}` : asText;
};
const liveBadge = (value, unit, decimals) => (
  <span className="live-badge">Live: {liveText(value, unit, decimals)}</span>
);
const riskClass = (risk) => `risk-pill risk-${String(risk || 'LOW').toLowerCase()}`;
const predictionEnergyDisplay = (pred) => (pred?.predicted_total_energy_wh ?? pred?.predicted_energy_wh);
const formatHms = (seconds) => {
  const total = Number(seconds);
  if (!Number.isFinite(total) || total < 0) return '-';
  const secs = Math.floor(total);
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  const pad = (v) => String(v).padStart(2, '0');
  return `${pad(h)}:${pad(m)}:${pad(s)}`;
};

const toDateTimeLocal = (ms) => {
  const value = Number(ms);
  if (!Number.isFinite(value)) return '';
  const d = new Date(value);
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
};
const getDownloadFilename = (response, fallback) => {
  const disposition = response?.headers?.get?.('content-disposition') || '';
  const utf8Match = disposition.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) return decodeURIComponent(utf8Match[1]);
  const plainMatch = disposition.match(/filename=\"?([^\";]+)\"?/i);
  if (plainMatch?.[1]) return plainMatch[1];
  return fallback;
};

const parseDateTimeLocal = (text) => {
  if (!text) return NaN;
  const t = Date.parse(text);
  return Number.isFinite(t) ? t : NaN;
};

const formatHistoryTick = (ts, spanMs = 0) => {
  const ms = Number(ts);
  if (!Number.isFinite(ms)) return '';
  const d = new Date(ms);
  if (spanMs <= 24 * 60 * 60 * 1000) {
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
  if (spanMs <= 7 * 24 * 60 * 60 * 1000) {
    return `${d.toLocaleDateString([], { month: 'short', day: '2-digit' })} ${d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}`;
  }
  return d.toLocaleDateString([], { month: 'short', day: '2-digit' });
};

const chooseHistoryStepSec = (fromMs, toMs) => {
  const start = Number(fromMs);
  const end = Number(toMs);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return 300;
  const spanSec = (end - start) / 1000;
  if (spanSec <= 60 * 60) return 5 * 60;                 // <= 1h  -> 5 min
  if (spanSec <= 6 * 60 * 60) return 15 * 60;            // <= 6h  -> 15 min
  if (spanSec <= 24 * 60 * 60) return 60 * 60;           // <= 24h -> 1 hour
  if (spanSec <= 3 * 24 * 60 * 60) return 90 * 60;       // <= 3d  -> 90 min
  if (spanSec <= 7 * 24 * 60 * 60) return 2 * 60 * 60;   // <= 7d  -> 2 hours
  if (spanSec <= 30 * 24 * 60 * 60) return 24 * 60 * 60; // <= 30d -> 1 day
  return 2 * 24 * 60 * 60;                               // > 30d  -> 2 days
};

const formatHistoryStepLabel = (sec) => {
  const s = Number(sec);
  if (!Number.isFinite(s) || s <= 0) return '-';
  if (s % 86400 === 0) {
    const d = s / 86400;
    return `${d} day${d === 1 ? '' : 's'}`;
  }
  if (s % 3600 === 0) {
    const h = s / 3600;
    return `${h} hour${h === 1 ? '' : 's'}`;
  }
  if (s % 60 === 0) return `${s / 60} min`;
  return `${s} sec`;
};

const computeSeriesDomain = (rows, keys) => {
  const values = [];
  for (const row of rows || []) {
    for (const key of keys || []) {
      const n = Number(row?.[key]);
      if (Number.isFinite(n)) values.push(n);
    }
  }
  if (values.length === 0) return [0, 1];
  let min = Math.min(...values);
  let max = Math.max(...values);
  if (min === max) {
    const pad = Math.max(0.5, Math.abs(max) * 0.1);
    return [min - pad, max + pad];
  }
  const pad = (max - min) * 0.08;
  min -= pad;
  max += pad;
  return [min, max];
};

const stopProcessPayload = () => ({ process: { enabled: false } });
const labelLoad = (name) => String(name || '').replace(/_/g, ' ').toUpperCase();
const policyLabel = (policy) => {
  const key = String(policy || '').toUpperCase();
  if (key === 'RULE_ONLY') return 'RULE_ONLY';
  if (key === 'HYBRID') return 'HYBRID';
  if (key === 'AI_PREFERRED') return 'AI_PREFERRED';
  if (key === 'NO_ENERGY_MANAGEMENT') return 'NO_ENERGY_MANAGEMENT';
  return key || '-';
};

function Metric({ label, value, unit, scaleKind, scaleSpec, unitSuffix, fallback }) {
  const scaled = scaleKind
    ? formatScaledMetric(value, scaleKind, { scaleSpec, unitSuffix, fallback })
    : null;
  const displayValue = scaled ? scaled.value : value;
  const displayUnit = scaled ? scaled.unit : unit;
  return (
    <div className="metric">
      <div className="metric-label">{label}</div>
      <div className="metric-value">
        {displayValue}
        {displayUnit ? <span className="metric-unit"> {displayUnit}</span> : null}
      </div>
    </div>
  );
}

function Panel({ title, subtitle, right, children }) {
  return (
    <section className="panel panel-wide">
      <header className="panel-head">
        <div>
          <h2>{title}</h2>
          {subtitle ? <p className="panel-subtitle">{subtitle}</p> : null}
        </div>
        {right}
      </header>
      {children}
    </section>
  );
}

export default function App() {
  const [state, setState] = useState(createInitialState());
  const [mode, setMode] = useState('live');
  const [tariffConfig, setTariffConfig] = useState(DEFAULT_TARIFF_CONFIG);
  const [tariffConfigMeta, setTariffConfigMeta] = useState({ source: 'default', versionCount: 0, effectiveFrom: '' });
  const [tariffConfigSaving, setTariffConfigSaving] = useState(false);
  const [loadDrafts, setLoadDrafts] = useState({});
  const [systemDraft, setSystemDraft] = useState({});
  const [processDraft, setProcessDraft] = useState({});
  const [toasts, setToasts] = useState([]);
  const [historyRows, setHistoryRows] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyExporting, setHistoryExporting] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [historySeries, setHistorySeries] = useState({
    actual: true,
    short: true,
    longRf: true,
    longLstm: true,
  });
  const [historyQuery, setHistoryQuery] = useState(() => {
    const now = Date.now();
    return {
      fromMs: now - (24 * 60 * 60 * 1000),
      toMs: now,
    };
  });
  const [historyDraft, setHistoryDraft] = useState(() => {
    const now = Date.now();
    const fromMs = now - (24 * 60 * 60 * 1000);
    return {
      from: toDateTimeLocal(fromMs),
      to: toDateTimeLocal(now),
    };
  });
  const [ukClockTickMs, setUkClockTickMs] = useState(() => Date.now());
  const toastIdRef = useRef(0);
  const lastAckToastTsRef = useRef(0);

  const clientRef = useRef(null);
  const demoTickRef = useRef(createDemoTick());
  const topics = useMemo(() => buildTopicSet(PLANT_ID), []);

  useEffect(() => {
    const id = setInterval(() => setUkClockTickMs(Date.now()), 30000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (!HISTORY_API_URL) return undefined;
    const controller = new AbortController();
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    fetch(`${base}/api/tariff-config`, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((body) => {
        if (!body?.ok || !body?.active) throw new Error(String(body?.error || 'tariff_config_fetch_failed'));
        setTariffConfig(normalizeTariffConfig(body.active));
        setTariffConfigMeta({
          source: 'backend',
          versionCount: Number(body.version_count || 0),
          effectiveFrom: String(body.active.effective_from || ''),
        });
      })
      .catch((err) => {
        if (err?.name === 'AbortError') return;
        setTariffConfig(DEFAULT_TARIFF_CONFIG);
        setTariffConfigMeta({ source: 'default', versionCount: 0, effectiveFrom: '' });
        setToasts((prev) => {
          const id = `tariff-init-${Date.now()}`;
          return [...prev, { id, type: 'warning', message: `Tariff config fallback in use: ${String(err?.message || err || 'load_failed')}` }];
        });
      });
    return () => controller.abort();
  }, []);

  useEffect(() => {
    if (mode === 'demo') {
      setState((prev) => ({ ...prev, connection: 'demo', lastError: '' }));
      const id = setInterval(() => {
        const frame = demoTickRef.current();
        setState((prev) => {
          let next = applyTelemetry(prev, frame.telemetry);
          next = applyPrediction(next, frame.shortPred, 'short');
          next = applyPrediction(next, frame.longPred, 'long_rf');
          return next;
        });
      }, 1200);
      return () => clearInterval(id);
    }

    const twin = createTwinClient({
      brokerUrl: BROKER_URL,
      username: USERNAME,
      password: PASSWORD,
      plantId: PLANT_ID,
      onState: setState,
    });

    clientRef.current = twin;
    return () => {
      twin.disconnect();
      clientRef.current = null;
    };
  }, [mode]);

  useEffect(() => {
    const loads = state.loads || {};
    setLoadDrafts((prev) => {
      const next = { ...prev };
      for (const [name, ld] of Object.entries(loads)) {
        const duty = Number(ld?.duty);
        const priority = Number(ld?.priority);
        const loadClass = String(ld?.class ?? 'IMPORTANT');
        if (!next[name]) {
          next[name] = {
            duty: Number.isFinite(duty) ? duty : 1,
            priority: Number.isFinite(priority) ? priority : 99,
            load_class: loadClass,
            dirty: { duty: false, priority: false, load_class: false },
          };
          continue;
        }
        const dirty = next[name].dirty || {};
        if (!dirty.duty && Number.isFinite(duty)) next[name].duty = duty;
        if (!dirty.priority && Number.isFinite(priority)) next[name].priority = priority;
        if (!dirty.load_class && loadClass) next[name].load_class = loadClass;
        const nextDirty = {
          duty: !!dirty.duty,
          priority: !!dirty.priority,
          load_class: !!dirty.load_class,
        };
        if (nextDirty.duty && Number.isFinite(duty) && Number(next[name].duty) === duty) nextDirty.duty = false;
        if (nextDirty.priority && Number.isFinite(priority) && Number(next[name].priority) === priority) nextDirty.priority = false;
        if (nextDirty.load_class && loadClass && String(next[name].load_class) === loadClass) nextDirty.load_class = false;
        next[name].dirty = nextDirty;
      }
      return next;
    });
  }, [state.loads]);

  const system = state.system || {};
  const env = state.environment || {};
  const energy = state.energy || {};
  const budget = energy.budget || {};
  const evaluation = state.evaluation || {};
  const evalControl = evaluation.control || {};
  const evalStability = evaluation.stability || {};
  const evalPredictor = evaluation.predictor || {};
  const evalPrediction = evalPredictor.prediction || evaluation.prediction || {};
  const evalForecast = evalPredictor.forecast_accuracy || evaluation.forecast_accuracy || {};
  const evalPredictorStability = evalPredictor.stability || {};
  const predictionAgeMsLive = normalizePredictionAgeMs(evalPrediction.prediction_age_ms);

  const energyWindow = energy.window_wh || {};
  const energyGoalWindowWh = Number(budget.consumed_window_wh ?? energyWindow.last_1d ?? energy.window_wh_1d ?? energy.total_energy_wh ?? 0);
  const energyCapWindowWh = Number(system.ENERGY_GOAL_VALUE_WH || system.MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH || 0);
  const energyGoalWindowMin = Number(system.ENERGY_GOAL_DURATION_MIN || budget.window_duration_min || 1440);
  const energyLeftGoalWh = energyCapWindowWh > 0 ? Math.max(0, energyCapWindowWh - energyGoalWindowWh) : null;
  const energyUsedGoalPct = energyCapWindowWh > 0 ? (energyGoalWindowWh / energyCapWindowWh) * 100 : null;
  const totalPowerW = Number(system.total_power_w || 0);
  const energyRateWhPerMin = totalPowerW / 60;
  const tariffState = resolveUkTouTariffState(ukClockTickMs, tariffConfig);
  const tariffRatePerKwh = resolveTariffRatePerKwh(tariffState, tariffConfig);
  const tariffRatePerWh = tariffRatePerKwh / 1000;
  const costRatePerMin = energyRateWhPerMin * tariffRatePerWh;
  const totalEnergyWh = Number(energy.total_energy_wh);
  const estimatedTotalCost = Number.isFinite(totalEnergyWh) ? totalEnergyWh * tariffRatePerWh : NaN;
  const estimatedGoalWindowCost = Number.isFinite(energyGoalWindowWh) ? energyGoalWindowWh * tariffRatePerWh : NaN;
  const tariffStateSource = tariffConfigMeta.source === 'backend'
    ? 'BACKEND_CONFIG'
    : tariffConfigMeta.source === 'draft'
      ? 'DRAFT_CONFIG'
      : 'DEFAULT_CONFIG';

  const process = state.process || {};
  const tank1 = process.tank1 || {};
  const tank2 = process.tank2 || {};
  const processEnabled = Boolean(process.enabled);
  const processLockActive = Boolean(process.lock_active);
  const processGoalReached = Boolean(process.goal_reached);
  const processState = process.state || 'IDLE';
  const processCycleCount = Number(process.cycle_count || 0);
  const processGoalCycles = Number(process.goal_cycles || 0);
  const processElapsedSec = Number(process.elapsed_sec || 0);
  const processEnergyWh = Number(state.processEnergyWh || 0);
  const processLastEnergyWh = Number(state.processLastEnergyWh || 0);
  const evalSettlingSec = evalPredictorStability.last_peak_settling_time_sec
    ?? evalStability.last_peak_settling_time_sec;

  const loads = Object.entries(state.loads || {}).sort((a, b) => String(a[0]).localeCompare(String(b[0])));
  const predictorModeRaw = String(system.predictor_mode || 'OFFLINE').toUpperCase();
  const predictorModeValue = PREDICTOR_MODE_OPTIONS.includes(predictorModeRaw) ? predictorModeRaw : 'OFFLINE';
  const predictionSourceStatus = system.prediction_sources || {};
  const activeAiSourcesValue = String(system.ai_source_active || 'NONE');
  const aiDynamicProcessSource = String(system.ai_dynamic_process_source || 'NONE');
  const aiDynamicProcessReason = String(system.ai_dynamic_process_reason || '');
  const selectedPolicyValue = String(systemDraft.controlPolicy ?? system.control_policy ?? '').toUpperCase();
  const selectedPolicyNoEnergy = selectedPolicyValue === 'NO_ENERGY_MANAGEMENT';
  const selectedPolicyAiDriven = selectedPolicyValue === 'HYBRID' || selectedPolicyValue === 'AI_PREFERRED';
  const aiDynamicPolicyLive = Boolean(system.ai_dynamic_process);
  const emergencyStopActive = Boolean(system.emergency_stop);
  const canControlLoads = !emergencyStopActive;
  const historySpanMs = Math.max(0, Number(historyQuery.toMs || 0) - Number(historyQuery.fromMs || 0));
  const historyStepSec = chooseHistoryStepSec(historyQuery.fromMs, historyQuery.toMs);
  const historyChartRows = historyRows;
  const analyticsCostRows = useMemo(
    () => buildAccumulatedCostSeries(state.powerHistory, {
      powerKey: 'power_w',
      energyKey: 'total_energy_wh',
    }, tariffConfig),
    [state.powerHistory, tariffConfig],
  );
  const analyticsCostDomain = useMemo(
    () => computeSeriesDomain(analyticsCostRows, ['total_cost']),
    [analyticsCostRows],
  );
  const analyticsCostSummary = analyticsCostRows.length ? analyticsCostRows[analyticsCostRows.length - 1] : null;
  const analyticsAccumulatedEnergyCost = Number(analyticsCostSummary?.accumulated_energy_cost);
  const analyticsDemandChargeCost = Number(analyticsCostSummary?.demand_charge_cost);
  const analyticsAccumulatedCost = Number(analyticsCostSummary?.total_cost);
  const analyticsMaxDemandKw = Number(analyticsCostSummary?.max_demand_kw);
  const livePowerScale = useMemo(() => chooseScaleSpec('power', collectSeriesValues(state.powerHistory, ['power_w'])), [state.powerHistory]);
  const liveCurrentScale = useMemo(() => chooseScaleSpec('current', collectSeriesValues(state.powerHistory, ['current_a'])), [state.powerHistory]);
  const liveEnergyScale = useMemo(() => chooseScaleSpec('energy', collectSeriesValues(state.powerHistory, ['total_energy_wh'])), [state.powerHistory]);
  const liveCostScale = useMemo(() => chooseScaleSpec('cost', collectSeriesValues(analyticsCostRows, ['total_cost'])), [analyticsCostRows]);
  const predictionPowerScale = useMemo(
    () => chooseScaleSpec('power', collectSeriesValues(state.predictionPowerHistory, PREDICTION_SERIES.map((series) => series.key))),
    [state.predictionPowerHistory],
  );
  const predictionEnergyScale = useMemo(
    () => chooseScaleSpec('energy', collectSeriesValues(state.predictionEnergyHistory, PREDICTION_SERIES.map((series) => series.key))),
    [state.predictionEnergyHistory],
  );
  const predictionCardPowerValues = useMemo(
    () => collectSeriesValues([
      state.predictions.short,
      state.predictions.longRf,
      state.predictions.longLstm,
    ], ['predicted_power_w']),
    [state.predictions.short, state.predictions.longRf, state.predictions.longLstm],
  );
  const predictionCardEnergyValues = useMemo(
    () => [
      predictionEnergyDisplay(state.predictions.short),
      predictionEnergyDisplay(state.predictions.longRf),
      predictionEnergyDisplay(state.predictions.longLstm),
    ].map((value) => Number(value)).filter((value) => Number.isFinite(value)),
    [state.predictions.short, state.predictions.longRf, state.predictions.longLstm],
  );
  const predictionCardPowerScale = useMemo(() => chooseScaleSpec('power', predictionCardPowerValues), [predictionCardPowerValues]);
  const predictionCardEnergyScale = useMemo(() => chooseScaleSpec('energy', predictionCardEnergyValues), [predictionCardEnergyValues]);
  const historyCostRows = useMemo(
    () => (
      historyChartRows.some((row) => Number.isFinite(Number(row?.actual_total_cost)))
        ? historyChartRows.map((row) => ({
          ...row,
          total_cost: Number(row?.actual_total_cost),
          accumulated_cost: Number(row?.actual_total_cost),
          accumulated_energy_cost: Number(row?.actual_accumulated_energy_cost),
          demand_charge_cost: Number(row?.actual_demand_charge_cost),
          max_demand_kw: Number(row?.actual_max_demand_kw),
          demand_interval_min: Number(row?.actual_demand_interval_min),
        }))
        : buildAccumulatedCostSeries(historyChartRows, {
          powerKey: 'actual_power_w',
          energyKey: 'actual_total_energy_wh',
        }, tariffConfig)
    ),
    [historyChartRows, tariffConfig],
  );
  const historyPowerKeys = [
    historySeries.actual ? 'actual_power_w' : null,
    historySeries.short ? 'pred_short_power_w' : null,
    historySeries.longRf ? 'pred_long_rf_power_w' : null,
    historySeries.longLstm ? 'pred_long_lstm_power_w' : null,
  ].filter(Boolean);
  const historyEnergyKeys = [
    historySeries.actual ? 'actual_total_energy_wh' : null,
    historySeries.short ? 'pred_short_energy_wh' : null,
    historySeries.longRf ? 'pred_long_rf_energy_wh' : null,
    historySeries.longLstm ? 'pred_long_lstm_energy_wh' : null,
  ].filter(Boolean);
  const historyPowerDomain = useMemo(() => computeSeriesDomain(historyChartRows, historyPowerKeys), [historyChartRows, historyPowerKeys]);
  const historyEnergyDomain = useMemo(() => computeSeriesDomain(historyChartRows, historyEnergyKeys), [historyChartRows, historyEnergyKeys]);
  const historyCostDomain = useMemo(() => computeSeriesDomain(historyCostRows, ['total_cost']), [historyCostRows]);
  const historyCostSummary = historyCostRows.length ? historyCostRows[historyCostRows.length - 1] : null;
  const historyAccumulatedCost = Number(historyCostSummary?.total_cost);
  const historyAccumulatedEnergyCost = Number(historyCostSummary?.accumulated_energy_cost);
  const historyDemandChargeCost = Number(historyCostSummary?.demand_charge_cost);
  const historyMaxDemandKw = Number(historyCostSummary?.max_demand_kw);
  const historyPowerScale = useMemo(() => chooseScaleSpec('power', collectSeriesValues(historyChartRows, historyPowerKeys)), [historyChartRows, historyPowerKeys]);
  const historyEnergyScale = useMemo(() => chooseScaleSpec('energy', collectSeriesValues(historyChartRows, historyEnergyKeys)), [historyChartRows, historyEnergyKeys]);
  const historyCostScale = useMemo(() => chooseScaleSpec('cost', collectSeriesValues(historyCostRows, ['total_cost'])), [historyCostRows]);
  const historyChartWidth = useMemo(() => {
    const pointCount = Math.max(1, historyChartRows.length);
    const pxPerPoint = historySpanMs <= 24 * 60 * 60 * 1000 ? 34
      : historySpanMs <= 7 * 24 * 60 * 60 * 1000 ? 24
        : 16;
    return Math.max(920, pointCount * pxPerPoint);
  }, [historyChartRows.length, historySpanMs]);

  const pushToast = (type, message, ttlMs = 4500) => {
    const id = `${Date.now()}-${toastIdRef.current++}`;
    setToasts((prev) => [...prev, { id, type, message }]);
    if (ttlMs > 0) {
      setTimeout(() => {
        setToasts((prev) => prev.filter((t) => t.id !== id));
      }, ttlMs);
    }
  };

  const dismissToast = (id) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  const roundTo = (value, decimals = 2) => {
    const num = Number(value);
    if (!Number.isFinite(num)) return undefined;
    const factor = 10 ** decimals;
    return Math.round(num * factor) / factor;
  };

  const maxHighPctForHeight = (heightCm) => {
    const h = Number(heightCm);
    if (!Number.isFinite(h) || h <= 0) return undefined;
    return roundTo(100 - ((ULTRASONIC_BLIND_SPOT_CM / h) * 100), 2);
  };

  const pickNumber = (...values) => {
    for (const value of values) {
      const num = Number(value);
      if (Number.isFinite(num)) return num;
    }
    return undefined;
  };

  const pickBoolean = (...values) => {
    for (const value of values) {
      if (typeof value === 'boolean') return value;
    }
    return undefined;
  };

  const clampNumber = (value, minValue, maxValue) => {
    let num = Number(value);
    if (!Number.isFinite(num)) return undefined;
    if (Number.isFinite(minValue)) num = Math.max(num, minValue);
    if (Number.isFinite(maxValue)) num = Math.min(num, maxValue);
    return num;
  };

  const clampWithToast = (label, value, minValue, maxValue, unit = '') => {
    const raw = Number(value);
    if (!Number.isFinite(raw)) return undefined;
    let out = raw;
    const unitSuffix = unit ? ` ${unit}` : '';
    if (Number.isFinite(minValue) && raw < minValue) {
      out = Number(minValue);
      pushToast('warning', `${label} adjusted to minimum ${Number(minValue).toFixed(2)}${unitSuffix}.`);
    }
    if (Number.isFinite(maxValue) && raw > maxValue) {
      out = Number(maxValue);
      pushToast('warning', `${label} adjusted to maximum ${Number(maxValue).toFixed(2)}${unitSuffix}.`);
    }
    return out;
  };

  const buildProcessStartPayload = () => {
    const processPayload = { enabled: true, restart: true };

    const goalCycles = pickNumber(process.goal_cycles);
    if (Number.isFinite(goalCycles) && goalCycles > 0) {
      processPayload.goal_cycles = Math.max(1, Math.round(goalCycles));
    }

    const tank1Height = pickNumber(process.tank1_height_cm, tank1.height_cm, process.tank_height_cm, system.tank_height_cm);
    const tank2Height = pickNumber(process.tank2_height_cm, tank2.height_cm, process.tank_height_cm, system.tank_height_cm);
    let tank1Low = pickNumber(tank1.low_level_pct, process.tank1_low_level_pct);
    let tank2Low = pickNumber(tank2.low_level_pct, process.tank2_low_level_pct);
    let tank1High = pickNumber(tank1.high_level_pct, process.tank1_high_level_pct);
    let tank2High = pickNumber(tank2.high_level_pct, process.tank2_high_level_pct);
    const tank1MaxHigh = maxHighPctForHeight(tank1Height);
    const tank2MaxHigh = maxHighPctForHeight(tank2Height);
    if (Number.isFinite(tank1High) && Number.isFinite(tank1MaxHigh) && tank1High > tank1MaxHigh) {
      tank1High = tank1MaxHigh;
      pushToast(
        'warning',
        `Tank1 high level adjusted to ${tank1MaxHigh.toFixed(1)}% (max for ${ULTRASONIC_BLIND_SPOT_CM}cm ultrasonic blind spot).`,
      );
    }
    if (Number.isFinite(tank2High) && Number.isFinite(tank2MaxHigh) && tank2High > tank2MaxHigh) {
      tank2High = tank2MaxHigh;
      pushToast(
        'warning',
        `Tank2 high level adjusted to ${tank2MaxHigh.toFixed(1)}% (max for ${ULTRASONIC_BLIND_SPOT_CM}cm ultrasonic blind spot).`,
      );
    }

    const tank1LevelTarget = clampNumber(
      pickNumber(tank1.target_level_pct, process.tank1_level_target_pct),
      tank1Low,
      tank1High,
    );
    if (Number.isFinite(tank1LevelTarget)) processPayload.tank1_level_target_pct = tank1LevelTarget;

    const tank2LevelTarget = clampNumber(
      pickNumber(tank2.target_level_pct, process.tank2_level_target_pct),
      tank2Low,
      tank2High,
    );
    if (Number.isFinite(tank2LevelTarget)) processPayload.tank2_level_target_pct = tank2LevelTarget;

    const tank1TempTarget = pickNumber(tank1.target_temp_c, process.tank1_temp_target_c);
    if (Number.isFinite(tank1TempTarget)) processPayload.tank1_temp_target_c = tank1TempTarget;

    const tank2TempTarget = pickNumber(tank2.target_temp_c, process.tank2_temp_target_c);
    if (Number.isFinite(tank2TempTarget)) processPayload.tank2_temp_target_c = tank2TempTarget;

    const predictorMode = String(process.predictor_mode || system.predictor_mode || 'OFFLINE').toUpperCase();
    if (predictorMode && predictorMode !== 'UNSET') processPayload.predictor_mode = predictorMode;

    if (Number.isFinite(tank1Height)) processPayload.tank1_height_cm = tank1Height;
    if (Number.isFinite(tank2Height)) processPayload.tank2_height_cm = tank2Height;
    if (Number.isFinite(tank1Low)) processPayload.tank1_low_level_pct = tank1Low;
    if (Number.isFinite(tank2Low)) processPayload.tank2_low_level_pct = tank2Low;
    if (Number.isFinite(tank1High)) processPayload.tank1_high_level_pct = tank1High;
    if (Number.isFinite(tank2High)) processPayload.tank2_high_level_pct = tank2High;
    if (Number.isFinite(tank1Height) && Number.isFinite(tank2Height) && tank1Height === tank2Height) {
      processPayload.tank_height_cm = tank1Height;
    }

    return { process: processPayload };
  };

  const tank1LowBound = roundTo(pickNumber(tank1.low_level_pct, process.tank1_low_level_pct), 2);
  const tank1HighBound = roundTo(pickNumber(tank1.high_level_pct, process.tank1_high_level_pct), 2);
  const tank2LowBound = roundTo(pickNumber(tank2.low_level_pct, process.tank2_low_level_pct), 2);
  const tank2HighBound = roundTo(pickNumber(tank2.high_level_pct, process.tank2_high_level_pct), 2);
  const tank1HeightLive = pickNumber(tank1.height_cm, process.tank1_height_cm, process.tank_height_cm, system.tank_height_cm);
  const tank2HeightLive = pickNumber(tank2.height_cm, process.tank2_height_cm, process.tank_height_cm, system.tank_height_cm);
  const tank1MaxHighPct = maxHighPctForHeight(tank1HeightLive);
  const tank2MaxHighPct = maxHighPctForHeight(tank2HeightLive);
  const tank1LevelValid = pickBoolean(tank1.level_valid, process.tank1_level_valid);
  const tank2LevelValid = pickBoolean(tank2.level_valid, process.tank2_level_valid);
  const tank1LevelLive = pickNumber(tank1.level_pct, process.tank1_level_pct);
  const tank2LevelLive = pickNumber(tank2.level_pct, process.tank2_level_pct);
  const tank1TempLive = pickNumber(tank1.temperature_c, process.tank1_temp_c);
  const tank2TempLive = pickNumber(tank2.temperature_c, process.tank2_temp_c);
  const tank1DistanceLive = pickNumber(tank1.ultrasonic_distance_cm, process.tank1_ultrasonic_distance_cm);
  const tank2DistanceLive = pickNumber(tank2.ultrasonic_distance_cm, process.tank2_ultrasonic_distance_cm);
  const tank1RawDistanceLive = pickNumber(tank1.ultrasonic_raw_cm, process.tank1_ultrasonic_raw_cm);
  const tank2RawDistanceLive = pickNumber(tank2.ultrasonic_raw_cm, process.tank2_ultrasonic_raw_cm);
  const tank1WaterHeightLive = (
    Number.isFinite(tank1HeightLive) && Number.isFinite(tank1DistanceLive)
      ? Math.max(0, Number(tank1HeightLive) - Number(tank1DistanceLive))
      : undefined
  );
  const tank2WaterHeightLive = (
    Number.isFinite(tank2HeightLive) && Number.isFinite(tank2DistanceLive)
      ? Math.max(0, Number(tank2HeightLive) - Number(tank2DistanceLive))
      : undefined
  );

  const publish = (payload) => {
    if (mode === 'demo') {
      setState((prev) => applyAck(prev, { demo: true, queued: payload }));
      return;
    }
    const ok = clientRef.current?.publish(payload);
    if (!ok) {
      setState((prev) => applyAck(prev, { local: true, error: 'MQTT not connected', payload }));
      pushToast('error', 'MQTT not connected.');
    }
  };

  const publishTopic = (topic, payload) => {
    if (mode === 'demo') {
      setState((prev) => applyAck(prev, { demo: true, topic, queued: payload }));
      return;
    }
    const ok = clientRef.current?.publishTopic(topic, payload);
    if (!ok) {
      setState((prev) => applyAck(prev, { local: true, error: 'MQTT not connected', topic, payload }));
      pushToast('error', 'MQTT not connected.');
    }
  };

  const handleStartProcess = () => {
    if (emergencyStopActive) {
      pushToast('warning', 'Emergency stop is active. Clear it before starting the process.');
      return;
    }
    const predictorMode = String(process.predictor_mode || system.predictor_mode || 'OFFLINE').toUpperCase();
    if (!predictorMode || predictorMode === 'UNSET') {
      pushToast('warning', 'Set Predictor Mode before starting the process.');
      return;
    }
    publish(buildProcessStartPayload());
  };

  useEffect(() => {
    if (!Array.isArray(state.ackFeed) || state.ackFeed.length === 0) return;
    const latest = state.ackFeed[state.ackFeed.length - 1];
    if (!latest || !Number.isFinite(latest.ts) || latest.ts <= lastAckToastTsRef.current) return;
    lastAckToastTsRef.current = latest.ts;

    const payload = latest.payload || {};
    if (payload.local || payload.demo) return;

    const ack = payload.ack || {};
    if (ack && ack.ok === false) {
      const msgRaw = String(ack.msg || 'command_blocked');
      const missingRaw = String(ack.missing || '').replace(/,+$/, '');
      const missingList = missingRaw
        .split(',')
        .map((s) => s.trim())
        .filter(Boolean);

      let text;
      if (msgRaw.includes('process_config_incomplete')) {
        text = missingList.length
          ? `Process start blocked. Missing: ${missingList.join(', ')}.`
          : 'Process start blocked. Required parameters are missing.';
      } else if (msgRaw === 'emergency_stop_active') {
        text = 'Command blocked. Emergency stop is active.';
      } else if (msgRaw === 'device_fault_lockout') {
        text = `Load command blocked by fault lockout (${ack.device || 'device'}: ${ack.fault_code || 'FAULT'}).`;
      } else if (msgRaw.includes('process_lock_active')) {
        text = 'Command blocked by process lock (allowed in ONLINE mode or after process stop).';
      } else {
        text = msgRaw.replace(/_/g, ' ');
        if (missingList.length) text += `. Missing: ${missingList.join(', ')}.`;
      }
      pushToast('warning', text, 6500);
      return;
    }

    if (payload.error) {
      pushToast('error', String(payload.error));
    }
  }, [state.ackFeed]);

  useEffect(() => {
    if (!HISTORY_API_URL) {
      setHistoryRows([]);
      setHistoryError('');
      setHistoryLoading(false);
      return;
    }
    const fromMs = Number(historyQuery.fromMs);
    const toMs = Number(historyQuery.toMs);
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs < fromMs) {
      return;
    }
    const stepSec = chooseHistoryStepSec(fromMs, toMs);

    const controller = new AbortController();
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    const estimatedPoints = Math.ceil((toMs - fromMs) / Math.max(1000, stepSec * 1000)) + 10;
    const maxPoints = Math.max(300, Math.min(8000, estimatedPoints));
    const params = new URLSearchParams({
      from: new Date(fromMs).toISOString(),
      to: new Date(toMs).toISOString(),
      step_sec: String(Math.max(1, Math.round(stepSec))),
      max_points: String(maxPoints),
    });
    const url = `${base}/api/history?${params.toString()}`;

    setHistoryLoading(true);
    setHistoryError('');
    fetch(url, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((body) => {
        if (!body?.ok) throw new Error(String(body?.error || 'history_query_failed'));
        const rows = Array.isArray(body?.rows) ? body.rows : [];
        setHistoryRows(rows);
        setHistoryError('');
      })
      .catch((err) => {
        if (err?.name === 'AbortError') return;
        setHistoryRows([]);
        setHistoryError(String(err?.message || err || 'history_query_failed'));
      })
      .finally(() => setHistoryLoading(false));

    return () => controller.abort();
  }, [historyQuery]);

  const applyHistoryQuery = () => {
    const fromMs = parseDateTimeLocal(historyDraft.from);
    const toMs = parseDateTimeLocal(historyDraft.to);
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs)) {
      pushToast('warning', 'Set a valid historical date/time range.');
      return;
    }
    if (toMs < fromMs) {
      pushToast('warning', 'History range is invalid: "To" must be after "From".');
      return;
    }
    setHistoryQuery({ fromMs, toMs });
  };

  const setHistoryQuickRangeHours = (hours) => {
    const now = Date.now();
    const fromMs = now - (hours * 60 * 60 * 1000);
    setHistoryDraft((prev) => ({
      ...prev,
      from: toDateTimeLocal(fromMs),
      to: toDateTimeLocal(now),
    }));
    setHistoryQuery((prev) => ({
      ...prev,
      fromMs,
      toMs: now,
    }));
  };

  const downloadChapter4Export = async () => {
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return;
    }
    const fromMs = Number(historyQuery.fromMs);
    const toMs = Number(historyQuery.toMs);
    if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs < fromMs) {
      pushToast('warning', 'Apply a valid history range before exporting.');
      return;
    }

    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    const params = new URLSearchParams({
      from: new Date(fromMs).toISOString(),
      to: new Date(toMs).toISOString(),
      prediction_step_sec: String(Math.max(1, Math.round(historyStepSec))),
    });
    const url = `${base}/api/chapter4-export?${params.toString()}`;

    setHistoryExporting(true);
    try {
      const response = await fetch(url);
      if (!response.ok) {
        let errorText = `HTTP ${response.status}`;
        try {
          const body = await response.json();
          errorText = String(body?.error || errorText);
        } catch {
          const text = await response.text();
          if (text) errorText = text;
        }
        throw new Error(errorText);
      }
      const blob = await response.blob();
      const fallbackName = `csv_export_${new Date(fromMs).toISOString().slice(0, 10)}_${new Date(toMs).toISOString().slice(0, 10)}.zip`;
      const downloadName = getDownloadFilename(response, fallbackName);
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = objectUrl;
      link.download = downloadName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(objectUrl);
      pushToast('info', 'CSV export downloaded.');
    } catch (err) {
      pushToast('error', `CSV export failed: ${String(err?.message || err || 'unknown_error')}`);
    } finally {
      setHistoryExporting(false);
    }
  };

  const setLoadDraft = (name, patch) => {
    setLoadDrafts((prev) => {
      const current = prev[name] || {};
      const dirty = { ...(current.dirty || {}) };
      if (Object.prototype.hasOwnProperty.call(patch, 'duty')) dirty.duty = true;
      if (Object.prototype.hasOwnProperty.call(patch, 'priority')) dirty.priority = true;
      if (Object.prototype.hasOwnProperty.call(patch, 'load_class')) dirty.load_class = true;
      if (Object.prototype.hasOwnProperty.call(patch, 'inject_current_a')) dirty.inject_current_a = true;
      if (Object.prototype.hasOwnProperty.call(patch, 'fault_limit_a')) dirty.fault_limit_a = true;
      if (Object.prototype.hasOwnProperty.call(patch, 'fault_trip_ms')) dirty.fault_trip_ms = true;
      if (Object.prototype.hasOwnProperty.call(patch, 'fault_clear_ms')) dirty.fault_clear_ms = true;
      if (Object.prototype.hasOwnProperty.call(patch, 'fault_restore_ms')) dirty.fault_restore_ms = true;
      return {
        ...prev,
        [name]: {
          ...current,
          ...patch,
          dirty,
        },
      };
    });
  };

  const clearLoadDirty = (name, field) => {
    setLoadDrafts((prev) => {
      const current = prev[name];
      if (!current || !current.dirty) return prev;
      return {
        ...prev,
        [name]: {
          ...current,
          dirty: { ...current.dirty, [field]: false },
        },
      };
    });
  };

  const sendLoadPower = (name, on) => {
    if (!canControlLoads) {
      pushToast('warning', 'Emergency stop active. Clear it to control loads.');
      return;
    }
    publish({ device: name, set: { on } });
  };

  const sendLoadDuty = (name) => {
    if (!canControlLoads) {
      pushToast('warning', 'Emergency stop active. Clear it to control loads.');
      return;
    }
    const d = Number(loadDrafts?.[name]?.duty);
    if (!Number.isFinite(d)) return;
    publish({ device: name, set: { duty: Math.max(0, Math.min(1, d)) } });
    clearLoadDirty(name, 'duty');
  };

  const sendLoadInject = (name, clear = false) => {
    if (clear) {
      publish({ device: name, set: { clear_inject: true } });
      clearLoadDirty(name, 'inject_current_a');
      return;
    }
    const val = Number(loadDrafts?.[name]?.inject_current_a);
    if (!Number.isFinite(val)) return;
    publish({ device: name, set: { inject_current_a: Math.max(0, val) } });
    clearLoadDirty(name, 'inject_current_a');
  };

  const sendLoadFaultConfig = (name) => {
    const limit = Number(loadDrafts?.[name]?.fault_limit_a);
    const trip = Number(loadDrafts?.[name]?.fault_trip_ms);
    const clear = Number(loadDrafts?.[name]?.fault_clear_ms);
    const restore = Number(loadDrafts?.[name]?.fault_restore_ms);
    const set = {};
    if (Number.isFinite(limit)) set.fault_limit_a = Math.max(0, limit);
    if (Number.isFinite(trip)) set.fault_trip_ms = Math.max(0, Math.round(trip));
    if (Number.isFinite(clear)) set.fault_clear_ms = Math.max(0, Math.round(clear));
    if (Number.isFinite(restore)) set.fault_restore_ms = Math.max(0, Math.round(restore));
    if (!Object.keys(set).length) return;
    publish({ device: name, set });
    clearLoadDirty(name, 'fault_limit_a');
    clearLoadDirty(name, 'fault_trip_ms');
    clearLoadDirty(name, 'fault_clear_ms');
    clearLoadDirty(name, 'fault_restore_ms');
  };

  const sendLoadPriority = (name) => {
    const priority = Number(loadDrafts?.[name]?.priority);
    if (!Number.isFinite(priority)) return;
    publish({ device: name, meta: { priority: Math.max(1, Math.round(priority)) } });
    clearLoadDirty(name, 'priority');
  };

  const sendLoadClass = (name) => {
    const loadClass = String(loadDrafts?.[name]?.load_class || '').toUpperCase();
    if (!LOAD_CLASS_OPTIONS.includes(loadClass)) return;
    publish({ device: name, meta: { class: loadClass } });
    clearLoadDirty(name, 'load_class');
  };

  const sendLoadOverride = (name, enabled) => {
    publish({ device: name, set: { override: !!enabled } });
  };

  const numberInputValue = (draft, fallback, decimals = 2) => {
    if (draft !== undefined && draft !== null && draft !== '') return draft;
    const num = Number(fallback);
    if (!Number.isFinite(num)) return '';
    if (decimals === 0) return String(Math.round(num));
    return Number(num).toFixed(decimals);
  };

  const parseDraftNumber = (value) => {
    if (value === undefined || value === null || value === '') return undefined;
    const num = Number(value);
    return Number.isFinite(num) ? num : undefined;
  };

  const setSystemDraftField = (field, value) => {
    setSystemDraft((prev) => ({ ...prev, [field]: value }));
  };

  const setTariffTimeField = (field, value) => {
    setTariffConfig((prev) => normalizeTariffConfig({
      ...prev,
      [field]: parseTimeInputToMinutes(value, prev[field]),
    }));
    setTariffConfigMeta((prev) => ({ ...prev, source: 'draft' }));
  };

  const setTariffRateField = (field, value) => {
    setTariffConfig((prev) => normalizeTariffConfig({
      ...prev,
      [field]: normalizeTariffRate(value, prev[field]),
    }));
    setTariffConfigMeta((prev) => ({ ...prev, source: 'draft' }));
  };

  const setTariffIntervalField = (field, value) => {
    setTariffConfig((prev) => normalizeTariffConfig({
      ...prev,
      [field]: normalizeDemandIntervalMin(value, prev[field]),
    }));
    setTariffConfigMeta((prev) => ({ ...prev, source: 'draft' }));
  };

  const saveTariffConfig = async () => {
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return;
    }
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    setTariffConfigSaving(true);
    try {
      const response = await fetch(`${base}/api/tariff-config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          config: {
            effective_from: new Date().toISOString(),
            timezone: UK_TIMEZONE,
            useDashboardTou: true,
            offpeakStartMin: tariffConfig.offpeakStartMin,
            offpeakEndMin: tariffConfig.offpeakEndMin,
            peakStartMin: tariffConfig.peakStartMin,
            peakEndMin: tariffConfig.peakEndMin,
            offpeakRatePerKwh: tariffConfig.offpeakRatePerKwh,
            midpeakRatePerKwh: tariffConfig.midpeakRatePerKwh,
            peakRatePerKwh: tariffConfig.peakRatePerKwh,
            demandChargePerKw: tariffConfig.demandChargePerKw,
            demandIntervalMin: tariffConfig.demandIntervalMin,
          },
        }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!body?.ok || !body?.active) throw new Error(String(body?.error || 'tariff_config_save_failed'));
      setTariffConfig(normalizeTariffConfig(body.active));
      setTariffConfigMeta({
        source: 'backend',
        versionCount: Number(body.version_count || 0),
        effectiveFrom: String(body.active.effective_from || ''),
      });
      pushToast('info', 'Tariff settings saved to backend.');
    } catch (err) {
      pushToast('error', `Tariff settings save failed: ${String(err?.message || err || 'unknown_error')}`);
    } finally {
      setTariffConfigSaving(false);
    }
  };

  const resetTariffConfig = () => {
    setTariffConfig(DEFAULT_TARIFF_CONFIG);
    setTariffConfigMeta((prev) => ({ ...prev, source: 'draft' }));
    pushToast('info', 'Tariff schedule and demand settings reset locally. Save to persist.');
  };

  const setProcessDraftField = (field, value) => {
    setProcessDraft((prev) => ({ ...prev, [field]: value }));
  };

  const sendVoltageInjection = (mode) => {
    const liveMagnitude = Number(system.VOLTAGE_INJECTION_MAGNITUDE_V);
    const draftMagnitude = parseDraftNumber(systemDraft.voltageInjectionMagnitudeV);
    const requestedMagnitude = mode === 'NONE'
      ? 0
      : (Number.isFinite(draftMagnitude) ? draftMagnitude : (Number.isFinite(liveMagnitude) && liveMagnitude > 0 ? liveMagnitude : 1.0));

    const magnitude = clampWithToast('Voltage Injection Magnitude', requestedMagnitude, 0, 24, 'V');
    if (!Number.isFinite(magnitude)) return;

    if (mode === 'NONE') {
      publish({ control: { CLEAR_VOLTAGE_INJECTION: true } });
      return;
    }

    publish({
      control: {
        VOLTAGE_INJECTION_MODE: mode,
        VOLTAGE_INJECTION_MAGNITUDE_V: magnitude,
      },
    });
  };

  const applySystemControls = () => {
    const draft = systemDraft || {};
    const hasDraft = Object.values(draft).some((v) => v !== undefined && v !== null && v !== '');
    let applied = false;

    if (draft.controlPolicy !== undefined) {
      const policy = String(draft.controlPolicy || '').toUpperCase();
      if (POLICY_OPTIONS.includes(policy)) {
        publish({ control_policy: policy });
        applied = true;
      } else {
        pushToast('warning', 'Select a valid control policy.');
      }
    }

    const controlPayload = {};
    const timingPayload = {};
    const predictorShortPayload = {};

    if (draft.maxPower !== undefined) {
      const v = clampWithToast('Maximum Power Threshold', draft.maxPower, 0, undefined, 'W');
      if (Number.isFinite(v)) {
        controlPayload.MAX_TOTAL_POWER_W = v;
        predictorShortPayload.MAX_TOTAL_POWER_W = v;
      }
    }
    if (draft.undervoltageThresholdV !== undefined) {
      const v = clampWithToast('Undervoltage Threshold', draft.undervoltageThresholdV, 0, undefined, 'V');
      if (Number.isFinite(v)) controlPayload.UNDERVOLTAGE_THRESHOLD_V = v;
    }
    if (draft.undervoltageTripDelayMs !== undefined) {
      const v = clampWithToast('Undervoltage Trip Delay', draft.undervoltageTripDelayMs, 0, undefined, 'ms');
      if (Number.isFinite(v)) controlPayload.UNDERVOLTAGE_TRIP_DELAY_MS = Math.round(v);
    }
    if (draft.undervoltageClearDelayMs !== undefined) {
      const v = clampWithToast('Undervoltage Clear Delay', draft.undervoltageClearDelayMs, 0, undefined, 'ms');
      if (Number.isFinite(v)) controlPayload.UNDERVOLTAGE_CLEAR_DELAY_MS = Math.round(v);
    }
    if (draft.undervoltageRestoreMarginV !== undefined) {
      const v = clampWithToast('Undervoltage Restore Margin', draft.undervoltageRestoreMarginV, 0, undefined, 'V');
      if (Number.isFinite(v)) controlPayload.UNDERVOLTAGE_RESTORE_MARGIN_V = v;
    }
    if (draft.overvoltageThresholdV !== undefined) {
      const v = clampWithToast('Overvoltage Threshold', draft.overvoltageThresholdV, 0, undefined, 'V');
      if (Number.isFinite(v)) controlPayload.OVERVOLTAGE_THRESHOLD_V = v;
    }
    if (draft.overvoltageTripDelayMs !== undefined) {
      const v = clampWithToast('Overvoltage Trip Delay', draft.overvoltageTripDelayMs, 0, undefined, 'ms');
      if (Number.isFinite(v)) controlPayload.OVERVOLTAGE_TRIP_DELAY_MS = Math.round(v);
    }
    if (draft.overvoltageClearDelayMs !== undefined) {
      const v = clampWithToast('Overvoltage Clear Delay', draft.overvoltageClearDelayMs, 0, undefined, 'ms');
      if (Number.isFinite(v)) controlPayload.OVERVOLTAGE_CLEAR_DELAY_MS = Math.round(v);
    }
    if (draft.overvoltageRestoreMarginV !== undefined) {
      const v = clampWithToast('Overvoltage Restore Margin', draft.overvoltageRestoreMarginV, 0, undefined, 'V');
      if (Number.isFinite(v)) controlPayload.OVERVOLTAGE_RESTORE_MARGIN_V = v;
    }
    if (draft.cooldownSec !== undefined) {
      const v = clampWithToast('Cooldown', draft.cooldownSec, 0, undefined, 's');
      if (Number.isFinite(v)) controlPayload.CONTROL_COOLDOWN_SEC = v;
    }
    if (draft.sampleIntervalSec !== undefined) {
      const v = clampWithToast('Sample Interval', draft.sampleIntervalSec, 0.1, undefined, 's');
      if (Number.isFinite(v)) timingPayload.SAMPLE_INTERVAL_SEC = v;
    }

    if (draft.energyCapWindowWh !== undefined) {
      const capWh = clampWithToast('Energy Cap per Window', draft.energyCapWindowWh, 0, undefined, 'Wh');
      if (Number.isFinite(capWh)) {
        controlPayload.ENERGY_GOAL_WH = capWh;
        predictorShortPayload.MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = capWh;
      }
    }
    if (draft.energyGoalWindowMin !== undefined) {
      const win = clampWithToast('Energy Goal Window', draft.energyGoalWindowMin, 1, undefined, 'min');
      if (Number.isFinite(win)) controlPayload.ENERGY_GOAL_DURATION_MIN = win;
    }

    if (Object.keys(controlPayload).length) {
      publish({ control: controlPayload });
      if (Object.keys(predictorShortPayload).length) {
        publishTopic(topics.predictorCmd, predictorShortPayload);
      }
      applied = true;
    }
    if (Object.keys(timingPayload).length) {
      publish({ timing: timingPayload });
      applied = true;
    }

    if (draft.predictorMode !== undefined) {
      const mode = String(draft.predictorMode || '').toUpperCase();
      if (PREDICTOR_MODE_OPTIONS.includes(mode)) {
        if (mode === 'EVAL_ONLY' || mode === 'ONLINE') {
          const notReady = [];
          const longRfReady = predictionSourceStatus.long_rf?.model_ready ?? state.predictions?.longRf?.model_ready;
          const longLstmReady = predictionSourceStatus.long_lstm?.model_ready ?? state.predictions?.longLstm?.model_ready;
          if (longRfReady === false) notReady.push('LONG_RF');
          if (longLstmReady === false) notReady.push('LONG_LSTM');
          if (notReady.length) {
            pushToast('warning', `Model not ready for ${notReady.join(', ')}. ${mode} may not produce predictions yet.`);
          }
        }
        publishTopic(topics.predictorLongCmd, { PREDICTOR_MODE: mode });
        publishTopic(topics.predictorLongLstmCmd, { PREDICTOR_MODE: mode });
        publishTopic(topics.cmd, { predictor_mode: mode });
        applied = true;
      } else {
        pushToast('warning', 'Select a valid predictor mode.');
      }
    }

    if (!applied) {
      pushToast(hasDraft ? 'warning' : 'info', hasDraft ? 'No valid system changes to apply.' : 'No system changes to apply.');
      return;
    }
    setSystemDraft({});
  };

  const applyProcessControls = () => {
    const draft = processDraft || {};
    const hasDraft = Object.values(draft).some((v) => v !== undefined && v !== null && v !== '');
    const processPayload = {};

    if (draft.goalCycles !== undefined) {
      const v = parseDraftNumber(draft.goalCycles);
      if (Number.isFinite(v)) processPayload.goal_cycles = Math.max(1, Math.round(v));
    }

    const tank1HeightCandidate = draft.tank1HeightCm !== undefined
      ? clampWithToast('Tank1 Height', draft.tank1HeightCm, 10, 500, 'cm')
      : undefined;
    if (Number.isFinite(tank1HeightCandidate)) processPayload.tank1_height_cm = tank1HeightCandidate;

    const tank2HeightCandidate = draft.tank2HeightCm !== undefined
      ? clampWithToast('Tank2 Height', draft.tank2HeightCm, 10, 500, 'cm')
      : undefined;
    if (Number.isFinite(tank2HeightCandidate)) processPayload.tank2_height_cm = tank2HeightCandidate;

    const tank1HeightForBounds = Number.isFinite(processPayload.tank1_height_cm) ? processPayload.tank1_height_cm : tank1HeightLive;
    const tank2HeightForBounds = Number.isFinite(processPayload.tank2_height_cm) ? processPayload.tank2_height_cm : tank2HeightLive;
    const tank1MaxHigh = maxHighPctForHeight(tank1HeightForBounds);
    const tank2MaxHigh = maxHighPctForHeight(tank2HeightForBounds);

    let tank1Low = Number.isFinite(tank1LowBound) ? tank1LowBound : 15;
    let tank1High = Number.isFinite(tank1HighBound) ? tank1HighBound : 90;
    let tank2Low = Number.isFinite(tank2LowBound) ? tank2LowBound : 15;
    let tank2High = Number.isFinite(tank2HighBound) ? tank2HighBound : 90;
    if (Number.isFinite(tank1MaxHigh)) tank1High = clampNumber(tank1High, undefined, tank1MaxHigh);
    if (Number.isFinite(tank2MaxHigh)) tank2High = clampNumber(tank2High, undefined, tank2MaxHigh);

    if (draft.tank1LowLevelPct !== undefined) {
      const raw = parseDraftNumber(draft.tank1LowLevelPct);
      const highCap = parseDraftNumber(draft.tank1HighLevelPct) ?? tank1High;
      const v = clampWithToast('Tank1 Low Level', raw, 0, highCap, '%');
      if (Number.isFinite(v)) {
        tank1Low = v;
        processPayload.tank1_low_level_pct = v;
      }
    }
    if (draft.tank1HighLevelPct !== undefined) {
      const minHigh = Number.isFinite(processPayload.tank1_low_level_pct) ? processPayload.tank1_low_level_pct : tank1Low;
      const v = clampWithToast('Tank1 High Level', draft.tank1HighLevelPct, minHigh, tank1MaxHigh ?? 98, '%');
      const raw = parseDraftNumber(draft.tank1HighLevelPct);
      if (Number.isFinite(raw) && Number.isFinite(tank1MaxHigh) && raw > tank1MaxHigh) {
        pushToast('warning', `Tank1 high level maximum is tied to ultrasonic blind spot (${ULTRASONIC_BLIND_SPOT_CM}cm).`);
      }
      if (Number.isFinite(v)) {
        tank1High = v;
        processPayload.tank1_high_level_pct = v;
      }
    }

    if (draft.tank2LowLevelPct !== undefined) {
      const raw = parseDraftNumber(draft.tank2LowLevelPct);
      const highCap = parseDraftNumber(draft.tank2HighLevelPct) ?? tank2High;
      const v = clampWithToast('Tank2 Low Level', raw, 0, highCap, '%');
      if (Number.isFinite(v)) {
        tank2Low = v;
        processPayload.tank2_low_level_pct = v;
      }
    }
    if (draft.tank2HighLevelPct !== undefined) {
      const minHigh = Number.isFinite(processPayload.tank2_low_level_pct) ? processPayload.tank2_low_level_pct : tank2Low;
      const v = clampWithToast('Tank2 High Level', draft.tank2HighLevelPct, minHigh, tank2MaxHigh ?? 98, '%');
      const raw = parseDraftNumber(draft.tank2HighLevelPct);
      if (Number.isFinite(raw) && Number.isFinite(tank2MaxHigh) && raw > tank2MaxHigh) {
        pushToast('warning', `Tank2 high level maximum is tied to ultrasonic blind spot (${ULTRASONIC_BLIND_SPOT_CM}cm).`);
      }
      if (Number.isFinite(v)) {
        tank2High = v;
        processPayload.tank2_high_level_pct = v;
      }
    }

    if (draft.tank1LevelTarget !== undefined) {
      const v = clampWithToast('Tank1 Target Level', draft.tank1LevelTarget, tank1Low, tank1High, '%');
      if (Number.isFinite(v)) processPayload.tank1_level_target_pct = v;
    }
    if (draft.tank2LevelTarget !== undefined) {
      const v = clampWithToast('Tank2 Target Level', draft.tank2LevelTarget, tank2Low, tank2High, '%');
      if (Number.isFinite(v)) processPayload.tank2_level_target_pct = v;
    }
    if (draft.tank1TempTarget !== undefined) {
      const v = clampWithToast('Tank1 Target Temp', draft.tank1TempTarget, 0, 95, 'C');
      if (Number.isFinite(v)) processPayload.tank1_temp_target_c = v;
    }
    if (draft.tank2TempTarget !== undefined) {
      const v = clampWithToast('Tank2 Target Temp', draft.tank2TempTarget, 0, 95, 'C');
      if (Number.isFinite(v)) processPayload.tank2_temp_target_c = v;
    }

    if (!Object.keys(processPayload).length) {
      pushToast(hasDraft ? 'warning' : 'info', hasDraft ? 'No valid process changes to apply.' : 'No process changes to apply.');
      return;
    }

    publish({ process: processPayload });
    setProcessDraft({});
  };

  const triggerCurrentRecalibration = () => {
    publish({ control: { CALIBRATE_CURRENT: true } });
    pushToast('info', 'Current sensor recalibration triggered. Keep loads at no-load briefly.');
  };

  return (
    <div className="app-shell">
      <div className="backdrop" />
      <div className="toast-stack">
        {toasts.map((t) => (
          <div key={t.id} className={`toast toast-${t.type || 'info'}`}>
            <span>{t.message}</span>
            <button type="button" className="toast-close" onClick={() => dismissToast(t.id)}>×</button>
          </div>
        ))}
      </div>
      <header className="topbar">
        <div>
          <p className="kicker">Factory Twin</p>
          <h1>Digital Energy Control Room</h1>
          <p className="subtitle">Plant: {PLANT_ID}</p>
        </div>
        <div className="status-row">
          <span className={`pill status-${state.connection}`}>{state.connection}</span>
          <button type="button" className="pill-btn" onClick={() => setMode((m) => (m === 'live' ? 'demo' : 'live'))}>
            {mode === 'live' ? 'Switch to Demo' : 'Switch to Live'}
          </button>
        </div>
      </header>

      <main className="dashboard-stack">
        <Panel title="1) Load Control" subtitle="Per-load live status and direct controls" right={<span className="mono">{topics.cmd}</span>}>
          <div className="load-grid">
            {loads.map(([name, ld]) => {
              const isHeater = String(name).toLowerCase().includes('heater');
              const dutyLive = Number.isFinite(Number(ld.duty_applied)) ? Number(ld.duty_applied) : Number(ld.duty);
              const dutyDirty = Boolean(loadDrafts?.[name]?.dirty?.duty);
              const dutyDraft = Number(loadDrafts?.[name]?.duty);
              const dutyDisplay = dutyDirty && Number.isFinite(dutyDraft) ? dutyDraft : dutyLive;
              const hasFault = Boolean(
                ld.fault_active
                || (ld.health && String(ld.health).toUpperCase() === 'FAULT')
                || (ld.fault_code && String(ld.fault_code).toUpperCase() !== 'NONE')
              );
              const loadOverrideOn = Boolean(ld.override ?? ld.policy_override);
              return (
              <article key={name} className="load-card">
                <div className="load-card-head">
                  <h3>{labelLoad(name)}</h3>
                  <span className={`load-state ${ld.on ? 'on' : 'off'}`}>{ld.on ? 'ON' : 'OFF'}</span>
                  {hasFault && (
                    <span className="load-fault">
                      {ld.fault_code && String(ld.fault_code).toUpperCase() !== 'NONE' ? ld.fault_code : 'FAULT'}
                    </span>
                  )}
                </div>
                <div className="load-meta">
                  <span>{ld.class || '-'}</span>
                  <span>Priority {ld.priority ?? '-'}</span>
                  <span>Override {loadOverrideOn ? 'ON' : 'OFF'}</span>
                </div>
                <div className="load-readings">
                  <Metric label="Power" value={ld.power_w} scaleKind="power" />
                  <Metric label="Current" value={ld.current_a} scaleKind="current" />
                  <Metric label="Voltage" value={f2(ld.voltage_v)} unit="V" />
                  <Metric label="Duty Target" value={f2(ld.duty)} />
                  <Metric label="Duty Applied" value={f2(ld.duty_applied ?? ld.duty)} />
                  <Metric label="Injected" value={ld.inject_current_a} scaleKind="current" />
                  <Metric label="Fault Limit" value={ld.fault_limit_a} scaleKind="current" />
                  <Metric label="Trip Delay" value={f2(ld.fault_trip_ms)} unit="ms" />
                  <Metric label="Clear Delay" value={f2(ld.fault_clear_ms)} unit="ms" />
                  <Metric label="Restore Delay" value={f2(ld.fault_restore_ms)} unit="ms" />
                </div>
                <>
                  <div className="control-row">
                    <button onClick={() => sendLoadPower(name, true)}>Turn ON</button>
                    <button className="warn-btn" onClick={() => sendLoadPower(name, false)}>Turn OFF</button>
                  </div>
                  {!isHeater && (
                  <div className="control-block">
                    <label>
                      Duty Cycle {liveBadge(dutyLive)}
                    </label>
                    <div className="control-inline threshold-grid">
                      <input
                        type="range"
                        min="0"
                        max="1"
                        step="0.05"
                        value={Number.isFinite(dutyDisplay) ? dutyDisplay : 0}
                        onChange={(e) => setLoadDraft(name, { duty: Number(e.target.value) })}
                      />
                      <input
                        type="number"
                        min="0"
                        max="1"
                        step="0.05"
                        value={Number.isFinite(dutyDisplay) ? dutyDisplay : 0}
                        onChange={(e) => setLoadDraft(name, { duty: Number(e.target.value) })}
                      />
                      <button onClick={() => sendLoadDuty(name)}>Apply</button>
                    </div>
                  </div>
                  )}
                  <div className="control-block">
                    <label>Policy Override {liveBadge(loadOverrideOn)}</label>
                    <div className="control-inline">
                      <button
                        type="button"
                        className={loadOverrideOn ? 'warn-btn' : ''}
                        onClick={() => sendLoadOverride(name, !loadOverrideOn)}
                      >
                        {loadOverrideOn ? 'Disable Override' : 'Enable Override'}
                      </button>
                    </div>
                    <small className="field-note">When ON, AI/rule policy does not manage this load. Fault protection still applies.</small>
                  </div>
                    <div className="control-block">
                      <label>
                        Fault Injection (A) {liveBadge(ld.inject_current_a, 'A')}
                      </label>
                      <div className="control-inline">
                        <input
                          type="number"
                          min="0"
                          step="0.1"
                          value={numberInputValue(loadDrafts?.[name]?.inject_current_a, ld.inject_current_a ?? 0)}
                          onChange={(e) => setLoadDraft(name, { inject_current_a: Number(e.target.value) })}
                        />
                        <button onClick={() => sendLoadInject(name)}>Apply</button>
                        <button className="ghost-btn" onClick={() => sendLoadInject(name, true)}>Clear</button>
                      </div>
                    </div>
                    <div className="control-block">
                      <label>
                        Fault Thresholds {liveBadge(ld.fault_limit_a, 'A')}
                      </label>
                      <div className="control-inline">
                        <label className="inline-field">
                          <span>Limit (A)</span>
                          <input
                            type="number"
                            min="0"
                            step="0.1"
                            value={numberInputValue(loadDrafts?.[name]?.fault_limit_a, ld.fault_limit_a ?? 0)}
                            onChange={(e) => setLoadDraft(name, { fault_limit_a: Number(e.target.value) })}
                          />
                        </label>
                        <label className="inline-field">
                          <span>Trip (ms)</span>
                          <input
                            type="number"
                            min="0"
                            step="10"
                            value={numberInputValue(loadDrafts?.[name]?.fault_trip_ms, ld.fault_trip_ms ?? 0)}
                            onChange={(e) => setLoadDraft(name, { fault_trip_ms: Number(e.target.value) })}
                          />
                        </label>
                        <label className="inline-field">
                          <span>Clear (ms)</span>
                          <input
                            type="number"
                            min="0"
                            step="10"
                            value={numberInputValue(loadDrafts?.[name]?.fault_clear_ms, ld.fault_clear_ms ?? 0)}
                            onChange={(e) => setLoadDraft(name, { fault_clear_ms: Number(e.target.value) })}
                          />
                        </label>
                        <label className="inline-field">
                          <span>Restore (ms)</span>
                          <input
                            type="number"
                            min="0"
                            step="100"
                            value={numberInputValue(loadDrafts?.[name]?.fault_restore_ms, ld.fault_restore_ms ?? 0)}
                            onChange={(e) => setLoadDraft(name, { fault_restore_ms: Number(e.target.value) })}
                          />
                        </label>
                        <button onClick={() => sendLoadFaultConfig(name)}>Apply</button>
                      </div>
                    </div>
                    <div className="control-block">
                      <label>
                        Priority {liveBadge(ld.priority, '', 0)}
                      </label>
                      <div className="control-inline">
                        <input
                          type="number"
                          min="1"
                          max="99"
                          step="1"
                          value={Number(loadDrafts?.[name]?.priority ?? ld.priority ?? 99)}
                          onChange={(e) => setLoadDraft(name, { priority: Number(e.target.value) })}
                        />
                        <button onClick={() => sendLoadPriority(name)}>Apply</button>
                      </div>
                    </div>
                    <div className="control-block">
                      <label>
                        Load Class {liveBadge(ld.class)}
                      </label>
                      <div className="control-inline">
                        <select
                          value={String(loadDrafts?.[name]?.load_class ?? ld.class ?? 'IMPORTANT')}
                          onChange={(e) => setLoadDraft(name, { load_class: String(e.target.value) })}
                        >
                          {LOAD_CLASS_OPTIONS.map((cls) => (
                            <option key={cls} value={cls}>{cls}</option>
                          ))}
                        </select>
                        <button onClick={() => sendLoadClass(name)}>Apply</button>
                      </div>
                    </div>
                </>
              </article>
              );
            })}
          </div>
        </Panel>

        <Panel title="2) Analytics" subtitle="Energy, system and stability performance">
          <div className="metrics-grid metrics-analytics">
            <Metric label="Policy" value={system.control_policy || '-'} />
            <Metric label="Active AI Sources" value={activeAiSourcesValue} />
            <Metric label="AI Process Source" value={aiDynamicProcessSource} />
            <Metric label="Process State" value={processState} />
            <Metric label="Process Elapsed" value={processEnabled ? formatHms(processElapsedSec) : '-'} />
            <Metric label="Cycles" value={`${processCycleCount}/${processGoalCycles || '-'}`} />
            <Metric label="Goal Reached" value={processGoalReached ? 'Yes' : 'No'} />
            <Metric
              label="Process Energy"
              value={processEnabled ? processEnergyWh : (processLastEnergyWh > 0 ? processLastEnergyWh : NaN)}
              scaleKind="energy"
              scaleSpec={liveEnergyScale}
              fallback="-"
            />
            <Metric label="Tank1 Level" value={tank1LevelValid === false ? '-' : f2(tank1LevelLive)} unit={tank1LevelValid === false ? '' : '%'} />
            <Metric label="Tank1 Temp" value={f2(tank1TempLive)} unit="C" />
            <Metric label="Tank1 Air Gap" value={tank1LevelValid === false ? '-' : f2(tank1DistanceLive)} unit={tank1LevelValid === false ? '' : 'cm'} />
            <Metric label="Tank1 Raw Distance" value={f2(tank1RawDistanceLive)} unit="cm" />
            <Metric label="Tank1 Water Height" value={tank1LevelValid === false ? '-' : f2(tank1WaterHeightLive)} unit={tank1LevelValid === false ? '' : 'cm'} />
            <Metric label="Tank2 Level" value={tank2LevelValid === false ? '-' : f2(tank2LevelLive)} unit={tank2LevelValid === false ? '' : '%'} />
            <Metric label="Tank2 Temp" value={f2(tank2TempLive)} unit="C" />
            <Metric label="Tank2 Air Gap" value={tank2LevelValid === false ? '-' : f2(tank2DistanceLive)} unit={tank2LevelValid === false ? '' : 'cm'} />
            <Metric label="Tank2 Raw Distance" value={f2(tank2RawDistanceLive)} unit="cm" />
            <Metric label="Tank2 Water Height" value={tank2LevelValid === false ? '-' : f2(tank2WaterHeightLive)} unit={tank2LevelValid === false ? '' : 'cm'} />
            <Metric label="Supply Voltage" value={f2(system.supply_v)} unit="V" />
            <Metric label="Supply Voltage Raw" value={f2(system.supply_v_raw ?? system.supply_v)} unit="V" />
            <Metric
              label="Voltage Injection"
              value={system.VOLTAGE_INJECTION_MODE === 'NONE' || !system.VOLTAGE_INJECTION_MODE
                ? 'NONE'
                : `${system.VOLTAGE_INJECTION_MODE} ${f2(system.VOLTAGE_INJECTION_MAGNITUDE_V)}V`}
            />
            <Metric label="Undervoltage Active" value={system.UNDERVOLTAGE_ACTIVE ? 'Yes' : 'No'} />
            <Metric label="Overvoltage Active" value={system.OVERVOLTAGE_ACTIVE ? 'Yes' : 'No'} />
            <Metric label="Current Total Power" value={totalPowerW} scaleKind="power" scaleSpec={livePowerScale} />
            <Metric label="Current Total Amperes" value={system.total_current_a} scaleKind="current" scaleSpec={liveCurrentScale} />
            <Metric label="Energy Flow" value={energyRateWhPerMin} scaleKind="energy" unitSuffix="/min" />
            <Metric label="Tariff State" value={tariffState} />
            <Metric label="Tariff Source" value={tariffStateSource} />
            <Metric label="Tariff Price" value={f2(tariffRatePerKwh)} unit={`${CURRENCY_CODE}/kWh`} />
            <Metric label="Unit Energy Cost" value={tariffRatePerWh} scaleKind="cost" unitSuffix="/Wh" />
            <Metric label="Cost Rate" value={costRatePerMin} scaleKind="cost" unitSuffix="/min" />
            <Metric label="Total Energy" value={energy.total_energy_wh} scaleKind="energy" scaleSpec={liveEnergyScale} />
            <Metric label={`Max Demand (${tariffConfig.demandIntervalMin} min avg)`} value={f2(analyticsMaxDemandKw)} unit="kW" />
            <Metric label="Accum. Energy Cost" value={analyticsAccumulatedEnergyCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Demand Charge" value={analyticsDemandChargeCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Total Cost" value={analyticsAccumulatedCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Energy Cap (Window)" value={energyCapWindowWh} scaleKind="energy" scaleSpec={liveEnergyScale} />
            <Metric label="Goal Window" value={f2(energyGoalWindowMin)} unit="min" />
            <Metric label="Energy Used (Window)" value={energyGoalWindowWh} scaleKind="energy" scaleSpec={liveEnergyScale} />
            <Metric label="Instant Energy Cost Est." value={estimatedTotalCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Instant Window Cost Est." value={estimatedGoalWindowCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Energy Left" value={energyLeftGoalWh == null ? NaN : energyLeftGoalWh} scaleKind="energy" scaleSpec={liveEnergyScale} fallback="-" />
            <Metric label="Energy Used %" value={energyUsedGoalPct == null ? '-' : f2(energyUsedGoalPct)} unit={energyUsedGoalPct == null ? '' : '%'} />
            <Metric label="Ambient Temp" value={f2(env.temperature_c)} unit="C" />
            <Metric label="Ambient Humidity" value={f2(env.humidity_pct)} unit="%" />
            <Metric label="Actions" value={f2(evalControl.total_control_actions)} />
            <Metric label="Shed/Curtail/Restore" value={`${f2(evalControl.shedding_event_count)}/${f2(evalControl.curtail_event_count)}/${f2(evalControl.restore_event_count)}`} />
            <Metric label="Max Overshoot" value={evalStability.max_overshoot_w} scaleKind="power" scaleSpec={livePowerScale} />
            <Metric label="Load Toggles" value={f2(evalStability.load_toggle_count)} />
            <Metric label="Prediction Age" value={formatHms((Number.isFinite(predictionAgeMsLive) ? predictionAgeMsLive : 0) / 1000)} />
            <Metric label="Prediction Delay" value={f2(evalPrediction.apply_delay_sec)} unit="s" />
            <Metric label="MAE / RMSE" value={`${f2(evalForecast.mae_w)} / ${f2(evalForecast.rmse_w)}`} />
            <Metric label="Peak Settling" value={evalSettlingSec == null ? '-' : f2(evalSettlingSec)} unit={evalSettlingSec == null ? '' : 's'} />
            <Metric label="Last Telemetry" value={state.telemetryAt ? new Date(state.telemetryAt).toLocaleTimeString() : '-'} />
          </div>

          <div className="chart-grid">
            <div className="chart-wrap">
              <div className="chart-static-legend">
                <span className="legend-chip"><span className="legend-dot legend-actual" />Power ({livePowerScale.unit})</span>
                <span className="legend-chip"><span className="legend-dot legend-current" />Current ({liveCurrentScale.unit})</span>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={state.powerHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis yAxisId="power" tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(livePowerScale)} />
                  <YAxis yAxisId="current" orientation="right" tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(liveCurrentScale)} />
                  <Tooltip formatter={makeTooltipFormatter({ power_w: livePowerScale, current_a: liveCurrentScale })} />
                  <Line yAxisId="power" type="monotone" dataKey="power_w" name={`Power (${livePowerScale.unit})`} stroke="#56e39f" dot={false} strokeWidth={2} />
                  <Line yAxisId="current" type="monotone" dataKey="current_a" name={`Current (${liveCurrentScale.unit})`} stroke="#5ab3ff" dot={false} strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="chart-wrap">
              <div className="chart-static-legend">
                <span className="legend-chip"><span className="legend-dot legend-energy" />Total Energy ({liveEnergyScale.unit})</span>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={state.powerHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(liveEnergyScale)} />
                  <Tooltip formatter={makeTooltipFormatter({ total_energy_wh: liveEnergyScale })} />
                  <Area type="monotone" dataKey="total_energy_wh" name={`Total Energy (${liveEnergyScale.unit})`} stroke="#ffd166" fill="rgba(255,209,102,0.22)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
            <div className="chart-wrap">
              <div className="chart-static-legend">
                <span className="legend-chip"><span className="legend-dot legend-rf" />Total Cost ({liveCostScale.unit})</span>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={analyticsCostRows}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} domain={analyticsCostDomain} tickFormatter={makeAxisTickFormatter(liveCostScale)} />
                  <Tooltip formatter={makeTooltipFormatter({ total_cost: liveCostScale })} />
                  <Area type="monotone" dataKey="total_cost" name={`Total Cost (${liveCostScale.unit})`} stroke="#ee6c4d" fill="rgba(238,108,77,0.22)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>
        </Panel>

        <Panel
          title="3) Predictive Analytics"
          subtitle="Short-term and long-term model outputs plus recommended actions"
          right={<span className={riskClass(state.predictions.short?.risk_level)}>{state.predictions.short?.risk_level || 'LOW'} risk</span>}
        >
          <div className="prediction-head">
            <div>
              <div className="pred-label">Short-Term</div>
              <div className="pred-value">
                {formatScaledValueOnly(state.predictions.short?.predicted_power_w, 'power', { scaleSpec: predictionCardPowerScale })} {predictionCardPowerScale.unit}
              </div>
              <div className="pred-meta">
                {formatScaledValueOnly(predictionEnergyDisplay(state.predictions.short), 'energy', { scaleSpec: predictionCardEnergyScale })} {predictionCardEnergyScale.unit}
              </div>
            </div>
            <div>
              <div className="pred-label">Long RF</div>
              <div className="pred-value">
                {formatScaledValueOnly(state.predictions.longRf?.predicted_power_w, 'power', { scaleSpec: predictionCardPowerScale })} {predictionCardPowerScale.unit}
              </div>
              <div className="pred-meta">
                {formatScaledValueOnly(predictionEnergyDisplay(state.predictions.longRf), 'energy', { scaleSpec: predictionCardEnergyScale })} {predictionCardEnergyScale.unit}
              </div>
            </div>
            <div>
              <div className="pred-label">Long LSTM</div>
              <div className="pred-value">
                {formatScaledValueOnly(state.predictions.longLstm?.predicted_power_w, 'power', { scaleSpec: predictionCardPowerScale })} {predictionCardPowerScale.unit}
              </div>
              <div className="pred-meta">
                {formatScaledValueOnly(predictionEnergyDisplay(state.predictions.longLstm), 'energy', { scaleSpec: predictionCardEnergyScale })} {predictionCardEnergyScale.unit}
              </div>
            </div>
          </div>

          <div className="chart-grid">
            <div className="chart-wrap">
              <div className="chart-static-legend">
                {PREDICTION_SERIES.map((series) => (
                  <span key={`power-${series.key}`} className="legend-chip">
                    <span className="legend-dot" style={{ background: series.color }} />{series.name} ({predictionPowerScale.unit})
                  </span>
                ))}
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={state.predictionPowerHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(predictionPowerScale)} />
                  <Tooltip formatter={makeTooltipFormatter(Object.fromEntries(PREDICTION_SERIES.map((series) => [series.key, predictionPowerScale])))} />
                  {PREDICTION_SERIES.map((series) => (
                    <Line
                      key={`power-line-${series.key}`}
                      type="monotone"
                      dataKey={series.key}
                      name={`${series.name} (${predictionPowerScale.unit})`}
                      stroke={series.color}
                      dot={false}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
            <div className="chart-wrap">
              <div className="chart-static-legend">
                {PREDICTION_SERIES.map((series) => (
                  <span key={`energy-${series.key}`} className="legend-chip">
                    <span className="legend-dot" style={{ background: series.color }} />{series.name} ({predictionEnergyScale.unit})
                  </span>
                ))}
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={state.predictionEnergyHistory}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(predictionEnergyScale)} />
                  <Tooltip formatter={makeTooltipFormatter(Object.fromEntries(PREDICTION_SERIES.map((series) => [series.key, predictionEnergyScale])))} />
                  {PREDICTION_SERIES.map((series) => (
                    <Line
                      key={`energy-line-${series.key}`}
                      type="monotone"
                      dataKey={series.key}
                      name={`${series.name} (${predictionEnergyScale.unit})`}
                      stroke={series.color}
                      dot={false}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="prediction-suggestions">
            <h3>Latest Suggestions</h3>
            <ul className="stack-list">
              {state.suggestions.length === 0 ? <li className="empty">No suggestions yet.</li> : null}
              {state.suggestions.map((s, idx) => (
                <li key={`${s.source}-${s.device}-${idx}`}>
                  <span className="mono">[{s.source}]</span> {s.device} {'->'} {JSON.stringify(s.set)} ({s.reason || s.action})
                </li>
              ))}
            </ul>
          </div>
        </Panel>

        <Panel title="4) Historical Data" subtitle="Stored actual vs predicted power/energy with interactive range filtering">
          {!HISTORY_API_URL ? (
            <div className="error-banner">
              Set <span className="mono">VITE_HISTORY_API_URL</span> to enable historical charts.
            </div>
          ) : null}

          <div className="history-controls">
            <div className="control-row history-range-btns">
              <button type="button" onClick={() => setHistoryQuickRangeHours(1)}>Last 1h</button>
              <button type="button" onClick={() => setHistoryQuickRangeHours(6)}>Last 6h</button>
              <button type="button" onClick={() => setHistoryQuickRangeHours(24)}>Last 24h</button>
              <button type="button" onClick={() => setHistoryQuickRangeHours(24 * 3)}>Last 3d</button>
              <button type="button" onClick={() => setHistoryQuickRangeHours(24 * 7)}>Last 7d</button>
              <button type="button" onClick={() => setHistoryQuickRangeHours(24 * 30)}>Last 30d</button>
            </div>

            <div className="row">
              <label>From</label>
              <input
                type="datetime-local"
                value={historyDraft.from}
                onChange={(e) => setHistoryDraft((prev) => ({ ...prev, from: e.target.value }))}
              />
            </div>
            <div className="row">
              <label>To</label>
              <input
                type="datetime-local"
                value={historyDraft.to}
                onChange={(e) => setHistoryDraft((prev) => ({ ...prev, to: e.target.value }))}
              />
            </div>
            <div className="control-row">
              <button type="button" onClick={applyHistoryQuery}>Apply Range</button>
              <button
                type="button"
                className="ghost-btn"
                onClick={() => {
                  const now = Date.now();
                  const fromMs = now - (24 * 60 * 60 * 1000);
                  setHistoryDraft({ from: toDateTimeLocal(fromMs), to: toDateTimeLocal(now) });
                  setHistoryQuery({ fromMs, toMs: now });
                }}
              >
                Reset
              </button>
              <button
                type="button"
                className="ghost-btn"
                onClick={downloadChapter4Export}
                disabled={historyExporting}
              >
                {historyExporting ? 'Preparing CSV Export...' : 'Download CSVs'}
              </button>
            </div>
            <div className="control-row">
              <span className="field-note">Dynamic step: {formatHistoryStepLabel(historyStepSec)} (auto from selected range)</span>
              <span className="field-note">
                {historyExporting
                  ? 'Building CSV export...'
                  : (historyLoading
                    ? 'Loading history...'
                    : (historyError
                      ? `History error: ${historyError}`
                      : (historyChartRows.length
                        ? `${historyChartRows.length} points • total ${f2(historyAccumulatedCost)} ${CURRENCY_CODE} • energy ${f2(historyAccumulatedEnergyCost)} • demand ${f2(historyDemandChargeCost)} • max ${f2(historyMaxDemandKw)} kW (${tariffConfig.demandIntervalMin} min avg)`
                        : '0 points')))}
              </span>
            </div>

            <div className="history-series-grid">
              <label className="toggle-item">
                <input
                  type="checkbox"
                  checked={historySeries.actual}
                  onChange={(e) => setHistorySeries((prev) => ({ ...prev, actual: e.target.checked }))}
                />
                <span>Actual</span>
              </label>
              <label className="toggle-item">
                <input
                  type="checkbox"
                  checked={historySeries.short}
                  onChange={(e) => setHistorySeries((prev) => ({ ...prev, short: e.target.checked }))}
                />
                <span>Short</span>
              </label>
              <label className="toggle-item">
                <input
                  type="checkbox"
                  checked={historySeries.longRf}
                  onChange={(e) => setHistorySeries((prev) => ({ ...prev, longRf: e.target.checked }))}
                />
                <span>Long RF</span>
              </label>
              <label className="toggle-item">
                <input
                  type="checkbox"
                  checked={historySeries.longLstm}
                  onChange={(e) => setHistorySeries((prev) => ({ ...prev, longLstm: e.target.checked }))}
                />
                <span>Long LSTM</span>
              </label>
            </div>
          </div>

          <div className="chart-grid">
            <div className="chart-wrap">
              <div className="chart-static-legend">
                {historySeries.actual ? <span className="legend-chip"><span className="legend-dot legend-actual" />Actual Power ({historyPowerScale.unit})</span> : null}
                {historySeries.short ? <span className="legend-chip"><span className="legend-dot legend-short" />Pred Short Power ({historyPowerScale.unit})</span> : null}
                {historySeries.longRf ? <span className="legend-chip"><span className="legend-dot legend-rf" />Pred Long RF Power ({historyPowerScale.unit})</span> : null}
                {historySeries.longLstm ? <span className="legend-chip"><span className="legend-dot legend-lstm" />Pred Long LSTM Power ({historyPowerScale.unit})</span> : null}
              </div>
              <div className="history-chart-scroll">
                <LineChart width={historyChartWidth} height={260} data={historyChartRows} syncId="history-sync">
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis
                    dataKey="ts"
                    tick={{ fill: '#8ea1cc', fontSize: 12 }}
                    tickFormatter={(value) => formatHistoryTick(value, historySpanMs)}
                    type="number"
                    domain={['dataMin', 'dataMax']}
                    scale="time"
                    tickCount={historySpanMs <= 24 * 60 * 60 * 1000 ? 10 : 8}
                  />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} domain={historyPowerDomain} tickFormatter={makeAxisTickFormatter(historyPowerScale)} />
                  <Tooltip formatter={makeTooltipFormatter({
                    actual_power_w: historyPowerScale,
                    pred_short_power_w: historyPowerScale,
                    pred_long_rf_power_w: historyPowerScale,
                    pred_long_lstm_power_w: historyPowerScale,
                  })} />
                  {historySeries.actual ? <Line type="monotone" dataKey="actual_power_w" name={`Actual Power (${historyPowerScale.unit})`} stroke="#56e39f" dot={false} strokeWidth={2} /> : null}
                  {historySeries.short ? <Line type="monotone" dataKey="pred_short_power_w" name={`Pred Short Power (${historyPowerScale.unit})`} stroke="#ffd166" dot={false} /> : null}
                  {historySeries.longRf ? <Line type="monotone" dataKey="pred_long_rf_power_w" name={`Pred Long RF Power (${historyPowerScale.unit})`} stroke="#ee6c4d" dot={false} /> : null}
                  {historySeries.longLstm ? <Line type="monotone" dataKey="pred_long_lstm_power_w" name={`Pred Long LSTM Power (${historyPowerScale.unit})`} stroke="#72efdd" dot={false} /> : null}
                </LineChart>
              </div>
            </div>

            <div className="chart-wrap">
              <div className="chart-static-legend">
                {historySeries.actual ? <span className="legend-chip"><span className="legend-dot legend-actual" />Actual Total Energy ({historyEnergyScale.unit})</span> : null}
                {historySeries.short ? <span className="legend-chip"><span className="legend-dot legend-short" />Pred Short Energy ({historyEnergyScale.unit})</span> : null}
                {historySeries.longRf ? <span className="legend-chip"><span className="legend-dot legend-rf" />Pred Long RF Energy ({historyEnergyScale.unit})</span> : null}
                {historySeries.longLstm ? <span className="legend-chip"><span className="legend-dot legend-lstm" />Pred Long LSTM Energy ({historyEnergyScale.unit})</span> : null}
              </div>
              <div className="history-chart-scroll">
                <LineChart width={historyChartWidth} height={260} data={historyChartRows} syncId="history-sync">
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis
                    dataKey="ts"
                    tick={{ fill: '#8ea1cc', fontSize: 12 }}
                    tickFormatter={(value) => formatHistoryTick(value, historySpanMs)}
                    type="number"
                    domain={['dataMin', 'dataMax']}
                    scale="time"
                    tickCount={historySpanMs <= 24 * 60 * 60 * 1000 ? 10 : 8}
                  />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} domain={historyEnergyDomain} tickFormatter={makeAxisTickFormatter(historyEnergyScale)} />
                  <Tooltip formatter={makeTooltipFormatter({
                    actual_total_energy_wh: historyEnergyScale,
                    pred_short_energy_wh: historyEnergyScale,
                    pred_long_rf_energy_wh: historyEnergyScale,
                    pred_long_lstm_energy_wh: historyEnergyScale,
                  })} />
                  {historySeries.actual ? <Line type="monotone" dataKey="actual_total_energy_wh" name={`Actual Total Energy (${historyEnergyScale.unit})`} stroke="#56e39f" dot={false} strokeWidth={2} /> : null}
                  {historySeries.short ? <Line type="monotone" dataKey="pred_short_energy_wh" name={`Pred Short Energy (${historyEnergyScale.unit})`} stroke="#ffd166" dot={false} /> : null}
                  {historySeries.longRf ? <Line type="monotone" dataKey="pred_long_rf_energy_wh" name={`Pred Long RF Energy (${historyEnergyScale.unit})`} stroke="#ee6c4d" dot={false} /> : null}
                  {historySeries.longLstm ? <Line type="monotone" dataKey="pred_long_lstm_energy_wh" name={`Pred Long LSTM Energy (${historyEnergyScale.unit})`} stroke="#72efdd" dot={false} /> : null}
                </LineChart>
              </div>
            </div>
            <div className="chart-wrap">
              <div className="chart-static-legend">
                <span className="legend-chip"><span className="legend-dot legend-rf" />Total Cost ({historyCostScale.unit})</span>
              </div>
              <div className="history-chart-scroll">
                <LineChart width={historyChartWidth} height={260} data={historyCostRows} syncId="history-sync">
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis
                    dataKey="ts"
                    tick={{ fill: '#8ea1cc', fontSize: 12 }}
                    tickFormatter={(value) => formatHistoryTick(value, historySpanMs)}
                    type="number"
                    domain={['dataMin', 'dataMax']}
                    scale="time"
                    tickCount={historySpanMs <= 24 * 60 * 60 * 1000 ? 10 : 8}
                  />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} domain={historyCostDomain} tickFormatter={makeAxisTickFormatter(historyCostScale)} />
                  <Tooltip formatter={makeTooltipFormatter({ total_cost: historyCostScale })} />
                  <Line type="monotone" dataKey="total_cost" name={`Total Cost (${historyCostScale.unit})`} stroke="#ee6c4d" dot={false} strokeWidth={2} />
                </LineChart>
              </div>
            </div>
          </div>
        </Panel>

        <Panel title="5) Controls" subtitle="System control, production-goal process, and command acknowledgements">
          <div className="cmd-grid">
            <div className="cmd-card">
              <h3>System Controls</h3>
              <div className="row">
                <label>Emergency Stop {liveBadge(system.emergency_stop)}</label>
                <button
                  type="button"
                  className="danger-btn"
                  onClick={() => publish({ control: { EMERGENCY_STOP: !emergencyStopActive } })}
                >
                  {emergencyStopActive ? 'Clear Emergency' : 'EMERGENCY STOP'}
                </button>
              </div>
              <div className="row">
                <label>Policy {liveBadge(system.control_policy)}</label>
                <select
                  value={String(systemDraft.controlPolicy ?? system.control_policy ?? '').toUpperCase()}
                  onChange={(e) => setSystemDraftField('controlPolicy', e.target.value)}
                >
                  <option value="" disabled>Select</option>
                  {POLICY_OPTIONS.map((o) => <option key={o} value={o}>{policyLabel(o)}</option>)}
                </select>
              </div>
              <div className="row">
                <small className="field-note">
                  {selectedPolicyNoEnergy
                    ? 'NO_ENERGY_MANAGEMENT: policy automation is disabled; direct operator control stays available.'
                    : 'Selected policy applies AI/rule energy management to loads without per-load override.'}
                </small>
              </div>
              <div className="row">
                <label>Active AI Sources {liveBadge(activeAiSourcesValue)}</label>
                <input value={activeAiSourcesValue} readOnly />
              </div>
              <div className="row">
                <small className="field-note">
                  AI Dynamic Process is policy-driven (Live: {aiDynamicPolicyLive ? 'on' : 'off'}).{' '}
                  {!selectedPolicyAiDriven
                    ? 'Set policy to HYBRID or AI_PREFERRED.'
                    : `${aiDynamicReasonText(aiDynamicProcessReason) || 'Ready.'} Auto source: ${aiDynamicProcessSource}.`}
                </small>
              </div>
              <div className="row">
                <label>Maximum Power Threshold (W) {liveBadge(system.MAX_TOTAL_POWER_W, 'W')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.maxPower, system.MAX_TOTAL_POWER_W, 2)}
                  onChange={(e) => setSystemDraftField('maxPower', e.target.value)}
                  placeholder="50"
                />
              </div>
              <div className="row">
                <label>Undervoltage Threshold (V) {liveBadge(system.UNDERVOLTAGE_THRESHOLD_V, 'V')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.undervoltageThresholdV, system.UNDERVOLTAGE_THRESHOLD_V, 2)}
                  onChange={(e) => setSystemDraftField('undervoltageThresholdV', e.target.value)}
                  placeholder="0 (disabled)"
                />
              </div>
              <div className="row">
                <label>Undervoltage Trip Delay (ms) {liveBadge(system.UNDERVOLTAGE_TRIP_DELAY_MS, 'ms')}</label>
                <input
                  type="number"
                  step="1"
                  value={numberInputValue(systemDraft.undervoltageTripDelayMs, system.UNDERVOLTAGE_TRIP_DELAY_MS, 0)}
                  onChange={(e) => setSystemDraftField('undervoltageTripDelayMs', e.target.value)}
                  placeholder="500"
                />
              </div>
              <div className="row">
                <label>Undervoltage Clear Delay (ms) {liveBadge(system.UNDERVOLTAGE_CLEAR_DELAY_MS, 'ms')}</label>
                <input
                  type="number"
                  step="1"
                  value={numberInputValue(systemDraft.undervoltageClearDelayMs, system.UNDERVOLTAGE_CLEAR_DELAY_MS, 0)}
                  onChange={(e) => setSystemDraftField('undervoltageClearDelayMs', e.target.value)}
                  placeholder="500"
                />
              </div>
              <div className="row">
                <label>Undervoltage Restore Margin (V) {liveBadge(system.UNDERVOLTAGE_RESTORE_MARGIN_V, 'V')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.undervoltageRestoreMarginV, system.UNDERVOLTAGE_RESTORE_MARGIN_V, 2)}
                  onChange={(e) => setSystemDraftField('undervoltageRestoreMarginV', e.target.value)}
                  placeholder="0.30"
                />
              </div>
              <div className="row">
                <label>Overvoltage Threshold (V) {liveBadge(system.OVERVOLTAGE_THRESHOLD_V, 'V')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.overvoltageThresholdV, system.OVERVOLTAGE_THRESHOLD_V, 2)}
                  onChange={(e) => setSystemDraftField('overvoltageThresholdV', e.target.value)}
                  placeholder="0 (disabled)"
                />
              </div>
              <div className="row">
                <label>Overvoltage Trip Delay (ms) {liveBadge(system.OVERVOLTAGE_TRIP_DELAY_MS, 'ms')}</label>
                <input
                  type="number"
                  step="1"
                  value={numberInputValue(systemDraft.overvoltageTripDelayMs, system.OVERVOLTAGE_TRIP_DELAY_MS, 0)}
                  onChange={(e) => setSystemDraftField('overvoltageTripDelayMs', e.target.value)}
                  placeholder="500"
                />
              </div>
              <div className="row">
                <label>Overvoltage Clear Delay (ms) {liveBadge(system.OVERVOLTAGE_CLEAR_DELAY_MS, 'ms')}</label>
                <input
                  type="number"
                  step="1"
                  value={numberInputValue(systemDraft.overvoltageClearDelayMs, system.OVERVOLTAGE_CLEAR_DELAY_MS, 0)}
                  onChange={(e) => setSystemDraftField('overvoltageClearDelayMs', e.target.value)}
                  placeholder="500"
                />
              </div>
              <div className="row">
                <label>Overvoltage Restore Margin (V) {liveBadge(system.OVERVOLTAGE_RESTORE_MARGIN_V, 'V')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.overvoltageRestoreMarginV, system.OVERVOLTAGE_RESTORE_MARGIN_V, 2)}
                  onChange={(e) => setSystemDraftField('overvoltageRestoreMarginV', e.target.value)}
                  placeholder="0.30"
                />
              </div>
              <div className="row">
                <label>Voltage Injection Mode {liveBadge(system.VOLTAGE_INJECTION_MODE || 'NONE')}</label>
                <input value={String(system.VOLTAGE_INJECTION_MODE || 'NONE')} readOnly />
              </div>
              <div className="row">
                <label>Voltage Injection Magnitude (V) {liveBadge(system.VOLTAGE_INJECTION_MAGNITUDE_V, 'V')}</label>
                <input
                  type="number"
                  min="0"
                  step="0.01"
                  value={numberInputValue(systemDraft.voltageInjectionMagnitudeV, system.VOLTAGE_INJECTION_MAGNITUDE_V, 2)}
                  onChange={(e) => setSystemDraftField('voltageInjectionMagnitudeV', e.target.value)}
                  placeholder="1.00"
                />
              </div>
              <div className="control-row">
                <button type="button" className="warn-btn" onClick={() => sendVoltageInjection('UNDERVOLTAGE')}>
                  Inject UV
                </button>
                <button type="button" className="warn-btn" onClick={() => sendVoltageInjection('OVERVOLTAGE')}>
                  Inject OV
                </button>
                <button type="button" className="ghost-btn" onClick={() => sendVoltageInjection('NONE')}>
                  Clear Voltage Injection
                </button>
              </div>
              <div className="row">
                <small className="field-note">
                  Live bus voltage: raw {f2(system.supply_v_raw ?? system.supply_v)} V, effective {f2(system.supply_v)} V.
                  Injection affects protection testing without changing your threshold settings.
                </small>
              </div>
              <div className="row">
                <label>Cooldown (s) {liveBadge(system.CONTROL_COOLDOWN_SEC, 's')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.cooldownSec, system.CONTROL_COOLDOWN_SEC, 2)}
                  onChange={(e) => setSystemDraftField('cooldownSec', e.target.value)}
                  placeholder="5"
                />
              </div>
              <div className="row">
                <label>Sample Interval (s) {liveBadge(system.SAMPLE_INTERVAL_SEC, 's')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.sampleIntervalSec, system.SAMPLE_INTERVAL_SEC, 2)}
                  onChange={(e) => setSystemDraftField('sampleIntervalSec', e.target.value)}
                  placeholder="1"
                />
              </div>
              <div className="row">
                <label>Energy Cap per Window (Wh) {liveBadge(energyCapWindowWh, 'Wh')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.energyCapWindowWh, energyCapWindowWh, 2)}
                  onChange={(e) => setSystemDraftField('energyCapWindowWh', e.target.value)}
                  placeholder="100"
                />
              </div>
              <div className="row">
                <label>Energy Goal Window (min) {liveBadge(energyGoalWindowMin, 'min')}</label>
                <input
                  type="number"
                  step="0.01"
                  value={numberInputValue(systemDraft.energyGoalWindowMin, energyGoalWindowMin, 2)}
                  onChange={(e) => setSystemDraftField('energyGoalWindowMin', e.target.value)}
                  placeholder="1440"
                />
              </div>
              <div className="row">
                <label>Offpeak Window Start</label>
                <input
                  type="time"
                  value={formatMinuteOfDay(tariffConfig.offpeakStartMin)}
                  onChange={(e) => setTariffTimeField('offpeakStartMin', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Offpeak Window End</label>
                <input
                  type="time"
                  value={formatMinuteOfDay(tariffConfig.offpeakEndMin)}
                  onChange={(e) => setTariffTimeField('offpeakEndMin', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Peak Window Start</label>
                <input
                  type="time"
                  value={formatMinuteOfDay(tariffConfig.peakStartMin)}
                  onChange={(e) => setTariffTimeField('peakStartMin', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Peak Window End</label>
                <input
                  type="time"
                  value={formatMinuteOfDay(tariffConfig.peakEndMin)}
                  onChange={(e) => setTariffTimeField('peakEndMin', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Offpeak Rate ({CURRENCY_CODE}/kWh)</label>
                <input
                  type="number"
                  min="0"
                  step="0.001"
                  value={numberInputValue(undefined, tariffConfig.offpeakRatePerKwh, 3)}
                  onChange={(e) => setTariffRateField('offpeakRatePerKwh', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Midpeak Rate ({CURRENCY_CODE}/kWh)</label>
                <input
                  type="number"
                  min="0"
                  step="0.001"
                  value={numberInputValue(undefined, tariffConfig.midpeakRatePerKwh, 3)}
                  onChange={(e) => setTariffRateField('midpeakRatePerKwh', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Peak Rate ({CURRENCY_CODE}/kWh)</label>
                <input
                  type="number"
                  min="0"
                  step="0.001"
                  value={numberInputValue(undefined, tariffConfig.peakRatePerKwh, 3)}
                  onChange={(e) => setTariffRateField('peakRatePerKwh', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Demand Charge ({CURRENCY_CODE}/kW)</label>
                <input
                  type="number"
                  min="0"
                  step="0.001"
                  value={numberInputValue(undefined, tariffConfig.demandChargePerKw, 3)}
                  onChange={(e) => setTariffRateField('demandChargePerKw', e.target.value)}
                />
              </div>
              <div className="row">
                <label>Demand Interval (min)</label>
                <input
                  type="number"
                  min="1"
                  step="1"
                  value={numberInputValue(undefined, tariffConfig.demandIntervalMin, 0)}
                  onChange={(e) => setTariffIntervalField('demandIntervalMin', e.target.value)}
                />
              </div>
              <div className="control-row">
                <button type="button" onClick={saveTariffConfig} disabled={tariffConfigSaving}>
                  {tariffConfigSaving ? 'Saving Tariff...' : 'Save Tariff Settings'}
                </button>
                <button type="button" className="ghost-btn" onClick={resetTariffConfig}>
                  Reset Tariff Defaults
                </button>
              </div>
              <div className="row">
                <small className="field-note">
                  Dashboard ToU cost schedule ({UK_TIMEZONE}): OFFPEAK {formatMinuteWindow(tariffConfig.offpeakStartMin, tariffConfig.offpeakEndMin)} + weekends/bank holidays,
                  PEAK {formatMinuteWindow(tariffConfig.peakStartMin, tariffConfig.peakEndMin)} weekdays, MIDPEAK otherwise.
                  Rates ({CURRENCY_CODE}/kWh): OFFPEAK {fx(tariffConfig.offpeakRatePerKwh, 3)}, MIDPEAK {fx(tariffConfig.midpeakRatePerKwh, 3)}, PEAK {fx(tariffConfig.peakRatePerKwh, 3)}.
                  Demand charge: {fx(tariffConfig.demandChargePerKw, 3)} {CURRENCY_CODE}/kW on the maximum {tariffConfig.demandIntervalMin}-minute average demand in the visible window.
                  These values drive live and historical cost calculations. Source: {tariffConfigMeta.source === 'backend' ? 'backend' : tariffConfigMeta.source === 'draft' ? 'unsaved draft' : 'defaults'}
                  {tariffConfigMeta.effectiveFrom ? `, effective from ${new Date(tariffConfigMeta.effectiveFrom).toLocaleString()}` : ''}.
                </small>
              </div>
              <div className="row">
                <label>Long Predictors Mode (RF + LSTM) {liveBadge(system.predictor_mode)}</label>
                <select
                  value={String(systemDraft.predictorMode ?? predictorModeValue ?? '').toUpperCase()}
                  onChange={(e) => setSystemDraftField('predictorMode', e.target.value)}
                >
                  <option value="" disabled>Select</option>
                  {PREDICTOR_MODE_OPTIONS.map((o) => <option key={o}>{o}</option>)}
                </select>
              </div>
              <div className="control-row">
                <button type="button" onClick={applySystemControls}>Apply System Settings</button>
                <button type="button" className="ghost-btn" onClick={triggerCurrentRecalibration}>
                  Calibrate Current Sensors
                </button>
              </div>
            </div>

            <div className="cmd-card">
                <h3>Production Process Controls</h3>
                <div className="control-row">
                  <button onClick={handleStartProcess}>Start Process</button>
                  <button className="warn-btn" onClick={() => publish(stopProcessPayload())}>Stop Process</button>
                  <button type="button" onClick={applyProcessControls}>Apply Process Settings</button>
                </div>
                <div className="row">
                  <label>Goal Cycles {liveBadge(process.goal_cycles, '', 0)}</label>
                  <input
                    type="number"
                    step="1"
                    value={numberInputValue(processDraft.goalCycles, process.goal_cycles, 0)}
                    onChange={(e) => setProcessDraftField('goalCycles', e.target.value)}
                    placeholder="4"
                  />
                </div>
                <div className="row">
                  <label>Tank1 Target Level (%) {liveBadge(tank1.target_level_pct, '%')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank1LevelTarget, tank1.target_level_pct, 2)}
                    onChange={(e) => setProcessDraftField('tank1LevelTarget', e.target.value)}
                    placeholder="70"
                    min={Number.isFinite(tank1LowBound) ? tank1LowBound : undefined}
                    max={Number.isFinite(tank1HighBound) ? tank1HighBound : undefined}
                  />
                </div>
                <div className="row">
                  <label>Tank2 Target Level (%) {liveBadge(tank2.target_level_pct, '%')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank2LevelTarget, tank2.target_level_pct, 2)}
                    onChange={(e) => setProcessDraftField('tank2LevelTarget', e.target.value)}
                    placeholder="70"
                    min={Number.isFinite(tank2LowBound) ? tank2LowBound : undefined}
                    max={Number.isFinite(tank2HighBound) ? tank2HighBound : undefined}
                  />
                </div>
                <div className="row">
                  <label>Tank1 Target Temp (C) {liveBadge(tank1.target_temp_c, 'C')}</label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    max="95"
                    value={numberInputValue(processDraft.tank1TempTarget, tank1.target_temp_c, 2)}
                    onChange={(e) => setProcessDraftField('tank1TempTarget', e.target.value)}
                    placeholder="30"
                  />
                </div>
                <div className="row">
                  <label>Tank2 Target Temp (C) {liveBadge(tank2.target_temp_c, 'C')}</label>
                  <input
                    type="number"
                    step="0.01"
                    min="0"
                    max="95"
                    value={numberInputValue(processDraft.tank2TempTarget, tank2.target_temp_c, 2)}
                    onChange={(e) => setProcessDraftField('tank2TempTarget', e.target.value)}
                    placeholder="30"
                  />
                </div>
                <div className="row">
                  <label>Tank1 Height (cm) {liveBadge(tank1.height_cm ?? process.tank1_height_cm, 'cm')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank1HeightCm, tank1.height_cm ?? process.tank1_height_cm, 2)}
                    onChange={(e) => setProcessDraftField('tank1HeightCm', e.target.value)}
                    placeholder="40"
                  />
                </div>
                <div className="row">
                  <label>Tank2 Height (cm) {liveBadge(tank2.height_cm ?? process.tank2_height_cm, 'cm')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank2HeightCm, tank2.height_cm ?? process.tank2_height_cm, 2)}
                    onChange={(e) => setProcessDraftField('tank2HeightCm', e.target.value)}
                    placeholder="40"
                  />
                </div>
                <div className="row">
                  <label>Tank1 Low Level (%) {liveBadge(tank1.low_level_pct ?? process.tank1_low_level_pct, '%')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank1LowLevelPct, tank1.low_level_pct ?? process.tank1_low_level_pct, 2)}
                    onChange={(e) => {
                      const raw = e.target.value;
                      if (raw === '') {
                        setProcessDraftField('tank1LowLevelPct', raw);
                        return;
                      }
                      const n = Number(raw);
                      setProcessDraftField('tank1LowLevelPct', Number.isFinite(n) ? n.toFixed(2) : raw);
                    }}
                    placeholder="15"
                    min="0"
                  />
                </div>
                <div className="row">
                  <label>Tank2 Low Level (%) {liveBadge(tank2.low_level_pct ?? process.tank2_low_level_pct, '%')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank2LowLevelPct, tank2.low_level_pct ?? process.tank2_low_level_pct, 2)}
                    onChange={(e) => {
                      const raw = e.target.value;
                      if (raw === '') {
                        setProcessDraftField('tank2LowLevelPct', raw);
                        return;
                      }
                      const n = Number(raw);
                      setProcessDraftField('tank2LowLevelPct', Number.isFinite(n) ? n.toFixed(2) : raw);
                    }}
                    placeholder="15"
                    min="0"
                  />
                </div>
                <div className="row">
                  <label>Tank1 High Level (%) {liveBadge(tank1.high_level_pct ?? process.tank1_high_level_pct, '%')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank1HighLevelPct, tank1.high_level_pct ?? process.tank1_high_level_pct, 2)}
                    onChange={(e) => setProcessDraftField('tank1HighLevelPct', e.target.value)}
                    placeholder="90"
                    max={Number.isFinite(tank1MaxHighPct) ? tank1MaxHighPct : undefined}
                  />
                </div>
                <div className="row">
                  <label>Tank2 High Level (%) {liveBadge(tank2.high_level_pct ?? process.tank2_high_level_pct, '%')}</label>
                  <input
                    type="number"
                    step="0.01"
                    value={numberInputValue(processDraft.tank2HighLevelPct, tank2.high_level_pct ?? process.tank2_high_level_pct, 2)}
                    onChange={(e) => setProcessDraftField('tank2HighLevelPct', e.target.value)}
                    placeholder="90"
                    max={Number.isFinite(tank2MaxHighPct) ? tank2MaxHighPct : undefined}
                  />
                </div>
                <div className="scene-status-grid">
                  <Metric label="Process" value={processState} />
                  <Metric label="Enabled" value={processEnabled ? 'Yes' : 'No'} />
                  <Metric label="Lock Active" value={processLockActive ? 'Yes' : 'No'} />
                  <Metric label="Goal Reached" value={processGoalReached ? 'Yes' : 'No'} />
                  <Metric label="Cycles" value={`${processCycleCount}/${processGoalCycles || '-'}`} />
                </div>
              </div>

            <div className="cmd-card">
              <h3>Command ACK Feed</h3>
              <ul className="stack-list">
                {state.ackFeed.length === 0 ? <li className="empty">No ack messages yet.</li> : null}
                {[...state.ackFeed].reverse().map((a, idx) => (
                  <li key={`${a.label}-${idx}`}>
                    <span className="mono">{a.label}</span> {JSON.stringify(a.payload)}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </Panel>
      </main>

      {state.lastError ? <div className="error-banner">{state.lastError}</div> : null}
    </div>
  );
}
