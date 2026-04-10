import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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
const ADMIN_TRAINING_PREDICTOR_MODE_OPTIONS = ['TRAIN_ONLY', 'ONLINE'];
const PREDICTION_SERIES = [
  { key: 'short', name: 'Short EMA', color: '#ffd166' },
  { key: 'long_rf', name: 'Long RF', color: '#ee6c4d' },
  { key: 'long_lstm', name: 'Long LSTM', color: '#72efdd' },
];
const ENERGY_WINDOW_OPTIONS = [
  { value: 1, label: '1 min' },
  { value: 5, label: '5 min' },
  { value: 15, label: '15 min' },
  { value: 30, label: '30 min' },
  { value: 60, label: '1 h' },
  { value: 180, label: '3 h' },
  { value: 360, label: '6 h' },
  { value: 1440, label: '1 d' },
  { value: 10080, label: '1 w' },
  { value: 43200, label: '30 d' },
];
const PREDICTOR_WINDOW_OPTIONS = [
  { value: 1, label: '1 min' },
  { value: 2, label: '2 min' },
  ...ENERGY_WINDOW_OPTIONS.filter((option) => ![1].includes(Number(option.value))),
];
const DEFAULT_ENERGY_WINDOW_MIN = 30;
const DEFAULT_PREDICTOR_HORIZON_MIN = {
  short: 2,
  long_rf: 30,
  long_lstm: 30,
};
const DEFAULT_PREDICTOR_WINDOW_MIN = {
  short: 2,
  long_rf: 30,
  long_lstm: 30,
};
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
const formatScaledInline = (value, kind, options = {}) => {
  const formatted = formatScaledMetric(value, kind, options);
  return formatted.unit ? `${formatted.value} ${formatted.unit}` : formatted.value;
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
const buildRollingEnergySeries = (rows, seriesMap, windowMinutes, {
  tsKey = 'ts',
  labelKeys = ['label', 'label_utc'],
} = {}) => {
  const windowMs = Math.max(1, Number(windowMinutes) || 60) * 60000;
  const trackers = Object.entries(seriesMap || {}).map(([outputKey, sourceKey]) => ({
    outputKey,
    sourceKey,
    samples: [],
  }));
  if (!trackers.length) return rows || [];

  return (rows || []).map((row) => {
    const ts = Number(row?.[tsKey]);
    const nextRow = { ...row };
    for (const tracker of trackers) {
      const value = Number(row?.[tracker.sourceKey]);
      if (Number.isFinite(ts) && Number.isFinite(value)) {
        tracker.samples.push({ ts, value });
        while (tracker.samples.length >= 2 && tracker.samples[1].ts <= (ts - windowMs)) {
          tracker.samples.shift();
        }
        const windowStartMs = ts - windowMs;
        let baseline = tracker.samples[0]?.value;
        if (tracker.samples.length >= 2 && tracker.samples[0].ts < windowStartMs && tracker.samples[1].ts > windowStartMs) {
          const a = tracker.samples[0];
          const b = tracker.samples[1];
          const ratio = (windowStartMs - a.ts) / Math.max(1, (b.ts - a.ts));
          baseline = a.value + ((b.value - a.value) * ratio);
        }
        nextRow[tracker.outputKey] = Math.max(0, value - (Number.isFinite(baseline) ? baseline : value));
      } else {
        nextRow[tracker.outputKey] = null;
      }
    }
    if (!Object.prototype.hasOwnProperty.call(nextRow, 'window_minutes')) nextRow.window_minutes = Number(windowMinutes);
    for (const key of labelKeys) {
      if (nextRow[key] !== undefined) break;
    }
    return nextRow;
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
const windowLabelForMinutes = (minutes, options = ENERGY_WINDOW_OPTIONS) => (
  options.find((item) => Number(item.value) === Number(minutes))?.label || `${Number(minutes)} min`
);
const horizonMinutesForPrediction = (pred, fallbackMinutes) => {
  const horizonSec = Number(pred?.horizon_sec);
  if (Number.isFinite(horizonSec) && horizonSec > 0) return Math.max(1, Math.ceil(horizonSec / 60));
  return Math.max(1, Number(fallbackMinutes) || 1);
};
const clampPredictorWindowMin = (minutes, minimumMinutes) => Math.max(
  Math.max(1, Number(minimumMinutes) || 1),
  Math.max(1, Number(minutes) || 1),
);
const predictorWindowOptionsForMin = (minimumMinutes) => {
  const minimum = Math.max(1, Math.ceil(Number(minimumMinutes) || 1));
  const options = PREDICTOR_WINDOW_OPTIONS.filter((option) => Number(option.value) >= minimum);
  if (!options.some((option) => Number(option.value) === minimum)) {
    options.unshift({ value: minimum, label: windowLabelForMinutes(minimum, PREDICTOR_WINDOW_OPTIONS) });
  }
  return options.sort((a, b) => Number(a.value) - Number(b.value));
};
const predictionEnergyForWindow = (pred, windowMin) => {
  const minutes = Math.max(1, Number(windowMin) || 1);
  const predictedPowerW = Number(pred?.predicted_power_w);
  if (Number.isFinite(predictedPowerW)) return Math.max(0, predictedPowerW) * (minutes / 60);
  const incrementalEnergyWh = Number(pred?.predicted_incremental_energy_wh ?? pred?.predicted_energy_wh);
  const horizonSec = Number(pred?.horizon_sec);
  if (Number.isFinite(incrementalEnergyWh) && Number.isFinite(horizonSec) && horizonSec > 0) {
    return Math.max(0, incrementalEnergyWh) * ((minutes * 60) / horizonSec);
  }
  return NaN;
};
const buildPredictionSeriesRows = (rows, windowMin) => {
  const minutes = Math.max(1, Number(windowMin) || 1);
  return (rows || [])
    .filter((row) => Number.isFinite(Number(row?.ts)) && Number.isFinite(Number(row?.power_w)))
    .map((row) => ({
      ts: Number(row.ts),
      value: Math.max(0, Number(row.power_w)) * (minutes / 60),
    }));
};
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
const experimentStatusLabel = (status) => {
  const key = String(status || '').toUpperCase();
  if (!key) return '-';
  return key.replace(/_/g, ' ');
};
const experimentEventTime = (value) => {
  const ts = Date.parse(String(value || ''));
  if (!Number.isFinite(ts)) return '-';
  return new Date(ts).toLocaleString();
};
const predictorPhaseLabel = (value) => {
  const key = String(value || '').trim().toUpperCase();
  if (!key) return '-';
  return key.replace(/_/g, ' ');
};
const predictorStatusTime = (value) => {
  const ts = Date.parse(String(value || ''));
  if (!Number.isFinite(ts)) return '-';
  return new Date(ts).toLocaleString();
};
const predictorProgressPct = (status) => {
  if (!status) return 0;
  const total = Number(status.bootstrap_total_records || 0);
  const processed = Number(status.bootstrap_processed_records || 0);
  const ratio = Number(status.bootstrap_progress_ratio);
  if (Number.isFinite(ratio) && ratio > 0) return Math.max(0, Math.min(100, ratio * 100));
  if (total > 0) return Math.max(0, Math.min(100, (processed / total) * 100));
  if (status.bootstrap_active) return 5;
  if (String(status.phase || '').toLowerCase() === 'training') return 90;
  if (status.model_ready) return 100;
  const minTrain = Number(status.min_train_sample_count || 0);
  const trainSamples = Number(status.train_samples || 0);
  if (minTrain > 0 && trainSamples > 0) return Math.max(0, Math.min(99, (trainSamples / minTrain) * 100));
  return 0;
};
const syntheticJobPhaseLabel = (value) => {
  const key = String(value || '').trim().toUpperCase();
  if (!key) return '-';
  return key.replace(/_/g, ' ');
};
const syntheticJobProgressPct = (job) => {
  if (!job) return 0;
  const ratio = Number(job.progress_ratio);
  if (Number.isFinite(ratio) && ratio > 0) return Math.max(0, Math.min(100, ratio * 100));
  const total = Number(job.estimated_records || 0);
  const done = Number(job.records_written || 0);
  if (total > 0) return Math.max(0, Math.min(100, (done / total) * 100));
  const phase = String(job.phase || '').toLowerCase();
  if (phase === 'publishing_bootstrap') return 98;
  if (phase === 'complete') return 100;
  if (phase === 'error') return 100;
  if (phase) return 5;
  return 0;
};
const syntheticJobIsActive = (job) => {
  const phase = String(job?.phase || '').toLowerCase();
  return ['queued', 'starting', 'generating', 'publishing_bootstrap', 'running'].includes(phase);
};
const mergePredictorStatus = (runtimeStatus, sourceStatus, predictionPayload, fallbackSource) => {
  const merged = {
    ...(predictionPayload || {}),
    ...(sourceStatus || {}),
    ...(runtimeStatus || {}),
  };
  if (!merged.source) merged.source = fallbackSource;
  return Object.keys(merged).length ? merged : null;
};
const normalizeExperimentDeviceName = (value) => {
  const key = String(value || '').trim().toLowerCase();
  if (key === 'light1') return 'lighting1';
  if (key === 'light2') return 'lighting2';
  return key;
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
  const pagePath = typeof window !== 'undefined' ? window.location.pathname : '/';
  const isAdminPage = /^\/admin\/?$/i.test(pagePath);
  const [state, setState] = useState(createInitialState());
  const [mode, setMode] = useState('live');
  const [tariffConfig, setTariffConfig] = useState(DEFAULT_TARIFF_CONFIG);
  const [tariffConfigMeta, setTariffConfigMeta] = useState({ source: 'default', versionCount: 0, effectiveFrom: '' });
  const [tariffConfigSaving, setTariffConfigSaving] = useState(false);
  const [loadDrafts, setLoadDrafts] = useState({});
  const [systemDraft, setSystemDraft] = useState({});
  const [processDraft, setProcessDraft] = useState({});
  const [toasts, setToasts] = useState([]);
  const [energyWindowMin, setEnergyWindowMin] = useState(DEFAULT_ENERGY_WINDOW_MIN);
  const [predictorWindowMin, setPredictorWindowMin] = useState(DEFAULT_PREDICTOR_WINDOW_MIN);
  const [historyRows, setHistoryRows] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyExporting, setHistoryExporting] = useState(false);
  const [historyError, setHistoryError] = useState('');
  const [liveLoadEnergySelection, setLiveLoadEnergySelection] = useState({});
  const [experimentTemplates, setExperimentTemplates] = useState([]);
  const [experimentTemplateId, setExperimentTemplateId] = useState('');
  const [experimentDraft, setExperimentDraft] = useState({});
  const [experimentRuns, setExperimentRuns] = useState([]);
  const [experimentLoading, setExperimentLoading] = useState(false);
  const [experimentExecuting, setExperimentExecuting] = useState(false);
  const [experimentExportingRunId, setExperimentExportingRunId] = useState('');
  const [experimentRun, setExperimentRun] = useState(null);
  const [evaluationDraft, setEvaluationDraft] = useState({});
  const [policySimulationDraft, setPolicySimulationDraft] = useState({});
  const [adminModels, setAdminModels] = useState([]);
  const [adminModelsLoading, setAdminModelsLoading] = useState(false);
  const [syntheticDatasets, setSyntheticDatasets] = useState([]);
  const [syntheticDatasetsLoading, setSyntheticDatasetsLoading] = useState(false);
  const [syntheticTrainingLoading, setSyntheticTrainingLoading] = useState(false);
  const [syntheticTrainingResult, setSyntheticTrainingResult] = useState(null);
  const [syntheticGenerationDraft, setSyntheticGenerationDraft] = useState(() => {
    const now = Date.now();
    return {
      start: toDateTimeLocal(now),
      days: 5,
      stepSec: 5,
      seed: 42,
      overwrite: false,
    };
  });
  const [syntheticTrainingDraft, setSyntheticTrainingDraft] = useState(() => {
    const now = Date.now();
    return {
      sourceMode: 'reuse',
      existingOutputDir: '',
      historyFrom: toDateTimeLocal(now - (24 * 60 * 60 * 1000)),
      historyTo: toDateTimeLocal(now),
      predictorMode: 'TRAIN_ONLY',
      deleteSavedModels: true,
    };
  });
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
  const lastExperimentExportToastKeyRef = useRef('');
  const experimentCancelRef = useRef(false);
  const stateRef = useRef(state);

  const clientRef = useRef(null);
  const demoTickRef = useRef(createDemoTick());
  const topics = useMemo(() => buildTopicSet(PLANT_ID), []);

  useEffect(() => {
    stateRef.current = state;
  }, [state]);

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
      const demoTick = createDemoTick();
      demoTickRef.current = demoTick;
      setState(() => {
        let next = { ...createInitialState(), connection: 'demo', lastError: '' };
        for (let i = 0; i < 72; i += 1) {
          const frame = demoTick();
          next = applyTelemetry(next, frame.telemetry);
          next = applyPrediction(next, frame.shortPred, 'short');
          next = applyPrediction(next, frame.longPred, 'long_rf');
          next = applyPrediction(next, frame.longLstmPred, 'long_lstm');
        }
        return next;
      });
      const id = setInterval(() => {
        const frame = demoTick();
        setState((prev) => {
          let next = applyTelemetry(prev, frame.telemetry);
          next = applyPrediction(next, frame.shortPred, 'short');
          next = applyPrediction(next, frame.longPred, 'long_rf');
          next = applyPrediction(next, frame.longLstmPred, 'long_lstm');
          return { ...next, connection: 'demo', lastError: '' };
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
  const energyGoalWindowStartedAt = String(system.ENERGY_GOAL_WINDOW_STARTED_AT_UTC || budget.started_at_utc || '');
  const energyGoalWindowEndsAt = String(system.ENERGY_GOAL_WINDOW_ENDS_AT_UTC || budget.ends_at_utc || '');
  const energyGoalWindowRemainingSec = Number(system.ENERGY_GOAL_WINDOW_REMAINING_SEC ?? budget.remaining_sec);
  const energyGoalWindowRemainingPct = Number(system.ENERGY_GOAL_WINDOW_REMAINING_RATIO ?? budget.remaining_ratio);
  const energyGoalWindowResetReason = String(system.ENERGY_GOAL_WINDOW_RESET_REASON || budget.reset_reason || '');
  const energyGoalWindowResetCount = Number(system.ENERGY_GOAL_WINDOW_RESET_COUNT ?? budget.reset_count);
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
  const predictorModeRaw = String(system.predictor_mode || 'ONLINE').toUpperCase();
  const predictorModeValue = PREDICTOR_MODE_OPTIONS.includes(predictorModeRaw) ? predictorModeRaw : 'ONLINE';
  const predictionSourceStatus = system.prediction_sources || {};
  const predictorRuntimeStatus = state.predictorStatus || {};
  const experimentExportStatusMap = state.experimentExportStatus || {};
  const syntheticJobStatus = state.syntheticTrainingStatus || null;
  const syntheticTrainingJob = syntheticJobStatus || syntheticTrainingResult?.job || null;
  const selectedSyntheticDataset = useMemo(
    () => syntheticDatasets.find((item) => item.path === syntheticTrainingDraft.existingOutputDir) || syntheticDatasets[0] || null,
    [syntheticDatasets, syntheticTrainingDraft.existingOutputDir],
  );
  const rfPredictorStatus = mergePredictorStatus(
    predictorRuntimeStatus.long_rf,
    predictionSourceStatus.long_rf,
    state.predictions?.longRf,
    'long_rf',
  );
  const lstmPredictorStatus = mergePredictorStatus(
    predictorRuntimeStatus.long_lstm,
    predictionSourceStatus.long_lstm,
    state.predictions?.longLstm,
    'long_lstm',
  );
  const syntheticTrainingBusy = Boolean(
    syntheticTrainingLoading
    || syntheticJobIsActive(syntheticTrainingJob)
    || rfPredictorStatus?.bootstrap_active
    || lstmPredictorStatus?.bootstrap_active
    || String(rfPredictorStatus?.phase || '').toLowerCase() === 'training'
    || String(lstmPredictorStatus?.phase || '').toLowerCase() === 'training'
  );
  const predictorsReadyForEvaluation = Boolean(rfPredictorStatus?.model_ready) && Boolean(lstmPredictorStatus?.model_ready);
  const evaluationSimulationReadinessIncomplete = !predictorsReadyForEvaluation;
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
  const energyWindowLabel = useMemo(
    () => windowLabelForMinutes(energyWindowMin, ENERGY_WINDOW_OPTIONS),
    [energyWindowMin],
  );
  const predictorMinWindowMin = useMemo(
    () => ({
      short: horizonMinutesForPrediction(state.predictions.short, DEFAULT_PREDICTOR_HORIZON_MIN.short),
      long_rf: horizonMinutesForPrediction(state.predictions.longRf, DEFAULT_PREDICTOR_HORIZON_MIN.long_rf),
      long_lstm: horizonMinutesForPrediction(state.predictions.longLstm, DEFAULT_PREDICTOR_HORIZON_MIN.long_lstm),
    }),
    [state.predictions.short, state.predictions.longRf, state.predictions.longLstm],
  );
  const predictorWindowOptions = useMemo(
    () => ({
      short: predictorWindowOptionsForMin(predictorMinWindowMin.short),
      long_rf: predictorWindowOptionsForMin(predictorMinWindowMin.long_rf),
      long_lstm: predictorWindowOptionsForMin(predictorMinWindowMin.long_lstm),
    }),
    [predictorMinWindowMin],
  );
  useEffect(() => {
    setPredictorWindowMin((prev) => {
      const next = {
        short: clampPredictorWindowMin(prev.short, predictorMinWindowMin.short),
        long_rf: clampPredictorWindowMin(prev.long_rf, predictorMinWindowMin.long_rf),
        long_lstm: clampPredictorWindowMin(prev.long_lstm, predictorMinWindowMin.long_lstm),
      };
      return (
        next.short === prev.short
        && next.long_rf === prev.long_rf
        && next.long_lstm === prev.long_lstm
      ) ? prev : next;
    });
  }, [predictorMinWindowMin]);
  const predictorWindowLabel = useMemo(
    () => ({
      short: windowLabelForMinutes(predictorWindowMin.short, predictorWindowOptions.short),
      long_rf: windowLabelForMinutes(predictorWindowMin.long_rf, predictorWindowOptions.long_rf),
      long_lstm: windowLabelForMinutes(predictorWindowMin.long_lstm, predictorWindowOptions.long_lstm),
    }),
    [predictorWindowMin, predictorWindowOptions],
  );
  const liveLoadNames = useMemo(() => Object.keys(state.loads || {}).sort(), [state.loads]);
  useEffect(() => {
    if (!liveLoadNames.length) return;
    setLiveLoadEnergySelection((prev) => {
      const next = {};
      let changed = false;
      for (const name of liveLoadNames) {
        if (Object.prototype.hasOwnProperty.call(prev, name)) {
          next[name] = Boolean(prev[name]);
        } else {
          next[name] = true;
          changed = true;
        }
      }
      if (Object.keys(prev).length !== Object.keys(next).length) changed = true;
      return changed ? next : prev;
    });
  }, [liveLoadNames]);
  const analyticsRollingEnergyRows = useMemo(
    () => buildRollingEnergySeries(state.powerHistory, {
      rolling_energy_wh: 'total_energy_wh',
    }, energyWindowMin),
    [state.powerHistory, energyWindowMin],
  );
  const analyticsRollingCostRows = useMemo(
    () => buildRollingEnergySeries(analyticsCostRows, {
      rolling_cost: 'total_cost',
    }, energyWindowMin),
    [analyticsCostRows, energyWindowMin],
  );
  const analyticsCostDomain = useMemo(
    () => computeSeriesDomain(analyticsRollingCostRows, ['rolling_cost']),
    [analyticsRollingCostRows],
  );
  const analyticsCostSummary = analyticsCostRows.length ? analyticsCostRows[analyticsCostRows.length - 1] : null;
  const analyticsAccumulatedEnergyCost = Number(analyticsCostSummary?.accumulated_energy_cost);
  const analyticsDemandChargeCost = Number(analyticsCostSummary?.demand_charge_cost);
  const analyticsAccumulatedCost = Number(analyticsCostSummary?.total_cost);
  const analyticsMaxDemandKw = Number(analyticsCostSummary?.max_demand_kw);
  const analyticsRollingEnergySummary = analyticsRollingEnergyRows.length ? analyticsRollingEnergyRows[analyticsRollingEnergyRows.length - 1] : null;
  const analyticsRollingEnergyWh = Number(analyticsRollingEnergySummary?.rolling_energy_wh);
  const analyticsRollingCostSummary = analyticsRollingCostRows.length ? analyticsRollingCostRows[analyticsRollingCostRows.length - 1] : null;
  const analyticsRollingCost = Number(analyticsRollingCostSummary?.rolling_cost);
  const liveLoadEnergySeriesMap = useMemo(
    () => Object.fromEntries(
      liveLoadNames
        .filter((name) => liveLoadEnergySelection?.[name])
        .map((name) => [name, `load_${name}_total_energy_wh`])
    ),
    [liveLoadEnergySelection, liveLoadNames],
  );
  const analyticsLoadEnergyRows = useMemo(
    () => buildRollingEnergySeries(state.powerHistory, liveLoadEnergySeriesMap, energyWindowMin),
    [state.powerHistory, liveLoadEnergySeriesMap, energyWindowMin],
  );
  const liveLoadEnergyKeys = useMemo(() => Object.keys(liveLoadEnergySeriesMap), [liveLoadEnergySeriesMap]);
  const livePowerScale = useMemo(() => chooseScaleSpec('power', collectSeriesValues(state.powerHistory, ['power_w'])), [state.powerHistory]);
  const liveCurrentScale = useMemo(() => chooseScaleSpec('current', collectSeriesValues(state.powerHistory, ['current_a'])), [state.powerHistory]);
  const liveTotalEnergyScale = useMemo(() => chooseScaleSpec('energy', collectSeriesValues(state.powerHistory, ['total_energy_wh'])), [state.powerHistory]);
  const liveRollingEnergyScale = useMemo(() => chooseScaleSpec('energy', collectSeriesValues(analyticsRollingEnergyRows, ['rolling_energy_wh'])), [analyticsRollingEnergyRows]);
  const liveLoadEnergyScale = useMemo(
    () => chooseScaleSpec('energy', collectSeriesValues(analyticsLoadEnergyRows, liveLoadEnergyKeys)),
    [analyticsLoadEnergyRows, liveLoadEnergyKeys],
  );
  const liveRollingCostScale = useMemo(() => chooseScaleSpec('cost', collectSeriesValues(analyticsRollingCostRows, ['rolling_cost'])), [analyticsRollingCostRows]);
  const liveCostScale = useMemo(() => chooseScaleSpec('cost', collectSeriesValues(analyticsCostRows, ['total_cost'])), [analyticsCostRows]);
  const predictionSeriesRows = useMemo(
    () => ({
      short: buildPredictionSeriesRows(state.predictionHistory?.short, predictorWindowMin.short),
      long_rf: buildPredictionSeriesRows(state.predictionHistory?.long_rf, predictorWindowMin.long_rf),
      long_lstm: buildPredictionSeriesRows(state.predictionHistory?.long_lstm, predictorWindowMin.long_lstm),
    }),
    [predictorWindowMin, state.predictionHistory],
  );
  const predictionCardEnergyValues = useMemo(
    () => [
      predictionEnergyForWindow(state.predictions.short, predictorWindowMin.short),
      predictionEnergyForWindow(state.predictions.longRf, predictorWindowMin.long_rf),
      predictionEnergyForWindow(state.predictions.longLstm, predictorWindowMin.long_lstm),
    ].map((value) => Number(value)).filter((value) => Number.isFinite(value)),
    [predictorWindowMin, state.predictions.short, state.predictions.longRf, state.predictions.longLstm],
  );
  const predictionCardEnergyScale = useMemo(() => chooseScaleSpec('energy', predictionCardEnergyValues), [predictionCardEnergyValues]);
  const predictionChartConfigs = useMemo(
    () => PREDICTION_SERIES.map((series) => ({
      ...series,
      rows: predictionSeriesRows[series.key] || [],
      unitScale: chooseScaleSpec('energy', collectSeriesValues(predictionSeriesRows[series.key] || [], ['value'])),
      windowLabel: predictorWindowLabel[series.key],
      minimumWindowLabel: windowLabelForMinutes(predictorMinWindowMin[series.key], predictorWindowOptions[series.key]),
      spanMs: (() => {
        const rows = predictionSeriesRows[series.key] || [];
        if (rows.length < 2) return 0;
        return Math.max(0, Number(rows[rows.length - 1]?.ts) - Number(rows[0]?.ts));
      })(),
    })),
    [predictionSeriesRows, predictorMinWindowMin, predictorWindowLabel, predictorWindowOptions],
  );
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
  const historyEnergyRows = useMemo(
    () => buildRollingEnergySeries(historyChartRows, {
      actual_window_energy_wh: 'actual_total_energy_wh',
    }, energyWindowMin),
    [historyChartRows, energyWindowMin],
  );
  const historicalPredictionRows = useMemo(
    () => ({
      short: buildRollingEnergySeries(historyChartRows, {
        actual_window_energy_wh: 'actual_total_energy_wh',
        pred_window_energy_wh: 'pred_short_energy_wh',
      }, predictorWindowMin.short),
      long_rf: buildRollingEnergySeries(historyChartRows, {
        actual_window_energy_wh: 'actual_total_energy_wh',
        pred_window_energy_wh: 'pred_long_rf_energy_wh',
      }, predictorWindowMin.long_rf),
      long_lstm: buildRollingEnergySeries(historyChartRows, {
        actual_window_energy_wh: 'actual_total_energy_wh',
        pred_window_energy_wh: 'pred_long_lstm_energy_wh',
      }, predictorWindowMin.long_lstm),
    }),
    [historyChartRows, predictorWindowMin],
  );
  const historyRollingCostRows = useMemo(
    () => buildRollingEnergySeries(historyCostRows, {
      rolling_cost: 'total_cost',
    }, energyWindowMin),
    [historyCostRows, energyWindowMin],
  );
  const historyCostDomain = useMemo(() => computeSeriesDomain(historyRollingCostRows, ['rolling_cost']), [historyRollingCostRows]);
  const historyCostSummary = historyCostRows.length ? historyCostRows[historyCostRows.length - 1] : null;
  const historyAccumulatedCost = Number(historyCostSummary?.total_cost);
  const historyAccumulatedEnergyCost = Number(historyCostSummary?.accumulated_energy_cost);
  const historyDemandChargeCost = Number(historyCostSummary?.demand_charge_cost);
  const historyMaxDemandKw = Number(historyCostSummary?.max_demand_kw);
  const historyRollingCostSummary = historyRollingCostRows.length ? historyRollingCostRows[historyRollingCostRows.length - 1] : null;
  const historyRollingCost = Number(historyRollingCostSummary?.rolling_cost);
  const historyRollingEnergySummary = historyEnergyRows.length ? historyEnergyRows[historyEnergyRows.length - 1] : null;
  const historyRollingEnergyWh = Number(historyRollingEnergySummary?.actual_window_energy_wh);
  const historyEnergyScale = useMemo(
    () => chooseScaleSpec('energy', collectSeriesValues(historyEnergyRows, ['actual_window_energy_wh'])),
    [historyEnergyRows],
  );
  const historyCostScale = useMemo(() => chooseScaleSpec('cost', collectSeriesValues(historyRollingCostRows, ['rolling_cost'])), [historyRollingCostRows]);
  const historicalPredictionCharts = useMemo(
    () => ([
      {
        key: 'short',
        enabled: historySeries.short,
        name: 'Short EMA',
        color: '#ffd166',
        rows: historicalPredictionRows.short,
        scale: chooseScaleSpec('energy', collectSeriesValues(historicalPredictionRows.short, ['actual_window_energy_wh', 'pred_window_energy_wh'])),
        windowLabel: predictorWindowLabel.short,
      },
      {
        key: 'long_rf',
        enabled: historySeries.longRf,
        name: 'Long RF',
        color: '#ee6c4d',
        rows: historicalPredictionRows.long_rf,
        scale: chooseScaleSpec('energy', collectSeriesValues(historicalPredictionRows.long_rf, ['actual_window_energy_wh', 'pred_window_energy_wh'])),
        windowLabel: predictorWindowLabel.long_rf,
      },
      {
        key: 'long_lstm',
        enabled: historySeries.longLstm,
        name: 'Long LSTM',
        color: '#72efdd',
        rows: historicalPredictionRows.long_lstm,
        scale: chooseScaleSpec('energy', collectSeriesValues(historicalPredictionRows.long_lstm, ['actual_window_energy_wh', 'pred_window_energy_wh'])),
        windowLabel: predictorWindowLabel.long_lstm,
      },
    ]),
    [historicalPredictionRows, historySeries.longLstm, historySeries.longRf, historySeries.short, predictorWindowLabel],
  );
  const historyChartWidth = useMemo(() => {
    const pointCount = Math.max(1, historyChartRows.length);
    const pxPerPoint = historySpanMs <= 24 * 60 * 60 * 1000 ? 34
      : historySpanMs <= 7 * 24 * 60 * 60 * 1000 ? 24
        : 16;
    return Math.max(920, pointCount * pxPerPoint);
  }, [historyChartRows.length, historySpanMs]);
  const experimentCurrentStep = experimentRun?.current_step
    || (Array.isArray(experimentRun?.plan) ? experimentRun.plan.find((step) => step?.status === 'running') : null)
    || null;

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

  const refreshExperimentTemplates = useCallback(async () => {
    if (!HISTORY_API_URL) {
      setExperimentTemplates([]);
      return;
    }
    try {
      const base = String(HISTORY_API_URL).replace(/\/+$/, '');
      const response = await fetch(`${base}/api/experiment-templates`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!body?.ok || !Array.isArray(body?.templates)) throw new Error(String(body?.error || 'experiment_templates_failed'));
      setExperimentTemplates(body.templates);
    } catch {
      setExperimentTemplates([]);
    }
  }, []);

  const refreshExperimentRuns = useCallback(async () => {
    if (!HISTORY_API_URL) {
      setExperimentRuns([]);
      return;
    }
    try {
      const base = String(HISTORY_API_URL).replace(/\/+$/, '');
      const response = await fetch(`${base}/api/experiment-runs?limit=20`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!body?.ok || !Array.isArray(body?.runs)) throw new Error(String(body?.error || 'experiment_runs_failed'));
      setExperimentRuns(body.runs);
    } catch {
      setExperimentRuns([]);
    }
  }, []);

  const refreshAdminModels = useCallback(async () => {
    if (!HISTORY_API_URL || !isAdminPage) {
      setAdminModels([]);
      return;
    }
    setAdminModelsLoading(true);
    try {
      const base = String(HISTORY_API_URL).replace(/\/+$/, '');
      const response = await fetch(`${base}/api/admin/models`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!body?.ok || !Array.isArray(body?.models)) throw new Error(String(body?.error || 'admin_models_failed'));
      setAdminModels(body.models);
    } catch (err) {
      setAdminModels([]);
      pushToast('error', `Model list failed: ${String(err?.message || err || 'unknown_error')}`);
    } finally {
      setAdminModelsLoading(false);
    }
  }, [isAdminPage]);

  const refreshSyntheticDatasets = useCallback(async () => {
    if (!HISTORY_API_URL || !isAdminPage) {
      setSyntheticDatasets([]);
      return;
    }
    setSyntheticDatasetsLoading(true);
    try {
      const base = String(HISTORY_API_URL).replace(/\/+$/, '');
      const response = await fetch(`${base}/api/admin/synthetic-datasets?limit=100`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!body?.ok || !Array.isArray(body?.datasets)) throw new Error(String(body?.error || 'synthetic_datasets_failed'));
      setSyntheticDatasets(body.datasets);
      setSyntheticTrainingDraft((prev) => {
        if (prev.sourceMode !== 'reuse') return prev;
        const stillExists = body.datasets.some((item) => item.path === prev.existingOutputDir);
        if (stillExists) return prev;
        return {
          ...prev,
          existingOutputDir: body.datasets[0]?.path || '',
        };
      });
    } catch (err) {
      setSyntheticDatasets([]);
      pushToast('error', `Synthetic dataset list failed: ${String(err?.message || err || 'unknown_error')}`);
    } finally {
      setSyntheticDatasetsLoading(false);
    }
  }, [HISTORY_API_URL, isAdminPage]);

  useEffect(() => {
    if (!HISTORY_API_URL) {
      setExperimentTemplates([]);
      setExperimentRuns([]);
      return;
    }
    if (isAdminPage) {
      refreshExperimentTemplates();
      refreshExperimentRuns();
      refreshAdminModels();
      refreshSyntheticDatasets();
      return;
    }
    setExperimentTemplates([]);
    setExperimentRuns([]);
    setAdminModels([]);
    setSyntheticDatasets([]);
  }, [isAdminPage, refreshAdminModels, refreshExperimentTemplates, refreshExperimentRuns, refreshSyntheticDatasets]);

  useEffect(() => {
    if (!isAdminPage) return;
    const phase = String(syntheticTrainingJob?.phase || '').toLowerCase();
    if (phase === 'complete' || phase === 'error') {
      refreshAdminModels();
      refreshSyntheticDatasets();
    }
  }, [isAdminPage, syntheticTrainingJob?.phase, refreshAdminModels, refreshSyntheticDatasets]);

  const selectedExperimentTemplate = useMemo(
    () => experimentTemplates.find((item) => item.id === experimentTemplateId) || experimentTemplates[0] || null,
    [experimentTemplates, experimentTemplateId],
  );

  useEffect(() => {
    if (!experimentTemplates.length) return;
    if (!experimentTemplateId) {
      setExperimentTemplateId(String(experimentTemplates[0]?.id || ''));
    }
  }, [experimentTemplates, experimentTemplateId]);

  useEffect(() => {
    if (!selectedExperimentTemplate) {
      setExperimentDraft({});
      return;
    }
    setExperimentDraft({ ...(selectedExperimentTemplate.defaults || {}) });
  }, [selectedExperimentTemplate?.id]);

  useEffect(() => {
    if (syntheticTrainingDraft.sourceMode !== 'reuse') return;
    if (syntheticTrainingDraft.existingOutputDir) return;
    if (!syntheticDatasets.length) return;
    setSyntheticTrainingDraft((prev) => (
      prev.sourceMode !== 'reuse' || prev.existingOutputDir
        ? prev
        : { ...prev, existingOutputDir: syntheticDatasets[0]?.path || '' }
    ));
  }, [syntheticDatasets, syntheticTrainingDraft.sourceMode, syntheticTrainingDraft.existingOutputDir]);

  const predictionComparisonTemplate = useMemo(
    () => experimentTemplates.find((item) => item.id === 'prediction_comparison') || null,
    [experimentTemplates],
  );
  const policyComparisonTemplate = useMemo(
    () => experimentTemplates.find((item) => item.id === 'policy_comparison') || null,
    [experimentTemplates],
  );
  const evaluationSourceMode = String(evaluationDraft.source_mode || 'live').toLowerCase();
  const policySimulationSourceMode = String(policySimulationDraft.source_mode || 'live').toLowerCase();
  const evaluationSyntheticDataset = useMemo(
    () => syntheticDatasets.find((item) => item.path === evaluationDraft.existing_output_dir) || syntheticDatasets[0] || null,
    [evaluationDraft.existing_output_dir, syntheticDatasets],
  );
  const policySimulationSyntheticDataset = useMemo(
    () => syntheticDatasets.find((item) => item.path === policySimulationDraft.existing_output_dir) || syntheticDatasets[0] || null,
    [policySimulationDraft.existing_output_dir, syntheticDatasets],
  );

  useEffect(() => {
    if (!predictionComparisonTemplate) return;
    setEvaluationDraft((prev) => (Object.keys(prev || {}).length ? prev : {
      ...(predictionComparisonTemplate.defaults || {}),
      control_policy: 'NO_ENERGY_MANAGEMENT',
      source_mode: 'live',
      existing_output_dir: '',
      history_from: historyDraft.from,
      history_to: historyDraft.to,
    }));
  }, [predictionComparisonTemplate?.id]);

  useEffect(() => {
    if (!policyComparisonTemplate) return;
    setPolicySimulationDraft((prev) => (Object.keys(prev || {}).length ? prev : {
      ...(policyComparisonTemplate.defaults || {}),
      source_mode: 'live',
      existing_output_dir: '',
      history_from: historyDraft.from,
      history_to: historyDraft.to,
    }));
  }, [policyComparisonTemplate?.id, historyDraft.from, historyDraft.to]);

  useEffect(() => {
    if (!syntheticDatasets.length) return;
    setEvaluationDraft((prev) => (
      String(prev.source_mode || 'live').toLowerCase() === 'synthetic' && !prev.existing_output_dir
        ? { ...prev, existing_output_dir: syntheticDatasets[0]?.path || '' }
        : prev
    ));
    setPolicySimulationDraft((prev) => (
      String(prev.source_mode || 'live').toLowerCase() === 'synthetic' && !prev.existing_output_dir
        ? { ...prev, existing_output_dir: syntheticDatasets[0]?.path || '' }
        : prev
    ));
  }, [syntheticDatasets, evaluationSourceMode, policySimulationSourceMode]);

  const setExperimentDraftField = (name, value, type = 'text') => {
    setExperimentDraft((prev) => ({
      ...prev,
      [name]: type === 'boolean' ? Boolean(value) : value,
    }));
  };

  const setQuickDraftField = (setter, name, value, type = 'text') => {
    setter((prev) => ({
      ...prev,
      [name]: type === 'boolean' ? Boolean(value) : value,
    }));
  };

  const buildExperimentConfigForTemplate = (template, draft) => {
    if (!template) return {};
    const config = { ...(template.defaults || {}) };
    for (const field of template.fields || []) {
      const raw = draft?.[field.name];
      if (field.type === 'boolean') {
        config[field.name] = Boolean(raw);
        continue;
      }
      if (raw === '' || raw === undefined || raw === null) {
        if (field.optional) config[field.name] = null;
        continue;
      }
      if (field.type === 'number') {
        const numeric = Number(raw);
        if (Number.isFinite(numeric)) config[field.name] = numeric;
        else if (field.optional) config[field.name] = null;
        continue;
      }
      config[field.name] = raw;
    }
    return config;
  };

  const buildExperimentConfig = () => buildExperimentConfigForTemplate(selectedExperimentTemplate, experimentDraft);

  const buildQuickWorkflowSource = (draft) => {
    const sourceMode = String(draft?.source_mode || 'live').toLowerCase();
    if (sourceMode === 'synthetic') {
      const fallbackDatasetPath = syntheticDatasets[0]?.path || '';
      return {
        source_mode: 'synthetic',
        existing_output_dir: String(draft?.existing_output_dir || fallbackDatasetPath).trim(),
      };
    }
    if (sourceMode === 'history') {
      const fromMs = parseDateTimeLocal(draft?.history_from);
      const toMs = parseDateTimeLocal(draft?.history_to);
      return {
        source_mode: 'history',
        history_from: Number.isFinite(fromMs) ? new Date(fromMs).toISOString() : undefined,
        history_to: Number.isFinite(toMs) ? new Date(toMs).toISOString() : undefined,
      };
    }
    return { source_mode: 'live' };
  };

  const delayWithCancel = (durationMs) => new Promise((resolve, reject) => {
    const endAt = Date.now() + Math.max(0, durationMs);
    const tick = () => {
      if (experimentCancelRef.current) {
        reject(new Error('__experiment_cancelled__'));
        return;
      }
      if (Date.now() >= endAt) {
        resolve();
        return;
      }
      setTimeout(tick, Math.min(500, Math.max(50, endAt - Date.now())));
    };
    tick();
  });

  const approxEqual = (actual, expected, tolerance = 0.05) => {
    const a = Number(actual);
    const e = Number(expected);
    if (!Number.isFinite(a) || !Number.isFinite(e)) return false;
    return Math.abs(a - e) <= tolerance;
  };

  const getLoadSnapshot = (deviceName) => {
    const key = normalizeExperimentDeviceName(deviceName);
    const loads = stateRef.current?.loads || {};
    return loads?.[key] || null;
  };

  const matchCommandSubset = (actual, expected) => {
    if (expected == null) return true;
    if (Array.isArray(expected)) {
      if (!Array.isArray(actual) || actual.length < expected.length) return false;
      return expected.every((item, index) => matchCommandSubset(actual[index], item));
    }
    if (typeof expected === 'object') {
      if (!actual || typeof actual !== 'object') return false;
      return Object.entries(expected).every(([key, value]) => matchCommandSubset(actual?.[key], value));
    }
    if (typeof expected === 'number') return approxEqual(actual, expected, 0.05);
    return String(actual) === String(expected);
  };

  const getMatchingRealAck = (afterTs, expectedCmd) => {
    const feed = Array.isArray(stateRef.current?.ackFeed) ? stateRef.current.ackFeed : [];
    for (let i = feed.length - 1; i >= 0; i -= 1) {
      const item = feed[i];
      if (!item || !Number.isFinite(item.ts) || item.ts <= afterTs) continue;
      const payload = item.payload;
      if (!payload?.ack) continue;
      if (matchCommandSubset(payload?.cmd || {}, expectedCmd)) return payload;
    }
    return null;
  };

  const waitForAckMatch = async (expectedCmd, { afterTs = 0, timeoutMs = 5000, label = 'command' } = {}) => {
    if (mode === 'demo') return { ack: { ok: true }, cmd: expectedCmd };
    const deadline = Date.now() + Math.max(250, timeoutMs);
    while (Date.now() <= deadline) {
      if (experimentCancelRef.current) throw new Error('__experiment_cancelled__');
      const matched = getMatchingRealAck(afterTs, expectedCmd);
      if (matched) {
        if (matched?.ack?.ok === false) {
          throw new Error(String(matched?.ack?.msg || `${label}_rejected`));
        }
        return matched;
      }
      await delayWithCancel(100);
    }
    throw new Error(`${label}_ack_timeout`);
  };

  const waitForState = async (predicate, { timeoutMs = 6000, label = 'state' } = {}) => {
    if (mode === 'demo') return true;
    const deadline = Date.now() + Math.max(250, timeoutMs);
    while (Date.now() <= deadline) {
      if (experimentCancelRef.current) throw new Error('__experiment_cancelled__');
      try {
        if (predicate(stateRef.current)) return true;
      } catch {
        // ignore transient predicate errors while state refreshes
      }
      await delayWithCancel(120);
    }
    throw new Error(`${label}_state_timeout`);
  };

  const runConfirmedCommand = async ({
    send,
    expectedCmd,
    ackTimeoutMs = 5000,
    stateTimeoutMs = 6000,
    statePredicate,
    label,
    attempts = 2,
  }) => {
    let lastError = null;
    for (let attempt = 1; attempt <= attempts; attempt += 1) {
      const ackBaseline = Number(stateRef.current?.ackAt || 0);
      const published = await send();
      if (!published) {
        lastError = new Error(`${label}_publish_failed`);
      } else {
        try {
          if (typeof statePredicate === 'function') {
            const deadline = Date.now() + Math.max(250, Math.max(ackTimeoutMs, stateTimeoutMs));
            let ackMatched = false;
            let stateMatched = false;
            let stateMatchedAt = 0;
            while (Date.now() <= deadline) {
              if (experimentCancelRef.current) throw new Error('__experiment_cancelled__');
              if (!ackMatched) {
                const matched = getMatchingRealAck(ackBaseline, expectedCmd);
                if (matched) {
                  if (matched?.ack?.ok === false) {
                    throw new Error(String(matched?.ack?.msg || `${label}_rejected`));
                  }
                  ackMatched = true;
                }
              }
              if (!stateMatched) {
                try {
                  stateMatched = Boolean(statePredicate(stateRef.current));
                  if (stateMatched && !stateMatchedAt) stateMatchedAt = Date.now();
                } catch {
                  // ignore transient predicate errors while state refreshes
                }
              }
              if (stateMatched && (ackMatched || (stateMatchedAt && (Date.now() - stateMatchedAt) >= 600))) {
                return true;
              }
              await delayWithCancel(120);
            }
            if (!ackMatched && !stateMatched) throw new Error(`${label}_ack_timeout`);
            if (ackMatched && !stateMatched) throw new Error(`${label}_state_timeout`);
            return true;
          }
          await waitForAckMatch(expectedCmd, { afterTs: ackBaseline, timeoutMs: ackTimeoutMs, label });
          return true;
        } catch (err) {
          lastError = err;
        }
      }
      if (attempt < attempts) await delayWithCancel(250);
    }
    throw lastError || new Error(`${label}_failed`);
  };

  const shouldConfirmLoadState = (setPayload) => {
    const keys = Object.keys(setPayload || {});
    if (keys.length === 0) return false;
    if (keys.some((key) => ['fault_limit_a', 'inject_current_a', 'clear_inject', 'override', 'policy_override'].includes(key))) return true;
    const policy = String(stateRef.current?.system?.control_policy || '').toUpperCase();
    return policy === 'NO_ENERGY_MANAGEMENT';
  };

  const makeLoadStatePredicate = (device, setPayload) => (snapshot) => {
    const load = (snapshot?.loads || {})[normalizeExperimentDeviceName(device)];
    if (!load) return false;
    if (Object.prototype.hasOwnProperty.call(setPayload, 'on') && Boolean(load?.on) !== Boolean(setPayload.on)) return false;
    if (Object.prototype.hasOwnProperty.call(setPayload, 'duty')) {
      const liveDuty = Number.isFinite(Number(load?.duty)) ? Number(load.duty) : Number(load?.duty_applied);
      if (!approxEqual(liveDuty, Number(setPayload.duty), 0.08)) return false;
    }
    if (Object.prototype.hasOwnProperty.call(setPayload, 'override')) {
      const liveOverride = Boolean(load?.override ?? load?.policy_override);
      if (liveOverride !== Boolean(setPayload.override)) return false;
    }
    if (Object.prototype.hasOwnProperty.call(setPayload, 'fault_limit_a') && !approxEqual(load?.fault_limit_a, Number(setPayload.fault_limit_a), 0.05)) return false;
    if (Object.prototype.hasOwnProperty.call(setPayload, 'inject_current_a') && !approxEqual(load?.inject_current_a, Number(setPayload.inject_current_a), 0.05)) return false;
    if (Object.prototype.hasOwnProperty.call(setPayload, 'clear_inject') && Number(load?.inject_current_a || 0) > 0.05) return false;
    return true;
  };

  const postExperimentEvent = async (runId, type, stepId, detail) => {
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    const response = await fetch(`${base}/api/experiment-runs/${encodeURIComponent(runId)}/event`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type,
        step_id: stepId || undefined,
        detail: detail || undefined,
      }),
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const body = await response.json();
    if (!body?.ok || !body?.run) throw new Error(String(body?.error || 'experiment_event_failed'));
    setExperimentRun(body.run);
    setExperimentRuns((prev) => {
      const next = [body.run, ...prev.filter((item) => item.id !== body.run.id)];
      return next.slice(0, 20);
    });
    return body.run;
  };

  const experimentExportNamingMode = isAdminPage ? 'report' : 'standard';

  const resolveExperimentExportJob = (run, namingMode = experimentExportNamingMode) => {
    if (!run?.id) return null;
    const normalizedMode = String(namingMode || experimentExportNamingMode || 'report').trim().toLowerCase() || 'report';
    const liveKey = `${String(run.id)}:${normalizedMode}`;
    const liveJob = experimentExportStatusMap?.[liveKey] || null;
    const storedJob = run?.export_jobs?.[normalizedMode] || null;
    if (!liveJob && !storedJob) return null;
    return {
      ...(storedJob || {}),
      ...(liveJob || {}),
      run_id: String(run.id),
      naming_mode: normalizedMode,
    };
  };

  const requestExperimentRunExport = async (run, { auto = false, namingMode = experimentExportNamingMode } = {}) => {
    if (!run?.id) {
      pushToast('warning', 'No experiment run selected.');
      return null;
    }
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return null;
    }
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    setExperimentExportingRunId(String(run.id));
    try {
      const response = await fetch(
        `${base}/api/experiment-runs/${encodeURIComponent(run.id)}/export-request?naming=${encodeURIComponent(namingMode)}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        },
      );
      if (!response.ok) {
        let errorText = `HTTP ${response.status}`;
        try {
          const body = await response.json();
          errorText = String(body?.error || errorText);
        } catch {
          // ignore JSON parse errors
        }
        throw new Error(errorText);
      }
      const body = await response.json();
      if (!body?.ok || !body?.run) throw new Error(String(body?.error || 'experiment_export_request_failed'));
      const updatedRun = body.run;
      setExperimentRun((prev) => (prev?.id === updatedRun.id ? updatedRun : prev));
      setExperimentRuns((prev) => [updatedRun, ...prev.filter((item) => item.id !== updatedRun.id)].slice(0, 20));
      const exportJob = body.export_job || resolveExperimentExportJob(updatedRun, namingMode);
      if (!auto) {
        if (exportJob?.download_ready) {
          pushToast('info', 'Experiment export is ready to download.');
        } else {
          pushToast('info', 'Experiment export is being prepared. You will be notified when it is ready.', 6500);
        }
      }
      return { run: updatedRun, exportJob };
    } catch (err) {
      pushToast('error', `Experiment export request failed: ${String(err?.message || err || 'unknown_error')}`);
      return null;
    } finally {
      setExperimentExportingRunId('');
    }
  };

  const downloadReadyExperimentRunExport = async (run, { namingMode = experimentExportNamingMode } = {}) => {
    if (!run?.id) {
      pushToast('warning', 'No experiment run selected.');
      return false;
    }
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return false;
    }
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    setExperimentExportingRunId(String(run.id));
    try {
      const link = document.createElement('a');
      link.href = `${base}/api/experiment-runs/${encodeURIComponent(run.id)}/export?naming=${encodeURIComponent(namingMode)}`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      pushToast('info', 'Experiment export download started.');
      return true;
    } catch (err) {
      pushToast('error', `Experiment export failed: ${String(err?.message || err || 'unknown_error')}`);
      return false;
    } finally {
      setExperimentExportingRunId('');
    }
  };

  const downloadExperimentRunExport = async (run, { namingMode = experimentExportNamingMode } = {}) => {
    const exportJob = resolveExperimentExportJob(run, namingMode);
    if (experimentExportReady(exportJob)) {
      return downloadReadyExperimentRunExport(run, { namingMode });
    }
    await requestExperimentRunExport(run, { auto: false, namingMode });
    return false;
  };

  const setSyntheticTrainingDraftField = (name, value, type = 'text') => {
    setSyntheticTrainingDraft((prev) => ({
      ...prev,
      [name]: type === 'boolean' ? Boolean(value) : value,
    }));
  };

  const setSyntheticGenerationDraftField = (name, value, type = 'text') => {
    setSyntheticGenerationDraft((prev) => ({
      ...prev,
      [name]: type === 'boolean' ? Boolean(value) : value,
    }));
  };

  const deleteSavedModels = async (modelType = '') => {
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return;
    }
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    setAdminModelsLoading(true);
    try {
      const response = await fetch(`${base}/api/admin/models/delete`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ model_type: modelType || undefined }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!body?.ok || !Array.isArray(body?.models)) throw new Error(String(body?.error || 'delete_models_failed'));
      setAdminModels(body.models);
      pushToast('info', body.deleted?.length ? `Deleted ${body.deleted.length} saved model files.` : 'No saved model files matched.');
    } catch (err) {
      pushToast('error', `Delete models failed: ${String(err?.message || err || 'unknown_error')}`);
    } finally {
      setAdminModelsLoading(false);
    }
  };

  const kickstartSyntheticTraining = async () => {
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return;
    }
    const sourceMode = String(syntheticTrainingDraft.sourceMode || 'reuse').toLowerCase();
    const reuseExisting = sourceMode === 'reuse';
    const useHistoryLogs = sourceMode === 'history';
    const historyFromMs = useHistoryLogs ? parseDateTimeLocal(syntheticTrainingDraft.historyFrom) : NaN;
    const historyToMs = useHistoryLogs ? parseDateTimeLocal(syntheticTrainingDraft.historyTo) : NaN;
    if (useHistoryLogs && (!Number.isFinite(historyFromMs) || !Number.isFinite(historyToMs) || historyToMs <= historyFromMs)) {
      pushToast('warning', 'Set a valid history-log training range.');
      return;
    }
    const predictorMode = String(syntheticTrainingDraft.predictorMode || 'TRAIN_ONLY').toUpperCase();
    if (!ADMIN_TRAINING_PREDICTOR_MODE_OPTIONS.includes(predictorMode)) {
      pushToast('warning', 'Training can only run in TRAIN_ONLY or ONLINE mode.');
      return;
    }
    const existingOutputDir = String(syntheticTrainingDraft.existingOutputDir || '').trim();
    if (reuseExisting && !existingOutputDir) {
      pushToast('warning', 'Select an existing synthetic dataset to reuse.');
      return;
    }
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    setSyntheticTrainingLoading(true);
    try {
      const response = await fetch(`${base}/api/admin/synthetic-training`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          source_mode: sourceMode,
          history_from: useHistoryLogs ? new Date(historyFromMs).toISOString() : undefined,
          history_to: useHistoryLogs ? new Date(historyToMs).toISOString() : undefined,
          predictor_mode: predictorMode,
          existing_output_dir: reuseExisting ? existingOutputDir : undefined,
          delete_saved_models: Boolean(syntheticTrainingDraft.deleteSavedModels),
          train_models: true,
        }),
      });
      if (!response.ok) {
        let errorText = `HTTP ${response.status}`;
        try {
          const body = await response.json();
          errorText = String(body?.error || errorText);
        } catch {
          // ignore
        }
        throw new Error(errorText);
      }
      const body = await response.json();
      if (!body?.ok || !body?.job) throw new Error(String(body?.error || 'synthetic_training_failed'));
      setSyntheticTrainingResult(body);
      pushToast('info', useHistoryLogs
        ? `History-log training job started (${String(body.job?.job_id || '').slice(0, 8) || 'pending'}).`
        : reuseExisting
          ? `Model training started on existing synthetic dataset (${String(body.job?.job_id || '').slice(0, 8) || 'pending'}).`
          : `Model training job started (${String(body.job?.job_id || '').slice(0, 8) || 'pending'}).`);
    } catch (err) {
      pushToast('error', `Synthetic training failed: ${String(err?.message || err || 'unknown_error')}`);
    } finally {
      setSyntheticTrainingLoading(false);
    }
  };

  const generateSyntheticDataset = async () => {
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return;
    }
    const startMs = parseDateTimeLocal(syntheticGenerationDraft.start);
    if (!Number.isFinite(startMs)) {
      pushToast('warning', 'Set a valid synthetic dataset start time.');
      return;
    }
    const days = Math.max(1, Math.round(Number(syntheticGenerationDraft.days) || 0));
    const stepSec = Math.max(1, Math.round(Number(syntheticGenerationDraft.stepSec) || 0));
    const seed = Math.round(Number(syntheticGenerationDraft.seed) || 42);
    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    setSyntheticTrainingLoading(true);
    try {
      const response = await fetch(`${base}/api/admin/synthetic-training`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          source_mode: 'generate',
          start: new Date(startMs).toISOString(),
          days,
          step_sec: stepSec,
          seed,
          overwrite: Boolean(syntheticGenerationDraft.overwrite),
          train_models: false,
          delete_saved_models: false,
        }),
      });
      if (!response.ok) {
        let errorText = `HTTP ${response.status}`;
        try {
          const body = await response.json();
          errorText = String(body?.error || errorText);
        } catch {
          // ignore
        }
        throw new Error(errorText);
      }
      const body = await response.json();
      if (!body?.ok || !body?.job) throw new Error(String(body?.error || 'synthetic_generation_failed'));
      setSyntheticTrainingResult(body);
      pushToast('info', `Synthetic dataset generation started (${String(body.job?.job_id || '').slice(0, 8) || 'pending'}).`);
    } catch (err) {
      pushToast('error', `Synthetic dataset generation failed: ${String(err?.message || err || 'unknown_error')}`);
    } finally {
      setSyntheticTrainingLoading(false);
    }
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

    const predictorMode = String(process.predictor_mode || system.predictor_mode || 'ONLINE').toUpperCase();
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
      return true;
    }
    const ok = clientRef.current?.publish(payload);
    if (!ok) {
      setState((prev) => applyAck(prev, { local: true, error: 'MQTT not connected', payload }));
      pushToast('error', 'MQTT not connected.');
    }
    return !!ok;
  };

  const publishTopic = (topic, payload) => {
    if (mode === 'demo') {
      setState((prev) => applyAck(prev, { demo: true, topic, queued: payload }));
      return true;
    }
    const ok = clientRef.current?.publishTopic(topic, payload);
    if (!ok) {
      setState((prev) => applyAck(prev, { local: true, error: 'MQTT not connected', topic, payload }));
      pushToast('error', 'MQTT not connected.');
    }
    return !!ok;
  };

  const executeExperimentSystemStep = async (params = {}) => {
    const policy = String(params.control_policy || '').toUpperCase();
    if (policy && POLICY_OPTIONS.includes(policy)) {
      const policyMatches = (snapshot) => String(snapshot?.system?.control_policy || '').toUpperCase() === policy;
      if (!policyMatches(stateRef.current)) {
        try {
          await runConfirmedCommand({
            send: () => Promise.resolve(publish({ control_policy: policy })),
            expectedCmd: { control_policy: policy },
            statePredicate: policyMatches,
            label: 'control_policy',
          });
        } catch (err) {
          const published = publish({ control_policy: policy });
          if (published) {
            await delayWithCancel(1500);
          }
          if (policyMatches(stateRef.current)) {
            pushToast('warning', `Policy ${policy} applied without command ack confirmation.`);
          } else {
            pushToast('warning', `Policy change to ${policy} could not be confirmed. Continuing with current live policy ${String(stateRef.current?.system?.control_policy || 'UNKNOWN')}.`);
          }
        }
      }
    }

    const controlPayload = {};
    const predictorShortPayload = {};
    const maxPower = Number(params.max_total_power_w);
    if (Number.isFinite(maxPower) && maxPower >= 0) {
      controlPayload.MAX_TOTAL_POWER_W = maxPower;
      predictorShortPayload.MAX_TOTAL_POWER_W = maxPower;
    }
    const energyGoalWh = Number(params.energy_goal_wh);
    if (Number.isFinite(energyGoalWh) && energyGoalWh >= 0) {
      controlPayload.ENERGY_GOAL_WH = energyGoalWh;
      predictorShortPayload.MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH = energyGoalWh;
    }
    const energyGoalDurationMin = Number(params.energy_goal_duration_min);
    if (Number.isFinite(energyGoalDurationMin) && energyGoalDurationMin >= 1) {
      controlPayload.ENERGY_GOAL_DURATION_MIN = energyGoalDurationMin;
    }
    if (params.control && typeof params.control === 'object' && !Array.isArray(params.control)) {
      Object.assign(controlPayload, params.control);
    }
    if (params.predictor_short && typeof params.predictor_short === 'object' && !Array.isArray(params.predictor_short)) {
      Object.assign(predictorShortPayload, params.predictor_short);
    }
    if (Object.keys(controlPayload).length) {
      await runConfirmedCommand({
        send: () => Promise.resolve(publish({ control: controlPayload })),
        expectedCmd: { control: controlPayload },
        statePredicate: (snapshot) => {
          const systemState = snapshot?.system || {};
          if (Object.prototype.hasOwnProperty.call(controlPayload, 'MAX_TOTAL_POWER_W') && !approxEqual(systemState?.MAX_TOTAL_POWER_W, controlPayload.MAX_TOTAL_POWER_W, 0.05)) return false;
          if (Object.prototype.hasOwnProperty.call(controlPayload, 'ENERGY_GOAL_WH') && !approxEqual(systemState?.ENERGY_GOAL_VALUE_WH ?? systemState?.MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH, controlPayload.ENERGY_GOAL_WH, 0.05)) return false;
          if (Object.prototype.hasOwnProperty.call(controlPayload, 'ENERGY_GOAL_DURATION_MIN') && !approxEqual(systemState?.ENERGY_GOAL_DURATION_MIN, controlPayload.ENERGY_GOAL_DURATION_MIN, 0.05)) return false;
          if (Object.prototype.hasOwnProperty.call(controlPayload, 'VOLTAGE_INJECTION_MODE') && String(systemState?.VOLTAGE_INJECTION_MODE || '').toUpperCase() !== String(controlPayload.VOLTAGE_INJECTION_MODE || '').toUpperCase()) return false;
          if (Object.prototype.hasOwnProperty.call(controlPayload, 'VOLTAGE_INJECTION_MAGNITUDE_V') && !approxEqual(systemState?.VOLTAGE_INJECTION_MAGNITUDE_V, controlPayload.VOLTAGE_INJECTION_MAGNITUDE_V, 0.05)) return false;
          return true;
        },
        label: 'control',
      });
    }
    if (Object.keys(predictorShortPayload).length) {
      const shortOk = publishTopic(topics.predictorCmd, predictorShortPayload);
      if (!shortOk) throw new Error('predictor_short_publish_failed');
    }

    const predictorMode = String(params.predictor_mode || '').toUpperCase();
    if (predictorMode && PREDICTOR_MODE_OPTIONS.includes(predictorMode)) {
      const longPayload = {
        PREDICTOR_MODE: predictorMode,
        ...((params.predictor_long && typeof params.predictor_long === 'object' && !Array.isArray(params.predictor_long)) ? params.predictor_long : {}),
      };
      const longLstmPayload = {
        PREDICTOR_MODE: predictorMode,
        ...((params.predictor_long_lstm && typeof params.predictor_long_lstm === 'object' && !Array.isArray(params.predictor_long_lstm)) ? params.predictor_long_lstm : {}),
      };
      const rfOk = publishTopic(topics.predictorLongCmd, longPayload);
      const lstmOk = publishTopic(topics.predictorLongLstmCmd, longLstmPayload);
      if (!rfOk || !lstmOk) throw new Error('predictor_mode_publish_failed');
      const predictorModeMatches = (snapshot) => String(snapshot?.system?.predictor_mode || '').toUpperCase() === predictorMode;
      if (!predictorModeMatches(stateRef.current)) {
        try {
          await runConfirmedCommand({
            send: () => Promise.resolve(publishTopic(topics.cmd, { predictor_mode: predictorMode })),
            expectedCmd: { predictor_mode: predictorMode },
            statePredicate: predictorModeMatches,
            label: 'predictor_mode',
          });
        } catch (err) {
          const published = publishTopic(topics.cmd, { predictor_mode: predictorMode });
          if (published) {
            await delayWithCancel(1500);
          }
          if (predictorModeMatches(stateRef.current)) {
            pushToast('warning', `Predictor mode ${predictorMode} applied without command ack confirmation.`);
          } else {
            pushToast('warning', `Predictor mode change to ${predictorMode} could not be confirmed. Continuing with current live predictor mode ${String(stateRef.current?.system?.predictor_mode || 'UNKNOWN')}.`);
          }
        }
      }
    }
    return true;
  };

  const executeExperimentStartProcess = async (params = {}) => {
    const payload = buildProcessStartPayload();
    payload.process = {
      ...(payload.process || {}),
      ...(params || {}),
      enabled: true,
    };
    await runConfirmedCommand({
      send: () => Promise.resolve(publish(payload)),
      expectedCmd: { process: { enabled: true } },
      statePredicate: (snapshot) => Boolean(snapshot?.process?.enabled),
      label: 'start_process',
      stateTimeoutMs: 10000,
    });
    return true;
  };

  const executeExperimentSetLoads = async (params = {}) => {
    const loadSteps = Array.isArray(params.loads) ? params.loads : [];
    for (const item of loadSteps) {
      const device = normalizeExperimentDeviceName(item?.device);
      const setPayload = item?.set && typeof item.set === 'object' && !Array.isArray(item.set) ? item.set : null;
      if (!device || !setPayload || !Object.keys(setPayload).length) continue;
      await runConfirmedCommand({
        send: () => Promise.resolve(publish({ device, set: setPayload })),
        expectedCmd: { device, set: setPayload },
        statePredicate: shouldConfirmLoadState(setPayload) ? makeLoadStatePredicate(device, setPayload) : undefined,
        label: `load_${device}`,
        attempts: 3,
        stateTimeoutMs: 5000,
      });
    }
    return true;
  };

  const executeExperimentAction = async (step) => {
    const action = String(step?.action || '');
    const params = step?.params || {};
    if (action === 'apply_system') return executeExperimentSystemStep(params);
    if (action === 'start_process') return executeExperimentStartProcess(params);
    if (action === 'stop_process') {
      await runConfirmedCommand({
        send: () => Promise.resolve(publish(stopProcessPayload())),
        expectedCmd: { process: { enabled: false } },
        statePredicate: (snapshot) => !Boolean(snapshot?.process?.enabled),
        label: 'stop_process',
        stateTimeoutMs: 10000,
      });
      return true;
    }
    if (action === 'set_loads') return executeExperimentSetLoads(params);
    if (action === 'voltage_injection') {
      const modeName = String(params.mode || 'NONE').toUpperCase();
      const magnitude = Number(params.magnitude_v);
      const control = {
        VOLTAGE_INJECTION_MODE: modeName,
        VOLTAGE_INJECTION_MAGNITUDE_V: Number.isFinite(magnitude) ? Math.max(0, magnitude) : 1.0,
      };
      await runConfirmedCommand({
        send: () => Promise.resolve(publish({ control })),
        expectedCmd: { control },
        statePredicate: (snapshot) => {
          const systemState = snapshot?.system || {};
          return String(systemState?.VOLTAGE_INJECTION_MODE || '').toUpperCase() === modeName
            && approxEqual(systemState?.VOLTAGE_INJECTION_MAGNITUDE_V, control.VOLTAGE_INJECTION_MAGNITUDE_V, 0.05);
        },
        label: 'voltage_injection',
      });
      return true;
    }
    if (action === 'clear_voltage_injection') {
      await runConfirmedCommand({
        send: () => Promise.resolve(publish({ control: { CLEAR_VOLTAGE_INJECTION: true } })),
        expectedCmd: { control: { CLEAR_VOLTAGE_INJECTION: true } },
        statePredicate: (snapshot) => String(snapshot?.system?.VOLTAGE_INJECTION_MODE || 'NONE').toUpperCase() === 'NONE',
        label: 'clear_voltage_injection',
      });
      return true;
    }
    return true;
  };

  const runExperimentTemplate = async (template, config, { setAsCurrentTemplate = false, source = null } = {}) => {
    if (!template) {
      pushToast('warning', 'No experiment template is available.');
      return;
    }
    if (!HISTORY_API_URL) {
      pushToast('error', 'History API URL is not configured.');
      return;
    }
    experimentCancelRef.current = false;
    setExperimentLoading(true);
    setExperimentExecuting(true);

    const base = String(HISTORY_API_URL).replace(/\/+$/, '');
    let createdRun = null;
    try {
      const response = await fetch(`${base}/api/experiment-runs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          template_id: template.id,
          config,
          ...(source && typeof source === 'object' ? source : {}),
        }),
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const body = await response.json();
      if (!body?.ok || !body?.run) throw new Error(String(body?.error || 'experiment_create_failed'));
      createdRun = body.run;
      if (setAsCurrentTemplate) setExperimentTemplateId(template.id);
      setExperimentRun(createdRun);
      setExperimentRuns((prev) => [createdRun, ...prev.filter((item) => item.id !== createdRun.id)].slice(0, 20));

      if (String(createdRun?.status || '').toLowerCase() === 'completed' || !Array.isArray(createdRun?.plan) || createdRun.plan.length === 0) {
        const sourceMode = String(createdRun?.source_mode || source?.source_mode || 'live').toLowerCase();
        pushToast(
          'info',
          sourceMode === 'live'
            ? `${template.name} export window is ready. Download the export when ready.`
            : `${template.name} used ${sourceMode} logs only. No live replay was run; download will export existing data from that source.`,
          7000,
        );
        if (['prediction_comparison', 'policy_comparison'].includes(String(template.id || '').toLowerCase())) {
          await requestExperimentRunExport(createdRun, { auto: true });
        }
        return;
      }

      let liveRun = await postExperimentEvent(createdRun.id, 'run_started');
      const plan = Array.isArray(liveRun?.plan) ? liveRun.plan : [];
      for (const step of plan) {
        if (experimentCancelRef.current) throw new Error('__experiment_cancelled__');
        liveRun = await postExperimentEvent(createdRun.id, 'step_started', step.id);
        const ok = await executeExperimentAction(step);
        if (!ok) throw new Error(`command_failed:${step.label || step.action}`);
        if (String(step.action) === 'wait') {
          const waitMs = Math.max(0, Number(step.duration_sec || 0)) * 1000;
          if (waitMs > 0) await delayWithCancel(waitMs);
        }
        liveRun = await postExperimentEvent(createdRun.id, 'step_completed', step.id);
      }
      liveRun = await postExperimentEvent(createdRun.id, 'run_completed');
      setExperimentRun(liveRun);
      pushToast('info', `${template.name} completed. Download the export when ready.`, 6500);
      if (['prediction_comparison', 'policy_comparison'].includes(String(template.id || '').toLowerCase())) {
        await requestExperimentRunExport(liveRun, { auto: true });
      }
    } catch (err) {
      const cancelled = String(err?.message || '') === '__experiment_cancelled__';
      if (createdRun?.id) {
        try {
          const updatedRun = await postExperimentEvent(
            createdRun.id,
            cancelled ? 'run_cancelled' : 'run_failed',
            undefined,
            cancelled ? 'Cancelled from control UI' : String(err?.message || err || 'experiment_failed'),
          );
          setExperimentRun(updatedRun);
        } catch {
          // ignore secondary event failures
        }
      }
      pushToast(cancelled ? 'warning' : 'error', cancelled ? 'Experiment cancelled.' : `Experiment failed: ${String(err?.message || err || 'unknown_error')}`, 7000);
    } finally {
      setExperimentLoading(false);
      setExperimentExecuting(false);
      refreshExperimentRuns();
    }
  };

  const startAutomatedExperiment = async () => {
    if (!selectedExperimentTemplate) {
      pushToast('warning', 'No experiment template is available.');
      return;
    }
    await runExperimentTemplate(selectedExperimentTemplate, buildExperimentConfig(), { setAsCurrentTemplate: true });
  };

  const cancelAutomatedExperiment = () => {
    if (!experimentExecuting) return;
    experimentCancelRef.current = true;
  };

  const evaluationRun = useMemo(() => {
    if (experimentRun?.template_id === 'prediction_comparison') return experimentRun;
    return experimentRuns.find((item) => item.template_id === 'prediction_comparison') || null;
  }, [experimentRun, experimentRuns]);

  const policySimulationRun = useMemo(() => {
    if (experimentRun?.template_id === 'policy_comparison') return experimentRun;
    return experimentRuns.find((item) => item.template_id === 'policy_comparison') || null;
  }, [experimentRun, experimentRuns]);

  const evaluationProgressPct = evaluationRun?.step_count
    ? Math.round(((Number(evaluationRun.completed_steps || 0) / Number(evaluationRun.step_count || 1)) * 100))
    : 0;
  const policySimulationProgressPct = policySimulationRun?.step_count
    ? Math.round(((Number(policySimulationRun.completed_steps || 0) / Number(policySimulationRun.step_count || 1)) * 100))
    : 0;
  const evaluationExportJob = resolveExperimentExportJob(evaluationRun);
  const policySimulationExportJob = resolveExperimentExportJob(policySimulationRun);
  const currentExperimentExportJob = resolveExperimentExportJob(experimentRun);
  const evaluationUsesOfflineSource = evaluationSourceMode !== 'live';
  const policySimulationUsesOfflineSource = policySimulationSourceMode !== 'live';
  const experimentExportReady = (job) => {
    const status = String(job?.status || '').trim().toLowerCase();
    return Boolean(job?.download_ready) || status === 'ready';
  };
  const experimentExportProgressPct = (job, fallbackPct = 0) => {
    const status = String(job?.status || '').trim().toLowerCase();
    const ratio = Number(job?.progress_ratio);
    if (Number.isFinite(ratio) && ratio > 0) return Math.max(0, Math.min(100, Math.round(ratio * 100)));
    if (status === 'ready') return 100;
    if (status === 'running') return Math.max(5, fallbackPct);
    if (status === 'queued') return 2;
    return fallbackPct;
  };
  const experimentExportDetail = (job) => {
    if (!job) return 'Idle';
    const message = String(job?.message || '').trim();
    if (message) return message;
    const phase = String(job?.phase || '').trim();
    if (phase) return phase.replace(/_/g, ' ');
    const status = String(job?.status || 'idle').trim();
    return status || 'idle';
  };
  const exportActionLabel = (job, readyLabel = 'Download Export') => {
    const status = String(job?.status || '').trim().toLowerCase();
    if (status === 'queued') return 'Queueing Export...';
    if (status === 'running') return 'Preparing Export...';
    if (experimentExportReady(job)) return readyLabel;
    return 'Prepare Export';
  };

  const startEvaluationWorkflow = async () => {
    if (!predictionComparisonTemplate) {
      pushToast('warning', 'Prediction Comparison template is not available.');
      return;
    }
    if (evaluationSourceMode === 'synthetic' && !String(evaluationDraft.existing_output_dir || evaluationSyntheticDataset?.path || '').trim()) {
      pushToast('warning', 'Select a synthetic dataset for evaluation.');
      return;
    }
    if (evaluationSourceMode === 'history') {
      const fromMs = parseDateTimeLocal(evaluationDraft.history_from);
      const toMs = parseDateTimeLocal(evaluationDraft.history_to);
      if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) {
        pushToast('warning', 'Set a valid history range for evaluation.');
        return;
      }
    }
    const config = {
      ...buildExperimentConfigForTemplate(predictionComparisonTemplate, evaluationDraft),
      control_policy: 'NO_ENERGY_MANAGEMENT',
    };
    await runExperimentTemplate(
      predictionComparisonTemplate,
      config,
      {
        setAsCurrentTemplate: true,
        source: buildQuickWorkflowSource(evaluationDraft),
      },
    );
  };

  const startPolicySimulationWorkflow = async () => {
    if (!policyComparisonTemplate) {
      pushToast('warning', 'Policy Comparison template is not available.');
      return;
    }
    if (policySimulationSourceMode === 'synthetic' && !String(policySimulationDraft.existing_output_dir || policySimulationSyntheticDataset?.path || '').trim()) {
      pushToast('warning', 'Select a synthetic dataset for policy simulation.');
      return;
    }
    if (policySimulationSourceMode === 'history') {
      const fromMs = parseDateTimeLocal(policySimulationDraft.history_from);
      const toMs = parseDateTimeLocal(policySimulationDraft.history_to);
      if (!Number.isFinite(fromMs) || !Number.isFinite(toMs) || toMs <= fromMs) {
        pushToast('warning', 'Set a valid history range for policy simulation.');
        return;
      }
    }
    await runExperimentTemplate(
      policyComparisonTemplate,
      buildExperimentConfigForTemplate(policyComparisonTemplate, policySimulationDraft),
      {
        setAsCurrentTemplate: true,
        source: buildQuickWorkflowSource(policySimulationDraft),
      },
    );
  };

  const handleStartProcess = () => {
    if (emergencyStopActive) {
      pushToast('warning', 'Emergency stop is active. Clear it before starting the process.');
      return;
    }
    const predictorMode = String(process.predictor_mode || system.predictor_mode || 'ONLINE').toUpperCase();
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
    const latest = state.lastExperimentExportStatus;
    const eventKey = String(latest?.event_key || '');
    if (!eventKey || eventKey === lastExperimentExportToastKeyRef.current) return;
    lastExperimentExportToastKeyRef.current = eventKey;
    const status = String(latest?.status || '').trim().toLowerCase();
    const templateName = String(latest?.template_name || latest?.template_id || 'Experiment').trim() || 'Experiment';
    if (status === 'ready') {
      pushToast('info', `${templateName} export is ready to download.`, 7000);
      refreshExperimentRuns();
      return;
    }
    if (status === 'error') {
      pushToast('error', `${templateName} export failed: ${String(latest?.error || 'unknown_error')}`, 8000);
      refreshExperimentRuns();
    }
  }, [state.lastExperimentExportStatus, refreshExperimentRuns]);

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
      naming: isAdminPage ? 'report' : 'standard',
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
      const fallbackName = isAdminPage
        ? `report_export_${new Date(fromMs).toISOString().slice(0, 10)}_${new Date(toMs).toISOString().slice(0, 10)}.zip`
        : `data_export_${new Date(fromMs).toISOString().slice(0, 10)}_${new Date(toMs).toISOString().slice(0, 10)}.zip`;
      const downloadName = getDownloadFilename(response, fallbackName);
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = objectUrl;
      link.download = downloadName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(objectUrl);
      pushToast('info', isAdminPage ? 'Admin export downloaded.' : 'Data export downloaded.');
    } catch (err) {
      pushToast('error', `Export failed: ${String(err?.message || err || 'unknown_error')}`);
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

    const shouldForceOnlineOnMain = !isAdminPage && predictorModeValue !== 'ONLINE';
    if (draft.predictorMode !== undefined || shouldForceOnlineOnMain) {
      const mode = !isAdminPage ? 'ONLINE' : String(draft.predictorMode || '').toUpperCase();
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
          <p className="kicker">{isAdminPage ? 'Factory Twin Admin' : 'Factory Twin'}</p>
          <h1>{isAdminPage ? 'Digital Energy Admin Console' : 'Digital Energy Control Room'}</h1>
          <p className="subtitle">Plant: {PLANT_ID}</p>
        </div>
        <div className="status-row">
          <a href="/" className={`pill-btn ${!isAdminPage ? 'active' : ''}`}>Main</a>
          <a href="/admin" className={`pill-btn ${isAdminPage ? 'active' : ''}`}>Admin</a>
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
          <div className="control-row" style={{ marginBottom: 12 }}>
            <label>Billing Window</label>
            <select value={String(energyWindowMin)} onChange={(e) => setEnergyWindowMin(Math.max(1, Number(e.target.value) || DEFAULT_ENERGY_WINDOW_MIN))}>
              {ENERGY_WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
            <span className="field-note">Energy and cost charts use rolling-window values. Default is 30 min to mirror UK smart-meter half-hour billing intervals.</span>
          </div>
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
              scaleSpec={liveTotalEnergyScale}
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
            <Metric label="Tariff State" value={tariffState} />
            <Metric label="Tariff Source" value={tariffStateSource} />
            <Metric label="Tariff Price" value={f2(tariffRatePerKwh)} unit={`${CURRENCY_CODE}/kWh`} />
            <Metric label="Unit Energy Cost" value={tariffRatePerWh} scaleKind="cost" unitSuffix="/Wh" />
            <Metric label="Cost Rate" value={costRatePerMin} scaleKind="cost" unitSuffix="/min" />
            <Metric label="Total Energy" value={energy.total_energy_wh} scaleKind="energy" scaleSpec={liveTotalEnergyScale} />
            <Metric label={`Energy (${energyWindowLabel})`} value={analyticsRollingEnergyWh} scaleKind="energy" scaleSpec={liveRollingEnergyScale} />
            <Metric label={`Max Demand (${tariffConfig.demandIntervalMin} min avg)`} value={f2(analyticsMaxDemandKw)} unit="kW" />
            <Metric label={`Cost (${energyWindowLabel})`} value={analyticsRollingCost} scaleKind="cost" scaleSpec={liveRollingCostScale} />
            <Metric label="Accum. Energy Cost" value={analyticsAccumulatedEnergyCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Demand Charge" value={analyticsDemandChargeCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Total Cost" value={analyticsAccumulatedCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Energy Cap (Window)" value={energyCapWindowWh} scaleKind="energy" scaleSpec={liveTotalEnergyScale} />
            <Metric label="Goal Window" value={f2(energyGoalWindowMin)} unit="min" />
            <Metric label="Goal Time Left" value={Number.isFinite(energyGoalWindowRemainingSec) ? formatHms(energyGoalWindowRemainingSec) : '-'} />
            <Metric label="Energy Used (Window)" value={energyGoalWindowWh} scaleKind="energy" scaleSpec={liveTotalEnergyScale} />
            <Metric label="Instant Energy Cost Est." value={estimatedTotalCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Instant Window Cost Est." value={estimatedGoalWindowCost} scaleKind="cost" scaleSpec={liveCostScale} />
            <Metric label="Energy Left" value={energyLeftGoalWh == null ? NaN : energyLeftGoalWh} scaleKind="energy" scaleSpec={liveTotalEnergyScale} fallback="-" />
            <Metric label="Energy Used %" value={energyUsedGoalPct == null ? '-' : f2(energyUsedGoalPct)} unit={energyUsedGoalPct == null ? '' : '%'} />
            <Metric label="Goal Time Left %" value={Number.isFinite(energyGoalWindowRemainingPct) ? f2(energyGoalWindowRemainingPct * 100) : '-'} unit={Number.isFinite(energyGoalWindowRemainingPct) ? '%' : ''} />
            <Metric label="Goal Started" value={energyGoalWindowStartedAt || '-'} />
            <Metric label="Goal Ends" value={energyGoalWindowEndsAt || '-'} />
            <Metric label="Goal Reset" value={energyGoalWindowResetReason || '-'} />
            <Metric label="Goal Reset Count" value={Number.isFinite(energyGoalWindowResetCount) ? String(Math.round(energyGoalWindowResetCount)) : '-'} />
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
                <span className="legend-chip"><span className="legend-dot legend-energy" />Rolling Energy {energyWindowLabel} ({liveRollingEnergyScale.unit})</span>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={analyticsRollingEnergyRows}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(liveRollingEnergyScale)} />
                  <Tooltip formatter={makeTooltipFormatter({ rolling_energy_wh: liveRollingEnergyScale })} />
                  <Area type="monotone" dataKey="rolling_energy_wh" name={`Rolling Energy ${energyWindowLabel} (${liveRollingEnergyScale.unit})`} stroke="#ffd166" fill="rgba(255,209,102,0.22)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
            <div className="chart-wrap">
              <div className="chart-static-legend">
                <span className="legend-chip"><span className="legend-dot legend-rf" />Rolling Cost {energyWindowLabel} ({liveRollingCostScale.unit})</span>
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <AreaChart data={analyticsRollingCostRows}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} domain={analyticsCostDomain} tickFormatter={makeAxisTickFormatter(liveRollingCostScale)} />
                  <Tooltip formatter={makeTooltipFormatter({ rolling_cost: liveRollingCostScale })} />
                  <Area type="monotone" dataKey="rolling_cost" name={`Rolling Cost ${energyWindowLabel} (${liveRollingCostScale.unit})`} stroke="#ee6c4d" fill="rgba(238,108,77,0.22)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>
            <div className="chart-wrap">
              <div className="chart-static-legend">
                {liveLoadEnergyKeys.length
                  ? liveLoadEnergyKeys.map((name, index) => (
                    <span key={`load-energy-${name}`} className="legend-chip">
                      <span
                        className="legend-dot"
                        style={{ background: ['#56e39f', '#ffd166', '#ee6c4d', '#72efdd', '#f28482', '#90be6d'][index % 6] }}
                      />
                      {labelLoad(name)} ({liveLoadEnergyScale.unit})
                    </span>
                  ))
                  : <span className="legend-chip">Select at least one load</span>}
              </div>
              <div className="history-series-grid" style={{ marginBottom: 10 }}>
                {liveLoadNames.map((name) => (
                  <label key={`live-load-energy-toggle-${name}`} className="toggle-item">
                    <input
                      type="checkbox"
                      checked={Boolean(liveLoadEnergySelection?.[name])}
                      onChange={(e) => setLiveLoadEnergySelection((prev) => ({ ...prev, [name]: e.target.checked }))}
                    />
                    <span>{labelLoad(name)}</span>
                  </label>
                ))}
              </div>
              <ResponsiveContainer width="100%" height={220}>
                <LineChart data={analyticsLoadEnergyRows}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                  <XAxis dataKey="label" tick={{ fill: '#8ea1cc', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(liveLoadEnergyScale)} />
                  <Tooltip formatter={makeTooltipFormatter(Object.fromEntries(liveLoadEnergyKeys.map((name) => [name, liveLoadEnergyScale])))} />
                  {liveLoadEnergyKeys.map((name, index) => (
                    <Line
                      key={`live-load-energy-line-${name}`}
                      type="monotone"
                      dataKey={name}
                      name={`${labelLoad(name)} (${liveLoadEnergyScale.unit})`}
                      stroke={['#56e39f', '#ffd166', '#ee6c4d', '#72efdd', '#f28482', '#90be6d'][index % 6]}
                      dot={false}
                    />
                  ))}
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>
        </Panel>

        <Panel
          title="3) Predictive Analytics"
          subtitle="Short-term and long-term model outputs with separate display windows per predictor"
          right={<span className={riskClass(state.predictions.short?.risk_level)}>{state.predictions.short?.risk_level || 'LOW'} risk</span>}
        >
          <div className="control-row" style={{ marginBottom: 12 }}>
            <label>EMA Window</label>
            <select value={String(predictorWindowMin.short)} onChange={(e) => setPredictorWindowMin((prev) => ({ ...prev, short: clampPredictorWindowMin(Number(e.target.value) || DEFAULT_PREDICTOR_WINDOW_MIN.short, predictorMinWindowMin.short) }))}>
              {predictorWindowOptions.short.map((option) => <option key={`pred-short-${option.value}`} value={option.value}>{option.label}</option>)}
            </select>
            <label>RF Window</label>
            <select value={String(predictorWindowMin.long_rf)} onChange={(e) => setPredictorWindowMin((prev) => ({ ...prev, long_rf: clampPredictorWindowMin(Number(e.target.value) || DEFAULT_PREDICTOR_WINDOW_MIN.long_rf, predictorMinWindowMin.long_rf) }))}>
              {predictorWindowOptions.long_rf.map((option) => <option key={`pred-rf-${option.value}`} value={option.value}>{option.label}</option>)}
            </select>
            <label>LSTM Window</label>
            <select value={String(predictorWindowMin.long_lstm)} onChange={(e) => setPredictorWindowMin((prev) => ({ ...prev, long_lstm: clampPredictorWindowMin(Number(e.target.value) || DEFAULT_PREDICTOR_WINDOW_MIN.long_lstm, predictorMinWindowMin.long_lstm) }))}>
              {predictorWindowOptions.long_lstm.map((option) => <option key={`pred-lstm-${option.value}`} value={option.value}>{option.label}</option>)}
            </select>
            <span className="field-note">Each predictor window is clamped to at least its forecast horizon.</span>
          </div>
          <div className="prediction-head">
            <div>
              <div className="pred-label">Short-Term</div>
              <div className="pred-value">
                {formatScaledValueOnly(predictionEnergyForWindow(state.predictions.short, predictorWindowMin.short), 'energy', { scaleSpec: predictionCardEnergyScale })} {predictionCardEnergyScale.unit}
              </div>
              <div className="pred-meta">{predictorWindowLabel.short} projected energy</div>
            </div>
            <div>
              <div className="pred-label">Long RF</div>
              <div className="pred-value">
                {formatScaledValueOnly(predictionEnergyForWindow(state.predictions.longRf, predictorWindowMin.long_rf), 'energy', { scaleSpec: predictionCardEnergyScale })} {predictionCardEnergyScale.unit}
              </div>
              <div className="pred-meta">{predictorWindowLabel.long_rf} projected energy</div>
            </div>
            <div>
              <div className="pred-label">Long LSTM</div>
              <div className="pred-value">
                {formatScaledValueOnly(predictionEnergyForWindow(state.predictions.longLstm, predictorWindowMin.long_lstm), 'energy', { scaleSpec: predictionCardEnergyScale })} {predictionCardEnergyScale.unit}
              </div>
              <div className="pred-meta">{predictorWindowLabel.long_lstm} projected energy</div>
            </div>
          </div>

          <div className="chart-grid">
            {predictionChartConfigs.map((series) => (
              <div key={`prediction-chart-${series.key}`} className="chart-wrap">
                <div className="chart-static-legend">
                  <span className="legend-chip">
                    <span className="legend-dot" style={{ background: series.color }} />{series.name} ({series.windowLabel}) ({series.unitScale.unit})
                  </span>
                </div>
                <div className="field-note" style={{ marginBottom: 8 }}>
                  Minimum allowed window: {series.minimumWindowLabel}
                </div>
                {series.rows.length === 0 ? (
                  <div className="empty" style={{ padding: '1rem 0' }}>No prediction history yet.</div>
                ) : (
                  <ResponsiveContainer width="100%" height={220}>
                    <LineChart data={series.rows}>
                      <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.08)" />
                      <XAxis
                        type="number"
                        dataKey="ts"
                        domain={series.rows.length < 2
                          ? [Number(series.rows[0]?.ts) - 60000, Number(series.rows[0]?.ts) + 60000]
                          : ['dataMin', 'dataMax']}
                        tick={{ fill: '#8ea1cc', fontSize: 12 }}
                        tickFormatter={(value) => formatHistoryTick(value, series.spanMs)}
                      />
                      <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(series.unitScale)} />
                      <Tooltip
                        labelFormatter={(value) => formatHistoryTick(value, series.spanMs)}
                        formatter={makeTooltipFormatter({ value: series.unitScale })}
                      />
                      <Line
                        type="monotone"
                        dataKey="value"
                        name={`${series.name} (${series.windowLabel}) (${series.unitScale.unit})`}
                        stroke={series.color}
                        dot={series.rows.length < 2 ? { r: 3, fill: series.color, stroke: series.color } : false}
                      />
                    </LineChart>
                  </ResponsiveContainer>
                )}
              </div>
            ))}
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

        <Panel title="4) Historical Data" subtitle="Stored actual vs predicted rolling energy and rolling cost with interactive range filtering">
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
                {historyExporting ? 'Preparing Export...' : (isAdminPage ? 'Download Report CSVs' : 'Download CSVs')}
              </button>
            </div>
            <div className="control-row">
              <span className="field-note">Dynamic step: {formatHistoryStepLabel(historyStepSec)} (auto from selected range)</span>
              <label>Billing Window</label>
              <select value={String(energyWindowMin)} onChange={(e) => setEnergyWindowMin(Math.max(1, Number(e.target.value) || DEFAULT_ENERGY_WINDOW_MIN))}>
                {ENERGY_WINDOW_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
              </select>
              <span className="field-note">
                {historyExporting
                  ? (isAdminPage ? 'Building report export...' : 'Building data export...')
                  : (historyLoading
                    ? 'Loading history...'
                    : (historyError
                      ? `History error: ${historyError}`
                      : (historyChartRows.length
                        ? `${historyChartRows.length} points • rolling energy ${formatScaledInline(historyRollingEnergyWh, 'energy', { scaleSpec: historyEnergyScale })} • rolling cost ${formatScaledInline(historyRollingCost, 'cost', { scaleSpec: historyCostScale })} • total cost ${formatScaledInline(historyAccumulatedCost, 'cost')} • energy cost ${formatScaledInline(historyAccumulatedEnergyCost, 'cost')} • demand cost ${formatScaledInline(historyDemandChargeCost, 'cost')} • max demand ${formatScaledInline(historyMaxDemandKw * 1000, 'power')} (${tariffConfig.demandIntervalMin} min avg)`
                        : '0 points')))}
              </span>
            </div>
            <div className="control-row">
              <label>EMA History Window</label>
              <select value={String(predictorWindowMin.short)} onChange={(e) => setPredictorWindowMin((prev) => ({ ...prev, short: clampPredictorWindowMin(Number(e.target.value) || DEFAULT_PREDICTOR_WINDOW_MIN.short, predictorMinWindowMin.short) }))}>
                {predictorWindowOptions.short.map((option) => <option key={`hist-pred-short-${option.value}`} value={option.value}>{option.label}</option>)}
              </select>
              <label>RF History Window</label>
              <select value={String(predictorWindowMin.long_rf)} onChange={(e) => setPredictorWindowMin((prev) => ({ ...prev, long_rf: clampPredictorWindowMin(Number(e.target.value) || DEFAULT_PREDICTOR_WINDOW_MIN.long_rf, predictorMinWindowMin.long_rf) }))}>
                {predictorWindowOptions.long_rf.map((option) => <option key={`hist-pred-rf-${option.value}`} value={option.value}>{option.label}</option>)}
              </select>
              <label>LSTM History Window</label>
              <select value={String(predictorWindowMin.long_lstm)} onChange={(e) => setPredictorWindowMin((prev) => ({ ...prev, long_lstm: clampPredictorWindowMin(Number(e.target.value) || DEFAULT_PREDICTOR_WINDOW_MIN.long_lstm, predictorMinWindowMin.long_lstm) }))}>
                {predictorWindowOptions.long_lstm.map((option) => <option key={`hist-pred-lstm-${option.value}`} value={option.value}>{option.label}</option>)}
              </select>
              <span className="field-note">Historical predictor comparisons are split by model so each can keep its own valid window.</span>
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
            {historicalPredictionCharts.filter((chart) => chart.enabled).map((chart) => (
              <div key={`history-prediction-${chart.key}`} className="chart-wrap">
                <div className="chart-static-legend">
                  {historySeries.actual ? <span className="legend-chip"><span className="legend-dot legend-actual" />Actual Energy {chart.windowLabel} ({chart.scale.unit})</span> : null}
                  <span className="legend-chip"><span className="legend-dot" style={{ background: chart.color }} />{chart.name} Energy {chart.windowLabel} ({chart.scale.unit})</span>
                </div>
                <div className="history-chart-scroll">
                  <LineChart width={historyChartWidth} height={260} data={chart.rows} syncId={`history-${chart.key}`}>
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
                    <YAxis tick={{ fill: '#8ea1cc', fontSize: 12 }} tickFormatter={makeAxisTickFormatter(chart.scale)} />
                    <Tooltip formatter={makeTooltipFormatter({
                      actual_window_energy_wh: chart.scale,
                      pred_window_energy_wh: chart.scale,
                    })} />
                    {historySeries.actual ? <Line type="monotone" dataKey="actual_window_energy_wh" name={`Actual Energy ${chart.windowLabel} (${chart.scale.unit})`} stroke="#56e39f" dot={false} strokeWidth={2} /> : null}
                    <Line type="monotone" dataKey="pred_window_energy_wh" name={`${chart.name} Energy ${chart.windowLabel} (${chart.scale.unit})`} stroke={chart.color} dot={false} />
                  </LineChart>
                </div>
              </div>
            ))}
            <div className="chart-wrap">
              <div className="chart-static-legend">
                <span className="legend-chip"><span className="legend-dot legend-rf" />Rolling Cost {energyWindowLabel} ({historyCostScale.unit})</span>
              </div>
              <div className="history-chart-scroll">
                <LineChart width={historyChartWidth} height={260} data={historyRollingCostRows} syncId="history-sync">
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
                  <Tooltip formatter={makeTooltipFormatter({ rolling_cost: historyCostScale })} />
                  <Line type="monotone" dataKey="rolling_cost" name={`Rolling Cost ${energyWindowLabel} (${historyCostScale.unit})`} stroke="#ee6c4d" dot={false} strokeWidth={2} />
                </LineChart>
              </div>
            </div>
          </div>
        </Panel>

        {isAdminPage ? (
          <Panel title="5) Admin Training" subtitle="Pre-system factory baseline generation, saved model management, and report-style exports">
            <div className="cmd-grid">
              <div className="cmd-card">
                <h3>Synthetic Data Generation</h3>
                <div className="row">
                  <small className="field-note">
                    Generate unmanaged factory-baseline telemetry first. Then use that dataset in the separate training section for RF and LSTM bootstrap.
                  </small>
                </div>
                <div className="row">
                  <label>Start</label>
                  <input
                    type="datetime-local"
                    value={syntheticGenerationDraft.start}
                    onChange={(e) => setSyntheticGenerationDraftField('start', e.target.value)}
                    disabled={syntheticTrainingLoading}
                  />
                </div>
                <div className="row">
                  <label>Days</label>
                  <input
                    type="number"
                    min="1"
                    step="1"
                    value={numberInputValue(syntheticGenerationDraft.days, 5, 0)}
                    onChange={(e) => setSyntheticGenerationDraftField('days', e.target.value)}
                    disabled={syntheticTrainingLoading}
                  />
                </div>
                <div className="row">
                  <label>Sample Step (s)</label>
                  <input
                    type="number"
                    min="1"
                    step="1"
                    value={numberInputValue(syntheticGenerationDraft.stepSec, 5, 0)}
                    onChange={(e) => setSyntheticGenerationDraftField('stepSec', e.target.value)}
                    disabled={syntheticTrainingLoading}
                  />
                </div>
                <div className="row">
                  <label>Seed</label>
                  <input
                    type="number"
                    step="1"
                    value={numberInputValue(syntheticGenerationDraft.seed, 42, 0)}
                    onChange={(e) => setSyntheticGenerationDraftField('seed', e.target.value)}
                    disabled={syntheticTrainingLoading}
                  />
                </div>
                <label className="toggle-item">
                  <input
                    type="checkbox"
                    checked={Boolean(syntheticGenerationDraft.overwrite)}
                    onChange={(e) => setSyntheticGenerationDraftField('overwrite', e.target.checked, 'boolean')}
                    disabled={syntheticTrainingLoading}
                  />
                  <span>Allow overwrite if the target synthetic folder already exists</span>
                </label>
                <div className="control-row">
                  <button type="button" onClick={generateSyntheticDataset} disabled={syntheticTrainingLoading}>
                    {syntheticTrainingLoading ? 'Starting Job...' : 'Generate Synthetic Dataset'}
                  </button>
                  <button type="button" className="ghost-btn" onClick={refreshSyntheticDatasets} disabled={syntheticTrainingLoading || syntheticDatasetsLoading}>
                    {syntheticDatasetsLoading ? 'Refreshing...' : 'Refresh Datasets'}
                  </button>
                </div>
              </div>

              <div className="cmd-card">
                <h3>Model Training</h3>
                <div className="row">
                  <small className="field-note">
                    Train RF and LSTM from an existing synthetic dataset or from a selected live-history range. Use this after generation so evaluation can use a separate unseen source.
                  </small>
                </div>
                <div className="row">
                  <label>Training Source</label>
                  <select
                    value={String(syntheticTrainingDraft.sourceMode || 'reuse')}
                    onChange={(e) => setSyntheticTrainingDraftField('sourceMode', e.target.value)}
                    disabled={syntheticTrainingLoading}
                  >
                    <option value="reuse">Existing Synthetic Data</option>
                    <option value="history">Live History Logs</option>
                  </select>
                </div>
                {String(syntheticTrainingDraft.sourceMode || 'reuse') === 'reuse' ? (
                  <>
                    <div className="row">
                      <label>Existing Dataset</label>
                      <select
                        value={String(syntheticTrainingDraft.existingOutputDir || selectedSyntheticDataset?.path || '')}
                        onChange={(e) => setSyntheticTrainingDraftField('existingOutputDir', e.target.value)}
                        disabled={syntheticTrainingLoading || syntheticDatasetsLoading}
                      >
                        {!syntheticDatasets.length ? <option value="">No datasets found</option> : null}
                        {syntheticDatasets.map((item) => (
                          <option key={item.path} value={item.path}>
                            {item.name} ({item.record_count || 0} records, {item.step_sec || '?'}s)
                          </option>
                        ))}
                      </select>
                    </div>
                    {selectedSyntheticDataset ? (
                      <div className="row">
                        <small className="field-note">
                          Selected: {selectedSyntheticDataset.path} • records {selectedSyntheticDataset.record_count || 0} • step {selectedSyntheticDataset.step_sec || '?'}s • profile {selectedSyntheticDataset.synthetic_profile || 'pre_system_factory_baseline'}
                        </small>
                      </div>
                    ) : null}
                  </>
                ) : (
                  <>
                    <div className="row">
                      <small className="field-note">
                        Trains from the real telemetry logs in the backend logger directory. Only records inside the selected UTC range are replayed into RF and LSTM.
                      </small>
                    </div>
                    <div className="row">
                      <label>History From</label>
                      <input
                        type="datetime-local"
                        value={syntheticTrainingDraft.historyFrom}
                        onChange={(e) => setSyntheticTrainingDraftField('historyFrom', e.target.value)}
                        disabled={syntheticTrainingLoading}
                      />
                    </div>
                    <div className="row">
                      <label>History To</label>
                      <input
                        type="datetime-local"
                        value={syntheticTrainingDraft.historyTo}
                        onChange={(e) => setSyntheticTrainingDraftField('historyTo', e.target.value)}
                        disabled={syntheticTrainingLoading}
                      />
                    </div>
                  </>
                )}
                <div className="row">
                  <label>Predictor Mode</label>
                  <select
                    value={String(syntheticTrainingDraft.predictorMode || 'TRAIN_ONLY').toUpperCase()}
                    onChange={(e) => setSyntheticTrainingDraftField('predictorMode', e.target.value)}
                    disabled={syntheticTrainingLoading}
                  >
                    {ADMIN_TRAINING_PREDICTOR_MODE_OPTIONS.map((option) => (
                      <option key={option} value={option}>{option}</option>
                    ))}
                  </select>
                </div>
                <label className="toggle-item">
                  <input
                    type="checkbox"
                    checked={Boolean(syntheticTrainingDraft.deleteSavedModels)}
                    onChange={(e) => setSyntheticTrainingDraftField('deleteSavedModels', e.target.checked, 'boolean')}
                    disabled={syntheticTrainingLoading}
                  />
                  <span>Delete saved models before bootstrap</span>
                </label>
                <div className="control-row">
                  <button type="button" onClick={kickstartSyntheticTraining} disabled={syntheticTrainingLoading}>
                    {syntheticTrainingLoading
                      ? 'Starting Training Job...'
                      : String(syntheticTrainingDraft.sourceMode || 'generate') === 'history'
                        ? 'Train From History Logs'
                        : 'Train From Synthetic Dataset'}
                  </button>
                  <button type="button" className="ghost-btn" onClick={downloadChapter4Export} disabled={historyExporting}>
                    {historyExporting ? 'Preparing Export...' : 'Download Report CSVs'}
                  </button>
                </div>
                <div className="row">
                  <small className="field-note">
                    {syntheticTrainingLoading
                      ? 'Progress: scheduling the backend training job.'
                      : syntheticTrainingBusy
                        ? 'Progress: predictors are bootstrapping/training. Keep this page open and watch the runtime status below.'
                        : 'Progress: idle'}
                  </small>
                </div>
                {(syntheticTrainingJob?.summary || syntheticTrainingResult?.summary) ? (
                  <div className="row">
                    <small className="field-note">
                      {(() => {
                        const summary = syntheticTrainingJob?.summary || syntheticTrainingResult?.summary || {};
                        return `Output: ${summary.output_dir} • records ${summary.records_written} • step ${summary.step_sec}s • profile ${summary.synthetic_profile || 'pre_system_factory_baseline'}`;
                      })()}
                    </small>
                  </div>
                ) : null}
                <div className="cmd-grid">
                  <div className="cmd-card">
                    <h3>Training Job</h3>
                    {!syntheticTrainingJob ? (
                      <small className="field-note">No dataset or training job has started yet.</small>
                    ) : (
                      <>
                        <div className="scene-status-grid">
                          <Metric label="Job ID" value={String(syntheticTrainingJob.job_id || '').slice(0, 8) || '-'} />
                          <Metric label="Phase" value={syntheticJobPhaseLabel(syntheticTrainingJob.phase)} />
                          <Metric label="Records" value={`${Number(syntheticTrainingJob.records_written || 0)}/${Number(syntheticTrainingJob.estimated_records || 0)}`} />
                          <Metric label="Last Update" value={syntheticTrainingJob.updated_at ? predictorStatusTime(syntheticTrainingJob.updated_at) : '-'} />
                        </div>
                        <div className="row">
                          <small className="field-note">
                            {syntheticTrainingJob.train_models === false ? 'Mode: dataset generation only' : 'Mode: model training / bootstrap'}
                          </small>
                        </div>
                        <progress max="100" value={syntheticJobProgressPct(syntheticTrainingJob)} style={{ width: '100%' }} />
                        <div className="row">
                          <small className="field-note">
                            Output: {String(syntheticTrainingJob.output_dir || '-')}
                          </small>
                        </div>
                        {syntheticTrainingJob.error ? (
                          <div className="row">
                            <small className="field-note">Error: {String(syntheticTrainingJob.error)}</small>
                          </div>
                        ) : null}
                      </>
                    )}
                  </div>
                  {[{ key: 'rf', label: 'RF Runtime', status: rfPredictorStatus }, { key: 'lstm', label: 'LSTM Runtime', status: lstmPredictorStatus }].map(({ key, label, status }) => (
                    <div key={key} className="cmd-card">
                      <h3>{label}</h3>
                      {!status ? (
                        <small className="field-note">No runtime status received yet.</small>
                      ) : (
                        <>
                          {!state.predictorStatus?.[key === 'rf' ? 'long_rf' : 'long_lstm'] ? (
                            <div className="row">
                              <small className="field-note">No live predictor-status MQTT message has been received yet. This usually means the predictor service was not redeployed or the bootstrap command did not reach it.</small>
                            </div>
                          ) : null}
                          <div className="scene-status-grid">
                            <Metric label="Phase" value={predictorPhaseLabel(status.phase)} />
                            <Metric label="Model Ready" value={status.model_ready ? 'YES' : 'NO'} />
                            <Metric label="Train Samples" value={Number(status.train_samples || 0)} />
                            <Metric label="Last Update" value={status.updated_at ? new Date(status.updated_at).toLocaleTimeString() : '-'} />
                          </div>
                          <progress max="100" value={predictorProgressPct(status)} style={{ width: '100%' }} />
                          <div className="row">
                            <small className="field-note">
                              {status.bootstrap_active
                                ? `Bootstrap ${Number(status.bootstrap_processed_records || 0)}/${Number(status.bootstrap_total_records || 0)} records`
                                : `Message: ${String(status.message || 'idle')}`}
                            </small>
                          </div>
                          <div className="row">
                            <small className="field-note">
                              Last trained: {predictorStatusTime(status.last_trained_at)} • Last saved: {predictorStatusTime(status.last_saved_at)}
                            </small>
                          </div>
                          {Array.isArray(status.model_meta_warning_fields) && status.model_meta_warning_fields.length ? (
                            <div className="row">
                              <small className="field-note">
                                Loaded model with compatibility warning: {status.model_meta_warning_fields.join(', ')}
                              </small>
                            </div>
                          ) : null}
                          {status.last_error ? (
                            <div className="row">
                              <small className="field-note">Error: {String(status.last_error)}</small>
                            </div>
                          ) : null}
                        </>
                      )}
                    </div>
                  ))}
                </div>
              </div>

              <div className="cmd-card">
                <h3>Saved Models</h3>
                <div className="control-row">
                  <button type="button" className="warn-btn" onClick={() => deleteSavedModels('')} disabled={adminModelsLoading}>
                    Delete All Saved Models
                  </button>
                  <button type="button" className="ghost-btn" onClick={() => deleteSavedModels('rf')} disabled={adminModelsLoading}>
                    Delete RF
                  </button>
                  <button type="button" className="ghost-btn" onClick={() => deleteSavedModels('lstm')} disabled={adminModelsLoading}>
                    Delete LSTM
                  </button>
                </div>
                <ul className="stack-list">
                  {adminModelsLoading ? <li className="empty">Loading saved models...</li> : null}
                  {!adminModelsLoading && adminModels.length === 0 ? <li className="empty">No saved model files reported.</li> : null}
                  {!adminModelsLoading && adminModels.map((item) => (
                    <li key={`${item.model_type}-${item.name}`}>
                      <span className="mono">{item.name}</span> — {String(item.model_type || '').toUpperCase()} — {item.exists ? `${item.size_bytes} bytes` : 'missing'}
                      <div><small className="field-note">{item.path}</small></div>
                    </li>
                  ))}
                </ul>
              </div>
            </div>
          </Panel>
        ) : null}

        {isAdminPage ? (
          <Panel title="6) Evaluation & Simulation" subtitle="One-click model evaluation and same-scenario policy comparison, with export and progress tracking">
            <div className="cmd-grid">
              <div className="cmd-card">
                <h3>Model Evaluation</h3>
                <div className="row">
                  <small className="field-note">
                    Runs the `prediction_comparison` workflow and exports EMA vs actual, RF vs actual, LSTM vs actual, MAE/RMSE comparison, plus model analytics such as feature importance and correlation heatmap.
                  </small>
                </div>
                <div className="row">
                  <label>Source</label>
                  <select
                    value={evaluationSourceMode}
                    onChange={(e) => setQuickDraftField(setEvaluationDraft, 'source_mode', e.target.value)}
                    disabled={experimentExecuting}
                  >
                    <option value="live">Live Hardware</option>
                    <option value="synthetic">Synthetic Dataset</option>
                    <option value="history">History Logs</option>
                  </select>
                </div>
                {evaluationSourceMode === 'synthetic' ? (
                  <>
                    <div className="row">
                      <label>Synthetic Dataset</label>
                      <select
                        value={String(evaluationDraft.existing_output_dir || evaluationSyntheticDataset?.path || '')}
                        onChange={(e) => setQuickDraftField(setEvaluationDraft, 'existing_output_dir', e.target.value)}
                        disabled={experimentExecuting || syntheticDatasetsLoading}
                      >
                        {!syntheticDatasets.length ? <option value="">No datasets found</option> : null}
                        {syntheticDatasets.map((item) => (
                          <option key={item.path} value={item.path}>
                            {item.name} ({item.record_count} records)
                          </option>
                        ))}
                      </select>
                    </div>
                    <div className="row">
                      <small className="field-note">
                        Synthetic-source evaluation exports from the selected dataset window. No live commands are sent to the ESP32 in this mode.
                      </small>
                    </div>
                  </>
                ) : null}
                {evaluationSourceMode === 'history' ? (
                  <>
                    <div className="row">
                      <label>History From</label>
                      <input
                        type="datetime-local"
                        value={evaluationDraft.history_from || historyDraft.from}
                        onChange={(e) => setQuickDraftField(setEvaluationDraft, 'history_from', e.target.value)}
                        disabled={experimentExecuting}
                      />
                    </div>
                    <div className="row">
                      <label>History To</label>
                      <input
                        type="datetime-local"
                        value={evaluationDraft.history_to || historyDraft.to}
                        onChange={(e) => setQuickDraftField(setEvaluationDraft, 'history_to', e.target.value)}
                        disabled={experimentExecuting}
                      />
                    </div>
                    <div className="row">
                      <small className="field-note">
                        History-source evaluation exports from the selected live-log range. No live commands are sent to the ESP32 in this mode.
                      </small>
                    </div>
                  </>
                ) : null}
                <div className="row">
                  <small className="field-note">
                    Control policy is fixed to `NO_ENERGY_MANAGEMENT` for model evaluation so the comparison stays focused on prediction accuracy, not policy response.
                  </small>
                </div>
                <div className="row">
                  <label>Run Duration (s)</label>
                  <input
                    type="number"
                    min="30"
                    step="1"
                    value={numberInputValue(evaluationDraft.run_duration_sec, predictionComparisonTemplate?.defaults?.run_duration_sec, 0)}
                    onChange={(e) => setQuickDraftField(setEvaluationDraft, 'run_duration_sec', e.target.value)}
                    disabled={experimentExecuting || evaluationSourceMode !== 'live'}
                  />
                </div>
                <div className="row">
                  <label>Predictor Mode</label>
                  <select
                    value={String(evaluationDraft.predictor_mode ?? predictionComparisonTemplate?.defaults?.predictor_mode ?? 'ONLINE').toUpperCase()}
                    onChange={(e) => setQuickDraftField(setEvaluationDraft, 'predictor_mode', e.target.value)}
                    disabled={experimentExecuting || evaluationSourceMode !== 'live'}
                  >
                    {PREDICTOR_MODE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                  </select>
                </div>
                <label className="toggle-item">
                  <input
                    type="checkbox"
                    checked={Boolean(evaluationDraft.start_process ?? predictionComparisonTemplate?.defaults?.start_process)}
                    onChange={(e) => setQuickDraftField(setEvaluationDraft, 'start_process', e.target.checked, 'boolean')}
                    disabled={experimentExecuting || evaluationSourceMode !== 'live'}
                  />
                  <span>Start process</span>
                </label>
                <label className="toggle-item">
                  <input
                    type="checkbox"
                    checked={Boolean(evaluationDraft.restart_process ?? predictionComparisonTemplate?.defaults?.restart_process)}
                    onChange={(e) => setQuickDraftField(setEvaluationDraft, 'restart_process', e.target.checked, 'boolean')}
                    disabled={experimentExecuting || evaluationSourceMode !== 'live'}
                  />
                  <span>Restart process before run</span>
                </label>
                <div className="control-row">
                  <button type="button" onClick={startEvaluationWorkflow} disabled={experimentExecuting || experimentLoading || !predictionComparisonTemplate}>
                    {experimentExecuting && experimentRun?.template_id === 'prediction_comparison'
                      ? 'Running Evaluation...'
                      : evaluationUsesOfflineSource
                        ? 'Prepare Evaluation Export'
                        : 'Run Model Evaluation'}
                  </button>
                  <button
                    type="button"
                    className="ghost-btn"
                    onClick={() => downloadExperimentRunExport(evaluationRun)}
                    disabled={!evaluationRun?.can_export || experimentExportingRunId === String(evaluationRun?.id || '')}
                  >
                    {experimentExportingRunId === String(evaluationRun?.id || '')
                      ? 'Preparing Export...'
                      : exportActionLabel(evaluationExportJob, 'Download Evaluation Export')}
                  </button>
                </div>
                <div className="row">
                  <small className="field-note">
                    Status: {experimentStatusLabel(evaluationRun?.status)} • {evaluationRun ? `${evaluationRun.completed_steps || 0}/${evaluationRun.step_count || 0}` : '0/0'}
                  </small>
                </div>
                <div className="row">
                  <small className="field-note">
                    Export: {String(evaluationExportJob?.status || 'idle').toUpperCase()} {experimentExportReady(evaluationExportJob) ? '• Ready to download' : ''}
                  </small>
                </div>
                <div className="row">
                  <small className="field-note">
                    Export detail: {experimentExportDetail(evaluationExportJob)}
                  </small>
                </div>
                <div className="row">
                  <small className="field-note">
                    Current step: {experimentRun?.template_id === 'prediction_comparison' ? (experimentCurrentStep?.label || '-') : '-'}
                  </small>
                </div>
                {syntheticTrainingBusy ? (
                  <div className="row">
                    <small className="field-note">
                      Bootstrap/training is still running. Evaluation will use live telemetry, but RF/LSTM may still be updating from the selected training source.
                    </small>
                  </div>
                ) : null}
                {evaluationSimulationReadinessIncomplete ? (
                  <div className="row">
                    <small className="field-note">
                      RF/LSTM readiness is incomplete. The run will proceed and the export will contain whatever prediction/evaluation data is available in that window.
                    </small>
                  </div>
                ) : null}
                {evaluationUsesOfflineSource ? (
                  <div className="row">
                    <small className="field-note">
                      Offline-source evaluation replays the selected synthetic/history logs through the saved predictors during export generation. The live ESP32 is not used.
                    </small>
                  </div>
                ) : null}
                <progress
                  max="100"
                  value={experimentExportReady(evaluationExportJob) || ['queued', 'running'].includes(String(evaluationExportJob?.status || '').toLowerCase())
                    ? experimentExportProgressPct(evaluationExportJob, evaluationProgressPct)
                    : evaluationProgressPct}
                  style={{ width: '100%' }}
                />
              </div>

              <div className="cmd-card">
                <h3>Policy Simulation</h3>
                <div className="row">
                  <small className="field-note">
                    Runs the `policy_comparison` workflow with the same scenario across policies. Export includes the policy comparison table, Figures 4.18-4.20, accumulated cost, and supporting plots.
                  </small>
                </div>
                <div className="row">
                  <label>Source</label>
                  <select
                    value={policySimulationSourceMode}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'source_mode', e.target.value)}
                    disabled={experimentExecuting}
                  >
                    <option value="live">Live Hardware</option>
                    <option value="synthetic">Synthetic Dataset</option>
                    <option value="history">History Logs</option>
                  </select>
                </div>
                {policySimulationSourceMode === 'synthetic' ? (
                  <>
                    <div className="row">
                      <label>Synthetic Dataset</label>
                      <select
                        value={String(policySimulationDraft.existing_output_dir || policySimulationSyntheticDataset?.path || '')}
                        onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'existing_output_dir', e.target.value)}
                        disabled={experimentExecuting || syntheticDatasetsLoading}
                      >
                        {!syntheticDatasets.length ? <option value="">No datasets found</option> : null}
                        {syntheticDatasets.map((item) => (
                          <option key={item.path} value={item.path}>
                            {item.name} ({item.record_count} records)
                          </option>
                        ))}
                      </select>
                    </div>
                    <div className="row">
                      <small className="field-note">
                        Synthetic-source policy comparison runs a backend offline replay across `NO_ENERGY_MANAGEMENT`, `RULE_ONLY`, `HYBRID`, and `AI_PREFERRED` using the selected dataset. No live commands are sent to the ESP32 in this mode.
                      </small>
                    </div>
                  </>
                ) : null}
                {policySimulationSourceMode === 'history' ? (
                  <>
                    <div className="row">
                      <label>History From</label>
                      <input
                        type="datetime-local"
                        value={policySimulationDraft.history_from || historyDraft.from}
                        onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'history_from', e.target.value)}
                        disabled={experimentExecuting}
                      />
                    </div>
                    <div className="row">
                      <label>History To</label>
                      <input
                        type="datetime-local"
                        value={policySimulationDraft.history_to || historyDraft.to}
                        onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'history_to', e.target.value)}
                        disabled={experimentExecuting}
                      />
                    </div>
                    <div className="row">
                      <small className="field-note">
                        History-source policy comparison runs a backend offline replay across the configured policies using the selected log range. No live commands are sent to the ESP32 in this mode.
                      </small>
                    </div>
                  </>
                ) : null}
                <div className="row">
                  <label>Max Power Threshold (W)</label>
                  <input
                    type="number"
                    min="0"
                    step="0.01"
                    value={numberInputValue(policySimulationDraft.max_total_power_w, policyComparisonTemplate?.defaults?.max_total_power_w, 2)}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'max_total_power_w', e.target.value)}
                    disabled={experimentExecuting}
                  />
                </div>
                <div className="row">
                  <label>Energy Goal Window (Wh)</label>
                  <input
                    type="number"
                    min="0"
                    step="0.01"
                    value={numberInputValue(policySimulationDraft.energy_goal_wh, policyComparisonTemplate?.defaults?.energy_goal_wh, 2)}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'energy_goal_wh', e.target.value)}
                    disabled={experimentExecuting}
                  />
                </div>
                <div className="row">
                  <small className="field-note">
                    For offline sources, leaving Max Power Threshold empty auto-derives it from the unmanaged baseline peak. Set it explicitly if you want a tighter comparison, e.g. `8 W`.
                  </small>
                </div>
                <div className="row">
                  <label>Duration per Policy (s)</label>
                  <input
                    type="number"
                    min="30"
                    step="1"
                    value={numberInputValue(policySimulationDraft.policy_duration_sec, policyComparisonTemplate?.defaults?.policy_duration_sec, 0)}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'policy_duration_sec', e.target.value)}
                    disabled={experimentExecuting || policySimulationSourceMode !== 'live'}
                  />
                </div>
                <div className="row">
                  <label>Cooldown (s)</label>
                  <input
                    type="number"
                    min="0"
                    step="1"
                    value={numberInputValue(policySimulationDraft.cooldown_sec, policyComparisonTemplate?.defaults?.cooldown_sec, 0)}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'cooldown_sec', e.target.value)}
                    disabled={experimentExecuting || policySimulationSourceMode !== 'live'}
                  />
                </div>
                <div className="row">
                  <label>Predictor Mode</label>
                  <select
                    value={String(policySimulationDraft.predictor_mode ?? policyComparisonTemplate?.defaults?.predictor_mode ?? 'ONLINE').toUpperCase()}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'predictor_mode', e.target.value)}
                    disabled={experimentExecuting || policySimulationSourceMode !== 'live'}
                  >
                    {PREDICTOR_MODE_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
                  </select>
                </div>
                <label className="toggle-item">
                  <input
                    type="checkbox"
                    checked={Boolean(policySimulationDraft.start_process ?? policyComparisonTemplate?.defaults?.start_process)}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'start_process', e.target.checked, 'boolean')}
                    disabled={experimentExecuting || policySimulationSourceMode !== 'live'}
                  />
                  <span>Start process for each policy</span>
                </label>
                <label className="toggle-item">
                  <input
                    type="checkbox"
                    checked={Boolean(policySimulationDraft.restart_process_each_policy ?? policyComparisonTemplate?.defaults?.restart_process_each_policy)}
                    onChange={(e) => setQuickDraftField(setPolicySimulationDraft, 'restart_process_each_policy', e.target.checked, 'boolean')}
                    disabled={experimentExecuting || policySimulationSourceMode !== 'live'}
                  />
                  <span>Restart process each policy</span>
                </label>
                <div className="control-row">
                  <button type="button" onClick={startPolicySimulationWorkflow} disabled={experimentExecuting || experimentLoading || !policyComparisonTemplate}>
                    {experimentExecuting && experimentRun?.template_id === 'policy_comparison'
                      ? 'Running Simulation...'
                      : policySimulationUsesOfflineSource
                        ? 'Prepare Policy Export'
                        : 'Run Policy Simulation'}
                  </button>
                  <button
                    type="button"
                    className="ghost-btn"
                    onClick={() => downloadExperimentRunExport(policySimulationRun)}
                    disabled={!policySimulationRun?.can_export || experimentExportingRunId === String(policySimulationRun?.id || '')}
                  >
                    {experimentExportingRunId === String(policySimulationRun?.id || '')
                      ? 'Preparing Export...'
                      : exportActionLabel(policySimulationExportJob, 'Download Policy Export')}
                  </button>
                </div>
                <div className="row">
                  <small className="field-note">
                    Status: {experimentStatusLabel(policySimulationRun?.status)} • {policySimulationRun ? `${policySimulationRun.completed_steps || 0}/${policySimulationRun.step_count || 0}` : '0/0'}
                  </small>
                </div>
                <div className="row">
                  <small className="field-note">
                    Export: {String(policySimulationExportJob?.status || 'idle').toUpperCase()} {experimentExportReady(policySimulationExportJob) ? '• Ready to download' : ''}
                  </small>
                </div>
                <div className="row">
                  <small className="field-note">
                    Export detail: {experimentExportDetail(policySimulationExportJob)}
                  </small>
                </div>
                <div className="row">
                  <small className="field-note">
                    Current step: {experimentRun?.template_id === 'policy_comparison' ? (experimentCurrentStep?.label || '-') : '-'}
                  </small>
                </div>
                {syntheticTrainingBusy ? (
                  <div className="row">
                    <small className="field-note">
                      Bootstrap/training is still running. Policy simulation will use live telemetry, but RF/LSTM may still be updating from the selected training source.
                    </small>
                  </div>
                ) : null}
                {evaluationSimulationReadinessIncomplete ? (
                  <div className="row">
                    <small className="field-note">
                      RF/LSTM readiness is incomplete. The run will proceed and the export will contain whatever prediction/policy data is available in that window.
                    </small>
                  </div>
                ) : null}
                {policySimulationUsesOfflineSource ? (
                  <div className="row">
                    <small className="field-note">
                      Offline-source mode prepares an export from existing logs only. It does not run a fresh replay or live hardware scenario.
                    </small>
                  </div>
                ) : null}
                <progress
                  max="100"
                  value={experimentExportReady(policySimulationExportJob) || ['queued', 'running'].includes(String(policySimulationExportJob?.status || '').toLowerCase())
                    ? experimentExportProgressPct(policySimulationExportJob, policySimulationProgressPct)
                    : policySimulationProgressPct}
                  style={{ width: '100%' }}
                />
              </div>
            </div>
          </Panel>
        ) : null}

        <Panel title={isAdminPage ? '7) Controls' : '5) Controls'} subtitle="System control, production-goal process, and command acknowledgements">
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
                {isAdminPage ? (
                  <select
                    value={String(systemDraft.predictorMode ?? predictorModeValue ?? '').toUpperCase()}
                    onChange={(e) => setSystemDraftField('predictorMode', e.target.value)}
                  >
                    <option value="" disabled>Select</option>
                    {PREDICTOR_MODE_OPTIONS.map((o) => <option key={o}>{o}</option>)}
                  </select>
                ) : (
                  <small className="field-note">Main view keeps long predictors in ONLINE mode. Use `/admin` for training mode changes.</small>
                )}
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

        {isAdminPage ? (
        <Panel title="8) Automated Experiments" subtitle="Backend-tracked run plans with frontend-driven MQTT execution and export by recorded timestamps">
          <div className="cmd-grid">
            <div className="cmd-card">
              <h3>Experiment Runner</h3>
              <div className="row">
                <label>Template</label>
                <select
                  value={selectedExperimentTemplate?.id || ''}
                  onChange={(e) => setExperimentTemplateId(e.target.value)}
                  disabled={experimentExecuting}
                >
                  {experimentTemplates.length === 0 ? <option value="">No templates available</option> : null}
                  {experimentTemplates.map((template) => (
                    <option key={template.id} value={template.id}>{template.name}</option>
                  ))}
                </select>
              </div>
              {selectedExperimentTemplate ? (
                <>
                  <div className="row">
                    <small className="field-note">{selectedExperimentTemplate.description}</small>
                  </div>
                  {(selectedExperimentTemplate.fields || []).map((field) => (
                    <div className="row" key={field.name}>
                      <label>{field.label}</label>
                      {field.type === 'boolean' ? (
                        <input
                          type="checkbox"
                          checked={Boolean(experimentDraft?.[field.name])}
                          onChange={(e) => setExperimentDraftField(field.name, e.target.checked, 'boolean')}
                          disabled={experimentExecuting}
                        />
                      ) : field.type === 'select' ? (
                        <select
                          value={String(experimentDraft?.[field.name] ?? selectedExperimentTemplate.defaults?.[field.name] ?? '')}
                          onChange={(e) => setExperimentDraftField(field.name, e.target.value)}
                          disabled={experimentExecuting}
                        >
                          {(field.options || []).map((option) => (
                            <option key={option} value={option}>{option}</option>
                          ))}
                        </select>
                      ) : (
                        <input
                          type="number"
                          min={field.min}
                          step={field.step || 'any'}
                          value={numberInputValue(experimentDraft?.[field.name], selectedExperimentTemplate.defaults?.[field.name], 2)}
                          onChange={(e) => setExperimentDraftField(field.name, e.target.value)}
                          placeholder={field.optional ? 'Optional' : undefined}
                          disabled={experimentExecuting}
                        />
                      )}
                    </div>
                  ))}
                  {(selectedExperimentTemplate.notes || []).length ? (
                    <div className="row">
                      <small className="field-note">
                        {(selectedExperimentTemplate.notes || []).join(' ')}
                      </small>
                    </div>
                  ) : null}
                  {(selectedExperimentTemplate.recommended_outputs || []).length ? (
                    <div className="row">
                      <small className="field-note">
                        Outputs: {(selectedExperimentTemplate.recommended_outputs || []).join(', ')}
                      </small>
                    </div>
                  ) : null}
                </>
              ) : (
                <div className="row">
                  <small className="field-note">History API templates are unavailable.</small>
                </div>
              )}
              <div className="control-row">
                <button type="button" onClick={startAutomatedExperiment} disabled={experimentExecuting || experimentLoading || !selectedExperimentTemplate}>
                  {experimentExecuting ? 'Running Experiment...' : 'Start Automated Run'}
                </button>
                <button type="button" className="warn-btn" onClick={cancelAutomatedExperiment} disabled={!experimentExecuting}>
                  Cancel Run
                </button>
                <button
                  type="button"
                  className="ghost-btn"
                  onClick={() => downloadExperimentRunExport(experimentRun)}
                  disabled={!experimentRun?.can_export || experimentExportingRunId === String(experimentRun?.id || '')}
                >
                  {experimentExportingRunId === String(experimentRun?.id || '')
                    ? 'Preparing Export...'
                    : exportActionLabel(currentExperimentExportJob, 'Download Current Export')}
                </button>
              </div>
            </div>

            <div className="cmd-card">
              <h3>Run Status</h3>
              <div className="scene-status-grid">
                <Metric label="Run ID" value={experimentRun?.id || '-'} />
                <Metric label="Template" value={experimentRun?.template_name || '-'} />
                <Metric label="Status" value={experimentStatusLabel(experimentRun?.status)} />
                <Metric label="Progress" value={experimentRun ? `${experimentRun.completed_steps || 0}/${experimentRun.step_count || 0}` : '-'} />
              </div>
              <div className="row">
                <label>Current Step</label>
                <input value={experimentCurrentStep?.label || '-'} readOnly />
              </div>
              <div className="row">
                <small className="field-note">
                  Created: {experimentEventTime(experimentRun?.created_at)} | Started: {experimentEventTime(experimentRun?.started_at)} | Finished: {experimentEventTime(experimentRun?.finished_at)}
                </small>
              </div>
              <h3 style={{ marginTop: 20 }}>Recent Runs</h3>
              <ul className="stack-list">
                {experimentRuns.length === 0 ? <li className="empty">No automated runs recorded yet.</li> : null}
                {experimentRuns.map((run) => (
                  <li key={run.id}>
                    <div>
                      <strong>{run.template_name || run.template_id}</strong>{' '}
                      <span className="mono">{run.id}</span>{' '}
                      <span>{experimentStatusLabel(run.status)}</span>{' '}
                      <span>{run.completed_steps || 0}/{run.step_count || 0}</span>
                    </div>
                    <div className="control-row" style={{ marginTop: 8 }}>
                      <button type="button" className="ghost-btn" onClick={() => setExperimentRun(run)}>
                        Load
                      </button>
                      <button type="button" className="ghost-btn" onClick={() => downloadExperimentRunExport(run)} disabled={!run.can_export}>
                        {exportActionLabel(resolveExperimentExportJob(run), 'Download Export')}
                      </button>
                    </div>
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </Panel>
        ) : null}
      </main>

      {state.lastError ? <div className="error-banner">{state.lastError}</div> : null}
    </div>
  );
}
