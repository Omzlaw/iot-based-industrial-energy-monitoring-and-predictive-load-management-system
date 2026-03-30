import mqtt from 'mqtt';

const MAX_POINTS = 120;

const clamp = (v, min, max) => Math.max(min, Math.min(max, v));
const toNumber = (v, fallback = 0) => {
  const num = Number(v);
  return Number.isFinite(num) ? num : fallback;
};

const toEpochMs = (value) => {
  const now = Date.now();
  const minValidEpochMs = Date.UTC(2000, 0, 1);
  const maxValidEpochMs = now + 1000 * 60 * 60 * 24 * 30; // 30 days ahead guard
  if (typeof value === 'number') {
    const ms = value < 1e11 ? value * 1000 : value;
    return ms >= minValidEpochMs && ms <= maxValidEpochMs ? ms : now;
  }
  if (typeof value === 'string') {
    const t = Date.parse(value);
    if (!Number.isNaN(t) && t >= minValidEpochMs && t <= maxValidEpochMs) return t;
  }
  return now;
};

export const createInitialState = () => ({
  connection: 'idle',
  lastError: '',
  telemetryAt: null,
  predictionAt: null,
  ackAt: null,
  processEnergyStartWh: null,
  processEnergyWh: 0,
  processLastEnergyWh: 0,
  system: {},
  environment: {},
  energy: {},
  evaluation: {},
  loads: {},
  process: {},
  predictions: {
    short: null,
    longRf: null,
    longLstm: null,
  },
  suggestions: [],
  ackFeed: [],
  powerHistory: [],
  predictionPowerHistory: [],
  predictionEnergyHistory: [],
});

const push = (arr, point, max = MAX_POINTS) => {
  const next = [...arr, point];
  if (point && typeof point.ts === 'number') {
    next.sort((a, b) => (Number(a?.ts) || 0) - (Number(b?.ts) || 0));
  }
  if (next.length <= max) return next;
  return next.slice(next.length - max);
};

const mergeSuggestions = (predictions) => {
  const out = [];
  const map = [
    ['short', predictions.short],
    ['long_rf', predictions.longRf],
    ['long_lstm', predictions.longLstm],
  ];

  for (const [source, p] of map) {
    if (!p || !Array.isArray(p.suggestions)) continue;
    for (const s of p.suggestions) {
      out.push({ ...s, source, risk_level: p.risk_level || 'LOW' });
    }
  }
  return out.slice(0, 12);
};

export const applyTelemetry = (prev, payload) => {
  const system = payload?.system || {};
  if (!Object.keys(system).length) return prev;

  const energy = payload?.energy || {};
  const process = payload?.process || {};
  const totalEnergyWh = toNumber(energy.total_energy_wh);
  const ts = toEpochMs(payload?.timestamp || system.timestamp);

  const point = {
    ts,
    label: new Date(ts).toLocaleTimeString(),
    power_w: toNumber(system.total_power_w),
    current_a: toNumber(system.total_current_a),
    total_energy_wh: toNumber(energy.total_energy_wh),
    max_power_w: toNumber(system.MAX_TOTAL_POWER_W),
  };

  const processEnabled = Boolean(process.enabled);
  const prevProcessEnabled = Boolean(prev.process?.enabled);
  let processEnergyStartWh = prev.processEnergyStartWh;
  let processEnergyWh = prev.processEnergyWh;
  let processLastEnergyWh = prev.processLastEnergyWh;

  if (processEnabled && !prevProcessEnabled) {
    processEnergyStartWh = totalEnergyWh;
  }

  if (processEnabled) {
    if (!Number.isFinite(processEnergyStartWh)) processEnergyStartWh = totalEnergyWh;
    processEnergyWh = Math.max(0, totalEnergyWh - processEnergyStartWh);
  } else {
    if (prevProcessEnabled) processLastEnergyWh = prev.processEnergyWh;
    processEnergyWh = 0;
    processEnergyStartWh = null;
  }

  return {
    ...prev,
    telemetryAt: ts,
    system,
    environment: payload?.environment || {},
    energy,
    evaluation: payload?.evaluation || {},
    loads: payload?.loads || {},
    process,
    processEnergyStartWh,
    processEnergyWh,
    processLastEnergyWh,
    powerHistory: push(prev.powerHistory, point),
  };
};

export const applyPrediction = (prev, payload, model) => {
  const tsBase = toEpochMs(payload?.timestamp);
  const horizonSec = toNumber(payload?.horizon_sec);
  const targetTs = tsBase + horizonSec * 1000;
  const modelKey = model === 'short' ? 'short' : model === 'long_lstm' ? 'longLstm' : 'longRf';
  const predictedEnergyForChart = toNumber(
    payload?.predicted_total_energy_wh ?? payload?.predicted_energy_wh
  );

  const nextPredictions = {
    ...prev.predictions,
    [modelKey]: payload,
  };

  const powerEntry = {
    ts: targetTs,
    label: new Date(targetTs).toLocaleTimeString(),
    short: model === 'short' ? toNumber(payload?.predicted_power_w) : null,
    long_rf: model === 'long_rf' ? toNumber(payload?.predicted_power_w) : null,
    long_lstm: model === 'long_lstm' ? toNumber(payload?.predicted_power_w) : null,
  };

  const energyEntry = {
    ts: targetTs,
    label: new Date(targetTs).toLocaleTimeString(),
    short: model === 'short' ? predictedEnergyForChart : null,
    long_rf: model === 'long_rf' ? predictedEnergyForChart : null,
    long_lstm: model === 'long_lstm' ? predictedEnergyForChart : null,
  };

  return {
    ...prev,
    predictionAt: Date.now(),
    predictions: nextPredictions,
    suggestions: mergeSuggestions(nextPredictions),
    predictionPowerHistory: push(prev.predictionPowerHistory, powerEntry),
    predictionEnergyHistory: push(prev.predictionEnergyHistory, energyEntry),
  };
};

export const applyAck = (prev, payload) => {
  const ts = Date.now();
  return {
    ...prev,
    ackAt: ts,
    ackFeed: push(prev.ackFeed, {
      ts,
      label: new Date(ts).toLocaleTimeString(),
      payload,
    }, 25),
  };
};

export const buildTopicSet = (plantId) => ({
  telemetry: `dt/${plantId}/telemetry`,
  shortPred: `dt/${plantId}/prediction`,
  longPred: `dt/${plantId}/prediction_long`,
  longPredLstm: `dt/${plantId}/prediction_long_lstm`,
  predictorCmd: `dt/${plantId}/predictor_cmd`,
  predictorLongCmd: `dt/${plantId}/predictor_long_cmd`,
  predictorLongLstmCmd: `dt/${plantId}/predictor_long_lstm_cmd`,
  ack: `dt/${plantId}/cmd_ack`,
  cmd: `dt/${plantId}/cmd`,
});

export const createTwinClient = ({ brokerUrl, username, password, plantId, onState }) => {
  const topics = buildTopicSet(plantId);
  let state = createInitialState();

  const emit = (patcher) => {
    state = typeof patcher === 'function' ? patcher(state) : { ...state, ...patcher };
    onState(state);
  };

  const client = mqtt.connect(brokerUrl, {
    username,
    password,
    clean: true,
    reconnectPeriod: 2000,
    connectTimeout: 10000,
    clientId: `react-ui-${plantId}-${Math.random().toString(16).slice(2, 8)}`,
  });

  client.on('connect', () => {
    emit({ connection: 'connected', lastError: '' });
    client.subscribe([topics.telemetry, topics.shortPred, topics.longPred, topics.longPredLstm, topics.ack], { qos: 0 });
  });

  client.on('reconnect', () => emit({ connection: 'reconnecting' }));
  client.on('offline', () => emit({ connection: 'offline' }));
  client.on('error', (err) => emit({ connection: 'error', lastError: err?.message || 'Unknown MQTT error' }));

  client.on('message', (topic, raw) => {
    let payload;
    try {
      payload = JSON.parse(String(raw));
    } catch {
      payload = { raw: String(raw) };
    }

    if (topic === topics.telemetry) {
      emit((prev) => applyTelemetry(prev, payload));
      return;
    }
    if (topic === topics.shortPred) {
      emit((prev) => applyPrediction(prev, payload, 'short'));
      return;
    }
    if (topic === topics.longPred) {
      emit((prev) => applyPrediction(prev, payload, 'long_rf'));
      return;
    }
    if (topic === topics.longPredLstm) {
      emit((prev) => applyPrediction(prev, payload, 'long_lstm'));
      return;
    }
    if (topic === topics.ack) {
      emit((prev) => applyAck(prev, payload));
    }
  });

  return {
    topics,
    disconnect: () => {
      client.end(true);
    },
    publish: (payload) => {
      if (!client.connected) return false;
      client.publish(topics.cmd, JSON.stringify(payload), { qos: 0 });
      emit((prev) => applyAck(prev, { local: true, queued: payload }));
      return true;
    },
    publishTopic: (topic, payload) => {
      if (!client.connected) return false;
      client.publish(topic, JSON.stringify(payload), { qos: 0 });
      emit((prev) => applyAck(prev, { local: true, topic, queued: payload }));
      return true;
    },
    getState: () => state,
  };
};

export const createDemoTick = () => {
  let t = 0;

  return () => {
    t += 1;

    const power = 38 + 8 * Math.sin(t / 5) + 5 * Math.cos(t / 7);
    const maxPower = 50;
    const energyGoalWh = 100;
    const energyGoalWindowMin = 1440;
    const temp = 24 + 2 * Math.sin(t / 10);
    const humidity = 55 + 6 * Math.cos(t / 8);

    const telemetry = {
      timestamp: new Date().toISOString(),
      system: {
        mode: 'PROCESS',
        control_policy: 'AI_PREFERRED',
        season: 'N/A',
        tariff_state: t % 20 > 12 ? 'PEAK' : 'OFF_PEAK',
        peak_event: power > 45,
        supply_v: 11.8 + Math.sin(t / 6) * 0.2,
        supply_v_raw: 11.8 + Math.sin(t / 6) * 0.2,
        total_power_w: clamp(power, 8, 75),
        total_current_a: clamp(power / 12, 0, 10),
        MAX_TOTAL_POWER_W: maxPower,
        VOLTAGE_INJECTION_MODE: 'NONE',
        VOLTAGE_INJECTION_MAGNITUDE_V: 0,
        VOLTAGE_INJECTION_ACTIVE: false,
        UNDERVOLTAGE_THRESHOLD_V: 10.5,
        UNDERVOLTAGE_TRIP_DELAY_MS: 500,
        UNDERVOLTAGE_RESTORE_MARGIN_V: 0.3,
        UNDERVOLTAGE_ACTIVE: false,
        OVERVOLTAGE_THRESHOLD_V: 13.2,
        OVERVOLTAGE_TRIP_DELAY_MS: 500,
        OVERVOLTAGE_RESTORE_MARGIN_V: 0.3,
        OVERVOLTAGE_ACTIVE: false,
        MAX_ENERGY_CONSUMPTION_FOR_THR_DAY_WH: energyGoalWh,
        ENERGY_GOAL_VALUE_WH: energyGoalWh,
        ENERGY_GOAL_DURATION_MIN: energyGoalWindowMin,
        ENERGY_GOAL_DURATION_SEC: energyGoalWindowMin * 60,
      scene: {
          enabled: true,
          lock_active: true,
          completed: false,
          step_index: t,
          total_steps: 240,
          name: 'ONE_HOUR',
          duration_sec: 3600,
          target_temperature_c: 30,
          target_humidity_pct: 62,
          last_step_summary: {
            row_count: 5,
            energy_kwh_sum: 10 + Math.sin(t / 8),
          },
        },
      },
      process: {
        enabled: true,
        lock_active: true,
        state: t % 40 < 10 ? 'HEAT_TANK1' : t % 40 < 20 ? 'TRANSFER_1_TO_2' : t % 40 < 30 ? 'HEAT_TANK2' : 'TRANSFER_2_TO_1',
        cycle_count: Math.floor(t / 40),
        goal_cycles: 6,
        goal_reached: Math.floor(t / 40) >= 6,
        tank1: {
          level_pct: 72 - (t % 20) * 1.1,
          temperature_c: 44 + Math.sin(t / 5) * 2,
          target_level_pct: 70,
          target_temp_c: 45,
          ultrasonic_distance_cm: 12 + (t % 20) * 0.4,
          ultrasonic_raw_cm: 12 + (t % 20) * 0.4 + Math.sin(t / 3) * 0.3,
        },
        tank2: {
          level_pct: 28 + (t % 20) * 1.1,
          temperature_c: 52 + Math.cos(t / 6) * 2,
          target_level_pct: 70,
          target_temp_c: 55,
          ultrasonic_distance_cm: 29 - (t % 20) * 0.4,
          ultrasonic_raw_cm: 29 - (t % 20) * 0.4 + Math.cos(t / 3) * 0.3,
        },
      },
      environment: {
        temperature_c: temp,
        humidity_pct: humidity,
      },
      energy: {
        total_energy_wh: 180 + t * 0.35,
        window_wh: {
          last_1d: 180 + t * 0.35,
        },
        budget: {
          goal_window_wh: energyGoalWh,
          window_duration_min: energyGoalWindowMin,
          window_duration_sec: energyGoalWindowMin * 60,
          consumed_window_wh: 180 + t * 0.35,
          remaining_window_wh: Math.max(0, energyGoalWh - (180 + t * 0.35)),
          used_ratio_window: (180 + t * 0.35) / energyGoalWh,
          goal_1d_wh: energyGoalWh,
          consumed_1d_wh: 180 + t * 0.35,
          remaining_1d_wh: Math.max(0, energyGoalWh - (180 + t * 0.35)),
          used_ratio_1d: (180 + t * 0.35) / energyGoalWh,
        },
      },
      loads: {
        motor1: { on: true, duty: 0.9, class: 'CRITICAL', priority: 1, power_w: 12 + Math.sin(t / 4), temperature_c: 62 + Math.sin(t / 6) * 4 },
        motor2: { on: true, duty: 0.85, class: 'CRITICAL', priority: 1, power_w: 11 + Math.cos(t / 5), temperature_c: 60 + Math.sin(t / 7) * 3 },
        heater1: { on: true, duty: 0.6, class: 'ESSENTIAL', priority: 2, power_w: 8 + Math.cos(t / 3) },
        heater2: { on: t % 6 !== 0, duty: 0.4, class: 'ESSENTIAL', priority: 2, power_w: 4 + Math.sin(t / 2) },
        lighting1: { on: true, duty: 0.7, class: 'IMPORTANT', priority: 3, power_w: 3.5 },
        lighting2: { on: true, duty: 0.5, class: 'SECONDARY', priority: 4, power_w: 2.1 },
        aux1: { on: t % 4 !== 0, duty: 1, class: 'NON_ESSENTIAL', priority: 5, power_w: 1.1 },
        aux2: { on: t % 5 !== 0, duty: 1, class: 'NON_ESSENTIAL', priority: 5, power_w: 0.9 },
      },
      evaluation: {
        control: {
          total_control_actions: Math.max(0, t - 1),
          shedding_event_count: Math.floor(t / 11),
          curtail_event_count: Math.floor(t / 4),
          restore_event_count: Math.floor(t / 7),
          control_actions_last_60s: 3 + (t % 4),
        },
        stability: {
          max_overshoot_w: Math.max(0, (power + 4) - maxPower),
          overshoot_event_count: Math.floor(t / 15),
          load_toggle_count: Math.floor(t / 6),
          last_peak_settling_time_sec: 8 + (t % 5),
        },
        predictor: {
          prediction: {
            prediction_age_ms: 200 + (t % 10) * 40,
            apply_delay_sec: 1.0,
            horizon_sec: 120,
          },
          forecast_accuracy: {
            mae_w: 1.5 + (t % 3) * 0.2,
            rmse_w: 2.1 + (t % 4) * 0.25,
          },
        },
      },
    };

    const shortPred = {
      timestamp: Date.now() / 1000,
      horizon_sec: 120,
      risk_level: power > 45 ? 'HIGH' : power > 42 ? 'MEDIUM' : 'LOW',
      peak_risk: power > 45,
      predicted_power_w: power + 4,
      predicted_energy_wh: (power + 4) * (120 / 3600),
      suggestions: power > 45 ? [{ device: 'lighting2', action: 'curtail', set: { duty: 0.4 }, reason: 'PEAK_RISK', class: 'SECONDARY', priority: 4 }] : [],
    };

    const longPred = {
      timestamp: Date.now() / 1000,
      horizon_sec: 1800,
      risk_level: power > 43 ? 'MEDIUM' : 'LOW',
      peak_risk: power > 43,
      predicted_power_w: power + 2,
      predicted_energy_wh: (power + 2) * (1800 / 3600),
      suggestions: power > 43 ? [{ device: 'aux1', action: 'shed', set: { on: false }, reason: 'LONG_TERM_RISK', class: 'NON_ESSENTIAL', priority: 5 }] : [],
    };

    return { telemetry, shortPred, longPred };
  };
};
