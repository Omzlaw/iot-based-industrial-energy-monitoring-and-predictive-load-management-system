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
  predictorStatus: {
    long_rf: null,
    long_lstm: null,
  },
  syntheticTrainingStatus: null,
  experimentExportStatus: {},
  lastExperimentExportStatus: null,
  suggestions: [],
  ackFeed: [],
  powerHistory: [],
  loadEnergyTotals: {},
  predictionPowerHistory: [],
  predictionEnergyHistory: [],
  predictionHistory: {
    short: [],
    long_rf: [],
    long_lstm: [],
  },
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

  const nextLoadEnergyTotals = { ...(prev.loadEnergyTotals || {}) };
  const currentLoads = payload?.loads || {};
  const prevLoads = prev.loads || {};
  const dtHours = Number.isFinite(prev.telemetryAt) && ts > prev.telemetryAt
    ? (ts - prev.telemetryAt) / 3600000
    : 0;
  const loadNames = new Set([...Object.keys(prevLoads), ...Object.keys(currentLoads)]);
  for (const loadName of loadNames) {
    const prevPower = toNumber(prevLoads?.[loadName]?.power_w, NaN);
    const currentPower = toNumber(currentLoads?.[loadName]?.power_w, NaN);
    const referencePower = Number.isFinite(prevPower) && Number.isFinite(currentPower)
      ? (prevPower + currentPower) * 0.5
      : (Number.isFinite(currentPower) ? currentPower : (Number.isFinite(prevPower) ? prevPower : NaN));
    const deltaWh = dtHours > 0 && Number.isFinite(referencePower) && referencePower >= 0
      ? referencePower * dtHours
      : 0;
    nextLoadEnergyTotals[loadName] = Math.max(0, toNumber(nextLoadEnergyTotals[loadName], 0) + deltaWh);
    point[`load_${loadName}_total_energy_wh`] = nextLoadEnergyTotals[loadName];
    point[`load_${loadName}_power_w`] = toNumber(currentLoads?.[loadName]?.power_w, 0);
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
    loadEnergyTotals: nextLoadEnergyTotals,
    powerHistory: push(prev.powerHistory, point),
  };
};

export const applyPrediction = (prev, payload, model) => {
  const tsBase = toEpochMs(payload?.timestamp);
  const horizonSec = toNumber(payload?.horizon_sec);
  const targetTs = tsBase + horizonSec * 1000;
  const modelKey = model === 'short' ? 'short' : model === 'long_lstm' ? 'longLstm' : 'longRf';
  const predictedEnergyForChart = toNumber(
    payload?.predicted_window_energy_wh
    ?? payload?.projected_energy_window_wh
    ?? payload?.predicted_energy_wh
    ?? payload?.predicted_incremental_energy_wh
    ?? payload?.predicted_total_energy_wh
  );
  const predictedPowerForChart = toNumber(
    payload?.predicted_power_w,
    Number.isFinite(predictedEnergyForChart) && horizonSec > 0
      ? Math.max(0, predictedEnergyForChart) / (horizonSec / 3600)
      : 0
  );

  const nextPredictions = {
    ...prev.predictions,
    [modelKey]: payload,
  };

  const powerEntry = {
    ts: targetTs,
    label: new Date(targetTs).toLocaleTimeString(),
    horizon_sec: horizonSec,
    short: model === 'short' ? predictedPowerForChart : null,
    long_rf: model === 'long_rf' ? predictedPowerForChart : null,
    long_lstm: model === 'long_lstm' ? predictedPowerForChart : null,
  };

  const energyEntry = {
    ts: targetTs,
    label: new Date(targetTs).toLocaleTimeString(),
    short: model === 'short' ? predictedEnergyForChart : null,
    long_rf: model === 'long_rf' ? predictedEnergyForChart : null,
    long_lstm: model === 'long_lstm' ? predictedEnergyForChart : null,
  };
  const perModelEntry = {
    ts: targetTs,
    label: new Date(targetTs).toLocaleTimeString(),
    horizon_sec: horizonSec,
    power_w: predictedPowerForChart,
    energy_wh: predictedEnergyForChart,
  };

  return {
    ...prev,
    predictionAt: Date.now(),
    predictions: nextPredictions,
    suggestions: mergeSuggestions(nextPredictions),
    predictionPowerHistory: push(prev.predictionPowerHistory, powerEntry),
    predictionEnergyHistory: push(prev.predictionEnergyHistory, energyEntry),
    predictionHistory: {
      ...(prev.predictionHistory || {}),
      [model]: push(prev.predictionHistory?.[model] || [], perModelEntry),
    },
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

export const applyPredictorStatus = (prev, payload, source) => {
  const key = source === 'long_lstm' ? 'long_lstm' : 'long_rf';
  return {
    ...prev,
    predictorStatus: {
      ...(prev.predictorStatus || {}),
      [key]: {
        ...(prev.predictorStatus?.[key] || {}),
        ...(payload || {}),
        updated_at: Date.now(),
      },
    },
  };
};

export const applySyntheticTrainingStatus = (prev, payload) => ({
  ...prev,
  syntheticTrainingStatus: {
    ...(prev.syntheticTrainingStatus || {}),
    ...((payload && payload.job) ? payload.job : payload || {}),
    updated_at: Date.now(),
  },
});

export const applyExperimentExportStatus = (prev, payload) => {
  const job = payload && payload.job && typeof payload.job === 'object' ? payload.job : payload || {};
  const runId = String(job?.run_id || payload?.run_id || '').trim();
  const namingMode = String(job?.naming_mode || payload?.naming_mode || 'report').trim().toLowerCase() || 'report';
  if (!runId) return prev;
  const key = `${runId}:${namingMode}`;
  const merged = {
    ...(prev.experimentExportStatus?.[key] || {}),
    ...(payload || {}),
    ...(job || {}),
    run_id: runId,
    naming_mode: namingMode,
    updated_at: Date.now(),
  };
  return {
    ...prev,
    experimentExportStatus: {
      ...(prev.experimentExportStatus || {}),
      [key]: merged,
    },
    lastExperimentExportStatus: {
      ...merged,
      event_key: `${key}:${Date.now()}`,
    },
  };
};

export const buildTopicSet = (plantId) => ({
  telemetry: `dt/${plantId}/telemetry`,
  shortPred: `dt/${plantId}/prediction`,
  longPred: `dt/${plantId}/prediction_long`,
  longPredLstm: `dt/${plantId}/prediction_long_lstm`,
  longPredStatus: `dt/${plantId}/prediction_long_status`,
  longPredLstmStatus: `dt/${plantId}/prediction_long_lstm_status`,
  syntheticTrainingStatus: `dt/${plantId}/synthetic_training_status`,
  experimentExportStatus: `dt/${plantId}/experiment_export_status`,
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
    client.subscribe([topics.telemetry, topics.shortPred, topics.longPred, topics.longPredLstm, topics.longPredStatus, topics.longPredLstmStatus, topics.syntheticTrainingStatus, topics.experimentExportStatus, topics.ack], { qos: 0 });
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
    if (topic === topics.longPredStatus) {
      emit((prev) => applyPredictorStatus(prev, payload, 'long_rf'));
      return;
    }
    if (topic === topics.longPredLstmStatus) {
      emit((prev) => applyPredictorStatus(prev, payload, 'long_lstm'));
      return;
    }
    if (topic === topics.syntheticTrainingStatus) {
      emit((prev) => applySyntheticTrainingStatus(prev, payload));
      return;
    }
    if (topic === topics.experimentExportStatus) {
      emit((prev) => applyExperimentExportStatus(prev, payload));
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
  const sampleSec = 60;
  const sampleMs = sampleSec * 1000;
  let simTs = Date.now() - (90 * sampleMs);
  let totalEnergyWh = 180;
  let previousPowerW = 36;
  const energySamples = [];

  const sumWindowWh = (windowMinutes) => {
    const cutoff = simTs - (Math.max(1, Number(windowMinutes) || 1) * 60000);
    let total = 0;
    for (const sample of energySamples) {
      if (sample.ts >= cutoff) total += sample.deltaWh;
    }
    return total;
  };

  const demoTariffState = (ts) => {
    const hour = new Date(ts).getHours();
    if (hour < 7) return 'OFF_PEAK';
    if (hour < 16) return 'MID_PEAK';
    if (hour < 20) return 'PEAK';
    return 'MID_PEAK';
  };

  const buildPrediction = (predictedPowerW, horizonSec, riskLevel, suggestions = []) => {
    const predictedWindowEnergyWh = Math.max(0, predictedPowerW) * (horizonSec / 3600);
    return {
      timestamp: simTs / 1000,
      horizon_sec: horizonSec,
      risk_level: riskLevel,
      peak_risk: riskLevel === 'HIGH',
      predicted_power_w: predictedPowerW,
      predicted_energy_wh: predictedWindowEnergyWh,
      predicted_incremental_energy_wh: predictedWindowEnergyWh,
      predicted_window_energy_wh: predictedWindowEnergyWh,
      projected_energy_window_wh: predictedWindowEnergyWh,
      predicted_total_energy_wh: totalEnergyWh + predictedWindowEnergyWh,
      suggestions,
    };
  };

  return () => {
    t += 1;
    simTs += sampleMs;

    const cyclePhase = t % 48;
    const heatingPhase = cyclePhase < 12 || (cyclePhase >= 24 && cyclePhase < 36);
    const transferPhase = cyclePhase >= 12 && cyclePhase < 24;
    const finishingPhase = cyclePhase >= 36;
    const basePower = heatingPhase ? 42 : transferPhase ? 34 : 28;
    const phaseSurge = (cyclePhase >= 8 && cyclePhase <= 12) || (cyclePhase >= 30 && cyclePhase <= 34) ? 7 : 0;
    const power = clamp(
      basePower
      + phaseSurge
      + 4.5 * Math.sin(t / 2.6)
      + 5.2 * Math.cos(t / 6.8)
      + 2.4 * Math.sin(t / 11.5),
      10,
      72
    );
    const maxPower = 50;
    const energyGoalWh = 150;
    const energyGoalWindowMin = 180;
    const temp = 24 + 1.6 * Math.sin(t / 9) + (heatingPhase ? 1.4 : -0.4);
    const humidity = 58 + 4 * Math.cos(t / 7) + (transferPhase ? 2.5 : -1.5);
    const tariffState = demoTariffState(simTs);
    const deltaWh = Math.max(0, ((previousPowerW + power) * 0.5) * (sampleSec / 3600));
    totalEnergyWh += deltaWh;
    previousPowerW = power;
    energySamples.push({ ts: simTs, deltaWh });
    while (energySamples.length && energySamples[0].ts < simTs - (24 * 60 * 60 * 1000)) {
      energySamples.shift();
    }
    const consumedWindowWh = sumWindowWh(energyGoalWindowMin);

    const processStage = cyclePhase < 12
      ? 'HEAT_TANK1'
      : cyclePhase < 24
        ? 'TRANSFER_1_TO_2'
        : cyclePhase < 36
          ? 'HEAT_TANK2'
          : 'TRANSFER_2_TO_1';
    const stageProgress = (cyclePhase % 12) / 12;
    const tank1Level = cyclePhase < 24
      ? 76 - (stageProgress * 24)
      : 52 + (stageProgress * 24);
    const tank2Level = 100 - tank1Level;
    const motor1Power = 10 + 2.8 * Math.sin(t / 4.2) + (transferPhase ? 3.6 : 1.2);
    const motor2Power = 9 + 2.4 * Math.cos(t / 5.1) + (finishingPhase ? 2.8 : 0.8);
    const heater1Power = heatingPhase ? 12 + 3.5 * Math.cos(t / 3.3) : 2.2 + 1.2 * Math.sin(t / 2.7);
    const heater2Power = heatingPhase || finishingPhase ? 7 + 2.6 * Math.sin(t / 3.8) : 1.1 + 0.8 * Math.cos(t / 2.4);
    const lighting1Power = 3.2 + 0.3 * Math.sin(t / 12);
    const lighting2Power = 2.1 + 0.25 * Math.cos(t / 10);
    const aux1Power = transferPhase ? 2.6 : 1.2;
    const aux2Power = finishingPhase ? 1.8 : 0.9;

    const telemetry = {
      timestamp: new Date(simTs).toISOString(),
      system: {
        mode: 'PROCESS',
        control_policy: 'AI_PREFERRED',
        season: 'N/A',
        tariff_state: tariffState,
        peak_event: power > 45,
        supply_v: 11.8 + Math.sin(t / 6) * 0.2,
        supply_v_raw: 11.8 + Math.sin(t / 6) * 0.2,
        total_power_w: clamp(power, 8, 75),
        total_current_a: clamp(power / 11.9, 0, 10),
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
          step_index: t % 240,
          total_steps: 240,
          name: 'ONE_HOUR',
          duration_sec: 3600,
          target_temperature_c: 30,
          target_humidity_pct: 62,
          last_step_summary: {
            row_count: 5,
            energy_kwh_sum: 10 + Math.sin(t / 8) * 0.6,
          },
        },
      },
      process: {
        enabled: true,
        lock_active: true,
        state: processStage,
        cycle_count: Math.floor(t / 48),
        goal_cycles: 6,
        goal_reached: Math.floor(t / 48) >= 6,
        tank1: {
          level_pct: clamp(tank1Level, 28, 84),
          temperature_c: 43.5 + Math.sin(t / 4.8) * 2.4 + (processStage === 'HEAT_TANK1' ? 2.8 : -1.4),
          target_level_pct: 70,
          target_temp_c: 45,
          ultrasonic_distance_cm: 12 + ((100 - clamp(tank1Level, 28, 84)) * 0.22),
          ultrasonic_raw_cm: 12 + ((100 - clamp(tank1Level, 28, 84)) * 0.22) + Math.sin(t / 3) * 0.3,
        },
        tank2: {
          level_pct: clamp(tank2Level, 16, 72),
          temperature_c: 49.5 + Math.cos(t / 5.7) * 2.2 + (processStage === 'HEAT_TANK2' ? 3.4 : -1.2),
          target_level_pct: 70,
          target_temp_c: 55,
          ultrasonic_distance_cm: 15 + ((100 - clamp(tank2Level, 16, 72)) * 0.2),
          ultrasonic_raw_cm: 15 + ((100 - clamp(tank2Level, 16, 72)) * 0.2) + Math.cos(t / 3) * 0.3,
        },
      },
      environment: {
        temperature_c: temp,
        humidity_pct: humidity,
      },
      energy: {
        total_energy_wh: totalEnergyWh,
        window_wh: {
          last_1d: consumedWindowWh,
        },
        budget: {
          goal_window_wh: energyGoalWh,
          window_duration_min: energyGoalWindowMin,
          window_duration_sec: energyGoalWindowMin * 60,
          consumed_window_wh: consumedWindowWh,
          remaining_window_wh: Math.max(0, energyGoalWh - consumedWindowWh),
          used_ratio_window: consumedWindowWh / energyGoalWh,
          goal_1d_wh: energyGoalWh,
          consumed_1d_wh: consumedWindowWh,
          remaining_1d_wh: Math.max(0, energyGoalWh - consumedWindowWh),
          used_ratio_1d: consumedWindowWh / energyGoalWh,
        },
      },
      loads: {
        motor1: { on: true, duty: transferPhase ? 0.96 : 0.82, class: 'CRITICAL', priority: 1, power_w: motor1Power, temperature_c: 62 + Math.sin(t / 6) * 4 },
        motor2: { on: true, duty: finishingPhase ? 0.92 : 0.78, class: 'CRITICAL', priority: 1, power_w: motor2Power, temperature_c: 60 + Math.sin(t / 7) * 3 },
        heater1: { on: heatingPhase, duty: heatingPhase ? 0.82 : 0.2, class: 'ESSENTIAL', priority: 2, power_w: heater1Power },
        heater2: { on: heatingPhase || finishingPhase, duty: heatingPhase || finishingPhase ? 0.58 : 0.16, class: 'ESSENTIAL', priority: 2, power_w: heater2Power },
        lighting1: { on: true, duty: 0.72, class: 'IMPORTANT', priority: 3, power_w: lighting1Power },
        lighting2: { on: true, duty: tariffState === 'PEAK' ? 0.35 : 0.55, class: 'SECONDARY', priority: 4, power_w: lighting2Power },
        aux1: { on: transferPhase, duty: transferPhase ? 1 : 0.25, class: 'NON_ESSENTIAL', priority: 5, power_w: aux1Power },
        aux2: { on: finishingPhase, duty: finishingPhase ? 1 : 0.3, class: 'NON_ESSENTIAL', priority: 5, power_w: aux2Power },
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
            prediction_age_ms: 140 + (t % 6) * 85,
            apply_delay_sec: 1.0,
            horizon_sec: 120,
          },
          forecast_accuracy: {
            mae_w: 1.2 + Math.abs(Math.sin(t / 9)) * 0.7,
            rmse_w: 1.8 + Math.abs(Math.cos(t / 8)) * 1.1,
          },
        },
      },
    };

    const shortPredPower = clamp(power + 4.2 * Math.sin(t / 1.9) + (power > 45 ? 3.5 : -1.1), 10, 78);
    const longPredPower = clamp(power + 2.6 * Math.cos(t / 5.5) + (tariffState === 'PEAK' ? 4.8 : 0.9), 10, 76);
    const longLstmPredPower = clamp(power + 2.1 * Math.sin(t / 7.4) + (finishingPhase ? -2.5 : 3.1), 10, 74);
    const shortRisk = shortPredPower > 47 ? 'HIGH' : shortPredPower > 42 ? 'MEDIUM' : 'LOW';
    const longRisk = longPredPower > 48 || consumedWindowWh > energyGoalWh * 0.96
      ? 'HIGH'
      : longPredPower > 43 || consumedWindowWh > energyGoalWh * 0.84
        ? 'MEDIUM'
        : 'LOW';
    const longLstmRisk = longLstmPredPower > 47 || consumedWindowWh > energyGoalWh * 0.94
      ? 'HIGH'
      : longLstmPredPower > 42 || consumedWindowWh > energyGoalWh * 0.8
        ? 'MEDIUM'
        : 'LOW';

    const shortPred = buildPrediction(
      shortPredPower,
      120,
      shortRisk,
      shortRisk !== 'LOW'
        ? [{ device: 'lighting2', action: 'curtail', set: { duty: tariffState === 'PEAK' ? 0.25 : 0.4 }, reason: 'PEAK_RISK', class: 'SECONDARY', priority: 4 }]
        : []
    );
    const longPred = buildPrediction(
      longPredPower,
      1800,
      longRisk,
      longRisk !== 'LOW'
        ? [{ device: 'aux1', action: 'shed', set: { on: false }, reason: 'LONG_TERM_RISK', class: 'NON_ESSENTIAL', priority: 5 }]
        : []
    );
    const longLstmPred = buildPrediction(
      longLstmPredPower,
      1800,
      longLstmRisk,
      longLstmRisk !== 'LOW'
        ? [{ device: 'heater2', action: 'curtail', set: { duty: 0.35 }, reason: 'PROCESS_ENERGY_PRESSURE', class: 'ESSENTIAL', priority: 2 }]
        : []
    );

    return { telemetry, shortPred, longPred, longLstmPred };
  };
};
