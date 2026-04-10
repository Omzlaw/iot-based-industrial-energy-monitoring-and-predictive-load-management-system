# React Factory Twin Dashboard

A clean React replacement for the Node-RED UI, showing:

- live telemetry (system, environment, loads)
- fused short/long predictions (EMA + RF + LSTM)
- power/current/energy/cost trends
- suggestions + command ACK feed
- command controls for mode/policy/scene/device actions
- historical charts via History API
- backend-saved tariff editor for ToU rates/windows/demand charge

## 1) Setup

```bash
cd DashboardReact
cp .env.example .env
npm install
npm run dev
```

Then open the local Vite URL (usually `http://localhost:5173`).

## 2) Environment variables

- `VITE_PLANT_ID` (default `factory1`)
- `VITE_MQTT_URL` (default HiveMQ websocket endpoint on `8884`)
- `VITE_MQTT_USERNAME`
- `VITE_MQTT_PASSWORD`
- `VITE_HISTORY_API_URL`
- `VITE_CURRENCY_CODE` (default `USD`)
- `VITE_TARIFF_OFFPEAK_PER_KWH` (default `0.12`)
- `VITE_TARIFF_MIDPEAK_PER_KWH` (default `0.18`)
- `VITE_TARIFF_PEAK_PER_KWH` (default `0.28`)
- `VITE_TARIFF_DEMAND_CHARGE_PER_KW` (default `0`)
- `VITE_TARIFF_DEMAND_INTERVAL_MIN` (default `30`)
- `VITE_TARIFF_USE_UK_TOU` (default `true`; derives tariff band from UK time)
- `VITE_TARIFF_UK_OFFPEAK_START_HOUR` (default `0`)
- `VITE_TARIFF_UK_OFFPEAK_END_HOUR` (default `7`)
- `VITE_TARIFF_UK_PEAK_START_HOUR` (default `16`)
- `VITE_TARIFF_UK_PEAK_END_HOUR` (default `20`)

## 3) MQTT topics used

- Subscribe:
  - `dt/<plant_id>/telemetry`
  - `dt/<plant_id>/prediction`
  - `dt/<plant_id>/prediction_long`
  - `dt/<plant_id>/prediction_long_lstm`
  - `dt/<plant_id>/cmd_ack`
- Publish:
  - `dt/<plant_id>/cmd`

## 4) Notes

- Live mode uses MQTT over WebSocket.
- Demo mode works without broker credentials and lets you preview the full UI.
- Historical charts, backend tariff config, and Chapter 4 CSV zip export require `VITE_HISTORY_API_URL`.
- Historical cost comes from the History API backend tariff configuration, not from browser-only local state.
- The Historical Data panel exports the Chapter 4 CSV zip through `/api/chapter4-export`.
