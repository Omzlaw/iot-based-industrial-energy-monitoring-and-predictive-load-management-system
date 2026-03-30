# VPS Deployment (GitHub Actions)

This folder contains remote deployment scripts used by GitHub Actions workflows:

- `react_vps_deploy.sh`
- `predictor_vps_deploy.sh`

## Workflows

- `.github/workflows/deploy-react-vps.yml`
- `.github/workflows/deploy-predictor-main-vps.yml`
- `.github/workflows/deploy-predictor-long-rf-vps.yml`
- `.github/workflows/deploy-predictor-long-lstm-vps.yml`
- `.github/workflows/deploy-telemetry-logger-vps.yml`
- `.github/workflows/deploy-history-api-vps.yml`

## Required repository secrets

Common:

- `VPS_HOST`
- `VPS_USER`
- `VPS_SSH_KEY`
- `VPS_PORT` (optional; default `22`)

React:

- `REACT_TARGET_DIR` (example: `/var/www/factory-dashboard`)
- `REACT_APP_DOMAIN` (example: `example.com`)
- `REACT_WEB_USER` (optional; default `www-data`)
- `REACT_WEB_GROUP` (optional; default `www-data`)
- `REACT_APACHE_SERVICE` (optional; default `apache2`)
- `REACT_ENV_FILE` (optional multiline `.env.production` content; include `VITE_BASE_PATH=/frontend/` when serving from a subpath, and set `VITE_HISTORY_API_URL=/history-api` for same-domain history API proxy)

Predictors:

- `PREDICTOR_KEEP_RELEASES` (optional; default `5`)

Main predictor:

- `PREDICTOR_MAIN_SERVICE`
- `PREDICTOR_MAIN_APP_DIR`
- `PREDICTOR_MAIN_ENV` (required multiline `.env`, must include `HIVEMQ_USER`, `HIVEMQ_PASS`, `MQTT_BROKER_HOST`, `MQTT_BROKER_PORT`, `PLANT_IDENTIFIER`)

Long RF predictor:

- `PREDICTOR_LONG_SERVICE`
- `PREDICTOR_LONG_APP_DIR`
- `PREDICTOR_LONG_ENV` (required multiline `.env`, must include `HIVEMQ_USER`, `HIVEMQ_PASS`, `MQTT_BROKER_HOST`, `MQTT_BROKER_PORT`, `PLANT_IDENTIFIER`)

Long LSTM predictor:

- `PREDICTOR_LSTM_SERVICE`
- `PREDICTOR_LSTM_APP_DIR`
- `PREDICTOR_LSTM_ENV` (required multiline `.env`, must include `HIVEMQ_USER`, `HIVEMQ_PASS`, `MQTT_BROKER_HOST`, `MQTT_BROKER_PORT`, `PLANT_IDENTIFIER`)

Telemetry logger:

- `TELEMETRY_LOGGER_SERVICE`
- `TELEMETRY_LOGGER_APP_DIR`
- `TELEMETRY_LOGGER_ENV` (required multiline `.env`, must include `HIVEMQ_USER`, `HIVEMQ_PASS`, `MQTT_BROKER_HOST`, `MQTT_BROKER_PORT`, `PLANT_IDENTIFIER`; optional `LOGGER_OUTPUT_DIR`)

History API:

- `HISTORY_API_SERVICE`
- `HISTORY_API_APP_DIR`
- `HISTORY_API_ENV` (required multiline `.env`, must include `PLANT_IDENTIFIER`, `LOGGER_OUTPUT_DIR`; optional `HISTORY_API_HOST`, `HISTORY_API_PORT`, `HISTORY_API_CORS_ORIGINS`)

## Notes

- React `.htaccess` is generated from `DashboardReact/public/.htaccess` and copied in Vite build output.
- `__APP_DOMAIN_REGEX__` is replaced on VPS by the React deploy script.
- Python services are deployed as `systemd` units with release symlink strategy:
  - `${APP_DIR}/releases/<timestamp>`
  - `${APP_DIR}/current` -> latest release
- Predictor deploy runs in user scope (no sudo/root expected):
  - default `APP_DIR=$HOME/apps/<service>`
  - default `SYSTEMD_DIR=$HOME/.config/systemd/user`
  - if user services are unavailable, ask server admin to enable linger for deploy user
- Requirements files:
  - Main predictor (`main.py`): `PythonPredictor/requirements.main.txt`
  - Long RF predictor (`long_term_predictor.py`): `PythonPredictor/requirements.main_long.txt`
  - Long LSTM predictor (`lstm_long_term_predictor.py`): `PythonPredictor/requirements.lstm.txt`
  - Telemetry logger (`telemetry_logger.py`): `PythonPredictor/requirements.logger.txt`
  - History API (`history_api.py`): `PythonPredictor/requirements.history_api.txt`
