#!/usr/bin/env bash
set -euo pipefail

RELEASE_ARCHIVE="${RELEASE_ARCHIVE:-/tmp/predictor-src.tgz}"
SERVICE_NAME="${SERVICE_NAME:-predictor-main}"
ENTRYPOINT="${ENTRYPOINT:-main.py}"
APP_DIR="${APP_DIR:-${HOME}/apps/${SERVICE_NAME}}"
VENV_DIR="${VENV_DIR:-${APP_DIR}/venv}"
REQUIREMENTS_FILE="${REQUIREMENTS_FILE:-}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
KEEP_RELEASES="${KEEP_RELEASES:-5}"
ENV_B64="${ENV_B64:-}"
SYSTEMD_DIR="${SYSTEMD_DIR:-${HOME}/.config/systemd/user}"

if [[ ! -f "${RELEASE_ARCHIVE}" ]]; then
  echo "[deploy-predictor] release archive not found: ${RELEASE_ARCHIVE}" >&2
  exit 1
fi

if [[ -z "${SERVICE_NAME}" || -z "${ENTRYPOINT}" ]]; then
  echo "[deploy-predictor] SERVICE_NAME and ENTRYPOINT are required" >&2
  exit 1
fi

# Enforce known entrypoint->requirements mapping to avoid workflow mix-ups.
expected_requirements=""
case "${ENTRYPOINT}" in
  main.py)
    expected_requirements="requirements.main.txt"
    ;;
  long_term_predictor.py)
    expected_requirements="requirements.main_long.txt"
    ;;
  lstm_long_term_predictor.py)
    expected_requirements="requirements.lstm.txt"
    ;;
  telemetry_logger.py)
    expected_requirements="requirements.logger.txt"
    ;;
  history_api.py)
    expected_requirements="requirements.history_api.txt"
    ;;
esac
if [[ -n "${expected_requirements}" ]]; then
  if [[ -z "${REQUIREMENTS_FILE}" ]]; then
    REQUIREMENTS_FILE="${expected_requirements}"
  elif [[ "${REQUIREMENTS_FILE}" != "${expected_requirements}" ]]; then
    echo "[deploy-predictor] overriding REQUIREMENTS_FILE=${REQUIREMENTS_FILE} -> ${expected_requirements} for ENTRYPOINT=${ENTRYPOINT}" >&2
    REQUIREMENTS_FILE="${expected_requirements}"
  fi
fi

if [[ -z "${REQUIREMENTS_FILE}" ]]; then
  echo "[deploy-predictor] REQUIREMENTS_FILE is required for unknown ENTRYPOINT=${ENTRYPOINT}" >&2
  exit 1
fi

is_path_writable_or_creatable() {
  local target="$1"
  if [[ -e "${target}" ]]; then
    [[ -w "${target}" ]]
    return
  fi
  # Walk up until we find an existing ancestor, then check writability there.
  local probe="${target}"
  while [[ ! -e "${probe}" && "${probe}" != "/" ]]; do
    probe="$(dirname "${probe}")"
  done
  [[ -d "${probe}" && -w "${probe}" ]]
}

if ! is_path_writable_or_creatable "${APP_DIR}" || ! is_path_writable_or_creatable "${SYSTEMD_DIR}"; then
  cat >&2 <<EOF
[deploy-predictor] Non-root deploy requires writable paths:
  APP_DIR=${APP_DIR}
  SYSTEMD_DIR=${SYSTEMD_DIR}
[deploy-predictor] Set APP_DIR/SYSTEMD_DIR to user-writable locations.
EOF
  exit 1
fi

timestamp="$(date +%Y%m%d%H%M%S)"
release_dir="${APP_DIR}/releases/${timestamp}"
shared_env="${APP_DIR}/shared/.env"
current_dir="${APP_DIR}/current"

mkdir -p "${APP_DIR}/releases" "${APP_DIR}/shared"
mkdir -p "${release_dir}"
tar -xzf "${RELEASE_ARCHIVE}" -C "${release_dir}"

if [[ ! -f "${release_dir}/${ENTRYPOINT}" ]]; then
  echo "[deploy-predictor] entrypoint not found: ${release_dir}/${ENTRYPOINT}" >&2
  exit 1
fi

case "${ENTRYPOINT}" in
  history_api.py)
    if [[ ! -f "${release_dir}/export_chapter4_data.py" ]]; then
      echo "[deploy-predictor] missing required helper for history_api.py: ${release_dir}/export_chapter4_data.py" >&2
      echo "[deploy-predictor] ensure the release archive includes export_chapter4_data.py alongside history_api.py" >&2
      exit 1
    fi
    ;;
esac

if [[ ! -f "${release_dir}/${REQUIREMENTS_FILE}" ]]; then
  echo "[deploy-predictor] requirements file not found: ${release_dir}/${REQUIREMENTS_FILE}" >&2
  echo "[deploy-predictor] release contents:" >&2
  ls -la "${release_dir}" >&2 || true
  exit 1
fi

if [[ ! -d "${VENV_DIR}" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

VENV_PYTHON=""
if [[ -x "${VENV_DIR}/bin/python3" ]]; then
  VENV_PYTHON="${VENV_DIR}/bin/python3"
elif [[ -x "${VENV_DIR}/bin/python" ]]; then
  VENV_PYTHON="${VENV_DIR}/bin/python"
else
  echo "[deploy-predictor] venv is missing python executable; rebuilding: ${VENV_DIR}" >&2
  "${PYTHON_BIN}" -m venv --clear "${VENV_DIR}"
  if [[ -x "${VENV_DIR}/bin/python3" ]]; then
    VENV_PYTHON="${VENV_DIR}/bin/python3"
  elif [[ -x "${VENV_DIR}/bin/python" ]]; then
    VENV_PYTHON="${VENV_DIR}/bin/python"
  else
    echo "[deploy-predictor] venv python executable not found after rebuild: ${VENV_DIR}/bin/python3|python" >&2
    exit 1
  fi
fi

"${VENV_PYTHON}" -m pip install --upgrade pip setuptools wheel
"${VENV_PYTHON}" -m pip install -r "${release_dir}/${REQUIREMENTS_FILE}"

if [[ -n "${ENV_B64}" ]]; then
  tmp_env="$(mktemp)"
  printf '%s' "${ENV_B64}" | base64 -d > "${tmp_env}"
  case "${ENTRYPOINT}" in
    history_api.py)
      if ! grep -q '^PLANT_IDENTIFIER=' "${tmp_env}"; then
        echo "[deploy-predictor] env is missing PLANT_IDENTIFIER" >&2
        rm -f "${tmp_env}"
        exit 1
      fi
      if ! grep -q '^LOGGER_OUTPUT_DIR=' "${tmp_env}"; then
        echo "[deploy-predictor] env is missing LOGGER_OUTPUT_DIR" >&2
        rm -f "${tmp_env}"
        exit 1
      fi
      ;;
    *)
      if ! grep -q '^HIVEMQ_USER=' "${tmp_env}"; then
        echo "[deploy-predictor] env is missing HIVEMQ_USER" >&2
        rm -f "${tmp_env}"
        exit 1
      fi
      if ! grep -q '^HIVEMQ_PASS=' "${tmp_env}"; then
        echo "[deploy-predictor] env is missing HIVEMQ_PASS" >&2
        rm -f "${tmp_env}"
        exit 1
      fi
      if ! grep -q '^MQTT_BROKER_HOST=' "${tmp_env}"; then
        echo "[deploy-predictor] env is missing MQTT_BROKER_HOST" >&2
        rm -f "${tmp_env}"
        exit 1
      fi
      if ! grep -q '^MQTT_BROKER_PORT=' "${tmp_env}"; then
        echo "[deploy-predictor] env is missing MQTT_BROKER_PORT" >&2
        rm -f "${tmp_env}"
        exit 1
      fi
      if ! grep -q '^PLANT_IDENTIFIER=' "${tmp_env}"; then
        echo "[deploy-predictor] env is missing PLANT_IDENTIFIER" >&2
        rm -f "${tmp_env}"
        exit 1
      fi
      ;;
  esac
  mkdir -p "$(dirname "${shared_env}")"
  cp "${tmp_env}" "${shared_env}"
  rm -f "${tmp_env}"
fi

if [[ -f "${shared_env}" ]]; then
  ln -sfn "${shared_env}" "${release_dir}/.env"
fi

ln -sfn "${release_dir}" "${current_dir}"

unit_tmp="$(mktemp)"
cat > "${unit_tmp}" <<EOF
[Unit]
Description=${SERVICE_NAME} service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${current_dir}
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=-${shared_env}
ExecStart=${VENV_PYTHON} ${current_dir}/${ENTRYPOINT}
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF

mkdir -p "${SYSTEMD_DIR}"
cp "${unit_tmp}" "${SYSTEMD_DIR}/${SERVICE_NAME}.service"
rm -f "${unit_tmp}"

if command -v systemctl >/dev/null 2>&1; then
  if [[ -z "${XDG_RUNTIME_DIR:-}" ]] && [[ -d "/run/user/$(id -u)" ]]; then
    export XDG_RUNTIME_DIR="/run/user/$(id -u)"
  fi
  if [[ -z "${DBUS_SESSION_BUS_ADDRESS:-}" ]] && [[ -n "${XDG_RUNTIME_DIR:-}" ]] && [[ -S "${XDG_RUNTIME_DIR}/bus" ]]; then
    export DBUS_SESSION_BUS_ADDRESS="unix:path=${XDG_RUNTIME_DIR}/bus"
  fi
  if ! systemctl --user daemon-reload; then
    cat >&2 <<EOF
[deploy-predictor] Failed to use user systemd (systemctl --user).
Ensure user services are available and linger is enabled for this user.
Ask your server admin to run:
  loginctl enable-linger $(id -un)
EOF
    exit 1
  fi
  systemctl --user enable "${SERVICE_NAME}"
  systemctl --user restart "${SERVICE_NAME}"
  systemctl --user --no-pager --full status "${SERVICE_NAME}" | head -n 20 || true
  # Fail deploy if service cannot reach active state after restart.
  service_active="no"
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    if [[ "$(systemctl --user is-active "${SERVICE_NAME}" 2>/dev/null || true)" == "active" ]]; then
      service_active="yes"
      break
    fi
    sleep 1
  done
  if [[ "${service_active}" != "yes" ]]; then
    echo "[deploy-predictor] service failed to become active: ${SERVICE_NAME}" >&2
    journalctl --user -u "${SERVICE_NAME}.service" -n 80 --no-pager || true
    exit 1
  fi
else
  echo "[deploy-predictor] systemctl not found; service file written but not started" >&2
fi

if [[ "${KEEP_RELEASES}" =~ ^[0-9]+$ ]] && [[ "${KEEP_RELEASES}" -gt 0 ]]; then
  old_releases="$(ls -1dt "${APP_DIR}"/releases/* 2>/dev/null | tail -n +"$((KEEP_RELEASES + 1))" || true)"
  if [[ -n "${old_releases}" ]]; then
    while IFS= read -r old; do
      [[ -n "${old}" ]] && rm -rf "${old}"
    done <<< "${old_releases}"
  fi
fi

echo "[deploy-predictor] deployed ${SERVICE_NAME} using ${ENTRYPOINT} (user scope)"
