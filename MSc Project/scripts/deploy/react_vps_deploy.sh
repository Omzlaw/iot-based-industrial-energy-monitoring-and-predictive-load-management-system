#!/usr/bin/env bash
set -euo pipefail

RELEASE_ARCHIVE="${RELEASE_ARCHIVE:-/tmp/react-dist.tgz}"
TARGET_DIR="${TARGET_DIR:-/var/www/factory-dashboard}"
WEB_USER="${WEB_USER:-www-data}"
WEB_GROUP="${WEB_GROUP:-www-data}"
APACHE_SERVICE="${APACHE_SERVICE:-apache2}"
APP_DOMAIN="${APP_DOMAIN:-}"

if [[ ! -f "${RELEASE_ARCHIVE}" ]]; then
  echo "[deploy-react] release archive not found: ${RELEASE_ARCHIVE}" >&2
  exit 1
fi

if [[ -z "${TARGET_DIR}" || "${TARGET_DIR}" == "/" ]]; then
  echo "[deploy-react] refusing unsafe TARGET_DIR=${TARGET_DIR}" >&2
  exit 1
fi

SUDO_CMD=()
if [[ "$(id -u)" -ne 0 ]] && command -v sudo >/dev/null 2>&1; then
  if sudo -n true >/dev/null 2>&1; then
    SUDO_CMD=(sudo -n)
  fi
fi

run() {
  if [[ ${#SUDO_CMD[@]} -gt 0 ]]; then
    "${SUDO_CMD[@]}" "$@"
  else
    "$@"
  fi
}

is_path_writable_or_creatable() {
  local target="$1"
  if [[ -e "${target}" ]]; then
    [[ -w "${target}" ]]
    return
  fi
  local parent
  parent="$(dirname "${target}")"
  [[ -d "${parent}" && -w "${parent}" ]]
}

if [[ "$(id -u)" -ne 0 ]] && [[ ${#SUDO_CMD[@]} -eq 0 ]]; then
  if ! is_path_writable_or_creatable "${TARGET_DIR}"; then
    cat >&2 <<EOF
[deploy-react] This deploy needs root privileges for:
  TARGET_DIR=${TARGET_DIR}
[deploy-react] Current user has no non-interactive sudo.
Use one of:
  1) SSH as root
  2) Configure NOPASSWD sudo for the deploy user
  3) Set TARGET_DIR to a user-writable path
EOF
    exit 1
  fi
fi

tmp_dir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

tar -xzf "${RELEASE_ARCHIVE}" -C "${tmp_dir}"

run mkdir -p "${TARGET_DIR}"
run find "${TARGET_DIR}" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
run cp -a "${tmp_dir}/." "${TARGET_DIR}/"

if [[ -f "${TARGET_DIR}/.htaccess" ]]; then
  domain_regex=".*"
  if [[ -n "${APP_DOMAIN}" ]]; then
    domain_regex="$(printf '%s' "${APP_DOMAIN}" | sed 's/[][(){}.^$+*?|\\/-]/\\&/g')"
  fi
  run sed -i "s|__APP_DOMAIN_REGEX__|${domain_regex}|g" "${TARGET_DIR}/.htaccess"
fi

run find "${TARGET_DIR}" -type d -exec chmod 755 {} +
run find "${TARGET_DIR}" -type f -exec chmod 644 {} +

if id "${WEB_USER}" >/dev/null 2>&1; then
  run chown -R "${WEB_USER}:${WEB_GROUP}" "${TARGET_DIR}" || true
fi

if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files | grep -q "^${APACHE_SERVICE}\\.service"; then
  run systemctl reload "${APACHE_SERVICE}" || true
fi

echo "[deploy-react] deployed to ${TARGET_DIR}"
