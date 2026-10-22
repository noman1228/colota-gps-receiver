#!/bin/sh
set -eu
cd "$(dirname "$0")"
if [ -e .env ]; then
  echo '.env already exists; leaving existing credentials unchanged.'
else
  umask 077
  if command -v openssl >/dev/null 2>&1; then
    password="$(openssl rand -hex 24)"
    token="$(openssl rand -hex 32)"
  elif command -v python3 >/dev/null 2>&1; then
    password="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"
    token="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
  else
    echo 'Install openssl or python3 to generate secure credentials.' >&2
    exit 1
  fi
  cat > .env <<EOF
APP_USERNAME=admin
APP_PASSWORD=$password
INGEST_TOKEN=$token
APP_TZ=America/New_York
PORT=8765
BIND_IP=0.0.0.0
PUID=$(id -u)
PGID=$(id -g)
MAX_GAP_MINUTES=20
EXIT_GRACE_MINUTES=5
MIN_INSIDE_SAMPLES=2
EOF
  chmod 600 .env
  echo 'Created private .env credentials.'
fi
mkdir -p data
if [ ! -f data/sites.json ]; then
  printf '{"sites": []}\n' > data/sites.json
fi
chmod 700 data
chmod 600 data/sites.json
printf '\nCredentials are stored in .env. Keep this file private.\n'
printf 'Run: docker compose up -d --build\n'
