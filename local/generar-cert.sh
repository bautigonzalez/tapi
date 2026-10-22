#!/usr/bin/env bash
# Certificado autofirmado para la API mock (HTTPS en 127.0.0.1). Solo para el entorno local.
# El worker exige https: así se prueba el mismo cliente que en producción, sin desactivar la verificación TLS.
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)/.certs"
mkdir -p "$DIR"
[ -f "$DIR/mock-api.pem" ] && exit 0

CONF="$(mktemp)"
trap 'rm -f "$CONF"' EXIT
cat >"$CONF" <<'EOF'
[req]
distinguished_name = dn
x509_extensions = ext
prompt = no
[dn]
CN = 127.0.0.1
[ext]
subjectAltName = IP:127.0.0.1, DNS:localhost
basicConstraints = critical, CA:TRUE
keyUsage = critical, digitalSignature, keyCertSign, keyEncipherment
extendedKeyUsage = serverAuth
EOF
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -config "$CONF" \
  -keyout "$DIR/mock-api.key" -out "$DIR/mock-api.pem" 2>/dev/null
chmod 600 "$DIR/mock-api.key"
echo "Certificado local en $DIR"
