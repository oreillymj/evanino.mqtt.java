#!/usr/bin/env bash

set -Eeuo pipefail

# Default broker hostname
BROKER_HOST="${1:-localhost}"

# Output directory (relative to current directory)
SSL_DIR="./ssl"

echo
echo "Creating Mosquitto SSL certificates"
echo "Broker hostname: ${BROKER_HOST}"
echo "Output directory: ${SSL_DIR}"
echo

# --------------------------------------------------
# Helper: check that an expected file exists
# --------------------------------------------------

check_file() {
    if [[ ! -s "$1" ]]; then
        echo "ERROR: Expected file missing or empty: $1" >&2
        exit 1
    fi
}

# --------------------------------------------------
# 1. Create Certificate Authority
# --------------------------------------------------

mkdir -p "$SSL_DIR"

echo "[1/6] Generating CA private key..."

openssl genrsa -out "$SSL_DIR/ca.key" 2048
check_file "$SSL_DIR/ca.key"

echo "[2/6] Generating CA certificate..."

openssl req -new -x509 \
    -key "$SSL_DIR/ca.key" \
    -sha256 \
    -days 3650 \
    -out "$SSL_DIR/ca.crt" \
    -subj "/CN=Mosquitto-Local-CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign"

check_file "$SSL_DIR/ca.crt"

# --------------------------------------------------
# 2. Create Server Certificate
# --------------------------------------------------

echo "[3/6] Generating server private key..."

openssl genrsa -out "$SSL_DIR/server.key" 2048
check_file "$SSL_DIR/server.key"

echo "Generating server CSR..."

openssl req -new \
    -key "$SSL_DIR/server.key" \
    -out "$SSL_DIR/server.csr" \
    -subj "/CN=${BROKER_HOST}" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth" \
    -addext "subjectAltName=DNS:${BROKER_HOST}"

check_file "$SSL_DIR/server.csr"

echo "Signing server certificate..."

openssl x509 -req \
    -in "$SSL_DIR/server.csr" \
    -CA "$SSL_DIR/ca.crt" \
    -CAkey "$SSL_DIR/ca.key" \
    -CAcreateserial \
    -out "$SSL_DIR/server.crt" \
    -days 825 \
    -sha256 \
    -copy_extensions copy

check_file "$SSL_DIR/server.crt"
check_file "$SSL_DIR/ca.srl"

# --------------------------------------------------
# 3. Create Client Certificate
# --------------------------------------------------

echo "[4/6] Generating client private key..."

openssl genrsa -out "$SSL_DIR/client.key" 2048
check_file "$SSL_DIR/client.key"

echo "Generating client CSR..."

openssl req -new \
    -key "$SSL_DIR/client.key" \
    -out "$SSL_DIR/client.csr" \
    -subj "/CN=Mosquitto-Client" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=clientAuth"

check_file "$SSL_DIR/client.csr"

echo "Signing client certificate..."

openssl x509 -req \
    -in "$SSL_DIR/client.csr" \
    -CA "$SSL_DIR/ca.crt" \
    -CAkey "$SSL_DIR/ca.key" \
    -CAserial "$SSL_DIR/ca.srl" \
    -out "$SSL_DIR/client.crt" \
    -days 825 \
    -sha256 \
    -copy_extensions copy

check_file "$SSL_DIR/client.crt"

# --------------------------------------------------
# 4. Verify certificates
# --------------------------------------------------

echo "[5/6] Verifying server certificate..."

openssl verify \
    -CAfile "$SSL_DIR/ca.crt" \
    -verify_hostname "$BROKER_HOST" \
    "$SSL_DIR/server.crt"

echo "Verifying client certificate..."

openssl verify \
    -CAfile "$SSL_DIR/ca.crt" \
    "$SSL_DIR/client.crt"

# --------------------------------------------------
# 5. Completion
# --------------------------------------------------

echo
echo "[6/6] Certificate generation completed successfully."
echo

echo "Generated files:"
ls -l "$SSL_DIR"

echo
echo "=================================================="
echo "Mosquitto configuration"
echo "=================================================="
echo
echo "Add the following to mosquitto.conf:"
echo
echo "listener 8883"
echo "cafile ${SSL_DIR}/ca.crt"
echo "certfile ${SSL_DIR}/server.crt"
echo "keyfile ${SSL_DIR}/server.key"
echo "tls_version tlsv1.2"
echo
echo "For mandatory client certificate authentication:"
echo
echo "require_certificate true"
echo
echo "Restart the Mosquitto broker after updating the config."
echo
echo "=================================================="
echo
echo "Client certificate files:"
echo
echo "CA certificate: ${SSL_DIR}/ca.crt"
echo "Client certificate: ${SSL_DIR}/client.crt"
echo "Client private key: ${SSL_DIR}/client.key"
echo
echo "Keep ca.key private and secure. It is not needed by"
echo "the running broker or MQTT clients."
echo
