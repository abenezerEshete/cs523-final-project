#!/bin/bash
# Host-side setup for the optional Grafana/Postgres visualization path.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

LAB_CONTAINER="${LAB_CONTAINER:-cs523bdt-lab}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-crypto-postgres}"
GRAFANA_PG_DB="${GRAFANA_PG_DB:-crypto_analytics}"
GRAFANA_PG_USER="${GRAFANA_PG_USER:-crypto}"
GRAFANA_PG_PASSWORD="${GRAFANA_PG_PASSWORD:-crypto_password}"

echo "=== CS523 Grafana Setup ==="
echo ""

echo "[1/3] Installing Postgres Python client in ${LAB_CONTAINER}..."
docker exec "$LAB_CONTAINER" \
  python3 -m pip install psycopg2-binary --break-system-packages -q
echo "✓ psycopg2-binary installed"

echo ""
echo "[2/3] Initializing Grafana Postgres schema in ${POSTGRES_CONTAINER}..."
docker exec -i -e PGPASSWORD="$GRAFANA_PG_PASSWORD" "$POSTGRES_CONTAINER" \
  psql -U "$GRAFANA_PG_USER" -d "$GRAFANA_PG_DB" \
  < "$PROJECT_DIR/grafana/sql/init.sql"
echo "✓ Grafana Postgres schema ready"

echo ""
echo "[3/3] Verifying tables..."
docker exec -e PGPASSWORD="$GRAFANA_PG_PASSWORD" "$POSTGRES_CONTAINER" \
  psql -U "$GRAFANA_PG_USER" -d "$GRAFANA_PG_DB" \
  -c "\dt crypto_*"

echo ""
echo "=== Grafana setup complete ==="
echo "Start Spark with ENABLE_GRAFANA_POSTGRES=1 to populate the Grafana tables."
