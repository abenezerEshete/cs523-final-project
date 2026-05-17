#!/bin/bash
# ─── One-time setup: install deps, create Kafka topic, HBase tables, upload to HDFS ───

set -euo pipefail

echo "=== CS523 Final Project Setup ==="
echo ""

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

get_hbase_zk_quorum() {
  python3 - <<'PY'
import xml.etree.ElementTree as ET

root = ET.parse("/opt/hbase/conf/hbase-site.xml").getroot()
for prop in root.findall("property"):
    name = prop.findtext("name")
    if name == "hbase.zookeeper.quorum":
        print(prop.findtext("value") or "localhost")
        break
else:
    print("localhost")
PY
}

reset_hbase_state() {
  local zk_quorum
  zk_quorum="$(get_hbase_zk_quorum)"

  echo "RESET_HBASE=1 set; resetting lab HBase state..."
  echo "This removes HBase data under /hbase in HDFS and clears /hbase in ZooKeeper."

  /opt/hbase/bin/stop-hbase.sh >/dev/null 2>&1 || true
  pkill -f "org.apache.hadoop.hbase.thrift.ThriftServer|org.apache.hadoop.hbase.master.HMaster|org.apache.hadoop.hbase.regionserver.HRegionServer|org.jruby.Main" >/dev/null 2>&1 || true
  sleep 5

  /opt/hadoop/bin/hdfs dfs -rm -r -skipTrash /hbase >/dev/null 2>&1 || true
  printf 'deleteall /hbase\nquit\n' | /opt/kafka/bin/zookeeper-shell.sh "${zk_quorum}:2181" >/dev/null 2>&1 || true

  /opt/hbase/bin/start-hbase.sh
  sleep 15
  /opt/hbase/bin/hbase thrift start >/tmp/hbase-thrift.log 2>&1 &
  sleep 5
}

wait_for_hbase() {
  local attempt

  for attempt in $(seq 1 12); do
    if timeout 20 /opt/hbase/bin/hbase shell -n >/tmp/hbase_status_check.log 2>&1 <<'HBASE'
status
HBASE
    then
      echo "✓ HBase is ready"
      return 0
    fi

    echo "Waiting for HBase to become ready (${attempt}/12)..."
    sleep 5
  done

  echo ""
  echo "ERROR: HBase is not ready."
  echo "If /tmp/hbase_status_check.log mentions hbase:meta is not online,"
  echo "rerun setup with:"
  echo "  RESET_HBASE=1 bash scripts/setup.sh"
  echo ""
  echo "Last HBase status output:"
  tail -40 /tmp/hbase_status_check.log || true
  return 1
}

# ── Python dependencies ───────────────────────────────────────
echo "[1/4] Installing Python dependencies..."
curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py 2>/dev/null
python3 /tmp/get-pip.py --break-system-packages -q
python3 -m pip install \
  kafka-python \
  websocket-client \
  happybase \
  streamlit \
  pandas \
  --break-system-packages -q
echo "✓ Python dependencies installed"

# ── Spark Kafka connector jars ────────────────────────────────
echo ""
echo "[2/4] Downloading Spark-Kafka connector jars..."
mkdir -p /opt/spark/kafka-jars
pushd /opt/spark/kafka-jars >/dev/null

SPARK_KAFKA="spark-sql-kafka-0-10_2.12-3.1.2.jar"
KAFKA_CLIENT="kafka-clients-2.6.0.jar"
TOKEN_PROVIDER="spark-token-provider-kafka-0-10_2.12-3.1.2.jar"
COMMONS_POOL="commons-pool2-2.6.2.jar"
BASE="https://repo1.maven.org/maven2"

[ -f "$SPARK_KAFKA" ]    || curl -sO "$BASE/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/$SPARK_KAFKA"
[ -f "$KAFKA_CLIENT" ]   || curl -sO "$BASE/org/apache/kafka/kafka-clients/2.6.0/$KAFKA_CLIENT"
[ -f "$TOKEN_PROVIDER" ] || curl -sO "$BASE/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.1.2/$TOKEN_PROVIDER"
[ -f "$COMMONS_POOL" ]   || curl -sO "$BASE/org/apache/commons/commons-pool2/2.6.2/$COMMONS_POOL"

echo "✓ Spark-Kafka jars ready at /opt/spark/kafka-jars/"
popd >/dev/null

# ── Kafka topic ───────────────────────────────────────────────
echo ""
echo "[3/4] Creating Kafka topic..."
unset CLASSPATH HADOOP_CONF_DIR HADOOP_HOME

TOPIC_EXISTS=$(/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list 2>/dev/null | grep "crypto-trades")

if [ -z "$TOPIC_EXISTS" ]; then
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic crypto-trades \
    --partitions 3 \
    --replication-factor 1
  echo "✓ Kafka topic 'crypto-trades' created"
else
  echo "✓ Kafka topic 'crypto-trades' already exists"
fi

# ── HBase tables ──────────────────────────────────────────────
echo ""
echo "[4/4] Creating HBase tables and uploading static data to HDFS..."

if [ "${RESET_HBASE:-0}" = "1" ]; then
  reset_hbase_state
fi

wait_for_hbase

timeout 180 /opt/hbase/bin/hbase shell -n << 'HBASE'
tables = ['crypto_windowed', 'crypto_moving_avg', 'crypto_anomalies']

tables.each do |table_name|
  if exists(table_name)
    disable table_name if is_enabled(table_name)
    drop table_name
  end
end

create 'crypto_windowed',   'price', 'volume'
create 'crypto_moving_avg', 'price', 'volume'
create 'crypto_anomalies',  'price', 'alert'
list
HBASE

/opt/hadoop/bin/hdfs dfs -mkdir -p /cs523/static
/opt/hadoop/bin/hdfs dfs -put -f \
  "$PROJECT_DIR/data/coin_metadata.csv" \
  /cs523/static/coin_metadata.csv

echo "✓ HBase tables created"
echo "✓ coin_metadata.csv uploaded to HDFS at /cs523/static/"

echo ""
echo "=== Setup complete! ==="
echo ""
echo "Next steps — open 3 terminals and run:"
echo "  Terminal 1 (Producer):   python3 src/producer.py"
echo "  Terminal 2 (Spark):      bash scripts/run_spark.sh spark_to_hbase"
echo "  Terminal 3 (Dashboard):  bash scripts/run_dashboard.sh"
