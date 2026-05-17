#!/bin/bash
# ─── Start all services required for CS523 Final Project ───

set -u

is_running() {
  pgrep -f "$1" >/dev/null 2>&1
}

wait_for_kafka() {
  local attempt

  for attempt in $(seq 1 12); do
    if timeout 10 /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 \
      --list >/tmp/kafka_start_check.log 2>&1; then
      return 0
    fi
    sleep 5
  done

  return 1
}

wait_for_hdfs() {
  local attempt

  for attempt in $(seq 1 12); do
    if /opt/hadoop/bin/hdfs dfs -ls / >/tmp/hdfs_start_check.log 2>&1; then
      return 0
    fi
    sleep 5
  done

  return 1
}

wait_for_hbase() {
  local attempt

  for attempt in $(seq 1 12); do
    if timeout 20 /opt/hbase/bin/hbase shell -n >/tmp/hbase_start_check.log 2>&1 <<'HBASE'
status
HBASE
    then
      return 0
    fi
    sleep 5
  done

  return 1
}

echo "[1/5] ZooKeeper"
if is_running "org.apache.zookeeper.server.quorum.QuorumPeerMain"; then
  echo "✓ ZooKeeper already running"
else
  /opt/kafka/bin/zookeeper-server-start.sh \
    -daemon /opt/kafka/config/zookeeper.properties
  sleep 6
  is_running "org.apache.zookeeper.server.quorum.QuorumPeerMain" \
    && echo "✓ ZooKeeper started" \
    || echo "✗ ZooKeeper failed to start"
fi

echo "[2/5] Kafka broker"
unset CLASSPATH HADOOP_CONF_DIR HADOOP_HOME
if is_running "kafka.Kafka"; then
  echo "✓ Kafka already running"
else
  CLASSPATH="" HADOOP_CONF_DIR="" HADOOP_HOME="" \
    /opt/kafka/bin/kafka-server-start.sh \
    -daemon /opt/kafka/config/server.properties
fi

if wait_for_kafka; then
  echo "✓ Kafka broker ready"
else
  echo "✗ Kafka broker not ready; check /opt/kafka/logs/server.log"
fi

echo "[3/5] HDFS"
if is_running "org.apache.hadoop.hdfs.server.namenode.NameNode" \
  && is_running "org.apache.hadoop.hdfs.server.datanode.DataNode"; then
  echo "✓ HDFS already running"
else
  /opt/hadoop/sbin/start-dfs.sh >/tmp/hdfs-start.log 2>&1
fi

if wait_for_hdfs; then
  echo "✓ HDFS ready"
else
  echo "✗ HDFS not ready; check /opt/hadoop/logs"
fi

echo "[4/5] HBase"
if is_running "org.apache.hadoop.hbase.master.HMaster" \
  && is_running "org.apache.hadoop.hbase.regionserver.HRegionServer"; then
  echo "✓ HBase master/regionserver already running"
else
  /opt/hbase/bin/start-hbase.sh >/tmp/hbase-start.log 2>&1
fi

if wait_for_hbase; then
  echo "✓ HBase ready"
else
  echo "✗ HBase not ready yet"
  echo "  If setup later reports hbase:meta is not online, run:"
  echo "  RESET_HBASE=1 bash scripts/setup.sh"
fi

echo "[5/5] HBase Thrift"
if is_running "org.apache.hadoop.hbase.thrift.ThriftServer"; then
  echo "✓ HBase Thrift already running"
else
  nohup /opt/hbase/bin/hbase thrift start >/tmp/hbase-thrift.log 2>&1 &
  sleep 8
fi

is_running "org.apache.hadoop.hbase.thrift.ThriftServer" \
  && echo "✓ HBase Thrift ready" \
  || echo "✗ HBase Thrift failed; check /tmp/hbase-thrift.log"

echo ""
echo "Service startup complete. Next run:"
echo "  bash scripts/setup.sh"
