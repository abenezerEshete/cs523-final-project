#!/bin/bash
# ─── Start all services required for CS523 Final Project ───

echo "[1/5] Starting ZooKeeper..."
/opt/kafka/bin/zookeeper-server-start.sh \
  -daemon /opt/kafka/config/zookeeper.properties
sleep 6

echo "[2/5] Starting Kafka broker..."
unset CLASSPATH HADOOP_CONF_DIR HADOOP_HOME
CLASSPATH="" HADOOP_CONF_DIR="" HADOOP_HOME="" \
/opt/kafka/bin/kafka-server-start.sh \
  -daemon /opt/kafka/config/server.properties
sleep 10

echo "[3/5] Starting HBase..."
/opt/hbase/bin/start-hbase.sh
sleep 8

echo "[4/5] Starting HBase Thrift server..."
/opt/hbase/bin/hbase thrift start &
sleep 8

echo "[5/5] Starting HDFS..."
/opt/hadoop/sbin/start-dfs.sh
sleep 6

echo ""
echo "✓ All services started. Verifying..."
echo ""

# Verify Kafka
tail -2 /opt/kafka/logs/server.log | grep -q "started" \
  && echo "✓ Kafka broker: OK" \
  || echo "✗ Kafka broker: FAILED — check /opt/kafka/logs/server.log"

# Verify HBase
ps aux | grep -q "HMaster" \
  && echo "✓ HBase Master: OK" \
  || echo "✗ HBase Master: FAILED"

# Verify Thrift
ps aux | grep -q "ThriftServer" \
  && echo "✓ HBase Thrift: OK" \
  || echo "✗ HBase Thrift: FAILED"

# Verify HDFS
/opt/hadoop/bin/hdfs dfs -ls / > /dev/null 2>&1 \
  && echo "✓ HDFS: OK" \
  || echo "✗ HDFS: FAILED"

echo ""
echo "Done. Proceed with setup.sh to initialize Kafka topic and HBase tables."
