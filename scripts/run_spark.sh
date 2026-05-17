#!/bin/bash
# Usage: bash scripts/run_spark.sh [spark_streaming|spark_to_hbase|spark_sql_enriched]

SCRIPT="${1:-spark_to_hbase}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JARS="/opt/spark/kafka-jars/spark-sql-kafka-0-10_2.12-3.1.2.jar,\
/opt/spark/kafka-jars/kafka-clients-2.6.0.jar,\
/opt/spark/kafka-jars/spark-token-provider-kafka-0-10_2.12-3.1.2.jar,\
/opt/spark/kafka-jars/commons-pool2-2.6.2.jar"

unset CLASSPATH HADOOP_CONF_DIR HADOOP_HOME

echo "Starting Spark job: src/${SCRIPT}.py"
echo ""

/opt/spark/bin/spark-submit \
  --master local[*] \
  --jars "$JARS" \
  "$PROJECT_DIR/src/${SCRIPT}.py"
