#!/bin/bash
echo "=== CS523 Pipeline Reset ==="
echo "This will clear ALL streamed data. Ctrl+C to cancel, Enter to continue"
read

# ── Stop Kafka and ZooKeeper ─────────────────────────────────
echo "[1/5] Stopping Kafka and ZooKeeper..."
/opt/kafka/bin/kafka-server-stop.sh 2>/dev/null
sleep 5
/opt/kafka/bin/zookeeper-server-stop.sh 2>/dev/null
sleep 5
echo "✓ Stopped"

# ── Clear Kafka data ─────────────────────────────────────────
echo "[2/5] Clearing Kafka logs and ZooKeeper state..."
rm -rf /tmp/kafka-logs/
rm -rf /tmp/zookeeper/
echo "✓ Kafka data cleared"

# ── Clear Spark checkpoints ───────────────────────────────────
echo "[3/5] Clearing Spark checkpoints..."
rm -rf /tmp/checkpoint/
rm -rf /tmp/spark-checkpoints/
echo "✓ Spark checkpoints cleared"

# ── Restart Kafka stack ───────────────────────────────────────
echo "[4/5] Restarting ZooKeeper and Kafka..."
/opt/kafka/bin/zookeeper-server-start.sh \
  -daemon /opt/kafka/config/zookeeper.properties
sleep 6

unset CLASSPATH HADOOP_CONF_DIR HADOOP_HOME
CLASSPATH="" HADOOP_CONF_DIR="" HADOOP_HOME="" \
/opt/kafka/bin/kafka-server-start.sh \
  -daemon /opt/kafka/config/server.properties
sleep 10

tail -3 /opt/kafka/logs/server.log | grep -q "started" \
  && echo "✓ Kafka broker restarted" \
  || echo "✗ Kafka failed to restart — check /opt/kafka/logs/server.log"

# ── Recreate Kafka topic ──────────────────────────────────────
unset CLASSPATH HADOOP_CONF_DIR HADOOP_HOME
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic crypto-trades \
  --partitions 3 \
  --replication-factor 1
echo "✓ Kafka topic 'crypto-trades' recreated"

# ── Truncate HBase tables ─────────────────────────────────────
echo "[5/5] Truncating HBase tables..."
/opt/hbase/bin/hbase shell << 'HBASE'
truncate 'crypto_windowed'
truncate 'crypto_moving_avg'
truncate 'crypto_anomalies'
exit
HBASE
echo "✓ HBase tables cleared"

echo ""
echo "=== Reset complete! ==="
echo ""
echo "Run the pipeline again:"
echo "  Terminal 1: python3 src/producer.py"
echo "  Terminal 2: bash scripts/run_spark.sh spark_to_hbase"
echo "  Terminal 3: bash scripts/run_dashboard.sh"
