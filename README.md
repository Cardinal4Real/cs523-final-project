# CS523 Big Data Technology — Final Project

## Architecture
Coinbase API (live prices)
↓ Python producer — polls 5 symbols every second
Kafka Topic: "crypto-trades" (3 partitions)
↓ Spark Structured Streaming
├── 30s windowed aggregations (avg/min/max/volume)
├── 2-min sliding moving average
└── Anomaly detection (price range %)
↓ foreachBatch → happybase
HBase (3 tables)
├── crypto_windowed
├── crypto_moving_avg
└── crypto_anomalies
↓
Streamlit Dashboard (live charts + KPI cards + anomaly log)
↓ BONUS (Part 5)
HDFS static dataset joined via Spark → enriched HBase rows
## Environment

| Component | Version |
|-----------|---------|
| Docker image | mmukadam/cs523bdt-lab:v4.0 |
| Kafka | 3.4.0 |
| Spark | 3.1.2 (Scala 2.12) |
| HBase | 2.2.0 |
| Hadoop | 3.2.1 |
| Python | 3.12.3 |

## Prerequisites

- Docker Desktop installed and running
- The course lab container running:

## Quick Start

### Step 1 — Clone the repository

```bash
git clone git@github.com:Cardinal4Real/cs523-final-project.git
```

### Step 2 — Copy files into the container

```bash
docker cp cs523-final-project/. cs523bdt-lab:/opt/my_code/cs523-final-project/
docker exec -it cs523bdt-lab bash
cd /opt/my_code/cs523-final-project
```

### Step 3 — Start all services

```bash
bash scripts/start_services.sh
```

### Step 4 — Run one-time setup

```bash
bash scripts/setup.sh
```

### Step 5 — Open 3 terminals and run the pipeline

**Terminal 1 — Kafka Producer:**
```bash
docker exec -it cs523bdt-lab bash
cd /opt/my_code/cs523-final-project
python3 src/producer.py
```

**Terminal 2 — Spark Streaming → HBase:**
```bash
docker exec -it cs523bdt-lab bash
cd /opt/my_code/cs523-final-project
bash scripts/run_spark.sh spark_to_hbase
```

**Terminal 3 — Streamlit Dashboard:**
```bash
docker exec -it cs523bdt-lab bash
cd /opt/my_code/cs523-final-project
bash scripts/run_dashboard.sh
```

Open browser at: **http://localhost:10000**

### Optional — Part 5 Bonus (Spark SQL + HDFS join)

```bash
# Instead of spark_to_hbase, run the enriched job in Terminal 2:
bash scripts/run_spark.sh spark_sql_enriched
```

## Project Structure
## Project Structure
cs523-final-project/
├── README.md
├── data/
│   └── coin_metadata.csv          # Static dataset uploaded to HDFS
├── docs/
│   └── FinalProject.pdf           # Original project specification
├── scripts/
│   ├── start_services.sh          # Start ZooKeeper/Kafka/HBase/HDFS
│   ├── setup.sh                   # One-time: pip, jars, topic, tables, HDFS
│   ├── run_spark.sh               # Submit any Spark job
│   └── run_dashboard.sh           # Launch Streamlit dashboard
└── src/
├── producer.py                # Part 1: Kafka producer (Coinbase API)
├── spark_streaming.py         # Part 2: Spark → console output
├── spark_to_hbase.py          # Part 3: Spark → HBase
├── dashboard.py               # Part 4: Streamlit dashboard
└── spark_sql_enriched.py      # Part 5: Spark SQL join with HDFS



## Known Issues & Fixes

### ZooKeeper version conflict
Kafka 3.4.0 needs ZooKeeper 3.6.3 but Hadoop injects 3.4.13.

**Fix:** Always unset Hadoop env vars before starting Kafka:
```bash
unset CLASSPATH HADOOP_CONF_DIR HADOOP_HOME
CLASSPATH="" HADOOP_CONF_DIR="" HADOOP_HOME="" \
/opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties
```

### pip not available on Python 3.12
```bash
curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py
python3 /tmp/get-pip.py --break-system-packages
```

## Data Flow Details

| Stage | Detail |
|-------|--------|
| Source | Coinbase spot prices: BTC, ETH, SOL, XRP, BNB |
| Kafka topic | `crypto-trades`, 3 partitions, replication factor 1 |
| Spark window 1 | 30s tumbling — avg/min/max price + total volume |
| Spark window 2 | 2min sliding (30s slide) — moving average |
| Spark window 3 | 1min tumbling — anomaly detection (>0.5% range) |
| HBase row key | `SYMBOL_windowstart` e.g. `BTCUSD_2026-05-09 15:10:00` |
| Dashboard port | 4040 (mapped from container) |

## Team Members

- [John Edem Adamfo]
- [Justine Okumu]
- [Abenezer Eshete Tilahun]
