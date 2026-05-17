# CS523 Big Data Technology - Final Project

Real-time cryptocurrency analytics pipeline using Coinbase prices, Kafka, Spark Structured Streaming, HBase, HDFS, Streamlit, and optional Grafana.

## What It Does

```text
Coinbase live prices
  -> Python producer
  -> Kafka topic: crypto-trades
  -> Spark Structured Streaming
  -> HBase tables
  -> Streamlit dashboard

Optional:
Spark also writes aggregates to Postgres for a Grafana dashboard.
```

The Spark job calculates:

- 30-second average, minimum, maximum price, and volume
- 2-minute moving average
- 1-minute anomaly detection based on price range percentage

## Prerequisites

- Docker
- The CS523 lab image or container named `cs523bdt-lab`
- The course `CLASS_PASS`, if your lab image requires it
- Docker Compose only if you want the optional Grafana dashboard

The project must be copied into the lab container at:

```text
/opt/my_code/cs523-final-project
```

Do not commit the course password to this repository.

## Project Files

```text
data/coin_metadata.csv                 Static HDFS metadata
scripts/start_services.sh              Starts Kafka, HBase, HBase Thrift, HDFS
scripts/setup.sh                       Installs deps, creates Kafka topic/tables
scripts/run_spark.sh                   Runs Spark jobs
scripts/run_dashboard.sh               Starts Streamlit
scripts/setup_grafana.sh               Initializes optional Grafana/Postgres path
src/producer.py                        Coinbase -> Kafka producer
src/spark_to_hbase.py                  Spark -> HBase and optional Postgres
src/spark_sql_enriched.py              Optional HDFS metadata enrichment job
src/dashboard.py                       Streamlit dashboard
docker-compose.grafana.yml             Optional Grafana/Postgres services
grafana/                               Grafana provisioning and SQL schema
```

## Start From Scratch With `cs523bdt-lab`

Run these commands from the host machine.

### 1. Go to the project folder

```bash
cd /path/to/cs523-final-project
export PROJECT_DIR="$(pwd)"
```

### 2. Start or create the lab container

If `cs523bdt-lab` already exists:

```bash
docker start cs523bdt-lab
docker ps --filter name=cs523bdt-lab
```

Important: Docker port mappings are set only when the container is created.
For the Streamlit dashboard to open from the host, `docker port cs523bdt-lab`
must show `10000/tcp`. If it does not, recreate the container with
`-p 10000:10000`.

### 3. Copy the project into the container

```bash
docker exec cs523bdt-lab mkdir -p /opt/my_code/cs523-final-project
docker cp "$PROJECT_DIR"/. cs523bdt-lab:/opt/my_code/cs523-final-project/
```

Verify:

```bash
docker exec cs523bdt-lab bash -lc 'ls /opt/my_code/cs523-final-project'
```

### 4. Start Kafka, HBase, Thrift, and HDFS

```bash
docker exec cs523bdt-lab bash -lc \
  'cd /opt/my_code/cs523-final-project && bash scripts/start_services.sh'
```

### 5. Run one-time project setup

```bash
docker exec cs523bdt-lab bash -lc \
  'cd /opt/my_code/cs523-final-project && bash scripts/setup.sh'
```

This setup creates:

- Kafka topic: `crypto-trades`
- HBase tables: `crypto_windowed`, `crypto_moving_avg`, `crypto_anomalies`
- HDFS file: `/cs523/static/coin_metadata.csv`

## Start the App

Start the producer:

```bash
docker exec -d cs523bdt-lab bash -lc \
  'cd /opt/my_code/cs523-final-project &&
   : > /tmp/crypto_producer.log &&
   python3 -u src/producer.py >> /tmp/crypto_producer.log 2>&1'
```

Start Spark streaming to HBase:

```bash
docker exec -d cs523bdt-lab bash -lc \
  'cd /opt/my_code/cs523-final-project &&
   : > /tmp/crypto_spark.log &&
   bash scripts/run_spark.sh spark_to_hbase >> /tmp/crypto_spark.log 2>&1'
```

Start Streamlit:

```bash
docker exec -d cs523bdt-lab bash -lc \
  'cd /opt/my_code/cs523-final-project &&
   : > /tmp/crypto_dashboard.log &&
   bash scripts/run_dashboard.sh >> /tmp/crypto_dashboard.log 2>&1'
```

Open the Streamlit dashboard:

```text
http://localhost:10000
```

## Optional Grafana Dashboard

Grafana is optional. Streamlit still works without it.

Grafana needs two extra containers: `crypto-postgres` and `crypto-grafana`.
Start them from the project folder:

If port `3000` is busy, choose another host port before starting Grafana:

```bash
export GRAFANA_HOST_PORT=3001
```

```bash
cd "$PROJECT_DIR"
PROJECT_DIR="$PROJECT_DIR" \
docker compose \
  -p cs523-grafana \
  -f docker-compose.grafana.yml \
  up -d crypto-postgres grafana
```

Connect the lab container to the Grafana compose network so Spark can reach
Postgres by the hostname `crypto-postgres`:

```bash
docker network connect cs523-grafana_default cs523bdt-lab
```

If Docker says the container is already connected, ignore that message.

Defaults:

| Item | Value |
| --- | --- |
| Grafana URL | `http://localhost:3000` |
| Grafana login | `admin` / `admin` |
| Postgres host port | `5433` |
| Postgres database | `crypto_analytics` |
| Postgres user | `crypto` |
| Postgres password | `crypto_password` |

If you set `GRAFANA_HOST_PORT`, use that port in the browser URL.

Initialize the Grafana tables and Spark Postgres dependency:

```bash
cd "$PROJECT_DIR"
bash scripts/setup_grafana.sh
```

For Grafana data, start Spark with Postgres writes enabled:

```bash
docker exec -d cs523bdt-lab bash -lc \
  'cd /opt/my_code/cs523-final-project &&
   : > /tmp/crypto_spark.log &&
   ENABLE_GRAFANA_POSTGRES=1 \
   GRAFANA_PG_HOST=crypto-postgres \
   GRAFANA_PG_PORT=5432 \
   GRAFANA_PG_DB=crypto_analytics \
   GRAFANA_PG_USER=crypto \
   GRAFANA_PG_PASSWORD=crypto_password \
   bash scripts/run_spark.sh spark_to_hbase >> /tmp/crypto_spark.log 2>&1'
```

Open Grafana at `http://localhost:3000`, or at your chosen
`GRAFANA_HOST_PORT`. The provisioned dashboard is under the `CS523` folder and
includes 15 widgets for price, volume, moving averages, anomalies, freshness,
and recent rows.

## Optional Enriched Job

Run this instead of `spark_to_hbase` if you want the HDFS metadata enrichment:

```bash
docker exec -d cs523bdt-lab bash -lc \
  'cd /opt/my_code/cs523-final-project &&
   : > /tmp/crypto_spark_enriched.log &&
   bash scripts/run_spark.sh spark_sql_enriched >> /tmp/crypto_spark_enriched.log 2>&1'
```

This requires `/cs523/static/coin_metadata.csv`, which `scripts/setup.sh` uploads.

## Useful Checks

Check app processes:

```bash
docker exec cs523bdt-lab bash -lc \
  'pgrep -af "src/producer.py|spark_to_hbase.py|spark_sql_enriched.py|streamlit|dashboard.py"'
```

View logs:

```bash
docker exec cs523bdt-lab tail -f /tmp/crypto_producer.log
docker exec cs523bdt-lab tail -f /tmp/crypto_spark.log
docker exec cs523bdt-lab tail -f /tmp/crypto_dashboard.log
```

Check Streamlit:

```bash
curl -sI http://localhost:10000
```

Check exposed lab-container ports:

```bash
docker port cs523bdt-lab
```

Check Grafana:

```bash
curl -s "http://localhost:${GRAFANA_HOST_PORT:-3000}/api/health"
```

Check Grafana Postgres rows:

```bash
docker exec -e PGPASSWORD=crypto_password crypto-postgres \
  psql -U crypto -d crypto_analytics \
  -c 'SELECT count(*) FROM crypto_windowed;'
```

## Troubleshooting

If `docker run` shows `ACCESS DENIED`, the course image needs `CLASS_PASS`.
Create the container with the instructor-provided password using
`-e CLASS_PASS=...`.

If `docker cp` fails because `/opt/my_code` does not exist, create the folder:

```bash
docker exec cs523bdt-lab mkdir -p /opt/my_code/cs523-final-project
docker cp . cs523bdt-lab:/opt/my_code/cs523-final-project/
```

If Grafana shows no data, confirm Spark was started with `ENABLE_GRAFANA_POSTGRES=1`, confirm `crypto-postgres` has rows, and set the Grafana time range to a recent window such as "Last 30 minutes".

If Spark cannot connect to `crypto-postgres`, make sure this command was run:

```bash
docker network connect cs523-grafana_default cs523bdt-lab
```

## Team Members

- John Edem Adamfo
- Justine Okumu
- Abenezer Eshete Tilahun
