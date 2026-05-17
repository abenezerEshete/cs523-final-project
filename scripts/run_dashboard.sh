#!/bin/bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Port 10000 is used because 4040/8088/9870/16010 are taken by Hadoop/HBase/Spark
echo "Starting Streamlit dashboard on http://localhost:10000"
echo ""

python3 -m streamlit run "$PROJECT_DIR/src/dashboard.py" \
  --server.port 10000 \
  --server.address 0.0.0.0 \
  --server.headless true
