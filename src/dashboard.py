import streamlit as st
import happybase
import pandas as pd
import time

st.set_page_config(
    page_title="Crypto Trade Analytics",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Real-Time Crypto Trade Analytics")
st.caption("Live data from Coinbase → Kafka → Spark → HBase")

# ── Sidebar ──────────────────────────────────────────────────
st.sidebar.header("Controls")
selected_symbol = st.sidebar.selectbox(
    "Select Symbol",
    ["BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "BNBUSD"]
)
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 15)
st.sidebar.markdown("---")
st.sidebar.markdown("**Pipeline Status**")
st.sidebar.success("Kafka: Running")
st.sidebar.success("Spark: Streaming")
st.sidebar.success("HBase: Connected")

# ── HBase reader functions ───────────────────────────────────
def format_hbase_error(table_name, error):
    message = str(error)
    if "TableNotFoundException" in message:
        return (
            f"HBase table `{table_name}` does not exist yet. "
            "Run `bash scripts/setup.sh` inside `/opt/my_code/cs523-final-project`, "
            "wait for `Setup complete`, then restart Spark and refresh this page."
        )

    first_line = message.splitlines()[0] if message else repr(error)
    return f"HBase error while reading `{table_name}`: {first_line}"


def read_windowed(symbol, limit=20):
    rows = []
    table_name = "crypto_windowed"
    try:
        conn = happybase.Connection("localhost")
        table = conn.table(table_name)
        for key, data in table.scan(row_prefix=symbol.encode(), limit=limit):
            rows.append({
                "window":       key.decode().replace(symbol + "_", ""),
                "avg_price":    float(data.get(b"price:avg", 0)),
                "min_price":    float(data.get(b"price:min", 0)),
                "max_price":    float(data.get(b"price:max", 0)),
                "total_volume": float(data.get(b"volume:total", 0)),
            })
        conn.close()
    except Exception as e:
        st.error(format_hbase_error(table_name, e))
    return pd.DataFrame(rows)

def read_moving_avg(symbol, limit=20):
    rows = []
    table_name = "crypto_moving_avg"
    try:
        conn = happybase.Connection("localhost")
        table = conn.table(table_name)
        for key, data in table.scan(row_prefix=symbol.encode(), limit=limit):
            rows.append({
                "window":           key.decode().replace(symbol + "_", ""),
                "moving_avg_price": float(data.get(b"price:moving_avg", 0)),
                "total_volume":     float(data.get(b"volume:total", 0)),
            })
        conn.close()
    except Exception as e:
        st.error(format_hbase_error(table_name, e))
    return pd.DataFrame(rows)

def read_anomalies(symbol, limit=20):
    rows = []
    table_name = "crypto_anomalies"
    try:
        conn = happybase.Connection("localhost")
        table = conn.table(table_name)
        for key, data in table.scan(row_prefix=symbol.encode(), limit=limit):
            rows.append({
                "window":           key.decode().replace(symbol + "_", ""),
                "avg_price":        float(data.get(b"price:avg", 0)),
                "max_price":        float(data.get(b"price:max", 0)),
                "min_price":        float(data.get(b"price:min", 0)),
                "price_range_pct":  float(data.get(b"alert:price_range_pct", 0)),
                "is_anomaly":       data.get(b"alert:is_anomaly", b"False").decode(),
            })
        conn.close()
    except Exception as e:
        st.error(format_hbase_error(table_name, e))
    return pd.DataFrame(rows)

# ── Dashboard loop ───────────────────────────────────────────
placeholder = st.empty()

while True:
    with placeholder.container():

        df_win  = read_windowed(selected_symbol)
        df_mavg = read_moving_avg(selected_symbol)
        df_anom = read_anomalies(selected_symbol)

        # Row 1: KPI cards
        st.subheader(f"📊 {selected_symbol} — Latest Window")
        if not df_win.empty:
            latest = df_win.iloc[-1]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Avg Price",    f"${latest['avg_price']:,.4f}")
            c2.metric("Min Price",    f"${latest['min_price']:,.4f}")
            c3.metric("Max Price",    f"${latest['max_price']:,.4f}")
            c4.metric("Total Volume", f"{latest['total_volume']:.4f}")
        else:
            st.info("Waiting for windowed data...")

        st.markdown("---")

        # Row 2: Charts
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Avg Price per 30s Window")
            if not df_win.empty:
                st.line_chart(
                    df_win[["window","avg_price","min_price","max_price"]].set_index("window")
                )
            else:
                st.info("No data yet...")

        with col2:
            st.subheader("2-Minute Moving Average")
            if not df_mavg.empty:
                df_dedup = df_mavg.drop_duplicates(subset=["window"]).set_index("window")
                st.line_chart(df_dedup[["moving_avg_price"]])
            else:
                st.info("No data yet...")

        st.markdown("---")

        # Row 3: Anomaly table
        st.subheader("🚨 Anomaly Detection Log")
        if not df_anom.empty:
            anomaly_count = (df_anom["is_anomaly"] == "True").sum()
            if anomaly_count > 0:
                st.warning(f"⚠️ {anomaly_count} anomalies detected!")
            else:
                st.success("No anomalies detected in current data")
            st.dataframe(df_anom, width="stretch", height=300)
        else:
            st.info("No anomaly data yet...")

        st.markdown("---")
        st.caption(f"Refreshing every {refresh_interval}s | Last update: {pd.Timestamp.now().strftime('%H:%M:%S')}")

    time.sleep(refresh_interval)
    placeholder.empty()
