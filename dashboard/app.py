"""
Retail-facing dashboard.

Consumes the `sentiment-insights` Kafka topic (produced by the Spark job)
in a background thread and renders live sentiment + momentum signals in
plain English, so a non-technical retail user can glance at the page and
understand "is the market feeling good or bad about this stock right now"
without needing to interpret raw scores.

Run with: streamlit run dashboard/app.py
"""

import json
import sys
import threading
from collections import deque
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from kafka import KafkaConsumer

sys.path.append("..")
from config.settings import KAFKA_BOOTSTRAP_SERVERS, TOPIC_SENTIMENT_INSIGHTS

MAX_POINTS = 500

st.set_page_config(page_title="Live Financial Sentiment", layout="wide")


@st.cache_resource
def get_shared_buffer():
    """A thread-safe deque shared across Streamlit reruns to hold the
    latest messages consumed from Kafka."""
    return deque(maxlen=MAX_POINTS), threading.Lock()


def consume_loop(buffer: deque, lock: threading.Lock, stop_flag: dict):
    consumer = KafkaConsumer(
        TOPIC_SENTIMENT_INSIGHTS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    while not stop_flag["stop"]:
        for msg in consumer:
            with lock:
                buffer.append(msg.value)
            if stop_flag["stop"]:
                break


@st.cache_resource
def start_consumer_thread(_buffer, _lock):
    stop_flag = {"stop": False}
    t = threading.Thread(target=consume_loop, args=(_buffer, _lock, stop_flag), daemon=True)
    t.start()
    return stop_flag


SIGNAL_COLORS = {
    "Strong Bullish": "#0f9d58",
    "Bullish": "#66bb6a",
    "Mixed / Neutral": "#9e9e9e",
    "Bearish": "#ef5350",
    "Strong Bearish": "#c62828",
}


def render():
    st.title("📈 Real-Time Financial Sentiment")
    st.caption(
        "Live NLP-driven sentiment fused with price momentum — "
        "streamed via Kafka, processed with Spark Structured Streaming."
    )

    buffer, lock = get_shared_buffer()
    start_consumer_thread(buffer, lock)

    with lock:
        data = list(buffer)

    if not data:
        st.info("Waiting for streaming data... make sure the producer and Spark job are running.")
        st.stop()

    df = pd.DataFrame(data)
    df["window_start"] = pd.to_datetime(df["window_start"])
    df = df.sort_values("window_start")

    latest_per_ticker = df.sort_values("window_start").groupby("ticker").tail(1)

    # --- Plain-language signal cards, one per ticker -----------------------
    cols = st.columns(len(latest_per_ticker)) if len(latest_per_ticker) else [st]
    for col_widget, (_, row) in zip(cols, latest_per_ticker.iterrows()):
        color = SIGNAL_COLORS.get(row["signal"], "#9e9e9e")
        col_widget.markdown(
            f"""
            <div style="border-radius:10px;padding:14px;background:{color}22;
                        border:1px solid {color};">
                <div style="font-size:14px;color:#666;">{row['ticker']}</div>
                <div style="font-size:22px;font-weight:700;color:{color};">
                    {row['signal']}
                </div>
                <div style="font-size:13px;color:#444;">
                    Price ${row['last_close']:.2f} &nbsp;|&nbsp;
                    Momentum {row['avg_momentum_pct']:+.2f}%
                </div>
                <div style="font-size:12px;color:#888;">
                    News sentiment: {row['avg_sentiment']:+.2f}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.divider()

    # --- Sentiment over time, per ticker ------------------------------------
    st.subheader("Sentiment trend")
    fig = px.line(
        df, x="window_start", y="avg_sentiment", color="ticker",
        labels={"window_start": "Time", "avg_sentiment": "News sentiment (-1 to 1)"},
    )
    fig.add_hline(y=0, line_dash="dot", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)

    # --- Momentum over time, per ticker -------------------------------------
    st.subheader("Price momentum")
    fig2 = px.line(
        df, x="window_start", y="avg_momentum_pct", color="ticker",
        labels={"window_start": "Time", "avg_momentum_pct": "Momentum (%)"},
    )
    fig2.add_hline(y=0, line_dash="dot", line_color="gray")
    st.plotly_chart(fig2, use_container_width=True)

    # --- Raw feed table ------------------------------------------------------
    with st.expander("Raw signal feed"):
        st.dataframe(df.sort_values("window_start", ascending=False), use_container_width=True)

    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')} — auto-refreshing")


if __name__ == "__main__":
    render()
    # simple auto-refresh loop
    st_autorefresh_ms = 5000
    st.markdown(
        f"<meta http-equiv='refresh' content='{st_autorefresh_ms // 1000}'>",
        unsafe_allow_html=True,
    )
