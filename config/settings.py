"""
Central configuration for the pipeline. Keeping these in one place means the
producer, Spark job, and dashboard all agree on topic names and tickers
without hardcoding strings in three different places.
"""

import os

# --- Tickers to track -------------------------------------------------
TICKERS = os.getenv("TICKERS", "AAPL,MSFT,TSLA,NVDA,AMZN").split(",")

# --- Kafka ---------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

TOPIC_MARKET_QUOTES = "market-quotes"
TOPIC_MARKET_NEWS = "market-news"
TOPIC_SENTIMENT_INSIGHTS = "sentiment-insights"

# --- Timing ----------------------------------------------------------------
QUOTE_POLL_INTERVAL_SEC = int(os.getenv("QUOTE_POLL_INTERVAL_SEC", "15"))
NEWS_POLL_INTERVAL_SEC = int(os.getenv("NEWS_POLL_INTERVAL_SEC", "60"))

# Spark streaming window for aggregating sentiment per ticker
SENTIMENT_WINDOW_DURATION = "5 minutes"
SENTIMENT_SLIDE_DURATION = "1 minute"
WATERMARK_DELAY = "2 minutes"

# --- Output ------------------------------------------------------------
PARQUET_OUTPUT_PATH = os.getenv("PARQUET_OUTPUT_PATH", "./data/sentiment_history")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "./data/_checkpoints")
