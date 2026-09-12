"""
Ingestion layer: pulls live quotes and news headlines from Yahoo Finance
and publishes them onto Kafka topics for downstream Spark processing.

Two independent polling loops run concurrently (quotes are cheap and
frequent; news is heavier and polled less often), each writing to its own
topic so the Spark job can process/join them at different cadences.
"""

import json
import logging
import sys
import threading
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.append("..")
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    NEWS_POLL_INTERVAL_SEC,
    QUOTE_POLL_INTERVAL_SEC,
    TICKERS,
    TOPIC_MARKET_NEWS,
    TOPIC_MARKET_QUOTES,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("producer")


def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
        linger_ms=50,
    )


def stream_quotes(producer: KafkaProducer, stop_event: threading.Event):
    """Poll live quote data for each ticker and publish to Kafka."""
    while not stop_event.is_set():
        try:
            data = yf.download(
                tickers=" ".join(TICKERS),
                period="1d",
                interval="1m",
                progress=False,
                group_by="ticker",
                threads=True,
            )
            now = datetime.now(timezone.utc).isoformat()

            for ticker in TICKERS:
                try:
                    last_row = data[ticker].dropna().iloc[-1]
                    prev_close = yf.Ticker(ticker).fast_info.get(
                        "previousClose", last_row["Close"]
                    )
                    momentum_pct = (
                        (last_row["Close"] - prev_close) / prev_close * 100
                        if prev_close
                        else 0.0
                    )
                    payload = {
                        "ticker": ticker,
                        "timestamp": now,
                        "open": float(last_row["Open"]),
                        "high": float(last_row["High"]),
                        "low": float(last_row["Low"]),
                        "close": float(last_row["Close"]),
                        "volume": int(last_row["Volume"]),
                        "momentum_pct": float(momentum_pct),
                    }
                    producer.send(TOPIC_MARKET_QUOTES, key=ticker, value=payload)
                    log.info("quote  %-6s close=%.2f momentum=%.2f%%",
                              ticker, payload["close"], momentum_pct)
                except (KeyError, IndexError) as e:
                    log.warning("no quote data for %s this cycle: %s", ticker, e)

            producer.flush()
        except KafkaError as e:
            log.error("Kafka send failed: %s", e)
        except Exception as e:
            log.error("quote poll failed: %s", e)

        stop_event.wait(QUOTE_POLL_INTERVAL_SEC)


def stream_news(producer: KafkaProducer, stop_event: threading.Event):
    """Poll recent news headlines per ticker and publish to Kafka."""
    seen_ids = set()

    while not stop_event.is_set():
        try:
            for ticker in TICKERS:
                articles = yf.Ticker(ticker).news or []
                for article in articles:
                    content = article.get("content", article)
                    uid = content.get("id") or content.get("title", "") + ticker
                    if uid in seen_ids:
                        continue
                    seen_ids.add(uid)

                    payload = {
                        "ticker": ticker,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "headline": content.get("title", ""),
                        "summary": content.get("summary", ""),
                        "publisher": content.get("provider", {}).get("displayName", "unknown")
                        if isinstance(content.get("provider"), dict) else "unknown",
                        "link": content.get("canonicalUrl", {}).get("url", "")
                        if isinstance(content.get("canonicalUrl"), dict) else "",
                    }
                    producer.send(TOPIC_MARKET_NEWS, key=ticker, value=payload)
                    log.info("news   %-6s %s", ticker, payload["headline"][:80])

            producer.flush()
            # keep the seen-id cache from growing unbounded
            if len(seen_ids) > 5000:
                seen_ids.clear()
        except KafkaError as e:
            log.error("Kafka send failed: %s", e)
        except Exception as e:
            log.error("news poll failed: %s", e)

        stop_event.wait(NEWS_POLL_INTERVAL_SEC)


def main():
    log.info("Starting producer for tickers: %s", ", ".join(TICKERS))
    log.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)

    producer = build_producer()
    stop_event = threading.Event()

    quote_thread = threading.Thread(target=stream_quotes, args=(producer, stop_event), daemon=True)
    news_thread = threading.Thread(target=stream_news, args=(producer, stop_event), daemon=True)

    quote_thread.start()
    news_thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Shutting down producer...")
        stop_event.set()
        quote_thread.join(timeout=5)
        news_thread.join(timeout=5)
        producer.close()


if __name__ == "__main__":
    main()
