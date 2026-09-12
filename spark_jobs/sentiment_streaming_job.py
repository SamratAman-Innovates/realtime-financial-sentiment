"""
Spark Structured Streaming job.

Reads two Kafka topics (market-quotes, market-news), scores news sentiment
with the finance-tuned NLP model, aggregates sentiment per ticker over a
sliding time window, fuses it with price momentum from the quotes stream,
and writes the resulting composite signal back to Kafka (for the dashboard)
and to Parquet (for historical backtesting).

Run with:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    spark_jobs/sentiment_streaming_job.py
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, from_json, last, pandas_udf, to_json, struct, window,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType, TimestampType,
)

sys.path.append("..")
from config.settings import (
    CHECKPOINT_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    PARQUET_OUTPUT_PATH,
    SENTIMENT_SLIDE_DURATION,
    SENTIMENT_WINDOW_DURATION,
    TOPIC_MARKET_NEWS,
    TOPIC_MARKET_QUOTES,
    TOPIC_SENTIMENT_INSIGHTS,
    WATERMARK_DELAY,
)
from nlp.sentiment_analyzer import FinancialSentimentAnalyzer, composite_signal

# --- Schemas matching the JSON payloads written by the producer -----------

QUOTE_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", IntegerType()),
    StructField("momentum_pct", DoubleType()),
])

NEWS_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("headline", StringType()),
    StructField("summary", StringType()),
    StructField("publisher", StringType()),
    StructField("link", StringType()),
])


@pandas_udf(DoubleType())
def sentiment_score_udf(headlines: "pd.Series") -> "pd.Series":
    """Vectorized (pandas UDF) NLP scoring: one FinancialSentimentAnalyzer
    per executor process, reused across the whole batch/partition rather
    than re-instantiated per row."""
    analyzer = sentiment_score_udf.analyzer if hasattr(sentiment_score_udf, "analyzer") else None
    if analyzer is None:
        analyzer = FinancialSentimentAnalyzer()
        sentiment_score_udf.analyzer = analyzer
    return headlines.fillna("").apply(lambda t: analyzer.analyze(t).compound)


def read_kafka_topic(spark: SparkSession, topic: str, schema: StructType):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )


def build_pipeline(spark: SparkSession):
    quotes = read_kafka_topic(spark, TOPIC_MARKET_QUOTES, QUOTE_SCHEMA)
    news = read_kafka_topic(spark, TOPIC_MARKET_NEWS, NEWS_SCHEMA)

    # --- News stream: score sentiment, then window-aggregate per ticker ---
    scored_news = news.withColumn(
        "text_for_scoring",
        col("headline")
    ).withColumn(
        "sentiment_score", sentiment_score_udf(col("text_for_scoring"))
    )

    news_watermarked = scored_news.withWatermark("timestamp", WATERMARK_DELAY)

    news_agg = (
        news_watermarked
        .groupBy(
            window(col("timestamp"), SENTIMENT_WINDOW_DURATION, SENTIMENT_SLIDE_DURATION),
            col("ticker"),
        )
        .agg(avg("sentiment_score").alias("avg_sentiment"))
    )

    # --- Quotes stream: window-aggregate momentum per ticker --------------
    quotes_watermarked = quotes.withWatermark("timestamp", WATERMARK_DELAY)

    quotes_agg = (
        quotes_watermarked
        .groupBy(
            window(col("timestamp"), SENTIMENT_WINDOW_DURATION, SENTIMENT_SLIDE_DURATION),
            col("ticker"),
        )
        .agg(
            last("close").alias("last_close"),
            avg("momentum_pct").alias("avg_momentum_pct"),
        )
    )

    # --- Stream-stream join on ticker + matching time window ---------------
    fused = news_agg.join(
        quotes_agg,
        on=["ticker", "window"],
        how="inner",
    )

    return fused


@pandas_udf(StringType())
def composite_signal_udf(avg_sentiment: "pd.Series", avg_momentum_pct: "pd.Series") -> "pd.Series":
    import pandas as pd
    return pd.Series([
        composite_signal(s, m) for s, m in zip(avg_sentiment, avg_momentum_pct)
    ])


def write_outputs(fused_df):
    result = fused_df.select(
        col("ticker"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_sentiment"),
        col("last_close"),
        col("avg_momentum_pct"),
        composite_signal_udf(col("avg_sentiment"), col("avg_momentum_pct")).alias("signal"),
    )

    # 1) Push composite signals back onto Kafka for the live dashboard
    kafka_query = (
        result
        .select(
            col("ticker").alias("key"),
            to_json(struct(
                "ticker", "window_start", "window_end",
                "avg_sentiment", "last_close", "avg_momentum_pct", "signal",
            )).alias("value"),
        )
        .writeStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", TOPIC_SENTIMENT_INSIGHTS)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/kafka_sink")
        .outputMode("update")
        .start()
    )

    # 2) Persist history to Parquet for backtesting / analytics
    parquet_query = (
        result.writeStream.format("parquet")
        .option("path", PARQUET_OUTPUT_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/parquet_sink")
        .outputMode("append")
        .trigger(processingTime="1 minute")
        .start()
    )

    # 3) Console output for local debugging
    console_query = (
        result.writeStream.format("console")
        .outputMode("update")
        .option("truncate", False)
        .start()
    )

    return [kafka_query, parquet_query, console_query]


def main():
    spark = (
        SparkSession.builder
        .appName("RealTimeFinancialSentimentPipeline")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    fused = build_pipeline(spark)
    queries = write_outputs(fused)

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
