# Real-Time Financial Sentiment Pipeline

A streaming pipeline that ingests live market data and financial news, scores
sentiment with NLP, fuses it with price momentum, and surfaces the result to
retail users through a live dashboard.

```
Yahoo Finance API
   |  (quotes + news headlines, polled every N sec)
   v
[ producer/market_stream_producer.py ] --publish--> Kafka topics
                                                       market-quotes
                                                       market-news
                                                          |
                                                          v
                                        [ spark_jobs/sentiment_streaming_job.py ]
                                        Spark Structured Streaming consumer:
                                          - parses JSON from Kafka
                                          - runs NLP sentiment scoring (nlp/sentiment_analyzer.py)
                                          - windows + aggregates sentiment per ticker
                                          - fuses sentiment with price momentum -> composite signal
                                          - writes to Kafka topic: sentiment-insights
                                          - writes Parquet history to /data/sentiment_history
                                                          |
                                                          v
                                        [ dashboard/app.py ] Streamlit app
                                        Consumes sentiment-insights and renders
                                        live charts + a plain-English signal feed
                                        for retail users.

All services run in containers, orchestrated by docker-compose.yml
(Zookeeper, Kafka broker, Spark master/worker, producer, dashboard).
```

## Why this design

- **Kafka** decouples ingestion from processing so the pipeline can absorb
  bursts (earnings days, market opens) without losing data, and multiple
  consumers (Spark, logging, backtesting) can read the same stream.
- **Spark Structured Streaming** does the heavy lifting: windowed
  aggregation, joins between the quotes stream and the news/sentiment
  stream, and stateful computation, all with exactly-once semantics.
- **NLP layer** is a finance-tuned VADER model (general-purpose VADER
  under-scores financial jargon like "beat estimates" or "guidance cut"),
  wrapped so it's easy to swap in a transformer model (e.g. FinBERT) later
  without touching the streaming job.
- **Docker Compose** containerizes every moving part so the whole stack
  comes up with one command and is portable across machines.
- **Streamlit dashboard** translates the composite signal (sentiment +
  momentum) into a plain-language readout ("Bullish", "Bearish", "Mixed")
  so non-technical retail users don't have to interpret raw scores.

## Project layout

```
realtime-financial-sentiment/
├── docker-compose.yml
├── requirements.txt
├── config/
│   └── settings.py          # tickers, Kafka topics, timing knobs
├── producer/
│   └── market_stream_producer.py   # Yahoo Finance -> Kafka
├── nlp/
│   └── sentiment_analyzer.py       # finance-tuned VADER sentiment
├── spark_jobs/
│   └── sentiment_streaming_job.py  # Kafka -> Spark -> Kafka/Parquet
└── dashboard/
    └── app.py                       # Streamlit retail-facing UI
```

## Running it

### 1. Bring up infrastructure

```bash
docker compose up -d zookeeper kafka
```

Wait ~15s for Kafka to finish leader election, then create topics (or let
`auto.create.topics.enable=true` in the compose file handle it):

```bash
docker exec -it kafka kafka-topics --create --topic market-quotes --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic market-news --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec -it kafka kafka-topics --create --topic sentiment-insights --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 2. Start the producer

```bash
pip install -r requirements.txt
python producer/market_stream_producer.py
```

### 3. Start the Spark streaming job

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_jobs/sentiment_streaming_job.py
```

### 4. Launch the dashboard

```bash
streamlit run dashboard/app.py
```

### Or, run everything with Docker

```bash
docker compose up --build
```

## Extending it

- Swap `nlp/sentiment_analyzer.py`'s VADER backend for a FinBERT
  transformer by implementing the same `.analyze(text)` interface.
- Add more sources (Reddit, X/Twitter, SEC filings) as additional Kafka
  producers writing to their own topics; join them in the Spark job.
- Persist `sentiment-insights` to a time-series DB (TimescaleDB/InfluxDB)
  for longer-term backtesting instead of only Parquet.
