# Real-Time Financial Sentiment Pipeline

A real-time financial data engineering and NLP project that ingests market data and financial news, processes streaming data using Apache Kafka and Apache Spark, and provides sentiment-driven market insights through a Streamlit dashboard.

## Project Overview

The Real-Time Financial Sentiment Pipeline is designed to analyze financial market data and news in real time. It combines streaming data processing, NLP sentiment analysis, and price momentum to generate understandable market signals for retail users.

## Key Features

* Real-time market data and financial news ingestion.
* Kafka-based data streaming and message processing.
* Apache Spark Structured Streaming for data processing.
* NLP-based financial sentiment analysis.
* Sentiment and price momentum-based composite signals.
* Streamlit dashboard for live market insights.
* Docker Compose-based service orchestration.
* Parquet-based sentiment history storage.

## Tech Stack

* **Programming:** Python
* **Streaming:** Apache Kafka
* **Big Data Processing:** Apache Spark
* **NLP:** VADER Sentiment Analyzer
* **Data Source:** Yahoo Finance
* **Dashboard:** Streamlit
* **Containerization:** Docker, Docker Compose
* **Storage:** Parquet

## System Architecture

Yahoo Finance API
↓
Kafka Producer
↓
Apache Kafka Topics
↓
Spark Structured Streaming
↓
NLP Sentiment Analysis + Price Momentum
↓
Sentiment Insights
↓
Streamlit Dashboard

## Project Structure

```text
realtime-financial-sentiment/
├── config/
├── producer/
├── nlp/
├── spark_jobs/
├── dashboard/
├── data/
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## How to Run

### 1. Clone the Repository

```bash
git clone https://github.com/SamratAman-Innovates/realtime-financial-sentiment.git
cd realtime-financial-sentiment
```

### 2. Start Kafka Infrastructure

```bash
docker compose up -d zookeeper kafka
```

### 3. Install Python Dependencies

```bash
pip install -r requirements.txt
```

### 4. Start the Data Producer

```bash
python producer/market_stream_producer.py
```

### 5. Start Spark Streaming

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark_jobs/sentiment_streaming_job.py
```

### 6. Launch the Dashboard

```bash
streamlit run dashboard/app.py
```

### Run the Full Stack with Docker

```bash
docker compose up --build
```

## Learning Outcomes

* Understanding real-time data ingestion using Apache Kafka.
* Working with Apache Spark Structured Streaming.
* Applying NLP sentiment analysis to financial news.
* Building data processing pipelines in Python.
* Containerizing and orchestrating services using Docker Compose.
* Developing dashboards for real-time data insights.

## Future Improvements

* Integrate FinBERT for finance-specific sentiment analysis.
* Add additional news sources.
* Implement long-term sentiment storage for backtesting.
* Add automated testing and CI/CD using GitHub Actions.

## Disclaimer

This project is for educational and research purposes only. The generated sentiment signals are not financial advice.
