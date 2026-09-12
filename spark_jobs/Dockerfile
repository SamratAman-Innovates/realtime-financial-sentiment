FROM bitnami/spark:3.5.1

WORKDIR /app

USER root
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config/ ./config/
COPY nlp/ ./nlp/
COPY spark_jobs/ ./spark_jobs/

CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "spark_jobs/sentiment_streaming_job.py"]
