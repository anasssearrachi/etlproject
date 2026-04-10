FROM apache/spark:3.5.0-python3

USER root

RUN pip install --no-cache-dir pyspark==3.5.0

COPY spark_job.py /opt/spark_job.py

CMD ["/opt/spark/bin/spark-submit", \
  "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0", \
  "/opt/spark_job.py"]