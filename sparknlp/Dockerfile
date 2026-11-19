FROM python:3.12-bookworm

# Spark 4.x requires Java 17
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# See https://sparknlp.org/docs/en/install#python
# spark-nlp 7.0.x: pyspark 4.0.1 and later *validated* Spark 4 versions use spark-nlp_2.13
# (pyspark 4.0.0 needs the dedicated spark-nlp-spark400_2.13 artifact)
# setuptools provides the distutils shim still imported by spark-nlp on Python 3.12
RUN pip install --no-cache-dir numpy setuptools spark-nlp==7.0.1 pyspark==4.0.1

# Source code is mounted at runtime: -v ./src:/usr/src/app
WORKDIR /usr/src/app

# Pass the script name as argument: docker run ... tap:sparknlp sentiment.py
ENTRYPOINT ["python3"]
CMD ["example.py"]
