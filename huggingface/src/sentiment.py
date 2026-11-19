"""Hugging Face sentiment analysis on Spark: three ways to run a transformer in Python workers.

docker build -t tap:sparkhf .
docker run --rm -it -v ./src:/opt/spark/work-dir -v huggingface-cache:/opt/spark/.cache/huggingface \
  tap:sparkhf /opt/spark/bin/spark-submit sentiment.py
"""
from typing import Iterator

import numpy as np
import pandas as pd
from pyspark.ml.functions import predict_batch_udf
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

MODEL = "distilbert/distilbert-base-uncased-finetuned-sst-2-english"


def load_pipeline():
    # Imported here so the driver does not need to load torch
    from transformers import pipeline
    return pipeline("sentiment-analysis", model=MODEL, device=-1)   # -1 = CPU


# --- 1. Pandas UDF + lazy global model (one load per Python worker) ---------------------------
_clf = None


def get_clf():
    global _clf
    if _clf is None:
        _clf = load_pipeline()
    return _clf


@pandas_udf("label string, score double")
def hf_sentiment(texts: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(get_clf()(texts.tolist(), batch_size=32, truncation=True))


# --- 2. Iterator Pandas UDF (explicit setup once per task) -------------------------------------
@pandas_udf("label string, score double")
def hf_sentiment_iter(batches: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
    clf = load_pipeline()
    for texts in batches:
        yield pd.DataFrame(clf(texts.tolist(), batch_size=32, truncation=True))


# --- 3. predict_batch_udf (official helper, Spark >= 3.4) ---------------------------------------
def make_predict_fn():
    clf = load_pipeline()

    def predict(texts: np.ndarray) -> dict:
        out = clf(texts.tolist(), truncation=True)
        return {"label": np.array([o["label"] for o in out]),
                "score": np.array([o["score"] for o in out])}
    return predict


sentiment_udf = predict_batch_udf(
    make_predict_fn,
    return_type=StructType([StructField("label", StringType()), StructField("score", DoubleType())]),
    batch_size=64)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Sentiment HF").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.createDataFrame([("I hate Spark",),
                                ("This lesson is not boring at all",),
                                ("Waiting for the 2 a.m. bus again",)], ["text"])

    print("1. pandas_udf + lazy model")
    df.withColumn("s", hf_sentiment("text")).select("text", "s.*").show(truncate=False)

    print("2. iterator pandas_udf")
    df.withColumn("s", hf_sentiment_iter("text")).select("text", "s.*").show(truncate=False)

    print("3. predict_batch_udf")
    df.withColumn("s", sentiment_udf("text")).select("text", "s.*").show(truncate=False)

    spark.stop()
