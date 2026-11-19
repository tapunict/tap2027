"""Run a small GGUF LLM *inside* Spark with AutoGGUFModel (llama.cpp embedded in the JVM).

Download the model first (≈400 MB):
  mkdir -p models && curl -L -o models/qwen2.5-0.5b-instruct-q4_k_m.gguf \
    https://huggingface.co/Qwen/Qwen2.5-0.5B-Instruct-GGUF/resolve/main/qwen2.5-0.5b-instruct-q4_k_m.gguf

docker run --rm -it -v sparknlp-ivy-cache:/root/.ivy2 -v ./src:/usr/src/app -v ./models:/models \
  tap:sparknlp gguf.py

This is the "bridge" demo: it works, but every executor loads the whole model.
See doc/Local Inference.ipynb for the served alternative.
"""
import os, time
import sparknlp
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import AutoGGUFModel
from pyspark.ml import Pipeline

os.environ["IVY_LOG_LEVEL"] = "error"
GGUF = "/models/qwen2.5-0.5b-instruct-q4_k_m.gguf"

spark = sparknlp.start(memory="8G")
spark.sparkContext.setLogLevel("ERROR")

document = DocumentAssembler().setInputCol("text").setOutputCol("document")
llm = (AutoGGUFModel.loadSavedModel(GGUF, spark)
       .setInputCols(["document"])
       .setOutputCol("completions")
       .setBatchSize(4)
       .setNPredict(48)          # max new tokens
       .setTemperature(0.2)
       .setNGpuLayers(0))        # CPU only

prompts = spark.createDataFrame([
    ["Summarize in 5 words: Apache Spark is a unified engine for large-scale data analytics."],
    ["Translate to Italian: Kafka is a distributed event streaming platform."],
    ["Is this tweet positive or negative? 'This lesson is not boring at all'. Answer with one word."],
]).toDF("text")

t0 = time.time()
(Pipeline(stages=[document, llm]).fit(prompts).transform(prompts)
 .selectExpr("text", "completions.result[0] AS completion")
 .show(truncate=False))
print(f"elapsed {time.time() - t0:.1f}s")
spark.stop()
