"""Import a Hugging Face model exported to ONNX (see ../export_onnx.sh) into Spark NLP.

docker run --rm -it -v sparknlp-ivy-cache:/root/.ivy2 -v sparknlp-models:/root/cache_pretrained \
  -v ./src:/usr/src/app -v ./models:/models tap:sparknlp onnx_import.py
"""
import os
import sparknlp
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import Tokenizer, DistilBertForSequenceClassification
from pyspark.ml import Pipeline

os.environ["IVY_LOG_LEVEL"] = "error"

ONNX_DIR = "/models/distilbert_sst2_onnx"          # output of export_onnx.sh
SPARKNLP_DIR = "/models/distilbert_sst2_spark_nlp"   # Spark NLP format, reusable

spark = sparknlp.start(memory="8G")
spark.sparkContext.setLogLevel("ERROR")
print("Spark NLP", sparknlp.version(), "- Apache Spark", spark.version)

if not os.path.exists(SPARKNLP_DIR):
    # loadSavedModel detects model.onnx and uses the ONNX Runtime engine inside the JVM
    clf = DistilBertForSequenceClassification.loadSavedModel(ONNX_DIR, spark)
    clf.write().overwrite().save(SPARKNLP_DIR)
    print("Saved Spark NLP model to", SPARKNLP_DIR)

document = DocumentAssembler().setInputCol("text").setOutputCol("document")
token = Tokenizer().setInputCols(["document"]).setOutputCol("token")
clf = (DistilBertForSequenceClassification.load(SPARKNLP_DIR)
       .setInputCols(["document", "token"])
       .setOutputCol("class")
       .setCaseSensitive(False)
       .setBatchSize(16))

pipeline = Pipeline(stages=[document, token, clf])
data = spark.createDataFrame([["This lesson is not boring at all"],
                              ["Waiting for the 2 a.m. bus again"],
                              ["Spark NLP runs ONNX models without Python on the executors"]]).toDF("text")

(pipeline.fit(data).transform(data)
 .selectExpr("text", "class.result[0] AS label", "class.metadata[0] AS scores")
 .show(truncate=False))

spark.stop()
