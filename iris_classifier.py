# Databricks notebook source
dbutils.library.installPyPI("mlflow")
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %python
# MAGIC sdf_iris = spark.read.csv('/FileStore/tables/iris-3.csv', header="true", inferSchema="true")

# COMMAND ----------

sdf_iris.cache()

# COMMAND ----------

display(sdf_iris)

# COMMAND ----------

sdf_iris.printSchema()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

from pyspark.ml.feature import StringIndexer
# Convert target into numerical categories

label_indexer = StringIndexer(inputCol="species", outputCol="species_label")

(sdf_iris_training, sdf_iris_test) = sdf_iris.randomSplit([0.7, 0.3], seed = 100)

sdf_iris_training.cache()
sdf_iris_test.cache()

from pyspark.ml.feature import VectorAssembler

vector_assembler = VectorAssembler(inputCols=["sepal_length", "sepal_width", "petal_length", "petal_width"], outputCol="features")


from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline

# Train a NaiveBayes model
nb = NaiveBayes(smoothing=1.0, modelType="multinomial", labelCol="species_label", featuresCol="features")

pipeline = Pipeline(stages=[label_indexer, vector_assembler, nb])

# Run stages in pipeline and train model
model = pipeline.fit(sdf_iris_training)


# COMMAND ----------

sdf_iris_preds = model.transform(sdf_iris_test)

# Display what results we can view
sdf_iris_preds.printSchema()

# COMMAND ----------

display(sdf_iris_preds.select("species_label", "prediction", "probability"))

# COMMAND ----------

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(labelCol="species_label", predictionCol="prediction",
                                              metricName="accuracy")
accuracy = evaluator.evaluate(sdf_iris_preds)
print(accuracy)

import mlflow
import mlflow.spark

with mlflow.start_run():
  model = pipeline.fit(sdf_iris_training)
  test_metric = evaluator.evaluate(model.transform(sdf_iris_test))
  mlflow.log_metric('test_' + evaluator.getMetricName(), test_metric) # Logs additional metrics
  mlflow.spark.log_model(spark_model=model, artifact_path='iris_model')

# COMMAND ----------

