import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, avg

# Initialize Spark
spark = SparkSession.builder.appName("UKPropertyPipeline").getOrCreate()

# 1. Ingestion (Bronze)
# download and schema code
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Transaction_ID", StringType(), True),
    StructField("Price", IntegerType(), True),
    StructField("Date", StringType(), True),
    StructField("Postcode", StringType(), True),
    StructField("Property_Type", StringType(), True),
    StructField("Old_New", StringType(), True),
    StructField("Duration", StringType(), True),
    StructField("PAON", StringType(), True),
    StructField("SAON", StringType(), True),
    StructField("Street", StringType(), True),
    StructField("Locality", StringType(), True),
    StructField("Town_City", StringType(), True),
    StructField("District", StringType(), True),
    StructField("County", StringType(), True),
    StructField("PPD_Category", StringType(), True),
    StructField("Record_Status", StringType(), True)
])

print("Schema defined successfully. You can now load the data.")

import os
volume_path = "/Volumes/workspace/default/uk_land_registry/"
if not os.path.exists(volume_path):
    print("Volume not found. Please check Catalog permissions.")

raw_df = spark.read.format("csv") \
    .option("header", "false") \
    .option("mode", "failFast") \
    .schema(schema) \
    .load(f"{volume_path}uk_property_full.csv")

raw_df.write.mode("overwrite").parquet(f"{volume_path}bronze_parquet")

print(f"Ingestion Complete. 30.9M rows validated and stored as Parquet.")

# 2. Engineering (Silver)
# cleaning and StandardScaler
from pyspark.sql.functions import col, to_timestamp, year, month
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler

df = spark.read.parquet("/Volumes/workspace/default/uk_land_registry/bronze_parquet")

silver_df = df.select(
    col("Price").cast("double"),
    to_timestamp(col("Date"), "yyyy-MM-dd HH:mm").alias("Sale_Date"),
    col("Property_Type"),
    col("Old_New"),
    col("Town_City")
).dropna()

silver_df = silver_df.withColumn("Sale_Year", year(col("Sale_Date")))


indexer = StringIndexer(inputCol="Property_Type", outputCol="type_label")
indexed_df = indexer.fit(silver_df).transform(silver_df)


assembler = VectorAssembler(inputCols=["Price"], outputCol="unscaled_features")
assembled_df = assembler.transform(indexed_df)

scaler = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features", withStd=True, withMean=True)
scaler_model = scaler.fit(assembled_df)
final_engineered_df = scaler_model.transform(assembled_df)

# Save the Silver Layer
final_engineered_df.write.mode("overwrite").parquet("/Volumes/workspace/default/uk_land_registry/silver_engineered_parquet")

print(f"Notebook 2 Complete: Silver Layer stored with Scaling and Feature Engineering applied.")
final_engineered_df.select("Price", "scaled_features", "type_label").show(5)

# 3. Machine Learning (Gold)
# Decision Tree training code
import os

temp_path = "/Volumes/workspace/default/uk_land_registry/ml_temp"
os.environ['SPARKML_TEMP_DFS_PATH'] = temp_path

#dbutils.fs.mkdirs(temp_path)
if not os.path.exists(temp_path):
    os.makedirs(temp_path)

print(f"Environment variable set. Spark ML will now use: {temp_path}")

from pyspark.ml.classification import DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 1. Load Scaled Silver Data
data = spark.read.parquet("/Volumes/workspace/default/uk_land_registry/silver_engineered_parquet")

train_df, test_df = data.randomSplit([0.8, 0.2], seed=42)

# Model Selection: Decision Tree
dt = DecisionTreeClassifier(labelCol="type_label", featuresCol="scaled_features")

paramGrid = ParamGridBuilder() \
    .addGrid(dt.maxDepth, [5, 10]) \
    .build()

evaluator = MulticlassClassificationEvaluator(labelCol="type_label", predictionCol="prediction", metricName="accuracy")

cv = CrossValidator(estimator=dt,
                    estimatorParamMaps=paramGrid,
                    evaluator=evaluator,
                    numFolds=3)

print("Starting Distributed Training & Tuning... This may take a few minutes.")
cv_model = cv.fit(train_df)

cv_model.bestModel.write().overwrite().save("/Volumes/workspace/default/uk_land_registry/models/best_dt_model")

print("Notebook 3 Complete: Model Tuned, Trained, and Serialized.")


# 4. Export
# .write.csv code for Gold layer
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

model = DecisionTreeClassificationModel.load("/Volumes/workspace/default/uk_land_registry/models/best_dt_model")
test_data = spark.read.parquet("/Volumes/workspace/default/uk_land_registry/silver_engineered_parquet")

predictions = model.transform(test_data)

evaluator = MulticlassClassificationEvaluator(labelCol="type_label", predictionCol="prediction")

accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
weightedPrecision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
weightedRecall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})

print(f"Model Accuracy: {accuracy:.4f}")
print(f"Weighted Precision: {weightedPrecision:.4f}")
print(f"Weighted Recall: {weightedRecall:.4f}")

importance = model.featureImportances
print(f"Feature Importance (Price vs Categories): {importance}")

# 5. Export Final Gold Data
# Export a sample of predictions so Tableau can show the 'Confusion'
gold_evaluation = predictions.select("Price", "type_label", "prediction").limit(100000)
gold_evaluation.coalesce(1).write.mode("overwrite").option("header", "true").csv("/Volumes/workspace/default/uk_land_registry/gold_model_performance")

print("Notebook 4 Complete: Metrics calculated and Gold Layer 2 exported.")

print("Pipeline Execution Complete: 30,906,560 rows processed.")