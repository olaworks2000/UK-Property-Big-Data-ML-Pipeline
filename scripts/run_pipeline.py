import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.ml import Transformer
from pyspark.ml.util import DefaultParamsWritable, DefaultParamsReadable
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp, year, lit, broadcast, round
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, LogisticRegressionModel, RandomForestClassificationModel
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# ==========================================
# 1. HELPER CLASSES
# ==========================================

class PriceSegmenter(Transformer, DefaultParamsWritable, DefaultParamsReadable):
    """Requirement 2a: Custom Transformer for Market Segmentation"""
    def _transform(self, dataset):
        return dataset.withColumn("Market_Segment", 
            F.when(F.col("Price") < 150000, "Budget")
             .when(F.col("Price") < 450000, "Standard")
             .otherwise("Premium"))

class PipelineProfiler:
    """Requirement 2b: Performance Monitoring"""
    def __init__(self):
        self.stats = []
    def track(self, stage, duration):
        self.stats.append((stage, duration))
        print(f"{stage} completed in {duration:.2f}s")

# ==========================================
# 2. SESSION CONFIGURATION
# ==========================================

def create_spark_session():
    return SparkSession.builder \
        .appName("UK_Property_Production_Pipeline") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.executor.memory", "8g") \
        .getOrCreate()

# ==========================================
# 3. MAIN PIPELINE EXECUTION
# ==========================================

def main():
    spark = create_spark_session()
    profiler = PipelineProfiler()
    volume_path = "/Volumes/workspace/default/uk_land_registry/"
    print("UK Land Registry Master Pipeline Initiated...")

    try:
        # --- STAGE 1: INGESTION ---
        start = time.time()
        print("Stage 1: Ingesting Raw Data...")
        schema = StructType([
            StructField("Transaction_ID", StringType(), True), StructField("Price", IntegerType(), True),
            StructField("Date", StringType(), True), StructField("Postcode", StringType(), True),
            StructField("Property_Type", StringType(), True), StructField("Old_New", StringType(), True),
            StructField("Duration", StringType(), True), StructField("PAON", StringType(), True),
            StructField("SAON", StringType(), True), StructField("Street", StringType(), True),
            StructField("Locality", StringType(), True), StructField("Town_City", StringType(), True),
            StructField("District", StringType(), True), StructField("County", StringType(), True),
            StructField("PPD_Category", StringType(), True), StructField("Record_Status", StringType(), True)
        ])

        raw_df = spark.read.format("csv").option("mode", "failFast").schema(schema).load(f"{volume_path}uk_property_full.csv")
        raw_df.write.mode("overwrite").partitionBy("County").parquet(f"{volume_path}bronze_parquet")
        profiler.track("Ingestion", time.time() - start)

        # --- STAGE 2: FEATURE ENGINEERING & SAMPLE EXPORT ---
        start = time.time()
        print("Stage 2: Feature Engineering & Geographic Encoding...")
        bronze_df = spark.read.parquet(f"{volume_path}bronze_parquet")
        
        silver_df = bronze_df.withColumn("source_file", lit("uk_property_full.csv")) \
                            .select(col("Price").cast("double"), to_timestamp(col("Date"), "yyyy-MM-dd HH:mm").alias("Sale_Date"),
                                    "Property_Type", "Old_New", "Town_City").dropna()
        silver_df = silver_df.withColumn("Sale_Year", year(col("Sale_Date")))

        # Geographic Encoding
        city_indexer = StringIndexer(inputCol="Town_City", outputCol="city_label", handleInvalid="skip")
        silver_df = city_indexer.fit(silver_df).transform(silver_df)

        # Target Indexing
        type_indexer = StringIndexer(inputCol="Property_Type", outputCol="type_label")
        silver_df = type_indexer.fit(silver_df).transform(silver_df)
        
        silver_df = PriceSegmenter().transform(silver_df)
        
        # Scaling & Vector Assembly
        price_assembler = VectorAssembler(inputCols=["Price"], outputCol="unscaled_price")
        silver_df = price_assembler.transform(silver_df)
        
        scaler = StandardScaler(inputCol="unscaled_price", outputCol="scaled_features", withStd=True, withMean=True)
        silver_df = scaler.fit(silver_df).transform(silver_df)

        final_assembler = VectorAssembler(inputCols=["scaled_features", "city_label"], outputCol="final_features")
        silver_df = final_assembler.transform(silver_df)
        
        # Save Silver Parquet
        silver_df.write.mode("overwrite").parquet(f"{volume_path}silver_engineered_parquet")

        # EXPORT FIX: 100k Tableau Sample (Cast Vectors to String to avoid CSV errors)
        print("Exporting 100k Silver Sample for Tableau...")
        tableau_df = silver_df.select(
            "*",
            col("unscaled_price").cast("string").alias("unscaled_price_str"),
            col("scaled_features").cast("string").alias("scaled_features_str"),
            col("final_features").cast("string").alias("final_features_str")
        ).drop("unscaled_price", "scaled_features", "final_features")
        
        tableau_df.limit(100000).coalesce(1).write.mode("overwrite").option("header", "true") \
                  .csv(f"{volume_path}gold_tableau_data")
        
        profiler.track("Feature Engineering", time.time() - start)

        # --- STAGE 3: MODEL TRAINING ---
        start = time.time()
        print("Stage 3: Distributed Training (Random Forest Optimized)...")
        data = spark.read.parquet(f"{volume_path}silver_engineered_parquet")
        train_df = data.filter(col("Sale_Year") < 2023)

        # Algorithms with High-Cardinality maxBins fix
        rf = RandomForestClassifier(labelCol="type_label", featuresCol="final_features", numTrees=20, maxBins=1200)
        lr = LogisticRegression(labelCol="type_label", featuresCol="final_features", maxIter=20)

        # Fit & Save
        rf.fit(train_df).write().overwrite().save(f"{volume_path}models/rf_model")
        lr.fit(train_df).write().overwrite().save(f"{volume_path}models/lr_model")
        
        profiler.track("Model Training", time.time() - start)

        # --- STAGE 4: EVALUATION & GOLD PERFORMANCE EXPORT ---
        start = time.time()
        print("Stage 4: Generating Prediction Metrics for Tableau...")
        test_df = data.filter(col("Sale_Year") >= 2023)
        best_model = RandomForestClassificationModel.load(f"{volume_path}models/rf_model")
        predictions = best_model.transform(test_df)

        # Export prediction results (100k rows)
        predictions.select("Price", "type_label", "prediction").limit(100000) \
                   .coalesce(1).write.mode("overwrite").option("header", "true") \
                   .csv(f"{volume_path}gold_model_performance")
        
        profiler.track("Evaluation", time.time() - start)

        # Final Performance Log for Dashboard 4
        perf_df = spark.createDataFrame(profiler.stats, ["Stage", "Duration_Sec"])
        perf_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{volume_path}gold_pipeline_performance")

        print("Master Pipeline execution successful. All layers synchronized.")

    except Exception as e:
        print(f"MASTER PIPELINE FAILURE: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()