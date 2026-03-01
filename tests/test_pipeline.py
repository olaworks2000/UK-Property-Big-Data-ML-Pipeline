import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull

def get_test_spark_session():
    """Initializes Spark for isolated data validation."""
    return SparkSession.builder \
        .appName("UK_Property_Data_QA_Gate") \
        .getOrCreate()

def run_data_quality_tests():
    """
    Implements mandatory Data Validation per Section 1(a).
    Performs a 'Circuit Break' check on the Silver Layer.
    """
    spark = get_test_spark_session()
    silver_path = "/Volumes/workspace/default/uk_land_registry/silver_engineered_parquet"
    
    print("="*60)
    print("STARTING DATA QUALITY GATE: SILVER LAYER AUDIT")
    print("="*60)
    
    # 1. Load the Silver Dataset
    df = spark.read.parquet(silver_path)
    
    # 2. Critical Validation: Null & Schema Audit
    # Fulfills Section 78: 'Data validation at ingestion'
    validation_metrics = df.select(
        count(when(isnull(col("Price")), 1)).alias("null_prices"),
        count(when(isnull(col("County")), 1)).alias("null_counties"),
        count(when(col("Price") <= 0, 1)).alias("invalid_prices")
    ).collect()[0]
    
    # 3. Structural Validation: Feature Existence
    # Ensures VectorAssembler will not fail in the Gold stage
    required_features = ["Year", "Month", "Property_Type", "County"]
    missing_features = [f for f in required_features if f not in df.columns]
    
    # --- ASSESSMENT LOGIC ---
    test_passed = True
    print(f"REPORT: Null Prices found: {validation_metrics['null_prices']}")
    print(f"REPORT: Null Counties found: {validation_metrics['null_counties']}")
    print(f"REPORT: Invalid Prices (<=0): {validation_metrics['invalid_prices']}")
    
    if validation_metrics['null_prices'] > 0 or validation_metrics['invalid_prices'] > 0:
        print("RESULT: [FAILED] Price data integrity issues detected.")
        test_passed = False
        
    if missing_features:
        print(f"RESULT: [FAILED] Missing schema columns: {missing_features}")
        test_passed = False

    # 4. Export QA Results for Tableau Dashboard 1 (Pipeline Monitoring)
    # Fulfills Section 3(a): 'Dashboard 1: Data quality and pipeline monitoring'
    qa_results = [("Silver_Layer_Audit", "Pass" if test_passed else "Fail", time.ctime())]
    spark.createDataFrame(qa_results, ["Audit_Type", "Status", "Timestamp"]) \
         .coalesce(1).write.mode("overwrite").option("header", "true") \
         .csv("/Volumes/workspace/default/uk_land_registry/gold_tableau_data/qa_audit_results.csv")

    print("="*60)
    if test_passed:
        print("GATE STATUS: [CLEARED] - Proceed to Model Training.")
    else:
        print("GATE STATUS: [BLOCKED] - Data Quality issues must be resolved.")
    print("="*60)
    
    return test_passed

if __name__ == "__main__":
    run_data_quality_tests()