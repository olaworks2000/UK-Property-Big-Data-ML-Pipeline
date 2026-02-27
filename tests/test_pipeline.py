import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

class TestUKPropertyPipeline(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Initialize a local Spark Session for testing
        cls.spark = SparkSession.builder.appName("UnitTesting").getOrCreate()
        cls.silver_path = "/Volumes/workspace/default/uk_land_registry/silver_engineered_parquet"
        cls.bronze_path = "/Volumes/workspace/default/uk_land_registry/bronze_parquet"

    def test_bronze_ingestion_quality(self):
        """Requirement: Data validation at ingestion"""
        df_bronze = self.spark.read.parquet(self.bronze_path)
        
        # Test 1: Price must be positive
        negative_prices = df_bronze.filter(col("Price") <= 0).count()
        self.assertEqual(negative_prices, 0, f"Found {negative_prices} invalid prices in Bronze.")
        
        # Test 2: Date and Geographic columns must be present
        self.assertIn("Date", df_bronze.columns)
        self.assertIn("Town_City", df_bronze.columns)

    def test_silver_schema_integrity(self):
        """Requirement: Ensure Feature Engineering schema matches model requirements"""
        df_silver = self.spark.read.parquet(self.silver_path)
        
        # Test 3: Null Check on critical ML features
        null_prices = df_silver.filter(col("Price").isNull()).count()
        self.assertEqual(null_prices, 0, "Null values found in Silver 'Price' column.")

        # Test 4: Check for required ML columns (Updated for Geographic Features)
        # We now check for 'final_features' and 'city_label'
        required_cols = ["scaled_features", "type_label", "city_label", "final_features", "Market_Segment"]
        for c in required_cols:
            self.assertIn(c, df_silver.columns, f"Missing required ML column: {c}")

    def test_geographic_encoding(self):
        """Requirement 2a: Verify Town_City was correctly indexed"""
        df_silver = self.spark.read.parquet(self.silver_path)
        
        # Test 5: city_label should be numeric
        field_type = [f.dataType for f in df_silver.schema.fields if f.name == "city_label"][0]
        self.assertEqual(str(field_type), "DoubleType()", "city_label must be a Double (numeric) for MLlib.")

    def test_data_volume(self):
        """Ensures the dataset hasn't been accidentally truncated"""
        df_silver = self.spark.read.parquet(self.silver_path)
        row_count = df_silver.count()
        # Using the actual row count from your summary
        self.assertGreater(row_count, 30900000, f"Expected 30.9M+ rows, found {row_count}.")

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestUKPropertyPipeline)
    unittest.TextTestRunner(verbosity=2).run(suite)