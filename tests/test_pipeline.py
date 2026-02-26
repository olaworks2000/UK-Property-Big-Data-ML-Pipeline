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
        
        # Test 1: Price must be positive (Basic business logic validation)
        negative_prices = df_bronze.filter(col("Price") <= 0).count()
        self.assertEqual(negative_prices, 0, f"Found {negative_prices} invalid prices in Bronze layer.")
        
        # Test 2: Date column must be present
        self.assertIn("Date", df_bronze.columns)

    def test_silver_schema_integrity(self):
        """Requirement: Ensure Feature Engineering didn't break the schema"""
        df_silver = self.spark.read.parquet(self.silver_path)
        
        # Test 3: Null Check on critical ML features
        null_prices = df_silver.filter(col("Price").isNull()).count()
        self.assertEqual(null_prices, 0, "Null values found in Silver 'Price' column.")

        # Test 4: Check for required ML columns (Requirement 2a)
        required_cols = ["scaled_features", "type_label", "Market_Segment"]
        for c in required_cols:
            self.assertIn(c, df_silver.columns, f"Missing required ML column: {c}")

    def test_data_volume(self):
        """Ensures the dataset hasn't been accidentally truncated"""
        df_silver = self.spark.read.parquet(self.silver_path)
        row_count = df_silver.count()
        self.assertGreater(row_count, 30000000, "Row count significantly lower than expected 30M+.")

if __name__ == "__main__":
    # Running the tests
    suite = unittest.TestLoader().loadTestsFromTestCase(TestUKPropertyPipeline)
    unittest.TextTestRunner(verbosity=2).run(suite)