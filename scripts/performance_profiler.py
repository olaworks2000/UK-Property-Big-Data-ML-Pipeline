import time
from pyspark.sql import functions as F

class PipelineProfiler:
    """Requirement 2b & 4: Performance Monitoring and Scalability Analysis"""
    def __init__(self):
        self.stats = []
        self.total_rows = 30906560 # Verified volume from unit tests

    def start_timer(self, stage_name):
        self.start_time = time.time()
        print(f"Starting Stage: {stage_name}...")

    def end_timer(self, stage_name):
        duration = time.time() - self.start_time
        
        # Calculate Throughput: How many million rows processed per second
        throughput = (self.total_rows / 1000000) / duration
        
        self.stats.append({
            "Stage": stage_name,
            "Duration_Sec": round(duration, 2),
            "Throughput_M_Rows_Per_Sec": round(throughput, 2)
        })
        
        print(f"{stage_name} completed in {duration:.2f}s ({throughput:.2f}M rows/sec)")
        return duration

    def export_stats(self, spark, output_path):
        """Generates the Gold Layer for Dashboard 4: Scalability"""
        df = spark.createDataFrame(self.stats)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        print(f"Scalability metrics exported to {output_path}")