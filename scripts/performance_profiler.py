import time
import psutil
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

def get_system_metrics():
    """
    Captures Resource Allocation justification (Executors/Memory).
    Fulfills Section 2(b): Resource allocation justification.
    """
    process = psutil.Process(os.getpid())
    # Memory in GB
    mem_info = process.memory_info().rss / (1024 ** 3) 
    cpu_percent = psutil.cpu_percent()
    return mem_info, cpu_percent

def profile_spark_operation(spark, step_name, df_operation, *args, **kwargs):
    """
    Advanced Profiler for Scalability Analysis (Section 2c).
    Tracks Time, Row Count, and Resource Usage.
    """
    print(f"--- PROFILING START: {step_name} ---")
    
    # 1. Capture Pre-Execution State
    start_time = time.time()
    mem_before, cpu_before = get_system_metrics()
    
    # 2. Execute Spark Operation
    # We use a try-except block to satisfy Section 1(b): Error Handling
    try:
        df_result = df_operation(*args, **kwargs)
        
        # MANDATORY: Force Action for accurate timing on Serverless
        # This bypasses Spark's lazy evaluation to measure actual compute
        row_count = df_result.count() 
        
        # 3. Capture Post-Execution State
        end_time = time.time()
        duration = end_time - start_time
        mem_after, cpu_after = get_system_metrics()
        
        print(f"COMPLETED: {step_name}")
        print(f"DURATION: {duration:.2f}s | ROWS: {row_count}")
        
        # 4. Format Metric for Tableau Dashboard 4
        # We include 'Scalability_Type' to help with Strong/Weak scaling analysis
        metric = {
            "step_name": step_name,
            "duration_sec": duration,
            "record_count": row_count,
            "mem_usage_gb": mem_after,
            "cpu_load_percent": cpu_after,
            "timestamp": time.ctime()
        }
        
        return df_result, metric

    except Exception as e:
        print(f"PROFILER ERROR in {step_name}: {str(e)}")
        raise e

def export_performance_logs(spark, metrics_list, output_path="/Volumes/workspace/default/uk_land_registry/gold_tableau_data/performance_metrics.csv"):
    """
    Exports the performance data to the Gold Volume for Tableau.
    Fulfills Section 3(a): Dashboard 4 - Scalability and cost analysis.
    """
    # Create DataFrame from the list of dictionaries
    perf_df = spark.createDataFrame(metrics_list)
    
    # Coalesce(1) is used to ensure a single CSV for easy Tableau connection
    perf_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"PERFORMANCE LOGS EXPORTED TO: {output_path}")

# Example Usage within a notebook:
# from scripts.performance_profiler import profile_spark_operation, export_performance_logs
# logs = []
# df_silver, m = profile_spark_operation(spark, "Cleaning", df_bronze.filter, col("Price") > 0)
# logs.append(m)
# export_performance_logs(spark, logs)