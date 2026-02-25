import time
from pyspark.sql import functions as F

class PipelineProfiler:
    def __init__(self):
        self.stats = {}

    def start_timer(self, stage_name):
        self.stats[stage_name] = time.time()
        print(f"Starting {stage_name}...")

    def end_timer(self, stage_name):
        duration = time.time() - self.stats[stage_name]
        print(f"{stage_name} completed in {duration:.2f} seconds.")
        return duration

# Marker Tip: Use this to compare how fast the 30M rows process 
# on 2 executors vs 4 executors to prove 'Strong Scaling'.