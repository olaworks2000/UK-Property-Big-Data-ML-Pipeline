import time

def run_pipeline():
    """
    Master Orchestrator for UK Property Big Data.
    Standardized for Coventry University Databricks Workspace.
    """
    print("="*60)
    print("STARTING PRODUCTION PIPELINE: UK PROPERTY BIG DATA")
    print("="*60)
    
    start_total = time.time()
    
    # These match your filenames exactly
    notebook_files = [
        "1_data_ingestion",
        "2_feature_engineering",
        "3_model_training",
        "4_evaluation"
    ]
    
    # This is the path seen in your environment
    folder_prefix = "/Users/odoboo@uni.coventry.ac.uk/Ola_UK_Land_Registry_Project/notebooks/"
    
    pipeline_success = True
    
    for notebook in notebook_files:
        step_start = time.time()
        
        # We try the three common pathing conventions for Databricks Repos/Workspaces
        possible_paths = [
            f"{folder_prefix}{notebook}",            # Standard Absolute
            f"/Workspace{folder_prefix}{notebook}",  # UC Absolute
            f"./{notebook}"                          # Local Relative (if script is in the same folder)
        ]
        
        executed = False
        for path in possible_paths:
            try:
                print(f"Attempting to run: {path}...")
                dbutils.notebook.run(path, 3600, {})
                print(f"SUCCESS: {notebook} finished.")
                executed = True
                break # Exit the path loop if success
            except Exception as e:
                # If it's a 'not found' error, we just try the next path
                if "does not exist" in str(e) or "Unable to access" in str(e):
                    continue
                else:
                    # If it's a code error INSIDE the notebook, we fail the pipeline
                    print(f"!!! CODE ERROR in {notebook} !!!")
                    print(str(e))
                    pipeline_success = False
                    break
        
        if not executed:
            print(f"!!! FAILED: Could not find {notebook} at any known path.")
            pipeline_success = False
            break
            
    total_duration = time.time() - start_total
    print("\n" + "="*60)
    if pipeline_success:
        print(f"PIPELINE COMPLETE: Total Time: {total_duration:.2f}s")
    else:
        print("PIPELINE FAILED: Path/Permission resolution failed.")
    print("="*60)

if __name__ == "__main__":
    run_pipeline()