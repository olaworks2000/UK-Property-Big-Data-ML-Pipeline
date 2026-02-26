import pandas as pd
import numpy as np
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import time

# --- TECHNICAL REQUIREMENT 2a: Compare with scikit-learn (single node) baseline ---

print("--- Starting Scikit-Learn (Single Node) Baseline Test ---")

try:
    # 1. UPDATED PATH: Pointing to the new clean sample generated in Notebook 2
    csv_path = "/Volumes/workspace/default/uk_land_registry/github_samples/silver_sample.csv"
    
    # We load the sample (Notebook 2 generated 1,000 rows, which is perfect for a CPU baseline)
    df_sample = pd.read_csv(csv_path)
    
    # 2. Preprocessing
    le = LabelEncoder()
    y = le.fit_transform(df_sample['Property_Type'])
    X = df_sample[['Price']]
    
    # Standardizing features (Mirroring Spark StandardScaler)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Train/Test Split (80/20)
    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

    # 3. Model Training (Single Node)
    start_train = time.time()
    clf = DecisionTreeClassifier(max_depth=5)
    clf.fit(X_train, y_train)
    train_time = time.time() - start_train
    
    # 4. Evaluation
    y_pred = clf.predict(X_test)
    baseline_acc = accuracy_score(y_test, y_pred)
    
    print("-" * 30)
    print(f"Scikit-learn training time (1,000 rows): {train_time:.4f}s")
    print(f"Scikit-learn Baseline Accuracy: {baseline_acc:.4f}")
    print("-" * 30)

    # 5. THE SCALABILITY ARGUMENT (For your Report)
    print("\n--- PERFORMANCE COMPARISON (FOR REPORT EVIDENCE) ---")
    print(f"1. Node Type: Single Machine (Local CPU)")
    print(f"2. Data Volume: 1,000 rows (Sampling 0.003% of total dataset)")
    print(f"3. Scaling Limit: Scikit-learn cannot utilize Spark's distributed memory for 30.9M rows.")
    print(f"4. Result: Successfully established baseline for Spark performance comparison.")

except Exception as e:
    print(f"Baseline comparison failed: {e}")