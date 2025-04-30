# retrain_model.py

import pandas as pd
import pickle
from xgboost import XGBClassifier
from datetime import datetime
import os
import shutil

# --- Setup paths ---
OLD_DATA_PATH = "data/old_training_data.csv"
NEW_DATA_PATH = "data/new_transactions.csv"
ARCHIVE_DIR = "archive/"
MODEL_DIR = "models/"
STATS_DIR = "stats/"

# --- Ensure folders exist ---
os.makedirs("data", exist_ok=True)
os.makedirs(ARCHIVE_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(STATS_DIR, exist_ok=True)

# --- Load datasets ---
old_data = pd.read_csv(OLD_DATA_PATH)
new_data = pd.read_csv(NEW_DATA_PATH)

# Combine old + new
full_data = pd.concat([old_data, new_data], ignore_index=True)
full_data = full_data.drop_duplicates()

# --- Preprocessing and Feature Engineering ---
model_df = full_data[['is_3d_encoded', 'bin', 'processor_name_encoded', 'status_encoded']].dropna(subset=['bin'])

# Create success_flag (1=approved, 0=declined)
model_df['success_flag'] = model_df['status_encoded'].apply(lambda x: 1 if x == 0 else 0)

# Create bin_prefix and bin_suffix
model_df['bin_prefix'] = model_df['bin'] // 1000
model_df['bin_suffix'] = model_df['bin'] % 1000

# Filter processors with at least 10 samples
valid_processors = model_df['processor_name_encoded'].value_counts()
valid_processors = valid_processors[valid_processors >= 10].index
model_df = model_df[model_df['processor_name_encoded'].isin(valid_processors)]

# Aggregations
bin_tx = model_df.groupby('bin').size().reset_index(name='bin_tx_count')
bin_success = model_df.groupby('bin')['success_flag'].mean().reset_index(name='bin_success_rate')
proc_success = model_df.groupby('processor_name_encoded')['success_flag'].mean().reset_index(name='processor_success_rate')

bin_proc_group = model_df.groupby(['bin', 'processor_name_encoded']).agg(
    bin_processor_tx_count=('success_flag', 'count'),
    bin_processor_success_count=('success_flag', 'sum')
).reset_index()

bin_proc_group['bin_processor_success_rate'] = (
    bin_proc_group['bin_processor_success_count'] / bin_proc_group['bin_processor_tx_count']
)

# Merge features
model_df = model_df.merge(bin_tx, on='bin', how='left')
model_df = model_df.merge(bin_success, on='bin', how='left')
model_df = model_df.merge(proc_success, on='processor_name_encoded', how='left')
model_df = model_df.merge(bin_proc_group, on=['bin', 'processor_name_encoded'], how='left')

model_df = model_df.fillna(0)

# Feature and label selection
features = [
    'bin', 'bin_prefix', 'bin_suffix', 'is_3d_encoded',
    'bin_tx_count', 'bin_success_rate', 'processor_success_rate',
    'bin_processor_tx_count', 'bin_processor_success_count', 'bin_processor_success_rate'
]
X = model_df[features]
y = model_df['success_flag']

# Train full model
model = XGBClassifier(
    eval_metric='logloss',
    n_estimators=200,
    learning_rate=0.05,
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)
model.fit(X, y)

# Save model
timestamp = datetime.now().strftime("%Y%m%d_%H%M")
model_path = os.path.join(MODEL_DIR, f"model_{timestamp}.pkl")
model_latest_path = os.path.join(MODEL_DIR, "model_latest.pkl")

with open(model_path, "wb") as f:
    pickle.dump(model, f)

with open(model_latest_path, "wb") as f:
    pickle.dump(model, f)

# Save supporting stats
with open(os.path.join(STATS_DIR, f"processor_success_stats_{timestamp}.pkl"), "wb") as f:
    pickle.dump({
        "bin_tx": bin_tx.set_index("bin").to_dict(orient="index"),
        "bin_success": bin_success.set_index("bin").to_dict(orient="index"),
        "proc_success": proc_success.set_index("processor_name_encoded").to_dict(orient="index"),
        "bin_proc_stats": bin_proc_group.set_index(['bin', 'processor_name_encoded']).to_dict(orient="index"),
        "all_processors": list(model_df['processor_name_encoded'].unique())
    }, f)

# Archive new_transactions
archive_filename = f"transactions_{datetime.now().strftime('%Y%m')}.csv"
archive_path = os.path.join(ARCHIVE_DIR, archive_filename)
shutil.move(NEW_DATA_PATH, archive_path)

# Reset new_transactions.csv
pd.DataFrame(columns=new_data.columns).to_csv(NEW_DATA_PATH, index=False)

print(f"""
✅ Full retraining completed!
- Model saved at: {model_path}
- Latest model saved at: {model_latest_path}
- Stats file saved at: {STATS_DIR}
- New transactions archived at: {archive_path}
Ready for next month's cycle! 🚀
""")
