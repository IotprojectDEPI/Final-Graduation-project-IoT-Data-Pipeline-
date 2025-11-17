# etl_final.py
import pandas as pd
import sqlite3
import time
from datetime import datetime

# ================================
# 1. CONNECT & LOAD DATA
# ================================
DB_PATH = "Readings.db"
XLSX_PATH = "Dataset.xlsx"

print("Starting ETL with Dataset.xlsx")

conn = sqlite3.connect(DB_PATH)
df = pd.read_excel(XLSX_PATH)

print(f"Raw rows loaded: {len(df)}")
print("First 5 rows:")
print(df.head())

# === FIX: ONLY CONVERT NUMERIC COLUMNS (SKIP 'fish') ===
# Identify numeric columns (exclude 'fish' and any non-sensor)
numeric_columns = ['temperature', 'humidity', 'light', 'pH', 
                'electrical_conductivity', 'turbidity', 'water_temp', 'TDS']

# Keep only known columns
df = df[['fish'] + numeric_columns].copy()

# Convert to float
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')

print("\nNaN count per sensor column:")
print(df[numeric_columns].isna().sum())

# Drop rows with NaN in sensor data
df_clean = df.dropna(subset=numeric_columns).copy().reset_index(drop=True)

print(f"\nValid rows after cleaning: {len(df_clean)}")
if len(df_clean) == 0:
    print("ERROR: No valid data! Check if column names match exactly.")
    conn.close()
    exit()

df = df_clean

# ================================
# 2. CONFIG
# ================================
rows_per_sample = 11
num_samples = len(df) // rows_per_sample

# Optimal ranges for 11 species (same as before)
optimum_ranges = [
    [(300, 450), (6.5, 8),   (0.2, 2),   (0, 50),   (26, 32),   (130, 1300)],
    [(300, 400), (7, 8.5),   (0.5, 3),   (0, 50),   (24, 32),   (325, 1950)],
    [(200, 350), (6.5, 8.5), (0.5, 4),   (0, 100),  (22, 28),   (325, 2600)],
    [(400, 600), (7, 8.5),   (0.15, 1.5),(0, 20),   (18, 24),   (100, 1000)],
    [(250, 400), (7.5, 8.5), (0.5, 5),   (0, 50),   (28, 32),   (325, 3250)],
    [(350, 500), (7, 8.5),   (0.1, 1.5), (0, 30),   (28, 32),   (65, 1000)],
    [(300, 450), (6.5, 8),   (0.5, 3),   (0, 60),   (25, 32),   (325, 1950)],
    [(300, 450), (7.8, 8.5), (25, 55),   (0, 50),   (28, 32),   (16000, 35000)],
    [(300, 400), (7, 8.5),   (0.5, 3),   (0, 50),   (24, 32),   (325, 1950)],
    [(300, 450), (6.5, 9),   (0.5, 5),   (0, 80),   (23, 30),   (325, 3250)],
    [(300, 450), (6.5, 8.5), (0.5, 4),   (0, 60),   (25, 32),   (325, 2600)]
]

optimum_Humidity = (60, 80)
optimum_AirTemp = (28, 35)

# ================================
# 3. CLASSIFICATION FUNCTION
# ================================
def classify_sensor(value, min_opt, max_opt):
    if pd.isna(value):
        return "N/A"
    value = float(value)
    if value > max_opt:
        diff = ((value - max_opt) / max_opt) * 100
        if diff <= 15: return "B+"
        elif diff <= 30: return "C+"
        elif diff <= 45: return "D+"
        elif diff <= 60: return "E+"
        elif diff <= 75: return "F+"
        else: return "F++"
    elif value < min_opt:
        diff = ((min_opt - value) / min_opt) * 100
        if diff <= 15: return "B-"
        elif diff <= 30: return "C-"
        elif diff <= 45: return "D-"
        elif diff <= 60: return "E-"
        elif diff <= 75: return "F-"
        else: return "F--"
    else:
        return "A"

# ================================
# 4. REAL-TIME ETL LOOP
# ================================
print("\nETL RUNNING | Inserting 11 rows every 5 seconds")
print("Press Ctrl+C to stop\n")

try:
    while True:
        for i in range(num_samples):
            start = i * rows_per_sample
            end = min(start + rows_per_sample, len(df))
            sample = df.iloc[start:end].copy()

            # Classification
            new_cols = [[] for _ in range(8)]
            for j in range(len(sample)):
                fish_row_idx = start + j
                for k in range(8):
                    if k == 0:
                        min_opt, max_opt = optimum_AirTemp
                    elif k == 1:
                        min_opt, max_opt = optimum_Humidity
                    else:
                        min_opt, max_opt = optimum_ranges[fish_row_idx % len(optimum_ranges)][k - 2]
                    val = sample.iat[j, k + 1]  # +1 to skip 'fish'
                    new_cols[k].append(classify_sensor(val, min_opt, max_opt))

            # Insert classification columns
            col_names = ['temperature', 'humidity', 'light', 'pH', 
                        'electrical_conductivity', 'turbidity', 'water_temp', 'TDS']
            for m in range(8):
                sample.insert(2 * (m + 1), f"{col_names[m]}_class", new_cols[m])

            # Add timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sample.insert(0, "Timestamp", timestamp)

            # Save to DB
            sample.to_sql("Readings", conn, if_exists="append", index=False)

            print(f"Inserted batch {i+1}/{num_samples} | {timestamp} | Rows: {len(sample)}")
            time.sleep(5)

except KeyboardInterrupt:
    print("\nETL Stopped by user.")
finally:
    conn.close()
    print("Database connection closed.")