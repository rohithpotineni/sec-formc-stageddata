# make_submission_csv_aligned.py
# Usage: python3 make_submission_csv_aligned.py
# Converts FORM_C_SUBMISSION.csv into FORM_C_SUBMISSION_for_import.csv
# so pgAdmin can import cleanly

import os
import pandas as pd

BASE_DIR = "/Users/rajanirohith/Downloads/2025Q2_cf"
SRC = os.path.join(BASE_DIR, "FORM_C_SUBMISSION.csv")
OUT = os.path.join(BASE_DIR, "FORM_C_SUBMISSION_for_import.csv")

# Table expects these columns
target_cols = ["submission_id", "cik", "filing_date", "intermediary", "portal"]

# Load the CSV
print(f"📂 Reading {SRC}")
df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
print(f"✅ Loaded {len(df)} rows, {len(df.columns)} columns")

# Clean and normalize column names
df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
print("🔍 Original CSV columns:", df.columns.tolist())

# Try mapping automatically
out_df = pd.DataFrame()
out_df["submission_id"] = df.get("accession_number", None)
out_df["cik"] = df.get("cik", None)
out_df["filing_date"] = df.get("filing_date", None)
out_df["intermediary"] = df.get("intermediary", None)
out_df["portal"] = df.get("file_number", None)

# Save new aligned CSV
out_df.to_csv(OUT, index=False)
print(f"💾 Wrote aligned CSV: {OUT}")
print("Columns in output:", out_df.columns.tolist())
