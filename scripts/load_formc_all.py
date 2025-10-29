# load_formc_all.py
# Requirements:
#   pip install pandas sqlalchemy psycopg2-binary python-dateutil

import os
import csv
import re
import pandas as pd
from sqlalchemy import create_engine
from dateutil import parser

# ----------------- EDIT THESE -----------------
PG_USER = "postgres"
PG_PASS = "yourpassword"          # <-- change to your postgres password
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB   = "sec_stageddata"
SCHEMA  = "formc_data"

# Folder where your TSV files are located (use absolute path)
BASE_DIR = "/Users/rajanirohith/Downloads/2025Q2_cf"
  # <-- change this
# Filenames expected
FILES = {
    "FORM_C_SUBMISSION.tsv": "formc_submission",
    "FORM_C_ISSUER_INFORMATION.tsv": "formc_issuer_information",
    "FORM_C_DISCLOSURE.tsv": "formc_disclosure",
    "FORM_C_SIGNATURE.tsv": "formc_signature"
}
# If you want the loader to append instead of replace, change this per file later.
IF_EXISTS = "replace"   # options: 'replace', 'append'
# ----------------------------------------------

engine = create_engine(
    f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    client_encoding='utf8'
)

def detect_encoding(path, sample_bytes=4096):
    # quick heuristic to guess encoding
    for enc in ("utf-8", "utf-16", "latin1", "cp1252"):
        try:
            with open(path, "rb") as f:
                raw = f.read(sample_bytes)
            raw.decode(enc)
            return enc
        except Exception:
            continue
    return "utf-8"

def detect_header_and_counts(path, delimiter='\t', n_lines=50):
    # returns header (list), sample counts list
    header = None
    counts = []
    # open with guessed encoding
    enc = detect_encoding(path)
    try:
        with open(path, newline='', encoding=enc) as fh:
            reader = csv.reader(fh, delimiter=delimiter)
            for i, row in enumerate(reader):
                if i == 0:
                    header = row
                counts.append(len(row))
                if i >= n_lines - 1:
                    break
    except Exception as e:
        print("Error reading for header detection:", e)
        return None, []
    return header, counts

def find_bad_rows(path, expected_cols, delimiter='\t', max_report=20):
    enc = detect_encoding(path)
    bad = []
    with open(path, newline='', encoding=enc) as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        for i, row in enumerate(reader, start=1):
            if len(row) != expected_cols:
                bad.append((i, len(row), row[:10]))  # keep preview of first 10 fields
                if len(bad) >= max_report:
                    break
    return bad

def normalize_columns(df):
    df.columns = [
        re.sub(r'\s+', '_', str(c).strip().lower())
        for c in df.columns
    ]
    return df

def guess_numeric_date_cols(df):
    cols = list(df.columns)
    numeric_cols = [c for c in cols if re.search(r'(amount|amt|total|number|num|count|fee|price|cost|balance)', c)]
    date_cols = [c for c in cols if re.search(r'(date|day|month|year|dt|posted|filedate|signature_date|effectivedate|first_sale_date)', c)]
    # make sure not to include obvious id columns
    numeric_cols = [c for c in numeric_cols if 'id' not in c and 'seq' not in c]
    return numeric_cols, date_cols

def clean_numeric_series(s):
    # remove common currency punctuation and convert to numeric (coerce errors -> NaN)
    return pd.to_numeric(s.astype(str).str.replace(r'[\$,]', '', regex=True).str.replace(r'\s+', '', regex=True).replace({'': None}), errors='coerce')

def parse_dates_safe(series):
    def _p(x):
        if pd.isna(x): return None
        s = str(x).strip()
        if s == '' or s.lower() in ('nan','none','null'): return None
        try:
            return parser.parse(s, dayfirst=False).date()
        except Exception:
            return None
    return series.apply(_p)

def load_table_from_tsv(file_path, table_name, if_exists=IF_EXISTS):
    print(f"\n--- Processing {file_path} -> {SCHEMA}.{table_name} ---")
    if not os.path.exists(file_path):
        print("File not found:", file_path)
        return False

    # detect header and column counts
    header, counts = detect_header_and_counts(file_path, delimiter='\t', n_lines=50)
    if header is None:
        print("Could not read header. Aborting file.")
        return False
    expected_cols = len(header)
    sample_counts = counts[:10]
    print("Detected header columns:", expected_cols, "Sample column counts:", sample_counts)

    # find bad rows (wrong number of columns)
    bad = find_bad_rows(file_path, expected_cols, delimiter='\t', max_report=20)
    if bad:
        print(f"Found {len(bad)} rows with wrong column counts (expected {expected_cols}). Sample:")
        for line_no, col_count, preview in bad[:10]:
            print(f"  Line {line_no}: cols={col_count} preview={preview}")
        print("Fix the TSV (remove/quote stray tabs or newlines) or ask me to attempt an auto-fix.")
        return False

    # read file with pandas (keep strings to avoid conversion errors)
    enc = detect_encoding(file_path)
    try:
        df = pd.read_csv(file_path, sep='\t', dtype=str, keep_default_na=False, encoding=enc)
    except Exception as e:
        print("pandas read_csv failed:", e)
        # try a more tolerant engine
        try:
            df = pd.read_csv(file_path, sep='\t', dtype=str, keep_default_na=False, engine='python', encoding=enc, on_bad_lines='skip')
            print("Read with python engine and skipped bad lines.")
        except Exception as e2:
            print("Second pandas attempt failed:", e2)
            return False

    print("Rows loaded by pandas:", len(df))
    if len(df) == 0:
        print("No rows to write.")
        return True

    # normalize columns
    df = normalize_columns(df)
    print("Normalized columns:", df.columns.tolist()[:30])

    # guess numeric and date columns heuristically and clean them
    numeric_cols, date_cols = guess_numeric_date_cols(df)
    if numeric_cols:
        print("Numeric-like columns:", numeric_cols)
        for c in numeric_cols:
            if c in df.columns:
                df[c] = clean_numeric_series(df[c])

    if date_cols:
        print("Date-like columns:", date_cols)
        for c in date_cols:
            if c in df.columns:
                df[c] = parse_dates_safe(df[c])

    # convert empty strings to None (so SQL NULL)
    df = df.where(df.notnull() & (df != ''), None)

    # write to Postgres
    try:
        df.to_sql(table_name, engine, schema=SCHEMA, if_exists=if_exists, index=False, method='multi', chunksize=1000)
        print(f"Successfully wrote {len(df)} rows to {SCHEMA}.{table_name} (if_exists='{if_exists}').")
        return True
    except Exception as e:
        print("Initial to_sql failed:", e)
        # fallback: write with all columns as text
        try:
            df2 = df.astype(object).where(df.notnull(), None)
            df2.to_sql(table_name, engine, schema=SCHEMA, if_exists=if_exists, index=False, method='multi', chunksize=500)
            print("Wrote to DB by forcing object dtype.")
            return True
        except Exception as e2:
            print("Fallback write failed:", e2)
            return False

def main():
    print("Starting Form C loader")
    print("Connecting to DB:", f"{PG_HOST}:{PG_PORT}/{PG_DB}")
    for filename, table in FILES.items():
        path = os.path.join(BASE_DIR, filename)
        ok = load_table_from_tsv(path, table, if_exists=IF_EXISTS)
        if not ok:
            print(f"ERROR: Failed to load {filename}. Stopping further processing.")
            return
    print("\nAll files processed successfully. Run verification queries in pgAdmin to confirm counts.")

if __name__ == "__main__":
    main()
