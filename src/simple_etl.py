import os
import glob
import re
import pandas as pd

DATA_DIR = 'data'
OUTPUT_FILE = os.path.join(DATA_DIR, 'cleaned_gaming_data.csv')

def clean_numeric(value):
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        value = value.replace('"', '').replace(',', '')
        try:
            return float(value) if '.' in value else int(value)
        except ValueError:
            return pd.NA
    return pd.NA

def clean_column(name):
    name = name.replace('"', '').strip()
    name = re.sub(r'\s+', '_', name)
    return name.lower()

def main():
    pattern = os.path.join(DATA_DIR, "*May*2022", "*.csv")
    files = glob.glob(pattern)
    if not files:
        print(f"No files found matching {pattern}")
        return

    dfs = []
    for f in files:
        filename = os.path.basename(f)
        match = re.match(r'(\d+)_(\w+)_(\d{4})\s*\.csv', filename)
        if not match:
            continue
        day, month, year = match.groups()
        try:
            date = pd.to_datetime(f"{day} {month} {year}")
        except:
            continue

        df = pd.read_csv(f)
        if df.empty:
            continue

        df.columns = [clean_column(c) for c in df.columns]
        expected = ['id', 'name', 'peak_no._of_players', 'hours_played']
        if not all(c in df.columns for c in expected):
            continue

        df = df[expected]
        df['date'] = date

        df['peak_no._of_players'] = df['peak_no._of_players'].apply(clean_numeric)
        df['hours_played'] = df['hours_played'].apply(clean_numeric)

        dfs.append(df)

    if not dfs:
        print("No valid data loaded.")
        return

    combined = pd.concat(dfs, ignore_index=True)
    combined.dropna(subset=['peak_no._of_players', 'hours_played'], inplace=True)
    combined.rename(columns={'peak_no._of_players': 'peak_players', 'name': 'game_name'}, inplace=True)

    combined.to_csv(OUTPUT_FILE, index=False)
    print(f"Cleaned data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
