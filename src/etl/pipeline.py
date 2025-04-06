import os
import glob
import random
import sqlite3
import re
from datetime import datetime, timedelta

import pandas as pd

from src.config import DATA_DIR, DB_NAME, AVG_SESSION_DURATION_MINUTES
from src.utils import clean_numeric_string, clean_column_name


def load_daily_data():
    pattern = os.path.join(DATA_DIR, "*May*2022", "*.csv")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"No daily CSV files found matching pattern: {pattern}")

    daily_data_list = []
    for f in files:
        filename = os.path.basename(f)
        match = re.match(r'(\d+)_(\w+)_(\d{4})\s*\.csv', filename)
        if not match:
            continue
        day, month_str, year = match.groups()
        try:
            file_date = datetime.strptime(f"{day} {month_str} {year}", "%d %B %Y").date()
        except ValueError:
            continue

        df = pd.read_csv(f)
        if df.empty:
            continue

        df.columns = [clean_column_name(c) for c in df.columns]
        expected_cols = ['id', 'name', 'peak_no._of_players', 'hours_played']
        if not all(col in df.columns for col in expected_cols):
            continue

        df = df[expected_cols]
        df['date'] = file_date
        daily_data_list.append(df)

    if not daily_data_list:
        raise ValueError("No valid daily data loaded.")

    df_all = pd.concat(daily_data_list, ignore_index=True)

    # Clean numeric columns
    for col in ['peak_no._of_players', 'hours_played']:
        if col in df_all.columns:
            df_all[col] = df_all[col].apply(clean_numeric_string)
        else:
            df_all[col] = pd.NA

    df_all.dropna(subset=['peak_no._of_players', 'hours_played'], inplace=True)

    # Rename columns
    df_all.rename(columns={
        'peak_no._of_players': 'peak_players',
        'name': 'game_name'
    }, inplace=True)

    return df_all


def load_game_metadata():
    path = os.path.join(DATA_DIR, "games.csv")
    try:
        df = pd.read_csv(path, low_memory=False)
        df.columns = [clean_column_name(c) for c in df.columns]
        return df
    except FileNotFoundError:
        return None
    except Exception:
        return None


def simulate_sessions(df):
    session_logs = []
    user_counter = 0

    for _, row in df.iterrows():
        try:
            total_hours = float(row['hours_played'])
        except (ValueError, TypeError):
            continue

        if total_hours <= 0:
            continue

        total_minutes = total_hours * 60
        num_sessions = int(total_minutes / AVG_SESSION_DURATION_MINUTES)
        if num_sessions <= 0:
            continue

        for _ in range(num_sessions):
            user_counter += 1
            user_id = f"user_{user_counter}"
            start_hour = random.randint(0, 23)
            start_minute = random.randint(0, 59)
            start_second = random.randint(0, 59)
            session_start = datetime.combine(row['date'], datetime.min.time()) + timedelta(
                hours=start_hour, minutes=start_minute, seconds=start_second
            )
            session_end = session_start + timedelta(minutes=AVG_SESSION_DURATION_MINUTES)

            session_logs.append({
                'user_id': user_id,
                'game_id': row['game_name'],
                'session_start': session_start,
                'session_end': session_end,
                'duration': AVG_SESSION_DURATION_MINUTES
            })

    if not session_logs:
        raise ValueError("No session logs simulated.")

    return pd.DataFrame(session_logs)


def setup_database(sessions_df):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS session_logs")
    cursor.execute("""
        CREATE TABLE session_logs (
            session_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            game_id TEXT,
            session_start TIMESTAMP,
            session_end TIMESTAMP,
            duration INTEGER
        )
    """)

    try:
        sessions_df.to_sql('session_logs', conn, if_exists='append', index=False)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_etl():
    daily_df = load_daily_data()
    metadata_df = load_game_metadata()

    # Optionally merge metadata if needed
    # For now, skip merge and use daily_df directly
    sessions_df = simulate_sessions(daily_df)
    setup_database(sessions_df)


if __name__ == "__main__":
    run_etl()
