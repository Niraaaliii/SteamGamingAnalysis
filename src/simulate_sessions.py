import os
import sqlite3
import random
from datetime import datetime, timedelta
import pandas as pd
import yaml

with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

DATA_FILE = config['cleaned_data_file']
DB_FILE = config['database_file']
AVG_SESSION_DURATION_MIN = config['avg_session_duration_min']
MAX_SESSIONS_PER_DAY = config['max_sessions_per_day']
SESSION_DURATION_MIN = config['session_duration_min']
SESSION_DURATION_MAX = config['session_duration_max']
SESSION_COUNT_NOISE = config['session_count_noise']
SESSION_PEAK_HOUR = config['session_peak_hour']
SESSION_PEAK_STDDEV = config['session_peak_stddev']

def main():
    if not os.path.exists(DATA_FILE):
        print(f"Cleaned data file not found: {DATA_FILE}")
        return

    df = pd.read_csv(DATA_FILE, parse_dates=['date'])
    session_logs = []
    user_counter = 0

    for _, row in df.iterrows():
        total_hours = row['hours_played']
        if total_hours <= 0:
            continue

        total_minutes = total_hours * 60

        # Add randomness to session count estimation
        est_sessions = total_minutes / AVG_SESSION_DURATION_MIN
        est_sessions += random.uniform(-SESSION_COUNT_NOISE, SESSION_COUNT_NOISE) * est_sessions
        num_sessions = max(1, int(est_sessions))
        num_sessions = min(num_sessions, MAX_SESSIONS_PER_DAY)

        for _ in range(num_sessions):
            user_counter += 1
            user_id = f"user_{user_counter}"

            # Bias start hour towards evening
            start_hour = int(random.gauss(SESSION_PEAK_HOUR, SESSION_PEAK_STDDEV))
            start_hour = max(0, min(23, start_hour))

            start_minute = random.randint(0, 59)
            start_second = random.randint(0, 59)

            session_start = datetime.combine(row['date'], datetime.min.time()) + timedelta(
                hours=start_hour, minutes=start_minute, seconds=start_second
            )

            # Randomize session duration
            duration = random.randint(SESSION_DURATION_MIN, SESSION_DURATION_MAX)
            session_end = session_start + timedelta(minutes=duration)

            session_logs.append({
                'user_id': user_id,
                'game_name': row['game_name'],
                'session_start': session_start,
                'session_end': session_end,
                'duration': duration
            })

    if not session_logs:
        print("No sessions simulated.")
        return

    sessions_df = pd.DataFrame(session_logs)

    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS session_logs")
        cursor.execute("""
            CREATE TABLE session_logs (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT,
                game_name TEXT,
                session_start TIMESTAMP,
                session_end TIMESTAMP,
                duration INTEGER
            )
        """)

        sessions_df.to_sql('session_logs', conn, if_exists='append', index=False)
        conn.commit()
        print(f"Simulated {len(sessions_df)} sessions saved to {DB_FILE}")
    except Exception as e:
        print(f"Error saving sessions to database: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
