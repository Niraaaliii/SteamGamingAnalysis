# Gaming Data ETL & Peak Hours Analysis

This project processes Steam gaming data, simulates session logs, and enables analysis of **peak gaming hours** and **weekend activity** using SQL and Tableau.

---

## Project Workflow

1. **Clean Raw Data**

- Loads daily CSV files from `data/`
- Cleans and combines them into one dataset
- Saves to `data/cleaned_gaming_data.csv`

Run:

```bash
python3 src/simple_etl.py
```

---

2. **Simulate Session Logs**

- Generates **mocked session-level data** based on cleaned daily totals
- Adds realistic variability in session times and durations
- Saves sessions into an SQLite database `gaming_sessions.db`

Run:

```bash
python3 src/simulate_sessions.py
```

---

3. **Analyze Peak Hours**

- Query `gaming_sessions.db` to find:
  - Peak hours per game
  - Weekend vs weekday activity
  - Hourly usage spikes

Use **DB Browser for SQLite**, Python, or Tableau.

---

## Configuration

All parameters are in `config.yaml`:

```yaml
data_dir: data
cleaned_data_file: data/cleaned_gaming_data.csv
database_file: gaming_sessions.db

avg_session_duration_min: 60
max_sessions_per_day: 20
session_duration_min: 20
session_duration_max: 120
session_count_noise: 0.3
session_peak_hour: 20
session_peak_stddev: 3
```

Adjust these to control simulation realism and file paths.

---

## Example SQL Queries

**Sessions per hour per game:**

```sql
SELECT game_name, strftime('%H', session_start) AS hour, COUNT(*) AS session_count
FROM session_logs
GROUP BY game_name, hour
ORDER BY game_name, hour;
```

**Sessions by weekday and hour:**

```sql
SELECT
  game_name,
  CASE strftime('%w', session_start)
    WHEN '0' THEN 'Sunday'
    WHEN '1' THEN 'Monday'
    WHEN '2' THEN 'Tuesday'
    WHEN '3' THEN 'Wednesday'
    WHEN '4' THEN 'Thursday'
    WHEN '5' THEN 'Friday'
    WHEN '6' THEN 'Saturday'
  END AS weekday,
  strftime('%H', session_start) AS hour,
  COUNT(*) AS session_count
FROM session_logs
GROUP BY game_name, weekday, hour
ORDER BY game_name, weekday, hour;
```

---

## Dependencies

- Python 3.x
- pandas
- pyyaml
- sqlite3 (standard library)

Install with:

```bash
pip3 install pandas pyyaml
```

---

## Summary

- **Clean** your raw gaming data.
- **Simulate** realistic session logs.
- **Analyze** peak hours and weekend activity.
- **Visualize** insights in Tableau or other BI tools.

This enables **data-driven decisions** for event planning and player engagement.
