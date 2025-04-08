# Gaming Session Log Generation & Peak Hours Analysis

This project processes daily Steam game data snapshots, generates a synthetic session log dataset, and prepares the data for analyzing **peak gaming hours** and **weekend activity** using tools like SQL or Tableau.

---

## Project Workflow

The core of the project is a single ETL script:

1.  **Generate Session Logs (`src/etl/generate_sessions.py`)**
    *   Loads daily CSV game data snapshots from the `data/` directory.
    *   Standardizes column names and cleans data (handling variations and numeric formatting).
    *   Calculates game popularity weights based on the average `peak_players` across available days.
    *   Simulates realistic user sessions (`user_id`, `game_id`, `session_start`, `session_end`, `session_duration_minutes`, `day_of_week`, `hour_of_day`) based on game popularity and typical time-of-day patterns (weekday vs. weekend peaks).
    *   Cleans the generated session data (checks durations, handles missing values).
    *   Saves the final cleaned session log data to `output/cleaned_sessions.csv` (path configurable in `config.yaml`).

    **Run the script:**
    ```bash
    python src/etl/generate_sessions.py
    ```
    *(Ensure you have the required dependencies installed and are in the project's root directory)*

2.  **Analyze Peak Hours**
    *   Load the generated `output/cleaned_sessions.csv` file into your preferred analysis tool (e.g., database via SQL import, Tableau, Python with pandas).
    *   Perform aggregations to find:
        *   Peak hours overall and per game.
        *   Differences in activity between weekdays and weekends.
        *   Hourly usage spikes.

---

## Configuration (`config.yaml`)

Key parameters for the session generation are controlled via `config.yaml`:

```yaml
# Project configuration for Gaming Analysis ETL

data_dir: data
output_dir: output
cleaned_sessions_file: output/cleaned_sessions.csv

# Parameters for src/etl/generate_sessions.py
session_generation:
  num_users: 1000
  simulation_days: 30
  start_date: "2022-05-01"
  target_session_count: 15000

# ... (other parameters)
```

Adjust these values in `config.yaml` to control the simulation details and file paths.

---

## Documentation

For more details on the ETL process and data generation logic, refer to:

*   **`ETL_Overview.md`**: High-level explanation of the script's functions and workflow.
*   **`Data_Processing_Details.md`**: In-depth technical details on data handling and session field calculations.

---

## Example SQL Query (using the generated CSV)

Assuming `cleaned_sessions.csv` is loaded into a table named `session_logs`:

**Sessions by Day of Week and Hour:**

```sql
SELECT
  day_of_week,
  hour_of_day,
  COUNT(*) AS session_count
FROM session_logs
GROUP BY day_of_week, hour_of_day
ORDER BY
  CASE day_of_week -- Order days correctly
    WHEN 'Monday' THEN 1
    WHEN 'Tuesday' THEN 2
    WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4
    WHEN 'Friday' THEN 5
    WHEN 'Saturday' THEN 6
    WHEN 'Sunday' THEN 7
  END,
  hour_of_day;
```

---

## Dependencies

*   Python 3.x
*   pandas
*   PyYAML
*   NumPy

Install required packages (preferably in a virtual environment):

```bash
pip install pandas pyyaml numpy
```

---

## Summary

- **Run** the ETL script (`src/etl/generate_sessions.py`) to generate session data.
- **Analyze** the output (`output/cleaned_sessions.csv`) to identify peak hours and weekend trends.
- **Visualize** insights using SQL, Tableau, or other BI tools.

This enables **data-driven decisions** for event planning and player engagement based on simulated session patterns.
