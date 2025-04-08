# Steam Peak Gaming Hours Analysis (API + SQL + Tableau)

This project fetches real-time player count data for top Steam games using the Steam API, stores it in an SQLite database, and provides guidance for analyzing peak gaming hours and weekend activity using SQL and Tableau.

---

## Project Workflow

1.  **Setup:**
    *   Install required Python dependencies.
    *   Obtain a Steam API key.
    *   Configure the project.
2.  **Data Fetching (`src/etl/fetch_steam_data.py`):**
    *   This script runs periodically (e.g., hourly via a scheduler).
    *   It connects to the Steam API using your key (set as `STEAM_API_KEY` environment variable).
    *   Fetches the current player counts for the top N games (configurable in `config.yaml`).
    *   Connects to the SQLite database specified in `config.yaml`.
    *   Updates a `games` table with AppIDs and names (fetching names via the Store API).
    *   Inserts the `timestamp`, `app_id`, and `player_count` into a `player_counts` table.
3.  **Analysis (SQL & Tableau):**
    *   Connect your preferred SQL client or Tableau to the generated SQLite database (`output/steam_player_counts.sqlite3` by default).
    *   Run SQL queries to aggregate player counts by hour, day of the week, and game to identify peak times and trends.
    *   Visualize the results in Tableau or another BI tool.

---

## Setup Instructions

1.  **Clone the Repository:**
    ```bash
    git clone <your-repo-url>
    cd GamingAnalysis
    ```
2.  **Create a Virtual Environment (Recommended):**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```
3.  **Install Dependencies:**
    ```bash
    pip install requests pyyaml
    ```
    *(Note: `sqlite3` is included with Python 3)*
4.  **Get a Steam API Key:**
    *   Visit the [Steam Community Developer page](https://steamcommunity.com/dev/apikey).
    *   Log in and register for an API key if you don't have one.
5.  **Set Environment Variable:**
    *   Set the `STEAM_API_KEY` environment variable to the key you obtained. How you do this depends on your operating system:
        *   **macOS/Linux (temporary for current session):**
            ```bash
            export STEAM_API_KEY='YOUR_API_KEY_HERE'
            ```
        *   **Windows (Command Prompt, temporary):**
            ```bash
            set STEAM_API_KEY=YOUR_API_KEY_HERE
            ```
        *   **Windows (PowerShell, temporary):**
            ```powershell
            $env:STEAM_API_KEY='YOUR_API_KEY_HERE'
            ```
        *   For persistent storage, add it to your shell profile (`.bashrc`, `.zshrc`, etc.) or system environment variables. **Remember to add the `export STEAM_API_KEY='YOUR_API_KEY_HERE'` line to your shell profile file (e.g., `.zshrc` or `.bashrc`) for the API key to persist across sessions.**
6.  **Configure `config.yaml` (Optional):**
    *   The default settings store the database in `output/steam_player_counts.sqlite3` and fetch the top 10 games.
    *   You can edit `config.yaml` to change the database path or the number of top games (`top_n_games`).

---

## Running the Data Fetcher

The Steam API endpoint used in this project is free to use. However, it's important to respect the API's rate limits to avoid being blocked.

Execute the script manually to fetch the current data:

```bash
python src/etl/fetch_steam_data.py
```

**Verify Data:**

After the script completes, use a tool like `sqlite3` or DB Browser for SQLite to connect to `output/steam_player_counts.sqlite3` and verify that the `games` and `player_counts` tables have been created and populated with data. For example, using `sqlite3`:

```bash
sqlite3 output/steam_player_counts.sqlite3 "SELECT * FROM games LIMIT 5;"
sqlite3 output/steam_player_counts.sqlite3 "SELECT COUNT(*) FROM player_counts;"
```

**Scheduling:**

To collect data over time (e.g., hourly), you need to schedule this script to run automatically:

*   **Linux/macOS:** Use `cron`. Edit your crontab (`crontab -e`) and add a line like this (adjust path and timing):
    ```cron
    0 * * * * /path/to/your/venv/bin/python /path/to/your/GamingAnalysis/src/etl/fetch_steam_data.py >> /path/to/your/GamingAnalysis/output/cron.log 2>&1
    ```
    *(This runs the script at the start of every hour)*
*   **Windows:** Use Task Scheduler. Create a new task that runs the Python interpreter with the script path as an argument, setting the schedule (e.g., trigger hourly). Ensure the task runs with the necessary environment variable (`STEAM_API_KEY`) set or that it's set system-wide.

---

## Database Schema

The script creates/uses an SQLite database (`output/steam_player_counts.sqlite3` by default) with two tables:

1.  **`games`**
    *   `app_id` (INTEGER, PRIMARY KEY): The Steam Application ID.
    *   `name` (TEXT): The name of the game.
    *   `first_seen_timestamp` (TEXT): ISO 8601 timestamp when the game was first added to the DB.
    *   `last_seen_timestamp` (TEXT): ISO 8601 timestamp when the game was last seen in the top N list.
2.  **`player_counts`**
    *   `timestamp` (TEXT): ISO 8601 timestamp of the data point (Primary Key with `app_id`).
    *   `app_id` (INTEGER): Foreign key referencing `games.app_id` (Primary Key with `timestamp`).
    *   `player_count` (INTEGER): Number of concurrent players at that timestamp.

---

## Analysis Examples

### SQL Queries (Connect to the SQLite DB)

**1. Peak Hours Overall (Average Players per Hour):**

```sql
SELECT
    CAST(strftime('%w', timestamp) AS INTEGER) AS day_of_week_num, -- 0=Sun, 1=Mon,... 6=Sat
    CASE CAST(strftime('%w', timestamp) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week_name,
    CAST(strftime('%H', timestamp) AS INTEGER) AS hour_of_day,
    AVG(player_count) AS avg_players,
    SUM(player_count) AS total_players,
    COUNT(*) AS data_points -- Number of entries for this hour/day combo
FROM player_counts
GROUP BY day_of_week_num, hour_of_day
ORDER BY day_of_week_num, hour_of_day;
```

**2. Peak Hours per Game (Example: Top 5 Games by Average Players):**

```sql
WITH GameAvgPlayers AS (
    SELECT
        g.name AS game_name,
        AVG(pc.player_count) AS overall_avg_players
    FROM player_counts pc
    JOIN games g ON pc.app_id = g.app_id
    GROUP BY g.name
    ORDER BY overall_avg_players DESC
    LIMIT 5
)
SELECT
    g.name AS game_name,
    CAST(strftime('%w', pc.timestamp) AS INTEGER) AS day_of_week_num,
    CASE CAST(strftime('%w', pc.timestamp) AS INTEGER)
        WHEN 0 THEN 'Sunday'
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
    END AS day_of_week_name,
    CAST(strftime('%H', pc.timestamp) AS INTEGER) AS hour_of_day,
    AVG(pc.player_count) AS avg_players_this_hour
FROM player_counts pc
JOIN games g ON pc.app_id = g.app_id
WHERE g.name IN (SELECT game_name FROM GameAvgPlayers) -- Filter for top games
GROUP BY g.name, day_of_week_num, hour_of_day
ORDER BY g.name, day_of_week_num, hour_of_day;

```

### Tableau Integration

1.  **Connect to Data:**
    *   Open Tableau Desktop.
    *   Under "Connect" -> "To a File", select "More..." and find your SQLite file (`output/steam_player_counts.sqlite3`). Alternatively, use the "SQLite" connector if available directly.
2.  **Data Model:**
    *   Drag the `player_counts` table onto the canvas.
    *   Drag the `games` table onto the canvas. Tableau should automatically detect the relationship based on `app_id`. If not, create an inner join between `player_counts.app_id` and `games.app_id`.
3.  **Create Visualizations:**
    *   **Peak Hours Heatmap:**
        *   Columns: `timestamp` (set to Hour).
        *   Rows: `timestamp` (set to Weekday).
        *   Color: `SUM(Player Count)` or `AVG(Player Count)`.
        *   Filter by `Name` (from `games` table) if desired.
    *   **Player Count Trends Over Time:**
        *   Columns: `timestamp` (set to Continuous Hour or Day).
        *   Rows: `SUM(Player Count)`.
        *   Color/Detail: `Name` (from `games` table).
    *   **Weekend vs. Weekday Activity:**
        *   Create a calculated field for "Weekend/Weekday": `IF DATEPART('weekday', [Timestamp]) = 1 OR DATEPART('weekday', [Timestamp]) = 7 THEN 'Weekend' ELSE 'Weekday' END`
        *   Columns: `timestamp` (set to Hour).
        *   Rows: `AVG(Player Count)`.
        *   Color: Your "Weekend/Weekday" calculated field.

---

## Cleanup (Optional)

Once you are satisfied with the new system, you can remove the old simulation components:

```bash
rm src/etl/generate_sessions.py
rm -rf data/  # Remove old CSV data if no longer needed
# Consider removing output/cleaned_sessions.csv if it exists
```

*(Ensure you have backed up anything important before deleting)*
