# Technical Details: Data Processing and Session Log Generation

This document details the technical implementation of data processing and the logic behind generating the fields in the synthetic session logs within `src/etl/generate_sessions.py`.

## 1. Input Data Handling (`_load_and_standardize_game_data`)

*   **Source:** Reads `.csv` files from the directory specified in `config.yaml` (`data_dir`).
*   **Column Name Standardization:**
    *   Initial cleaning converts names to lowercase, strips whitespace, and replaces spaces/hyphens with underscores (e.g., "Peak Players " -> `peak_players`).
    *   A predefined `standard_columns` dictionary maps known variations (e.g., `peakplayers`, `peak_concurrent`) to standard names (`peak_players`).
    *   The script attempts to find variations for each standard column in the cleaned CSV columns and renames them accordingly.
    *   Required columns (`name`, `peak_players`) must be found (in some variation) for a file to be processed further.
*   **Data Type Cleaning:**
    *   **Numeric Columns (`peak_players`, `current_players`, `hours_played`):** The `_clean_numeric_column` function is applied. It converts the column to string, uses regex `[^0-9]` to remove all non-digit characters, replaces any resulting empty strings with '0', and converts the final string to an integer. This handles values like "1,234" or potentially erroneous non-numeric entries.
    *   **String Columns (`name`):** Ensures the column is string type and strips leading/trailing whitespace.
*   **Output:** A list containing only the essential `name` and `peak_players` columns (as pandas DataFrames) from each successfully processed input file.

## 2. Game Popularity Weight Calculation (`_calculate_game_weights`)

*   **Aggregation:** All the extracted `name` and `peak_players` DataFrames are concatenated into one large DataFrame.
*   **Metric:** The average (`mean`) of `peak_players` is calculated for each unique game name (`groupby('name')`). This average across all available days serves as the primary indicator of popularity.
*   **Ranking:** `rank(method='dense')` assigns a numerical rank to each game based on its average peak players. Higher average = higher rank. Dense ranking ensures consecutive integer ranks.
*   **Output:** A dictionary (`game_weights`) mapping game names to their calculated popularity rank (weight).

## 3. Session Log Field Generation (`_simulate_user_sessions`)

This function simulates `target_session_count` sessions, generating each field as follows:

*   **`user_id`**: Randomly chosen from a pre-generated list of unique IDs (`USER_0001`, `USER_0002`, ...).
*   **`game_id`**: Chosen randomly based on probability. The `game_weights` (ranks) are normalized into probabilities (summing to 1). `np.random.choice` uses these probabilities, making games with higher weights more likely to be selected.
*   **`session_start`**:
    *   A random date within the `simulation_days` range from the `start_date` (from config) is chosen.
    *   The hour is determined probabilistically based on the day of the week:
        *   **Weekends (Sat/Sun):** `np.random.triangular(12, 20, 23)` - Simulates activity peaking around 8 PM, spread between noon and 11 PM.
        *   **Weekdays (Mon-Fri):** `np.random.normal(19, 2.5)` - Simulates activity peaking around 7 PM, following a normal distribution.
    *   The generated hour is clipped to the range [0, 23].
    *   A random minute [0, 59] is chosen.
    *   The date, hour, and minute are combined into a datetime object.
*   **`session_duration`** (in minutes):
    *   Generated using `np.random.lognormal(mean=3.8, sigma=1.0)`. The adjusted parameters favor longer durations compared to the previous version.
    *   The result is clamped between 15 and 600 minutes (10 hours) and converted to an integer, allowing for more realistic multi-hour sessions.
*   **`session_end`**: Calculated by adding `session_duration` (in minutes) to `session_start`.
*   **`day_of_week`**: Extracted from `session_start` using `strftime('%A')` (e.g., "Monday").
*   **`hour_of_day`**: The integer hour (0-23) calculated during `session_start` generation.

## 4. Final Cleaning (`clean_sessions`)

*   **Datetime Conversion:** `pd.to_datetime` ensures `session_start` and `session_end` are proper datetime objects.
*   **Duration Recalculation:** Duration (`session_duration`) is recalculated precisely using `(session_end - session_start).dt.total_seconds() / 60` for accuracy and consistency.
*   **Validation:** Rows with `session_duration <= 0` or any missing values (`dropna()`) are removed.

## Output File (`output/cleaned_sessions.csv`)

The final output CSV contains the following columns, ready for analysis:
`user_id`, `game_id`, `session_start`, `session_end`, `session_duration`, `day_of_week`, `hour_of_day`.
