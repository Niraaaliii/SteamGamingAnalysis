# ETL Script Overview: `src/etl/generate_sessions.py`

This document provides a high-level overview of the ETL (Extract, Transform, Load) script used to generate synthetic gaming session data for analysis.

## Objective

The primary goal of this script is to produce a clean dataset (`output/cleaned_sessions.csv`) simulating user gaming sessions. This dataset is designed to support analysis of peak gaming hours and weekend activity patterns, based on the popularity of games derived from input data.

## Main Components and Flow

The script operates through several key functions:

1.  **`load_config(config_path='config.yaml')`**:
    *   **Purpose:** Reads configuration parameters (like number of users, simulation duration, input/output paths) from the `config.yaml` file.
    *   **Benefit:** Allows easy modification of simulation parameters without changing the script code.

2.  **`generate_sessions(config)`**:
    *   **Purpose:** Orchestrates the main ETL process. It takes the loaded configuration as input.
    *   **Internal Steps:**
        *   Calls `_load_and_standardize_game_data` to process input CSVs.
        *   Calls `_calculate_game_weights` to determine game popularity.
        *   Calls `_simulate_user_sessions` to generate the raw session data based on weights and time patterns.
    *   **Output:** Returns a pandas DataFrame containing the raw simulated session data.

3.  **`_load_and_standardize_game_data(data_dir)`**: (Helper for `generate_sessions`)
    *   **Purpose:** Reads all `.csv` files from the specified data directory. For each file, it cleans column names, maps them to a standard schema (handling variations like "Peak Players" vs "peak_concurrent"), cleans numeric data (removing commas, converting to integers), and extracts the essential `name` and `peak_players` columns.
    *   **Output:** A list of DataFrames, each containing the standardized `name` and `peak_players` from one input file.

4.  **`_calculate_game_weights(all_games_data)`**: (Helper for `generate_sessions`)
    *   **Purpose:** Takes the list of standardized data from the previous step, combines it, calculates the *average* `peak_players` for each unique game across all input files, and then ranks the games based on this average.
    *   **Output:** A dictionary mapping each game name to its popularity rank (weight).

5.  **`_simulate_user_sessions(...)`**: (Helper for `generate_sessions`)
    *   **Purpose:** Generates the synthetic session log entries. It uses the calculated game weights to probabilistically select games, and employs different statistical distributions to simulate realistic session start times (weekday vs. weekend peaks) and durations.
    *   **Output:** A pandas DataFrame containing the simulated sessions with columns like `user_id`, `game_id`, `session_start`, `session_end`, etc.

6.  **`clean_sessions(df)`**:
    *   **Purpose:** Takes the raw simulated session DataFrame, converts time columns to the correct format, recalculates durations for accuracy, removes invalid sessions (e.g., zero duration), and handles any missing values.
    *   **Output:** A cleaned pandas DataFrame ready for analysis.

7.  **Main Execution Block (`if __name__ == "__main__":`)**:
    *   **Purpose:** Controls the script execution when run directly.
    *   **Steps:** Loads config, calls `generate_sessions`, calls `clean_sessions`, and saves the final cleaned DataFrame to the path specified in `config.yaml` (e.g., `output/cleaned_sessions.csv`). Includes basic logging for monitoring progress and errors.

## Outcome

The script produces the `output/cleaned_sessions.csv` file, containing structured, simulated session data suitable for analyzing gaming activity patterns, particularly focusing on hourly and daily trends.
