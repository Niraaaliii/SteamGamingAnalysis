#!/usr/bin/env python3
import requests
import sqlite3
import yaml
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import time

# --- Constants ---
STEAM_API_BASE_URL = "https://api.steampowered.com"
# Endpoint for top played games (includes player counts)
TOP_GAMES_ENDPOINT = "/ISteamChartsService/GetMostPlayedGames/v1/"
# Endpoint to get app details (like name) - Use sparingly due to rate limits
APP_DETAILS_ENDPOINT = "https://store.steampowered.com/api/appdetails"

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration Loading ---
def load_config(config_path='config.yaml'):
    """Loads configuration settings from a YAML file."""
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
            logging.info(f"Configuration loaded successfully from {config_path}")
            return config
    except FileNotFoundError:
        logging.error(f"Configuration file not found at {config_path}")
        return None
    except yaml.YAMLError as exc:
        logging.error(f"Error parsing YAML file {config_path}: {exc}")
        return None

# --- API Key Retrieval ---
def get_api_key():
    """Retrieves the Steam API key from the environment variable."""
    api_key = os.environ.get('STEAM_API_KEY')
    if not api_key:
        logging.error("STEAM_API_KEY environment variable not set.")
        raise ValueError("STEAM_API_KEY environment variable is required.")
    logging.info("Steam API key retrieved successfully.")
    return api_key

# --- Database Operations ---
def init_db(db_path_str):
    """Initializes the SQLite database and creates tables if they don't exist."""
    db_path = Path(db_path_str)
    # Ensure the output directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f"Initializing database at: {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create games table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS games (
                app_id INTEGER PRIMARY KEY,
                name TEXT,
                first_seen_timestamp TEXT,
                last_seen_timestamp TEXT
            )
        ''')

        # Create player_counts table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS player_counts (
                timestamp TEXT,
                app_id INTEGER,
                player_count INTEGER,
                PRIMARY KEY (timestamp, app_id),
                FOREIGN KEY (app_id) REFERENCES games(app_id)
            )
        ''')

        conn.commit()
        logging.info("Database tables ensured.")
        return conn
    except sqlite3.Error as e:
        logging.error(f"Database error during initialization: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during DB initialization: {e}")
        raise

def get_game_name(app_id):
    """Fetches the game name for a given AppID from the Steam Store API."""
    logging.debug(f"Fetching name for AppID: {app_id}")
    try:
        response = requests.get(APP_DETAILS_ENDPOINT, params={'appids': app_id})
        response.raise_for_status()
        data = response.json()
        
        # Check response structure
        app_data = data.get(str(app_id))
        if app_data and app_data.get('success'):
            name = app_data.get('data', {}).get('name')
            if name:
                logging.debug(f"Found name for AppID {app_id}: {name}")
                return name
            else:
                 logging.warning(f"Name not found in successful response for AppID {app_id}")
                 return None
        else:
            logging.warning(f"Failed to get details or unsuccessful response for AppID {app_id}: {data.get('success', 'N/A')}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching game name for AppID {app_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error fetching name for AppID {app_id}: {e}")
        return None

def update_games_table(conn, games_data, current_timestamp_iso):
    """Updates the games table with new games or updates last_seen timestamp."""
    cursor = conn.cursor()
    updated_count = 0
    inserted_count = 0
    
    app_ids_in_batch = {game['appid'] for game in games_data}

    # Fetch existing games from DB to minimize lookups
    cursor.execute("SELECT app_id, name FROM games WHERE app_id IN ({})".format(','.join('?'*len(app_ids_in_batch))), tuple(app_ids_in_batch))
    existing_games = {row[0]: row[1] for row in cursor.fetchall()} # {app_id: name}

    games_to_insert = []
    app_ids_to_update_timestamp = []

    for game in games_data:
        app_id = game['appid']
        
        if app_id not in existing_games:
            # Game not in DB, need to fetch name and insert
            game_name = get_game_name(app_id)
            # Use AppID as fallback name if API fails or name is missing
            if not game_name:
                 logging.warning(f"Using AppID {app_id} as fallback name.")
                 game_name = f"AppID_{app_id}"
            
            games_to_insert.append((app_id, game_name, current_timestamp_iso, current_timestamp_iso))
            inserted_count += 1
            # Add a small delay to avoid hitting rate limits on appdetails endpoint
            time.sleep(1.5) # Adjust as needed
        else:
            # Game exists, just need to update last_seen
            app_ids_to_update_timestamp.append(app_id)
            updated_count += 1
            # Check if name needs updating (e.g., if it was a fallback)
            if existing_games[app_id] == f"AppID_{app_id}":
                game_name = get_game_name(app_id)
                if game_name and game_name != existing_games[app_id]:
                    logging.info(f"Updating name for AppID {app_id} from fallback to '{game_name}'")
                    cursor.execute("UPDATE games SET name = ? WHERE app_id = ?", (game_name, app_id))
                    time.sleep(1.5) # Delay after name fetch


    # Batch insert new games
    if games_to_insert:
        cursor.executemany('''
            INSERT INTO games (app_id, name, first_seen_timestamp, last_seen_timestamp)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(app_id) DO NOTHING 
        ''', games_to_insert)
        logging.info(f"Inserted {inserted_count} new games into the games table.")

    # Batch update last_seen_timestamp for existing games
    if app_ids_to_update_timestamp:
        cursor.executemany('''
            UPDATE games 
            SET last_seen_timestamp = ? 
            WHERE app_id = ?
        ''', [(current_timestamp_iso, app_id) for app_id in app_ids_to_update_timestamp])
        logging.info(f"Updated last_seen_timestamp for {updated_count} existing games.")

    conn.commit()


def insert_player_counts(conn, games_data, timestamp_iso):
    """Inserts player count data into the player_counts table."""
    cursor = conn.cursor()
    counts_to_insert = []
    for game in games_data:
        app_id = game.get('appid')
        player_count = game.get('peak_in_game')
        if app_id is not None and player_count is not None:
            counts_to_insert.append((timestamp_iso, app_id, player_count))
        else:
            logging.warning(f"Missing appid or peak_in_game in game data: {game}")

    if counts_to_insert:
        try:
            cursor.executemany('''
                INSERT INTO player_counts (timestamp, app_id, player_count)
                VALUES (?, ?, ?)
                ON CONFLICT(timestamp, app_id) DO NOTHING
            ''', counts_to_insert)
            conn.commit()
            logging.info(f"Inserted {len(counts_to_insert)} player count records for timestamp {timestamp_iso}.")
        except sqlite3.Error as e:
            logging.error(f"Database error inserting player counts: {e}")
            conn.rollback() # Rollback on error
    else:
        logging.warning("No valid player count data to insert.")


# --- Steam API Fetching ---
def fetch_top_games_data(api_key):
    """Fetches the list of top played games from the Steam API."""
    url = f"{STEAM_API_BASE_URL}{TOP_GAMES_ENDPOINT}"
    params = {'key': api_key}
    logging.info("Fetching top games data from Steam API...")
    try:
        response = requests.get(url, params=params, timeout=30) # Added timeout
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        data = response.json()
        
        # Navigate the response structure (adjust based on actual API response)
        # Example structure assumption: {'response': {'ranks': [...]}}
        ranks = data.get('response', {}).get('ranks', [])
        if ranks:
             logging.info(f"Successfully fetched data for {len(ranks)} top games.")
             return ranks
        else:
             logging.warning("No game ranks found in the API response.")
             logging.debug(f"API Response structure: {data}")
             return []
             
    except requests.exceptions.Timeout:
        logging.error("Request to Steam API timed out.")
        return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching top games from Steam API: {e}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred during API fetch: {e}")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    logging.info("Starting Steam data fetching script...")

    config = load_config()
    if not config:
        exit(1)

    try:
        api_key = get_api_key()
        db_path = config.get('database', {}).get('path', 'output/steam_data.sqlite3') # Default path if not in config
        top_n = config.get('steam_api', {}).get('top_n_games', 10) # Default N if not in config

        conn = init_db(db_path)
        
        top_games_full_list = fetch_top_games_data(api_key)

        if top_games_full_list is not None:
            # Select only the top N games
            top_n_games = top_games_full_list[:top_n]
            
            if not top_n_games:
                 logging.warning("No top games data received from API or list was empty.")
            else:
                logging.info(f"Processing top {len(top_n_games)} games.")
                current_timestamp = datetime.now(timezone.utc)
                current_timestamp_iso = current_timestamp.isoformat()

                # Update games table (add new games, update last seen)
                update_games_table(conn, top_n_games, current_timestamp_iso)

                # Insert player counts for the current timestamp
                insert_player_counts(conn, top_n_games, current_timestamp_iso)

                logging.info("Data fetching and insertion complete.")
        else:
            logging.error("Failed to fetch top games data from Steam API. No data inserted.")

    except ValueError as e: # Catch API key error specifically
        logging.error(f"Configuration error: {e}")
        exit(1)
    except sqlite3.Error as e:
        logging.error(f"A database error occurred: {e}")
        exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
        exit(1)
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            logging.info("Database connection closed.")

    logging.info("Steam data fetching script finished.")
