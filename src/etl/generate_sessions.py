import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import yaml
import logging
from pathlib import Path

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration Loading ---
def load_config(config_path='config.yaml'):
    """Loads configuration settings from a YAML file.

    Args:
        config_path (str, optional): Path to the configuration file. 
                                     Defaults to 'config.yaml'.

    Returns:
        dict: A dictionary containing the configuration settings, 
              or None if loading fails.
    """
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

# --- Helper Functions for generate_sessions ---

def _clean_numeric_column(series):
    """Cleans a pandas Series assumed to contain numeric data as strings.
    
    Removes non-digit characters, replaces empty strings with '0', 
    and converts to integer type.

    Args:
        series (pd.Series): The pandas Series to clean.

    Returns:
        pd.Series: The cleaned pandas Series as integer type.
    """
    return (series.astype(str)
            .str.replace('[^0-9]', '', regex=True)
            .replace('', '0')
            .astype(int))

def _load_and_standardize_game_data(data_dir):
    """Loads game data from CSV files, standardizes columns, and cleans data.

    Args:
        data_dir (str): The directory containing the input CSV files.

    Returns:
        list: A list of pandas DataFrames, each containing the standardized 
              'name' and 'peak_players' columns from a successfully processed CSV file.
              Returns an empty list if no valid data could be processed.
    """
    standard_columns = {
        'name': {'variations': ['name', 'game', 'title'], 'dtype': 'string', 'required': True},
        'current_players': {'variations': ['current_players', 'players_now'], 'dtype': 'int', 'required': False},
        'current_players': {'variations': ['current_players', 'players_now'], 'dtype': 'int', 'required': False},
        'peak_players': {'variations': ['peak_players', 'peakplayers', 'peak_concurrent'], 'dtype': 'int', 'required': True},
        'hours_played': {'variations': ['hours_played', 'hoursplayed', 'playtime'], 'dtype': 'int', 'required': False}
    }
    
    all_games_data = []
    processed_files_count = 0
    data_path = Path(data_dir)

    if not data_path.is_dir():
        logging.error(f"Data directory not found: {data_dir}")
        return []

    for csv_file in data_path.glob('*.csv'):
        processed_files_count += 1
        logging.info(f"Processing file: {csv_file.name}")
        try:
            df = pd.read_csv(csv_file)
            
            # Initial column cleaning
            df.columns = df.columns.str.strip().str.lower().str.replace(r'[\s\-]+', '_', regex=True)
            
            rename_map = {}
            missing_required = False
            
            # Map variations to standard names
            for std_name, details in standard_columns.items():
                found_col = None
                for variation in details['variations']:
                    cleaned_variation = variation.lower().replace(' ', '_')
                    if cleaned_variation in df.columns:
                        found_col = cleaned_variation
                        break
                
                if found_col:
                    if found_col != std_name:
                         rename_map[found_col] = std_name
                elif details['required']:
                    logging.warning(f"Required column '{std_name}' (or variations) not found in {csv_file.name}. Skipping file.")
                    missing_required = True
                    break
            
            if missing_required:
                continue

            # Apply renaming
            df = df.rename(columns=rename_map)

            # Clean data types and values
            for std_name, details in standard_columns.items():
                 if std_name in df.columns:
                    try:
                        if details['dtype'] == 'int':
                            df[std_name] = _clean_numeric_column(df[std_name])
                        elif details['dtype'] == 'string':
                             df[std_name] = df[std_name].astype(str).str.strip()
                    except Exception as e:
                         logging.error(f"Error cleaning column '{std_name}' in {csv_file.name}: {e}. Skipping column.")
                         df = df.drop(columns=[std_name])

            # Select required columns for weighting
            if 'name' in df.columns and 'peak_players' in df.columns:
                 all_games_data.append(df[['name', 'peak_players']])
            else:
                 logging.warning(f"Could not extract 'name' and 'peak_players' columns from {csv_file.name} after standardization. Skipping file for weighting.")

        except FileNotFoundError:
             logging.error(f"File not found {csv_file}. Skipping.")
        except pd.errors.EmptyDataError:
             logging.warning(f"File {csv_file.name} is empty. Skipping.")
        except Exception as e:
            logging.error(f"Error processing file {csv_file.name}: {e}")

    logging.info(f"Finished processing input files. Found {processed_files_count} CSVs, successfully extracted data from {len(all_games_data)}.")
    return all_games_data


def _calculate_game_weights(all_games_data):
    """Calculates game popularity weights based on average peak players.

    Args:
        all_games_data (list): A list of DataFrames, each with 'name' and 
                               'peak_players' columns.

    Returns:
        dict: A dictionary mapping game names to their popularity weight (rank).
              Returns an empty dictionary if input data is empty.
    """
    if not all_games_data:
        logging.warning("No game data provided for weight calculation.")
        return {}
        
    game_df = pd.concat(all_games_data)
    game_weights = (game_df.groupby('name')['peak_players']
                   .mean()
                   .rank(method='dense')  # Normalize weights
                   .to_dict())
    logging.info(f"Calculated weights for {len(game_weights)} unique games.")
    return game_weights


def _simulate_user_sessions(game_weights, num_users, simulation_days, start_date, target_session_count):
    """Simulates user gaming sessions based on game weights and time patterns.

    Args:
        game_weights (dict): Dictionary mapping game names to popularity weights.
        num_users (int): Number of unique users to simulate.
        simulation_days (int): Number of days over which to simulate sessions.
        start_date (datetime): The starting date for the simulation period.
        target_session_count (int): The total number of sessions to simulate.

    Returns:
        pd.DataFrame: A DataFrame containing the simulated session data.
                      Returns an empty DataFrame if no games are provided.
    """
    if not game_weights:
        logging.error("Cannot simulate sessions without game weights.")
        return pd.DataFrame()

    games = list(game_weights.keys())
    weights_array = np.array(list(game_weights.values()))
    probabilities = weights_array / sum(weights_array) # Normalize weights to probabilities

    user_ids = [f'USER_{i:04d}' for i in range(1, num_users + 1)]
    
    sessions = []
    np.random.seed(42) # For reproducibility

    logging.info(f"Simulating {target_session_count} sessions for {num_users} users over {simulation_days} days...")
    for i in range(target_session_count):
        if (i + 1) % (target_session_count // 10) == 0: # Log progress every 10%
             logging.info(f"Simulation progress: {((i+1)/target_session_count)*100:.0f}% complete")

        user = np.random.choice(user_ids)
        game = np.random.choice(games, p=probabilities)
        
        # Generate session start time
        day_offset = np.random.randint(0, simulation_days)
        current_date = start_date + timedelta(days=day_offset)
        is_weekend = current_date.weekday() >= 5
        
        if is_weekend:
            base_hour = np.random.triangular(12, 20, 23) # Weekend peak: Noon-11pm, centered around 8pm
        else:
            # Increased stddev slightly for more weekday start time variability
            base_hour = np.random.normal(19, 3.0) # Weekday peak: Centered around 7pm, slightly more spread
            
        hour = int(np.clip(base_hour, 0, 23))
        minute = np.random.randint(0, 59)
        session_start = current_date.replace(hour=hour, minute=minute)
        
        # Generate session duration - Adjusted parameters for longer sessions
        # Increased mean and sigma, increased clamp range
        duration = np.random.lognormal(mean=3.8, sigma=1.0) # Adjusted log-normal parameters
        duration = max(15, min(600, int(duration)))  # Clamp between 15min and 10hr (600 min)
        session_end = session_start + timedelta(minutes=duration)
        
        sessions.append({
            'user_id': user,
            'game_id': game,
            'session_start': session_start.isoformat(),
            'session_end': session_end.isoformat(),
            'session_duration': duration, # Renamed column
            'day_of_week': session_start.strftime('%A'),
            'hour_of_day': hour
        })
    
    logging.info("Session simulation complete.")
    return pd.DataFrame(sessions)


# --- Main Generation Function ---
def generate_sessions(config):
    """Generates a synthetic dataset of user gaming sessions.

    Reads configuration, loads and standardizes game data from CSVs,
    calculates game popularity weights, simulates sessions based on weights
    and time patterns.

    Args:
        config (dict): The configuration dictionary loaded from config.yaml.

    Returns:
        pd.DataFrame: A DataFrame containing the raw simulated session data,
                      or None if a critical error occurs (e.g., config error, 
                      no valid input data).
    """
    if not config or 'session_generation' not in config:
        logging.error("Session generation configuration missing in config file.")
        return None
        
    gen_config = config['session_generation']
    data_dir = config.get('data_dir', 'data') # Get data_dir from main config section

    # Validate and get parameters
    try:
        num_users = int(gen_config['num_users'])
        simulation_days = int(gen_config['simulation_days'])
        start_date_str = gen_config['start_date']
        target_session_count = int(gen_config['target_session_count'])
        start_date = datetime.fromisoformat(start_date_str)
    except (KeyError, ValueError, TypeError) as e:
        logging.error(f"Invalid or missing parameter in session_generation config: {e}")
        return None

    # Step 1: Load and Standardize Data
    all_games_data = _load_and_standardize_game_data(data_dir)
    if not all_games_data:
        logging.error("Failed to load or standardize any game data. Aborting session generation.")
        return None

    # Step 2: Calculate Weights
    game_weights = _calculate_game_weights(all_games_data)
    if not game_weights:
         logging.error("Failed to calculate game weights. Aborting session generation.")
         return None

    # Step 3: Simulate Sessions
    sessions_df = _simulate_user_sessions(game_weights, num_users, simulation_days, start_date, target_session_count)
    
    return sessions_df


# --- Data Cleaning Function ---
def clean_sessions(df):
    """Cleans the generated session DataFrame.

    Converts time columns to datetime objects, recalculates duration (now 'session_duration'),
    removes invalid sessions (zero/negative duration), and drops missing values.

    Args:
        df (pd.DataFrame): The raw session DataFrame generated by 
                           _simulate_user_sessions.

    Returns:
        pd.DataFrame: The cleaned session DataFrame.
    """
    if df is None or df.empty:
        logging.warning("Received empty DataFrame for cleaning.")
        return pd.DataFrame()
        
    logging.info("Cleaning generated session data...")
    # Convert to datetime
    df['session_start'] = pd.to_datetime(df['session_start'])
    df['session_end'] = pd.to_datetime(df['session_end'])
    
    # Fix any duration miscalculations - using renamed column
    df['session_duration'] = (
        (df['session_end'] - df['session_start']).dt.total_seconds() / 60
    ).round().astype(int)
    
    # Remove invalid sessions - using renamed column
    initial_rows = len(df)
    df = df[df['session_duration'] > 0]
    df = df.dropna()
    final_rows = len(df)
    logging.info(f"Cleaning complete. Removed {initial_rows - final_rows} invalid/incomplete sessions.")
    
    return df

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("Starting ETL script: generate_sessions.py")
    
    # Load configuration
    config = load_config()
    if config is None:
        logging.error("Exiting due to configuration loading error.")
        exit(1) 

    # Generate and process data
    logging.info("Starting session generation process...")
    sessions_df = generate_sessions(config) 

    if sessions_df is not None and not sessions_df.empty:
        cleaned_df = clean_sessions(sessions_df)
        
        # Save cleaned dataset using path from config
        output_file = config.get('cleaned_sessions_file', 'output/cleaned_sessions_default.csv') 
        output_path = Path(output_file)
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True) 
        
        logging.info(f"Saving cleaned sessions to {output_path}...")
        try:
            cleaned_df.to_csv(output_path, index=False)
            logging.info("Script finished successfully.")
        except Exception as e:
             logging.error(f"Failed to save output file {output_path}: {e}")
             exit(1)
    else:
        logging.error("Session generation failed or produced no data. No output file created.")
        exit(1)
