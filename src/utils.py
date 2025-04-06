import pandas as pd
import re

def clean_numeric_string(value):
    """Removes commas and converts to numeric, handling errors."""
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        cleaned_value = value.replace('"', '').replace(',', '')
        try:
            if '.' in cleaned_value:
                return float(cleaned_value)
            else:
                return int(cleaned_value)
        except ValueError:
            return pd.NA
    return pd.NA

def clean_column_name(name):
    """Cleans column names: removes quotes, trims spaces, replaces spaces with underscores, lowercase."""
    name = name.replace('"', '').strip()
    name = re.sub(r'\s+', '_', name)
    return name.lower()
