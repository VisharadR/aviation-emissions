import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def save_parquet(df: pd.DataFrame, name: str, overwrite: bool = True) -> str:
    """Legacy function - now saves as CSV instead of parquet."""
    return save_csv(df, name, overwrite=overwrite)

def load_parquet(name: str) -> pd.DataFrame:
    """
    Legacy function - now loads from CSV first, falls back to parquet for backward compatibility.
    New files will be saved as CSV.
    """
    csv_path = os.path.join(DATA_DIR, f"{name}.csv")
    parquet_path = os.path.join(DATA_DIR, f"{name}.parquet")
    
    # Try CSV first (new format) - use optimized reading
    if os.path.exists(csv_path):
        # Use optimized CSV reading with chunking for large files
        # pandas.read_csv is already optimized, but we can use dtype optimization
        return pd.read_csv(csv_path, low_memory=False)
    # Fall back to parquet (old format) for backward compatibility
    elif os.path.exists(parquet_path):
        return pd.read_parquet(parquet_path)
    else:
        raise FileNotFoundError(f"Neither {csv_path} nor {parquet_path} found")

def save_csv(df: pd.DataFrame, name: str, overwrite: bool = True) -> str:
    """
    Save DataFrame as CSV file.
    
    Args:
        df: DataFrame to save
        name: File name (without extension)
        overwrite: If True, overwrite existing file. If False, skip if exists.
    
    Returns:
        Path to saved file, or None if skipped
    """
    ensure_data_dir()
    path = os.path.join(DATA_DIR, f"{name}.csv")
    
    # Check if file already exists
    if os.path.exists(path) and not overwrite:
        print(f"File {name}.csv already exists. Skipping save (overwrite=False).")
        return path
    
    df.to_csv(path, index=False)
    
    # Clean up old parquet file if it exists (since we're using CSV now)
    parquet_path = os.path.join(DATA_DIR, f"{name}.parquet")
    if os.path.exists(parquet_path):
        try:
            os.remove(parquet_path)
            print(f"Removed old parquet file: {name}.parquet")
        except Exception as e:
            print(f"Warning: Could not remove {name}.parquet: {e}")
    
    return path

# Note: load_csv is not used - we use load_parquet which handles both CSV and parquet