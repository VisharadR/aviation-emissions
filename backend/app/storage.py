import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)

def save_parquet(df: pd.DataFrame, name: str) -> str:
    ensure_data_dir()
    path = os.path.join(DATA_DIR, f"{name}.parquet")
    df.to_parquet(path, index=False)
    return path

def load_parquet(name: str) -> pd.DataFrame:
    path = os.path.join(DATA_DIR, f"{name}.parquet")
    return pd.read_parquet(path)