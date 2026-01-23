import math
import numpy as np
import pandas as pd

# Try to import GPU support (optional)
try:
    import cupy as cp
    GPU_AVAILABLE = True
except ImportError:
    GPU_AVAILABLE = False
    cp = None


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Calculate distance between two points (single values)."""
    R = 6371.0  # Earth radius in kilometers
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmbda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dlmbda/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def haversine_km_vectorized(lat1, lon1, lat2, lon2, use_gpu=False):
    """
    Vectorized Haversine distance calculation (much faster).
    Can use GPU if cupy is available and use_gpu=True.
    
    Args:
        lat1, lon1: Arrays of departure coordinates
        lat2, lon2: Arrays of arrival coordinates
        use_gpu: Whether to use GPU acceleration (requires cupy)
    
    Returns:
        Array of distances in kilometers
    """
    if use_gpu and GPU_AVAILABLE:
        # GPU-accelerated calculation
        R = cp.float32(6371.0)
        lat1_gpu = cp.radians(cp.asarray(lat1, dtype=cp.float32))
        lon1_gpu = cp.radians(cp.asarray(lon1, dtype=cp.float32))
        lat2_gpu = cp.radians(cp.asarray(lat2, dtype=cp.float32))
        lon2_gpu = cp.radians(cp.asarray(lon2, dtype=cp.float32))
        
        dphi = lat2_gpu - lat1_gpu
        dlmbda = lon2_gpu - lon1_gpu
        
        a = cp.sin(dphi/2)**2 + cp.cos(lat1_gpu) * cp.cos(lat2_gpu) * cp.sin(dlmbda/2)**2
        distances = 2 * R * cp.arcsin(cp.sqrt(a))
        
        return cp.asnumpy(distances)  # Convert back to numpy
    else:
        # CPU-optimized vectorized calculation (using NumPy)
        R = 6371.0
        lat1_rad = np.radians(np.asarray(lat1, dtype=np.float32))
        lon1_rad = np.radians(np.asarray(lon1, dtype=np.float32))
        lat2_rad = np.radians(np.asarray(lat2, dtype=np.float32))
        lon2_rad = np.radians(np.asarray(lon2, dtype=np.float32))
        
        dphi = lat2_rad - lat1_rad
        dlmbda = lon2_rad - lon1_rad
        
        a = np.sin(dphi/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlmbda/2)**2
        distances = 2 * R * np.arcsin(np.sqrt(a))
        
        return distances


def co2_from_distance_km(distance_km, fuel_kg_per_km: float = 3.0, fixed_fuel_kg: float = 500.0):
    """
    Calculate CO2 emissions from distance(s).
    Works with both single values and arrays (vectorized).
    
    Args:
        distance_km: Distance(s) in kilometers (scalar or array)
        fuel_kg_per_km: Fuel consumption per km
        fixed_fuel_kg: Fixed fuel overhead
    
    Returns:
        CO2 in kg (scalar or array)
    """
    if isinstance(distance_km, (pd.Series, np.ndarray)):
        # Vectorized calculation
        fuel_kg = fixed_fuel_kg + fuel_kg_per_km * distance_km
        return fuel_kg * 3.16
    else:
        # Single value
        fuel_kg = fixed_fuel_kg + fuel_kg_per_km * distance_km
        return fuel_kg * 3.16


def compute_emissions_vectorized(df: pd.DataFrame, use_gpu: bool = False) -> pd.DataFrame:
    """
    Optimized vectorized computation of distances and emissions for a DataFrame.
    
    Args:
        df: DataFrame with columns: dep_lat, dep_lon, arr_lat, arr_lon
        use_gpu: Whether to use GPU acceleration (requires cupy)
    
    Returns:
        DataFrame with added columns: distance_km, co2_kg
    """
    # Create mask for valid coordinates
    mask = df["dep_lat"].notna() & df["arr_lat"].notna()
    
    if mask.sum() == 0:
        df["distance_km"] = None
        df["co2_kg"] = None
        return df
    
    # Vectorized distance calculation (much faster than apply)
    distances = haversine_km_vectorized(
        df.loc[mask, "dep_lat"].values,
        df.loc[mask, "dep_lon"].values,
        df.loc[mask, "arr_lat"].values,
        df.loc[mask, "arr_lon"].values,
        use_gpu=use_gpu
    )
    
    # Vectorized CO2 calculation
    co2_values = co2_from_distance_km(distances)
    
    # Assign results back to dataframe
    df.loc[mask, "distance_km"] = distances
    df.loc[mask, "co2_kg"] = co2_values
    
    return df