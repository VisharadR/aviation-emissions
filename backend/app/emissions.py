import math


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0  # Earth radius in kilometers
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmbda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dlmbda/2)**2
    return 2 * R * math.asin(math.sqrt(a))


def co2_from_distance_km(distance_km: float, fuel_kg_per_km: float = 3.0, fixed_fuel_kg: float = 500.0) -> float:
    """
    MVP fuel model (configurable):
      fuel_kg ≈ fixed_fuel_kg + fuel_kg_per_km * distance_km
      CO2_kg = fuel_kg * 3.16  (jet fuel combustion factor) :contentReference[oaicite:6]{index=6}
    NOTE: These defaults are placeholders; we'll calibrate using a published dataset later.
    """
    fuel_kg = fixed_fuel_kg + fuel_kg_per_km * distance_km
    co2_kg = fuel_kg * 3.16
    return co2_kg