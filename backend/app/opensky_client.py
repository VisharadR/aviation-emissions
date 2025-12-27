import os
import json
import time 
import requests
from typing import List, Dict, Optional, Tuple

OPENSKY_API_BASE = "https://opensky-network.org/api"
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)

# New OAuth of opensky api
class OpenSkyOAuthClient:
    def __init__(self, credentials_path: str = "credentials.json", client_id: Optional[str] = None, client_secret: Optional[str] = None):
        self.client_id = client_id
        self.client_secret = client_secret

        # Else read JSON file
        if (not self.client_id) or (not self.client_secret):
            if os.path.exists(credentials_path):
                with open(credentials_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)

                def pick_creds(obj):
                    if not isinstance(obj, dict):
                        return None, None
                    ci = {str(k).lower(): v for k, v in obj.items()}

                    cid = ci.get("clientid") or ci.get("client_id") or ci.get("client-id")
                    csec = ci.get("clientsecret") or ci.get("client_secret") or ci.get("client-secret")
                    return cid, csec

                # try flat first
                cid, csec = pick_creds(raw)

                # if not found, try common nesting patterns
                if not cid or not csec:
                    for k in ["credentials", "auth", "opensky", "api", "oauth", "client"]:
                        if isinstance(raw, dict) and isinstance(raw.get(k), dict):
                            cid2, csec2 = pick_creds(raw[k])
                            cid = cid or cid2
                            csec = csec or csec2

                self.client_id = self.client_id or cid
                self.client_secret = self.client_secret or csec
        
        self._access_token: Optional[str] = None
        self._token_expiry_epoch: float = 0.0 # when we should refresh


    def _get_token(self) -> Tuple[str, float]:
        """
        POST token request with grant_type=client_credentials. :contentReference[oaicite:5]{index=5}
        We cache token until expiry.
        """
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        } 

        r = requests.post(
            OPENSKY_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=data,
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()

        token = payload["access_token"]
        expires_in = payload.get("expires_in", 1800) # docs say ~30 mins :contentReference[oaicite:6]{index=6}

        refresh_at = time.time() + max(30, expires_in - 60)
        return token, refresh_at
    
    def _ensure_token(self) -> str:
        if (self._access_token is None) or (time.time() >= self._token_expiry_epoch):
            token, refresh_at = self._get_token()
            self._access_token = token
            self._token_expiry_epoch = refresh_at
        return self._access_token
    
    def _request(self, path: str, params:Dict) -> List[Dict]:
        """
        Sends Bearer token in Authorization header. :contentReference[oaicite:7]{index=7}
        If token expired and we get 401, refresh once and retry. :contentReference[oaicite:8]{index=8}
        """
        token = self._ensure_token()
        url = f"{OPENSKY_API_BASE}{path}"

        headers = {"Authorization": f"Bearer {token}"}
        r = requests.get(url, headers=headers, params=params, timeout=60)

        if r.status_code == 401:
            # Token expired, try to refresh and retry once
            self._access_token = None
            token = self._ensure_token()
            headers = {"Authorization": f"Bearer {token}"}
            r = requests.get(url, headers=headers, params=params, timeout=60)

        if r.status_code == 404:
            return []
        
        r.raise_for_status()
        return r.json()
    
    def flights_all(self, begin: int, end: int) -> List[Dict]:
        """
        GET /flights/all?begin=...&end=...
        Note: OpenSky limits this endpoint’s time interval (we chunk requests). :contentReference[oaicite:9]{index=9}
        """
        return self._request("/flights/all", {"begin": begin, "end": end})
    
    def flights_all_chunked(self, range_begin: int, range_end: int, chunk_seconds: int = 2 * 3600):
        # Generator over <=2-hour chunks (Safe default).
        t = range_begin
        while t < range_end:
            t2 = min(t + chunk_seconds, range_end)
            yield (t, t2, self.flights_all(t, t2))
            # Be polite to rate limits ( tune later)
            time.sleep(1.0)
            t = t2




# Older auth method of opensky api
# class OpenSkyClient:
#     def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
#         self.username = username or os.getenv("OPENSKY_USERNAME")
#         self.password = password or os.getenv("OPENSKY_PASSWORD")

#     def flights_all(self, begin: int, end: int) -> List[Dict]:
#         url = f"{BASE_URL}/flights/all"
#         params = {"begin": begin, "end": end}
        
#         auth = (self.username, self.password) if self.username and self.password else None
#         r = requests.get(url, params=params, auth=auth, timeout=60)
#         if r.status_code == 404:
#             return []
#         r.rasie_for_status()
#         return r.json()
    
#     def flights_all_chunked(self, day_begin_utc:int, day_end_utc: int, chunk_seconds: int = 2 * 3600):
#         t = day_begin_utc
#         while t < day_end_utc:
#             t2 = min(t + chunk_seconds, day_end_utc)
#             yield (t, t2, self.flights_all(t, t2))
#             # Be polite to rate limits ( tune later)
#             time.sleep(1.0)
#             t = t2