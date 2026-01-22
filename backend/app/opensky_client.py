import os
import json
import time 
import requests
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore
import threading
from collections import deque

OPENSKY_API_BASE = "https://opensky-network.org/api"
OPENSKY_TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network/"
    "protocol/openid-connect/token"
)

# Rate limiting constants - CONSERVATIVE TO AVOID 429 ERRORS
# OpenSky: 4,000 credits/day for authenticated users
# Each /flights/all request = 4 credits (global query)
# Max ~1,000 requests/day = ~0.69 requests/minute = ~1 request every 86 seconds (theoretical max)
# Strategy: Conservative to avoid rate limits - better to be slower than hit 429 errors
# When rate limited, we wait 10+ seconds, which is much slower than being conservative upfront
MIN_REQUEST_INTERVAL = 3.0  # 3 seconds base interval (more conservative to avoid 429)
MAX_CONCURRENT_REQUESTS = 4  # Reduced from 8 to 4 to avoid overwhelming API
CREDITS_PER_REQUEST = 4  # Each /flights/all request costs 4 credits
DAILY_CREDIT_LIMIT = 4000  # Daily credit limit

# Global rate limiter (shared across all client instances)
_rate_limiter_lock = Lock()
_last_request_time = 0.0
_request_queue = deque()
_active_requests = 0
_max_active = Semaphore(MAX_CONCURRENT_REQUESTS)
_remaining_credits = None  # Track remaining credits if available
_credits_lock = Lock()
_daily_reset_time = None  # Track when credits reset

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
        self._token_lock = Lock()  # Thread-safe token access
        self._session = requests.Session()  # Connection pooling for better performance
        # Configure session for better reliability
        adapter = requests.adapters.HTTPAdapter(
            max_retries=requests.adapters.Retry(
                total=0,  # We handle retries manually
                backoff_factor=0,
                status_forcelist=[]
            ),
            pool_connections=10,
            pool_maxsize=20
        )
        self._session.mount('http://', adapter)
        self._session.mount('https://', adapter)
        
        self._rate_limited_count = 0  # Track consecutive rate limits
        self._consecutive_errors = 0  # Track consecutive errors


    def _get_token(self) -> Tuple[str, float]:
        """
        POST token request with grant_type=client_credentials.
        We cache token until expiry.
        """
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        } 

        try:
            r = self._session.post(
                OPENSKY_TOKEN_URL,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                data=data,
                timeout=15,  # Token requests are fast, 15s is enough
            )
            r.raise_for_status()
            payload = r.json()

            token = payload["access_token"]
            expires_in = payload.get("expires_in", 1800) # docs say ~30 mins

            refresh_at = time.time() + max(30, expires_in - 60)
            return token, refresh_at
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to get OAuth token: {str(e)}")
    
    def _ensure_token(self) -> str:
        # Thread-safe token refresh
        with self._token_lock:
            if (self._access_token is None) or (time.time() >= self._token_expiry_epoch):
                token, refresh_at = self._get_token()
                self._access_token = token
                self._token_expiry_epoch = refresh_at
            return self._access_token
    
    def _update_credits(self, response):
        """Update remaining credits from response headers if available."""
        global _remaining_credits, _daily_reset_time, _credits_lock
        
        with _credits_lock:
            # Check for rate limit headers
            remaining = response.headers.get("X-Rate-Limit-Remaining")
            reset_time = response.headers.get("X-Rate-Limit-Reset")
            
            if remaining:
                try:
                    _remaining_credits = int(remaining)
                except:
                    pass
            
            if reset_time:
                try:
                    # Reset time might be Unix timestamp or seconds until reset
                    reset_val = float(reset_time)
                    if reset_val > 1000000000:  # Likely Unix timestamp
                        _daily_reset_time = reset_val
                    else:  # Likely seconds until reset
                        _daily_reset_time = time.time() + reset_val
                except:
                    pass
    
    def _get_adaptive_delay(self) -> float:
        """Calculate adaptive delay based on remaining credits and active requests."""
        global _remaining_credits, _daily_reset_time, _credits_lock, _active_requests
        
        base_delay = MIN_REQUEST_INTERVAL
        
        # More conservative: don't reduce delay too much for parallel requests
        # This helps avoid rate limits when multiple workers are active
        # Only reduce delay slightly when fewer workers are active
        active_factor = max(0.7, 1.0 - (_active_requests / MAX_CONCURRENT_REQUESTS) * 0.2)  # Less aggressive reduction
        adjusted_delay = base_delay * active_factor
        
        with _credits_lock:
            if _remaining_credits is not None:
                # If we're running low on credits, increase delay
                if _remaining_credits < 200:  # Less than 5% remaining
                    return adjusted_delay * 3  # Triple the delay
                elif _remaining_credits < 500:  # Less than 12.5% remaining
                    return adjusted_delay * 2  # Double the delay
                elif _remaining_credits < 1000:  # Less than 25% remaining
                    return adjusted_delay * 1.5  # 1.5x the delay
            
            # Check if we're close to daily reset
            if _daily_reset_time and time.time() < _daily_reset_time:
                time_until_reset = _daily_reset_time - time.time()
                if time_until_reset < 3600:  # Less than 1 hour until reset
                    # We can be more aggressive if reset is soon
                    return adjusted_delay * 0.8
        
        return adjusted_delay
    
    def _wait_for_rate_limit(self):
        """Wait to respect rate limits - allows parallel workers to proceed more quickly."""
        global _last_request_time, _active_requests, _rate_limiter_lock, _max_active
        
        # Acquire semaphore to limit concurrent requests
        _max_active.acquire()
        _active_requests += 1
        
        try:
            with _rate_limiter_lock:
                now = time.time()
                time_since_last = now - _last_request_time
                
                # Use adaptive delay based on remaining credits
                required_interval = self._get_adaptive_delay()
                
                # More conservative: maintain minimum delay even with parallel workers
                # This prevents rate limits when multiple workers are active
                # Only reduce delay slightly when fewer workers are active
                parallel_factor = max(0.6, 1.0 - (_active_requests / MAX_CONCURRENT_REQUESTS) * 0.3)  # Less aggressive
                effective_interval = required_interval * parallel_factor
                
                if time_since_last < effective_interval:
                    wait_time = effective_interval - time_since_last
                    # Always wait to respect rate limits (removed 0.1s threshold)
                    if wait_time > 0.05:  # Small threshold to avoid micro-delays
                        time.sleep(wait_time)
                        now = time.time()
                
                _last_request_time = now
        finally:
            pass  # Keep semaphore until request completes
    
    def _release_rate_limit(self):
        """Release the rate limiter semaphore after request completes."""
        global _active_requests, _max_active
        _active_requests -= 1
        _max_active.release()
    
    def _request(self, path: str, params: Dict, max_retries: int = 5) -> List[Dict]:
        """
        Sends Bearer token in Authorization header.
        Comprehensive error handling with retries for transient errors.
        """
        # Wait for rate limit slot before making request
        self._wait_for_rate_limit()
        
        try:
            token = self._ensure_token()
            url = f"{OPENSKY_API_BASE}{path}"
            
            last_exception = None
            for attempt in range(max_retries):
                try:
                    headers = {"Authorization": f"Bearer {token}"}
                    # Optimized timeout - 20 seconds is enough for most requests, faster failure on issues
                    r = self._session.get(url, headers=headers, params=params, timeout=20)
                    
                    # Update credits from response
                    self._update_credits(r)
                    
                    # Handle 404 (no data) - return empty immediately, no retries
                    if r.status_code == 404:
                        self._consecutive_errors = 0
                        return []
                    
                    # Handle token expiration
                    if r.status_code == 401:
                        # Token expired, try to refresh and retry once
                        with self._token_lock:
                            self._access_token = None
                        token = self._ensure_token()
                        headers = {"Authorization": f"Bearer {token}"}
                        # Retry immediately with new token
                        r = self._session.get(url, headers=headers, params=params, timeout=20)
                        self._update_credits(r)
                    
                    # Handle rate limiting (429) with exponential backoff
                    if r.status_code == 429:
                        self._rate_limited_count += 1
                        self._consecutive_errors += 1
                        
                        # Increase global delay if we're getting rate limited frequently
                        # Be more aggressive in backing off to prevent repeated 429 errors
                        if self._rate_limited_count > 2:  # Lower threshold - back off sooner
                            global MIN_REQUEST_INTERVAL
                            MIN_REQUEST_INTERVAL = min(MIN_REQUEST_INTERVAL * 1.5, 180.0)  # Increase by 50% (was 20%)
                            print(f"⚠️  Rate limit detected {self._rate_limited_count} times. Increasing delay to {MIN_REQUEST_INTERVAL:.1f}s")
                        
                        if attempt < max_retries - 1:
                            # Exponential backoff: longer waits for rate limits
                            retry_after = r.headers.get("Retry-After")
                            if retry_after:
                                try:
                                    wait_time = float(retry_after)
                                except:
                                    wait_time = min(2 ** attempt * 10, 300)  # Cap at 5 minutes
                            else:
                                wait_time = min(2 ** attempt * 10, 300)  # 10s, 20s, 40s, 80s, 160s (capped)
                            
                            # Log wait time for debugging
                            if attempt == 0:
                                print(f"⚠️  Rate limited (429). Waiting {wait_time:.1f}s before retry {attempt + 1}/{max_retries}...")
                            
                            time.sleep(wait_time)
                            continue
                        else:
                            # Max retries reached
                            raise Exception(f"Rate limited (429) after {max_retries} attempts. Please wait before trying again.")
                    
                    # Handle 500/502/503 (server errors) - retry with backoff
                    if r.status_code in [500, 502, 503, 504]:
                        if attempt < max_retries - 1:
                            wait_time = min(2 ** attempt * 2, 60)  # 2s, 4s, 8s, 16s, 32s (capped at 60s)
                            time.sleep(wait_time)
                            continue
                        else:
                            r.raise_for_status()
                    
                    # Reset error counters on success
                    if r.status_code < 400:
                        self._consecutive_errors = 0
                        if self._rate_limited_count > 0:
                            self._rate_limited_count = max(0, self._rate_limited_count - 1)
                    
                    # Handle other client errors (400, 403, etc.) - fail fast, no retries
                    if r.status_code >= 400:
                        r.raise_for_status()
                    
                    # Success - parse and return data
                    response_data = r.json()
                    
                    # OpenSky /flights/all returns an object with 'states' array or a list directly
                    # Handle both formats
                    if isinstance(response_data, dict):
                        # If it's a dict, check for 'states' key (state vectors format)
                        if 'states' in response_data:
                            # This is state vectors format - not what we want for /flights/all
                            # /flights/all should return a list of flight objects
                            print(f"⚠️  Warning: Received state vectors format instead of flights. Response keys: {list(response_data.keys())}")
                            return []
                        # If it's a dict with flight data, try to extract flights
                        elif 'flights' in response_data:
                            flights = response_data.get('flights', [])
                            print(f"✓ Fetched {len(flights)} flights from OpenSky API")
                            return flights
                        else:
                            # Unknown dict format - log it
                            print(f"⚠️  Warning: Unexpected response format. Keys: {list(response_data.keys())}")
                            return []
                    elif isinstance(response_data, list):
                        # Direct list of flights - this is what we expect
                        if len(response_data) > 0:
                            print(f"✓ Fetched {len(response_data)} flights from OpenSky API")
                        return response_data
                    else:
                        # Unexpected format
                        print(f"⚠️  Warning: Unexpected response type: {type(response_data)}")
                        return []
                    
                except requests.exceptions.Timeout as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt * 2, 30)  # Exponential backoff for timeouts
                        print(f"⏱️  Request timeout. Retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Request timeout after {max_retries} attempts: {str(e)}")
                
                except requests.exceptions.ConnectionError as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt * 3, 60)  # Longer backoff for connection errors
                        print(f"🔌 Connection error. Retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Connection error after {max_retries} attempts: {str(e)}")
                
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    # For other request exceptions, retry once more
                    if attempt < max_retries - 1:
                        wait_time = min(2 ** attempt * 2, 30)
                        print(f"⚠️  Request error: {str(e)[:100]}. Retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Request failed after {max_retries} attempts: {str(e)}")
            
            # Should not reach here, but handle it
            if last_exception:
                raise last_exception
            raise Exception("Request failed for unknown reason")
            
        finally:
            # Always release the rate limiter
            self._release_rate_limit()
    
    def flights_all(self, begin: int, end: int) -> List[Dict]:
        """
        GET /flights/all?begin=...&end=...
        Note: OpenSky limits this endpoint's time interval (we chunk requests).
        Returns a list of flight dictionaries with fields like:
        - icao24, callsign, firstSeen, lastSeen, estDepartureAirport, estArrivalAirport
        """
        result = self._request("/flights/all", {"begin": begin, "end": end})
        
        # Log warnings only for unexpected cases
        if result is None:
            print(f"⚠️  Warning: flights_all returned None for begin={begin}, end={end}")
        
        return result
    
    def flights_all_chunked(self, range_begin: int, range_end: int, chunk_seconds: int = 2 * 3600, max_workers: int = 3, progress_callback=None):
        """
        Generator over <=2-hour chunks with optional parallel fetching and progress tracking.
        OPTIMIZED for large date ranges (6 months).
        
        Args:
            range_begin: Start timestamp
            range_end: End timestamp
            chunk_seconds: Size of each chunk (max 7200 = 2 hours per OpenSky limit)
            max_workers: Number of parallel workers (default 3, respects rate limits via shared limiter)
            progress_callback: Optional callback function(completed, total, flights_count)
        
        Yields:
            (begin, end, flights) tuples
        """
        # Generate all chunk ranges
        chunks = []
        t = range_begin
        while t < range_end:
            t2 = min(t + chunk_seconds, range_end)
            chunks.append((t, t2))
            t = t2
        
        total_chunks = len(chunks)
        completed_chunks = 0
        
        # Sequential fetching (safer, respects rate limits better)
        if max_workers == 1:
            for idx, (t1, t2) in enumerate(chunks, 1):
                try:
                    flights = self.flights_all(t1, t2)
                    completed_chunks = idx
                    if progress_callback:
                        progress_callback(completed_chunks, total_chunks, len(flights))
                    yield (t1, t2, flights)
                except Exception as e:
                    error_msg = str(e)
                    print(f"❌ Error fetching chunk {idx}/{total_chunks}: {error_msg[:200]}")
                    # Yield empty result on error, but continue
                    if progress_callback:
                        progress_callback(idx, total_chunks, 0)
                    yield (t1, t2, [])
        else:
            # Parallel fetching (respects rate limits via global limiter)
            def fetch_chunk(chunk_tuple):
                t1, t2 = chunk_tuple
                try:
                    flights = self.flights_all(t1, t2)
                    return (t1, t2, flights, True, None)
                except Exception as e:
                    error_msg = str(e)
                    # Check if it's a rate limit error - these should be retried
                    if "429" in error_msg or "Rate limited" in error_msg:
                        # Rate limiter already handles retries, but log it
                        return (t1, t2, [], False, f"Rate limited: {error_msg[:100]}")
                    else:
                        # Other errors - return empty result
                        return (t1, t2, [], False, f"Error: {error_msg[:100]}")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all chunks
                future_to_chunk = {executor.submit(fetch_chunk, chunk): chunk for chunk in chunks}
                
                # Collect results as they complete and update progress
                results = {}
                errors = []
                for future in as_completed(future_to_chunk):
                    chunk = future_to_chunk[future]
                    try:
                        t1, t2, flights, success, error = future.result()
                        results[chunk] = (t1, t2, flights)
                        
                        if not success and error:
                            errors.append((chunk, error))
                        
                        # Update progress in real-time
                        completed_chunks = len(results)
                        if progress_callback:
                            progress_callback(completed_chunks, total_chunks, len(flights))
                    except Exception as e:
                        print(f"❌ Error processing chunk {chunk}: {str(e)[:200]}")
                        results[chunk] = (chunk[0], chunk[1], [])  # Empty result on error
                        errors.append((chunk, str(e)))
                        completed_chunks = len(results)
                        if progress_callback:
                            progress_callback(completed_chunks, total_chunks, 0)
                
                # Log summary of errors
                if errors:
                    print(f"⚠️  Completed with {len(errors)} errors out of {total_chunks} chunks")
                
                # Yield in chronological order
                for chunk in chunks:
                    if chunk in results:
                        yield results[chunk]
                    else:
                        yield (chunk[0], chunk[1], [])  # Empty result if failed