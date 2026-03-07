import urllib.request
import json
import time
import threading
from urllib.error import HTTPError, URLError

class DexChecker:
    def __init__(self, cache_ttl_paid=3600, cache_ttl_boosts=300, rate_limit_delay=0.8):
        self.cache = {}  # {key: (value, timestamp)}
        self.lock = threading.Lock()
        self.cache_ttl_paid = cache_ttl_paid      # 1 hour for paid (almost permanent)
        self.cache_ttl_boosts = cache_ttl_boosts  # 5 min for boosts (dynamic)
        self.rate_limit_delay = rate_limit_delay  # seconds between calls in bulk

    def _get_from_cache(self, key, ttl):
        with self.lock:
            if key in self.cache:
                value, ts = self.cache[key]
                if time.time() - ts < ttl:
                    return value
        return None

    def _set_to_cache(self, key, value):
        with self.lock:
            self.cache[key] = (value, time.time())

    def _make_request(self, url, retries=3):
        for attempt in range(retries):
            try:
                # Use standard urllib with User-Agent header
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status == 200:
                        return json.loads(resp.read().decode('utf-8'))
                    if resp.status == 429:
                        sleep = self.rate_limit_delay * (2 ** attempt)
                        time.sleep(sleep)
                        continue
                    raise ValueError(f"HTTP {resp.status}")
            except (HTTPError, URLError, json.JSONDecodeError, ValueError, Exception) as e:
                if attempt == retries - 1:
                    # Final attempt failed
                    if isinstance(e, HTTPError) and e.code == 404:
                        return {} # Return empty for 404s (e.g. no orders yet)
                    raise RuntimeError(f"Failed to fetch {url}: {e}") from e
                time.sleep(self.rate_limit_delay * (2 ** attempt))
        raise RuntimeError("Max retries exceeded")

    def check_paid(self, chain_id: str, token_address: str) -> bool:
        key = f"paid_{chain_id.lower()}_{token_address.lower()}"
        cached = self._get_from_cache(key, self.cache_ttl_paid)
        if cached is not None:
            return cached

        try:
            orders_data = self._make_request(f"https://api.dexscreener.com/orders/v1/{chain_id}/{token_address}")
            orders = orders_data.get('orders', []) if isinstance(orders_data, dict) else []
            
            is_paid = any(
                o.get('type') in ('tokenProfile', 'communityTakeover', 'tokenAd', 'trendingBarAd')
                and o.get('status') == 'approved'
                for o in orders
            )
            self._set_to_cache(key, is_paid)
            return is_paid
        except Exception:
            return False

    def check_boosts(self, chain_id: str, token_address: str) -> int:
        key = f"boosts_{chain_id.lower()}_{token_address.lower()}"
        cached = self._get_from_cache(key, self.cache_ttl_boosts)
        if cached is not None:
            return cached

        # Try latest first (most efficient)
        try:
            for endpoint in ("https://api.dexscreener.com/token-boosts/latest/v1",
                             "https://api.dexscreener.com/token-boosts/top/v1"):
                data = self._make_request(endpoint)
                if isinstance(data, list):
                    for b in data:
                        if (b.get('chainId') == chain_id and
                            b.get('tokenAddress', '').lower() == token_address.lower()):
                            active = b.get('activeBoostsCount', b.get('totalAmount', 0))
                            self._set_to_cache(key, active)
                            return active
        except Exception:
            pass

        self._set_to_cache(key, 0)
        return 0

    def check_dex_info(self, chain_id: str, token_address: str) -> dict:
        info = {
            'dex_paid': self.check_paid(chain_id, token_address),
            'active_boosts': self.check_boosts(chain_id, token_address),
        }
        return info

    def bulk_check(self, tokens: list) -> dict:
        """
        tokens = [('solana', 'addr1'), ('solana', 'addr2'), ...]
        Returns exactly the payload your broadcastDexInfo expects:
        { 'addr1': {'dex_paid': True, 'active_boosts': 520, 'show_golden': True, 'timestamp': 1741400000000}, ... }
        """
        results = {}
        for i, (chain_id, token_address) in enumerate(tokens):
            if i > 0 and i % 50 == 0:  # safety throttle
                time.sleep(self.rate_limit_delay)

            info = self.check_dex_info(chain_id, token_address)
            addr_lower = token_address.lower()
            results[addr_lower] = {
                'dex_paid': info['dex_paid'],
                'active_boosts': info['active_boosts'],
                'show_golden': info['active_boosts'] >= 500,
                'timestamp': int(time.time() * 1000)
            }
        return results

if __name__ == "__main__":
    checker = DexChecker()
    print("DexChecker Final Production Component Ready.")
