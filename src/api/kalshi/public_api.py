"""Read-only Kalshi HTTP operations and websocket authentication."""
import asyncio
import base64
import time
from pathlib import Path

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, ed25519


class KalshiPublicAPI:
    def __init__(self, key_id=None, private_key_path=None, *,
                 base_url="https://external-api.kalshi.com/trade-api/v2", read_interval=1.0):
        if read_interval <= 0:
            raise ValueError("Explicit positive HTTP read interval required")
        self.key_id, self.private_key_path = key_id, private_key_path
        self.base_url, self.read_interval = base_url.rstrip("/"), read_interval
        self.lock, self.last_read = asyncio.Lock(), 0.0

    async def get(self, path, params=None):
        async with self.lock:
            delay = self.read_interval - (time.monotonic() - self.last_read)
            if delay > 0:
                await asyncio.sleep(delay)
            self.last_read = time.monotonic()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as client:
            async with client.get(self.base_url + path, params=params) as response:
                response.raise_for_status()
                return await response.json()

    def websocket_headers(self):
        if not self.key_id or not self.private_key_path:
            raise ValueError("Kalshi websocket credentials are required")
        key = serialization.load_pem_private_key(Path(self.private_key_path).read_bytes(), password=None)
        stamp = str(time.time_ns() // 1_000_000)
        message = (stamp + "GET" + "/trade-api/ws/v2").encode()
        if isinstance(key, ed25519.Ed25519PrivateKey):
            signature = key.sign(message)
        else:
            signature = key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                                 salt_length=hashes.SHA256().digest_size), hashes.SHA256())
        return {"KALSHI-ACCESS-KEY": self.key_id,
                "KALSHI-ACCESS-TIMESTAMP": stamp,
                "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode()}
