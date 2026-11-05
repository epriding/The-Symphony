"""Read-only Kalshi HTTP operations and websocket authentication."""
import asyncio
import base64
import time
import math
import os
from pathlib import Path

import aiohttp
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, ed25519, rsa


_MAX_PRIVATE_KEY_BYTES = 64 * 1024
_KEY_INPUT_ERRORS = (OSError, ValueError, TypeError, UnsupportedAlgorithm)


def _decode_private_key(material):
    if material.lstrip().startswith(b'-----BEGIN'):
        material = material.strip().replace(b'\\r\\n', b'\n').replace(b'\\n', b'\n')
        key = serialization.load_pem_private_key(material, password=None)
    else:
        key = serialization.load_der_private_key(material, password=None)
    if not isinstance(key, (rsa.RSAPrivateKey, ed25519.Ed25519PrivateKey)):
        raise ValueError('Unsupported Kalshi authentication key type')
    return key


def _load_private_key(source, max_bytes):
    """Accept bounded in-memory key material or the established key-file input.

    Parsing and filesystem errors never expose credential material or paths in
    the exception, its cause, or its formatted traceback.
    """
    try:
        if isinstance(source, str):
            if len(source) > max_bytes:
                raise ValueError('Private key input exceeds byte capacity')
            try:
                material = source.encode('ascii')
                if material.lstrip().startswith(b'-----BEGIN'):
                    return _decode_private_key(material)
                decoded = base64.b64decode(b''.join(material.split()), validate=True)
                return _decode_private_key(decoded)
            except _KEY_INPUT_ERRORS:
                # Existing callers may supply any valid filesystem name,
                # including a name that happens to be valid base64 text.
                pass
        elif not isinstance(source, os.PathLike):
            raise ValueError('Private key input must be material or a path')
        with Path(source).open('rb') as handle:
            material = handle.read(max_bytes + 1)
        if len(material) > max_bytes:
            raise ValueError('Private key input exceeds byte capacity')
        return _decode_private_key(material)
    except _KEY_INPUT_ERRORS:
        pass
    # Raise outside the handler so a path-bearing OSError is not retained as
    # exception context or rendered by the runtime logger.
    raise ValueError('Kalshi websocket private key is invalid or unavailable')


class KalshiPublicAPI:
    def __init__(self, key_id=None, private_key_path=None, *,
                 base_url="https://external-api.kalshi.com/trade-api/v2", read_interval=1.0,
                 max_event_bytes=None):
        if isinstance(read_interval, bool) or not math.isfinite(read_interval) or read_interval <= 0:
            raise ValueError("Explicit positive HTTP read interval required")
        self.key_id, self.private_key_path = key_id, private_key_path
        self.base_url, self.read_interval = base_url.rstrip("/"), read_interval
        self.lock, self.last_read = asyncio.Lock(), 0.0
        self.max_event_bytes = max_event_bytes
        if max_event_bytes is not None:
            from ..interfaces.bounded_public import positive_limit
            positive_limit(max_event_bytes)

    async def get(self, path, params=None):
        async with self.lock:
            delay = self.read_interval - (time.monotonic() - self.last_read)
            if delay > 0:
                await asyncio.sleep(delay)
            self.last_read = time.monotonic()
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),
                                        auto_decompress=self.max_event_bytes is None) as client:
            async with client.get(self.base_url + path, params=params,
                                  headers={'Accept-Encoding': 'identity'} if self.max_event_bytes else None) as response:
                response.raise_for_status()
                if self.max_event_bytes is not None:
                    from ..interfaces.bounded_public import read_json_limited
                    return await read_json_limited(response, self.max_event_bytes)
                return await response.json()

    def websocket_headers(self):
        if not self.key_id or not self.private_key_path:
            raise ValueError("Kalshi websocket credentials are required")
        key = _load_private_key(self.private_key_path,
            min(self.max_event_bytes or _MAX_PRIVATE_KEY_BYTES, _MAX_PRIVATE_KEY_BYTES))
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
