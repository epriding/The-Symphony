"""Explicit local service; clients never own the bot's process lifetime."""
import asyncio
from contextlib import suppress
import os
from pathlib import Path
import socket
import stat
import struct
import time
from uuid import uuid4

from .protocol import (
    DEADLINE, MAX_REQUEST, ProtocolError, decode_request, encode_response,
)
from ..paper_trading.runtime_config import validate_bot_config


def checked_path(path):
    path = Path(os.path.abspath(path))
    for node in (path, *path.parents):
        if node.is_symlink():
            raise ValueError('Service paths must not contain symlinks')
    return path


def private_directory(path):
    path = checked_path(path)
    missing = []
    parent = path
    while not parent.exists():
        missing.append(parent)
        parent = parent.parent
    for node in reversed(missing):
        node.mkdir(mode=0o700)
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o777 != 0o700:
        raise ValueError('Service directory must be owned by this user with mode 0700')
    return path


class ControlService:
    def __init__(self, config, *, output_root):
        self.config = validate_bot_config(config, require_runtime=True)
        self.output_root = checked_path(output_root)
        self.service_id = str(uuid4())
        self.session_id = None
        self.socket_path = None
        self.revision = 0
        self._socket_identity = None
        self._server = None
        self._clients = {}
        self._queue = asyncio.Queue(maxsize=32)
        self._worker = None
        self._closing = False

    async def start(self, path):
        if self._server is not None or self._closing:
            raise ValueError('Service is already started or closed')
        self.socket_path = checked_path(path)
        private_directory(self.socket_path.parent)
        if self.socket_path.exists() or self.socket_path.is_symlink():
            raise ValueError('Socket path already exists; refusing takeover')
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            # Bind ourselves: asyncio may otherwise unlink an existing socket.
            sock.bind(str(self.socket_path))
            info = self.socket_path.lstat()
            self._socket_identity = info.st_dev, info.st_ino
            self.socket_path.chmod(0o600)
            sock.setblocking(False)
            self._server = await asyncio.start_unix_server(self._accept, sock=sock,
                limit=MAX_REQUEST)
            self._worker = asyncio.create_task(self._work())
        except BaseException:
            sock.close()
            self._remove_owned_socket()
            raise

    @staticmethod
    def _peer_uid(writer):
        peer = writer.get_extra_info('socket')
        return struct.unpack('3i', peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[1]

    def _accept(self, reader, writer):
        if self._closing or len(self._clients) >= 8 or self._peer_uid(writer) != os.getuid():
            writer.close()
            return
        task = asyncio.create_task(self._connection(reader, writer))
        self._clients[task] = writer
        task.add_done_callback(lambda done: self._clients.pop(done, None))

    async def _connection(self, reader, writer):
        try:
            while not self._closing:
                request = None
                try:
                    raw = await asyncio.wait_for(reader.readline(), DEADLINE)
                    if not raw:
                        return
                    request = decode_request(raw)
                    future = asyncio.get_running_loop().create_future()
                    try:
                        self._queue.put_nowait((request, future))
                    except asyncio.QueueFull:
                        raise ProtocolError('busy', 'Control queue is full') from None
                    response = await asyncio.wait_for(future, DEADLINE)
                except ProtocolError as error:
                    response = self._envelope(request, error=error)
                except ValueError:  # StreamReader rejected an oversized line.
                    return
                try:
                    raw_response = await asyncio.wait_for(asyncio.to_thread(encode_response, response), DEADLINE)
                except ProtocolError as error:
                    raw_response = encode_response(self._envelope(request, error=error))
                writer.write(raw_response)
                await asyncio.wait_for(writer.drain(), DEADLINE)
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            with suppress(OSError, asyncio.TimeoutError):
                await asyncio.wait_for(writer.wait_closed(), DEADLINE)

    def _envelope(self, request, *, data=None, error=None):
        response = dict(version=1, request_id=request['request_id'] if request else None,
            ok=error is None, service_id=self.service_id, session_id=self.session_id,
            revision=self.revision, observed_at=time.time(), data=data)
        if error:
            response['error'] = dict(code=error.code, message=str(error))
        return response

    async def _work(self):
        while True:
            request, future = await self._queue.get()
            try:
                if future.cancelled():
                    continue
                try:
                    data = await self._execute(request)
                    response = self._envelope(request, data=data)
                except ProtocolError as error:
                    response = self._envelope(request, error=error)
                except Exception:
                    response = self._envelope(request,
                        error=ProtocolError('internal_error', 'Control operation failed'))
                if not future.done():
                    future.set_result(response)
            finally:
                self._queue.task_done()

    async def _execute(self, request):
        if request['service_id'] not in (None, self.service_id) or request['session_id'] is not None:
            raise ProtocolError('conflict', 'Service or session target changed')
        return dict(lifecycle='idle', reason='', operator_state='running',
            source=self.config['runtime']['source'],
            observe_only=self.config['runtime']['observe_only'],
            live_execution_supported=False)

    def _remove_owned_socket(self):
        if self.socket_path is not None and self._socket_identity:
            with suppress(FileNotFoundError):
                info = self.socket_path.lstat()
                if (info.st_dev, info.st_ino) == self._socket_identity and stat.S_ISSOCK(info.st_mode):
                    self.socket_path.unlink()

    async def close(self):
        self._closing = True
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        clients = list(self._clients)
        for task in clients:
            task.cancel()
        if clients:
            await asyncio.gather(*clients, return_exceptions=True)
        if self._worker:
            self._worker.cancel()
            with suppress(asyncio.CancelledError):
                await self._worker
        self._remove_owned_socket()
