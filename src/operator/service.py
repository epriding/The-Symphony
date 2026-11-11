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
    CONTROL_OPS, DEADLINE, MAX_REQUEST, QUERY_RESERVE, SERVICE_BUDGET, STATIC_RESERVE,
    ProtocolError, bounded_text, decode_request, encode_response,
)
from .observations import Observations, safe_text, validate_projection
from ..bot import BotRunner
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
    def __init__(self, config=None, *, output_root, smoke=False, runner_factory=None):
        if smoke:
            if config is not None:
                raise ValueError('Choose config or smoke, not both')
            from ..strategies.smoke import smoke_fixture
            config = smoke_fixture()[0]
        self.config = validate_bot_config(config, require_runtime=True)
        validate_projection(self.config)
        self.smoke = smoke
        self._runner_factory = runner_factory or BotRunner
        self.runner = None
        self.session_task = None
        self._session_error = None
        self._cleanup_blocked = False
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
        self._control_queue = asyncio.Queue(maxsize=32)
        self._control_worker = None
        self._queued = 0
        self._query_leases = 0
        self.memory_highwater = STATIC_RESERVE
        self.client_highwater = 0
        self.queue_highwater = 0
        self._closing = False
        self.observations = Observations(self.config)
        self._observation_error = None
        self._publication = None
        self._finding_identity = None
        self._finding_failed = False

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
            self._worker = asyncio.create_task(self._work(self._queue))
            self._control_worker = asyncio.create_task(self._work(self._control_queue))
        except BaseException:
            sock.close()
            self._remove_owned_socket()
            raise

    @staticmethod
    def _peer_uid(writer):
        peer = writer.get_extra_info('socket')
        return struct.unpack('3i', peer.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))[1]

    def _accept(self, reader, writer):
        try:
            permitted=self._peer_uid(writer)==os.getuid()
        except (OSError,AttributeError,struct.error):
            permitted=False
        if self._closing or len(self._clients) >= 8 or not permitted:
            writer.close()
            return
        task = asyncio.create_task(self._connection(reader, writer))
        self._clients[task] = writer
        self.client_highwater=max(self.client_highwater,len(self._clients))
        task.add_done_callback(self._client_done)

    def _client_done(self,task):
        self._clients.pop(task,None)
        if not task.cancelled():
            task.exception()

    def _claim_query(self):
        projected=STATIC_RESERVE+(self._query_leases+1)*QUERY_RESERVE
        if projected>SERVICE_BUDGET:
            raise ProtocolError('busy','Read response budget is occupied; retry observation later')
        self._query_leases+=1
        self.memory_highwater=max(self.memory_highwater,projected)

    async def _connection(self, reader, writer):
        try:
            while not self._closing:
                request = None
                leased=False
                try:
                    try:
                        raw = await asyncio.wait_for(reader.readline(), DEADLINE)
                        if not raw:
                            return
                        request = decode_request(raw)
                        if request['op'] not in CONTROL_OPS:
                            self._claim_query()
                            leased=True
                        if self._queued>=32:
                            raise ProtocolError('busy','Control queue is full')
                        future = asyncio.get_running_loop().create_future()
                        queue=self._control_queue if request['op'] in CONTROL_OPS else self._queue
                        queue.put_nowait((request,future))
                        self._queued+=1
                        self.queue_highwater=max(self.queue_highwater,self._queued)
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
                finally:
                    if leased:
                        self._query_leases-=1
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            try:
                await asyncio.wait_for(writer.wait_closed(), DEADLINE)
            except (OSError,asyncio.TimeoutError):
                writer.transport.abort()

    def _envelope(self, request, *, data=None, error=None):
        response = dict(version=1, request_id=request['request_id'] if request else None,
            ok=error is None, service_id=self.service_id, session_id=self.session_id,
            revision=self.revision, observed_at=time.time(), data=data)
        if error:
            response['error'] = dict(code=error.code, message=str(error))
        elif request and request['op']=='publish_finding' and data:
            response['session_id']=data['session_id']
        return response

    async def _work(self,queue):
        while True:
            request, future = await queue.get()
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
                self._queued-=1
                queue.task_done()

    async def _execute(self, request):
        op = request['op']
        discovery = op == 'overview' and request['service_id'] is None and request['session_id'] is None
        if not discovery and (request['service_id'] != self.service_id or
                request['session_id'] != self.session_id):
            raise ProtocolError('conflict', 'Service or session target changed')
        if op == 'overview':
            return self._overview()
        if op in ('strategies','strategy'):
            if self._observation_error:
                raise ProtocolError('limit_exceeded','Current observation is unavailable')
            if op == 'strategies':
                return self.observations.page(self.observations.strategies.values(),
                    tag=['strategies'],**request['params'])
            name=request['params']['name']
            if name not in self.observations.strategies:
                raise ProtocolError('not_found','Unknown strategy')
            return self.observations.read(name,request['params'].get('collection'),
                **{k:v for k,v in request['params'].items() if k in ('limit','cursor')})
        if op in ('findings','publish_finding'):
            name=request['params'].get('name')
            if name is not None and name not in self.observations.strategies:
                raise ProtocolError('not_found','Unknown strategy')
            if op=='findings':
                rows=[row for row in self.observations.findings if name is None or row['strategy']==name]
                return self.observations.page(rows,scope='recent',tag=['findings',name],
                    window=self.observations.window(self.observations.findings,self.observations.finding_sequence),
                    **{k:v for k,v in request['params'].items() if k in ('limit','cursor')})
            if self.session_id is None:
                raise ProtocolError('conflict','A session is required for diagnostic publication')
            if self._finding_failed:
                raise ProtocolError('internal_error','Finding append is unavailable after an I/O failure')
            if self._publication and not self._publication.done():
                raise ProtocolError('busy','A finding append is still pending; observe before retrying')
            row=self.observations.finding(request['params'],request['request_id'])
            row['session_id']=self.session_id
            self._publication=asyncio.create_task(self._commit_finding(row,self.observations))
            self._publication.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
            try:
                return await asyncio.wait_for(asyncio.shield(self._publication),DEADLINE)
            except asyncio.TimeoutError:
                raise ProtocolError('busy','Finding durability is unknown; observe before retrying') from None
        active = self.session_task is not None and not self.session_task.done()
        if op == 'start':
            if self._publication and not self._publication.done():
                raise ProtocolError('busy','Diagnostic append must finish before session replacement')
            if active or self._cleanup_blocked:
                raise ProtocolError('conflict', 'A session is active or cleanup remains unresolved')
            try:
                private_directory(self.output_root)
                session_id = str(uuid4())
                output = checked_path(self.output_root/session_id)
                options = dict(output_directory=output, operator_controls=True,
                    operator_observer=self._observe)
                config = self.config
                if self.smoke:
                    from ..strategies.smoke import smoke_fixture
                    config, options['feeds'], options['clock'] = smoke_fixture()
                runner = self._runner_factory(config, **options)
            except ValueError:
                raise ProtocolError('invalid_request', 'Cannot start configured session; check configuration and output root') from None
            self.runner, self.session_id = runner, session_id
            self.observations = Observations(config)
            self._observation_error = None
            self._finding_identity = None
            self._finding_failed = False
            self._session_error = None
            self.revision += 1
            self.session_task = asyncio.create_task(self._run_session(runner))
        elif op == 'stop' and self.session_task is not None:
            if active:
                self.runner.request_stop()
        elif not active:
            raise ProtocolError('conflict', 'No active session')
        elif op == 'pause':
            if self.runner._stop.is_set():
                raise ProtocolError('conflict', 'Session is stopping')
            self.runner.request_pause()
        elif op == 'resume':
            if self.runner._stop.is_set() or self.runner.operator_state == 'pausing':
                raise ProtocolError('conflict', 'Session is stopping or pause has not quiesced')
            self.runner.request_resume()
        self.revision += 1
        overview = self._overview()
        return dict(accepted=True, lifecycle=overview['lifecycle'],
                    operator_state=overview['operator_state'])

    def _observe(self, runner, kind, payload):
        if kind=='metadata':
            try:
                bounded_text(payload['exchange'],256,identifier=True)
                bounded_text(payload['market_id'],256,identifier=True)
            except ProtocolError:
                raise ValueError('Resolved metadata identifier exceeds control projection limits') from None
            return
        try:
            if self.observations.observe(runner,kind,payload):
                self.revision += 1
        except (ValueError,KeyError,ArithmeticError):
            # An inspection failure must not become an account mutation or
            # interrupt market processing. Reads report unavailable explicitly.
            self._observation_error = True

    async def _run_session(self, runner):
        try:
            if self.config['runtime']['source'] == 'public':
                from dotenv import load_dotenv
                load_dotenv()
            await runner.run()
        except BaseException as error:
            # The runner owns detailed redacted diagnostics and capture outcome.
            # Keep this task observed without failing the independent service.
            self._session_error = type(error).__name__
        finally:
            self._cleanup_blocked = (
                bool(runner._session and runner._unresolved())
                or any(not task.done() for task in (*runner._tasks, *runner._zombies))
                or bool(runner._store and runner._store._thread.is_alive()))
            self.revision += 1
            self._observe(runner,'final',{})

    def _append_finding(self,row):
        path=checked_path(self.runner.output_directory/'operator_findings.jsonl')
        flags=os.O_WRONLY|os.O_APPEND|os.O_NOFOLLOW
        flags |= os.O_CREAT|os.O_EXCL if self._finding_identity is None else 0
        fd=os.open(path,flags,0o600)
        try:
            info=os.fstat(fd)
            identity=info.st_dev,info.st_ino
            if (not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid()
                    or info.st_mode & 0o777 != 0o600 or info.st_nlink!=1
                    or self._finding_identity not in (None,identity)):
                raise OSError('Diagnostic artifact ownership changed')
            self._finding_identity=identity
            raw=encode_response(row)
            offset=0
            while offset<len(raw):
                written=os.write(fd,raw[offset:])
                if written<=0:
                    raise OSError('Incomplete finding append')
                offset+=written
            os.fsync(fd)
        finally:
            os.close(fd)

    async def _commit_finding(self,row,observations):
        try:
            await asyncio.to_thread(self._append_finding,row)
        except (OSError,ValueError):
            self._finding_failed=True
            raise ProtocolError('internal_error','Finding append failed; artifact durability is unknown') from None
        observations.accept_finding(row)
        self.revision+=1
        return row

    def _overview(self):
        status = self.runner.status if self.runner else {'phase':'idle', 'reason':''}
        lifecycle = 'failed' if self._session_error else status['phase']
        return dict(lifecycle=lifecycle, reason=safe_text(status['reason'] or self._session_error or ''),
            operator_state=self.runner.operator_state if self.runner else 'running',
            source=self.config['runtime']['source'],
            observe_only=self.config['runtime']['observe_only'],
            live_execution_supported=False, cleanup_unresolved=self._cleanup_blocked,
            strategy_count=len(self.observations.strategies),
            strategy_ready_count=sum(s['ready'] for s in self.observations.strategies.values()),
            venue_count=len(self.config['feeds']),
            venue_ready_count=sum(v['ready'] for v in self.observations.venues.values()),
            active_order_count=len(self.observations.orders) if not self._observation_error else None,
            position_count=self.observations.position_count if not self._observation_error else None,
            market_time=self.observations.market_time,snapshot_at=self.observations.snapshot_at,
            observation_available=self._observation_error is None,
            recent_events=list(self.observations.events)[-8:],
            event_window=self.observations.window_info(self.observations.events,self.observations.event_sequence),
            recent_findings=list(self.observations.findings)[-8:],
            finding_window=self.observations.window_info(self.observations.findings,self.observations.finding_sequence),
            finding_publication_available=not self._finding_failed,
            finding_append_pending=bool(self._publication and not self._publication.done()),
            service_limits=dict(memory_budget_bytes=SERVICE_BUDGET,
                reserved_bytes=STATIC_RESERVE+self._query_leases*QUERY_RESERVE,
                memory_highwater_bytes=self.memory_highwater,
                projection_bytes=self.observations.storage_bytes(),
                clients=len(self._clients),client_highwater=self.client_highwater,
                queued=self._queued,queue_highwater=self.queue_highwater),
            capture_id=self.config['execution'].get('session_id'),
            capture_directory=str(self.runner.output_directory) if self.runner else None)

    def _remove_owned_socket(self):
        if self.socket_path is not None and self._socket_identity:
            with suppress(FileNotFoundError):
                info = self.socket_path.lstat()
                if (info.st_dev, info.st_ino) == self._socket_identity and stat.S_ISSOCK(info.st_mode):
                    self.socket_path.unlink()

    async def close(self):
        self._closing = True
        close_failed=False
        if self._server:
            self._server.close()
        clients = list(self._clients)
        for task in clients:
            self._clients[task].transport.abort()
            task.cancel()
        if clients:
            await asyncio.gather(*clients, return_exceptions=True)
        if self._server:
            await asyncio.wait_for(self._server.wait_closed(),DEADLINE)
        workers=[t for t in (self._worker,self._control_worker) if t is not None]
        for worker in workers:
            worker.cancel()
        if workers:
            await asyncio.gather(*workers,return_exceptions=True)
        for queue in (self._queue,self._control_queue):
            while not queue.empty():
                _,future=queue.get_nowait()
                future.cancel()
                self._queued-=1
                queue.task_done()
        if self._publication and not self._publication.done():
            try:
                await asyncio.wait_for(asyncio.shield(self._publication),DEADLINE)
            except ProtocolError:
                pass
            except asyncio.TimeoutError:
                close_failed=True
        if self.session_task and not self.session_task.done():
            self.runner.request_stop()
            runtime = self.config['runtime']
            timeout = (runtime['startup_timeout_seconds'] * len(self.config['feeds'])
                       + 4*runtime['shutdown_grace_seconds'] + 2)
            try:
                await asyncio.wait_for(asyncio.shield(self.session_task), timeout)
            except asyncio.TimeoutError:
                close_failed=True
                self.session_task.cancel()
                done, pending = await asyncio.wait({self.session_task},
                    timeout=runtime['shutdown_grace_seconds']+1)
                if pending:
                    self._cleanup_blocked = True
        self._remove_owned_socket()
        if self._cleanup_blocked or close_failed:
            raise RuntimeError('Service cleanup remains unresolved')
