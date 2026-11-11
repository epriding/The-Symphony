"""Real private Unix-socket acceptance for the runtime-access provider."""
import asyncio
import json
import os
from pathlib import Path
import socket

import pytest

from bot_v1_support import valid_config


async def connect(service):
    return await asyncio.open_unix_connection(str(service.socket_path))


async def rpc(reader, writer, op='overview', *, service_id=None, session_id=None,
              params=None, request_id='test'):
    writer.write((json.dumps(dict(version=1, request_id=request_id, op=op,
        service_id=service_id, session_id=session_id, params=params or {}))+'\n').encode())
    await writer.drain()
    return json.loads(await asyncio.wait_for(reader.readline(), 2))


def service_type():
    from src.operator.service import ControlService
    return ControlService


@pytest.mark.asyncio
async def test_idle_attach_is_inert_and_private(tmp_path, monkeypatch):
    import dotenv
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda **kwargs: pytest.fail('idle credential read'))
    service = service_type()(valid_config(strategies=[], source='public', observe_only=True),
        output_root=tmp_path / 'runs')
    await service.start(tmp_path / 'private/control.sock')
    try:
        reader, writer = await connect(service)
        response = await rpc(reader, writer)
        assert response['ok'] and response['data']['lifecycle'] == 'idle'
        assert response['session_id'] is None
        assert response['data']['live_execution_supported'] is False
        assert not (tmp_path / 'runs').exists()
        assert service.socket_path.stat().st_mode & 0o777 == 0o600
        assert service.socket_path.parent.stat().st_mode & 0o777 == 0o700
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()
    assert not service.socket_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('payload', [
    b'{"version":1,"version":1}\n',
    b'{"version":NaN}\n',
    b'not-json\n',
    b'\xff\n',
    json.dumps(dict(version=True, request_id='x', op='overview', params={},
        service_id=None, session_id=None)).encode()+b'\n',
    json.dumps(dict(version=1, request_id='x', op='overview', params={'shell':'bad'},
        service_id=None, session_id=None)).encode()+b'\n',
])
async def test_malformed_wire_request_is_rejected_without_starting_a_run(tmp_path, payload):
    service = service_type()(valid_config(), output_root=tmp_path / 'runs')
    await service.start(tmp_path / 'private/control.sock')
    try:
        reader, writer = await connect(service)
        writer.write(payload)
        await writer.drain()
        response = json.loads(await asyncio.wait_for(reader.readline(), 2))
        assert response['ok'] is False
        assert response['error']['code'] == 'invalid_request'
        assert service.session_id is None
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_existing_socket_symlink_or_insecure_directory_is_never_taken_over(tmp_path):
    cls = service_type()
    private = tmp_path / 'private'
    private.mkdir(mode=0o700)
    path = private / 'control.sock'
    original = socket.socket(socket.AF_UNIX)
    original.bind(str(path))
    try:
        with pytest.raises(ValueError):
            await cls(valid_config(), output_root=tmp_path/'runs').start(path)
        assert path.exists()
    finally:
        original.close()
        path.unlink()
    path.symlink_to(tmp_path/'elsewhere')
    with pytest.raises(ValueError):
        await cls(valid_config(), output_root=tmp_path/'runs').start(path)
    assert path.is_symlink()
    path.unlink()
    private.chmod(0o755)
    with pytest.raises(ValueError):
        await cls(valid_config(), output_root=tmp_path/'runs').start(path)


@pytest.mark.asyncio
async def test_close_does_not_unlink_replacement_entry(tmp_path):
    service = service_type()(valid_config(), output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    service.socket_path.unlink()
    service.socket_path.write_text('foreign replacement')
    await service.close()
    assert service.socket_path.read_text() == 'foreign replacement'


@pytest.mark.asyncio
async def test_peer_uid_policy_rejects_foreign_peer(tmp_path, monkeypatch):
    cls = service_type()
    service = cls(valid_config(), output_root=tmp_path/'runs')
    monkeypatch.setattr(service, '_peer_uid', lambda writer: os.getuid()+1)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader, writer = await connect(service)
        assert await asyncio.wait_for(reader.read(), 2) == b''
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()
