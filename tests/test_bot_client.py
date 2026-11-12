"""Bounded client behavior against real Unix sockets and the paper provider."""
import asyncio
import json

import pytest

from bot_v1_support import valid_config
from test_bot_service import service_type


def client_type():
    from src.operator.client import RuntimeClient
    return RuntimeClient


@pytest.mark.asyncio
async def test_client_discovers_idle_service_without_creating_a_session(tmp_path, monkeypatch):
    import dotenv
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda: pytest.fail('credential read'))
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    client=client_type()(service.socket_path)
    try:
        view=await client.request('overview')
        assert view['data']['lifecycle']=='idle'
        assert view['session_id'] is None
        assert not (tmp_path/'runs').exists()
        page=await client.request('strategies',target=(view['service_id'],None))
        assert page['data']['items'][0]['name']=='one'
        assert client.connection_count==0
    finally:
        await client.close()
        await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['correlation','version','target','duplicate','nonfinite','deep','shape','oversized'])
async def test_client_rejects_invalid_replies_without_retry(tmp_path,change):
    from src.operator.client import ClientError
    calls=[]
    async def reply(reader,writer):
        request=json.loads(await reader.readline());calls.append(request)
        value=dict(version=1,request_id=request['request_id'],ok=True,service_id='service',
            session_id=None,revision=0,observed_at=1,data=dict(lifecycle='idle'))
        if change=='correlation':value['request_id']='other'
        if change=='version':value['version']=2
        if change=='target':value['service_id']='other'
        if change=='nonfinite':value['observed_at']=float('nan')
        if change=='shape':value['ok']='yes'
        if change=='deep':value['data']=[[[[[[[[[[[[[[[[[[0]]]]]]]]]]]]]]]]]]
        raw=json.dumps(value).encode()+b'\n'
        if change=='duplicate':raw=raw.replace(b'"version": 1',b'"version": 1,"version": 1')
        if change=='oversized':raw=b'x'*(1024*1024+1)+b'\n'
        try:
            writer.write(raw);await writer.drain()
        finally:
            writer.close();await writer.wait_closed()
    server=await asyncio.start_unix_server(reply,path=str(tmp_path/'test.sock'))
    client=client_type()(tmp_path/'test.sock')
    try:
        with pytest.raises(ClientError):
            await client.request('overview',target=('service',None))
        assert len(calls)==1 and client.connection_count==0
    finally:
        await client.close();server.close();await server.wait_closed()


@pytest.mark.asyncio
async def test_control_timeout_is_unknown_once_and_cancellation_closes_reads(tmp_path):
    from src.operator.client import ClientError
    entered=asyncio.Event();release=asyncio.Event();seen=[]
    async def blocked(reader,writer):
        seen.append(json.loads(await reader.readline()))
        entered.set()
        try:
            await release.wait()
        finally:
            writer.close();await writer.wait_closed()
    server=await asyncio.start_unix_server(blocked,path=str(tmp_path/'test.sock'))
    client=client_type()(tmp_path/'test.sock',timeout=.05)
    try:
        with pytest.raises(ClientError) as caught:
            await client.request('stop',target=('service','session'))
        assert caught.value.unknown and len(seen)==1
        entered.clear()
        pending=asyncio.create_task(client.request('overview'))
        await asyncio.wait_for(entered.wait(),1)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):await pending
        assert client.connection_count==0
    finally:
        release.set();await client.close();server.close();await server.wait_closed()
        await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_client_controls_real_service_and_rejects_stale_session(tmp_path):
    from src.operator.client import ClientError
    from bot_v1_support import eventually
    service=service_type()(smoke=True,output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    client=client_type()(service.socket_path)
    try:
        started=await client.request('start',target=(service.service_id,None))
        assert started['data']['accepted'] and started['session_id']
        await eventually(lambda:service.session_task.done())
        with pytest.raises(ClientError) as caught:
            await client.request('stop',target=(service.service_id,'old'))
        assert caught.value.code=='conflict' and not caught.value.unknown
        page=await client.request('strategy',target=(service.service_id,service.session_id),
            params={'name':'smoke','collection':'fills'})
        assert page['data']['items']
    finally:
        await client.close();await service.close()
