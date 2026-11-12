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
@pytest.mark.parametrize('change', ['valid','correlation','version','target','duplicate','nonfinite','deep','shape','oversized'])
async def test_client_rejects_invalid_replies_without_retry(tmp_path,change):
    from src.operator.client import ClientError
    calls=[]
    baseline=service_type()(valid_config(),output_root=tmp_path/'unused')._overview()
    async def reply(reader,writer):
        request=json.loads(await reader.readline());calls.append(request)
        value=dict(version=1,request_id=request['request_id'],ok=True,service_id='service',
            session_id=None,revision=0,observed_at=1,data=baseline)
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
        if change=='valid':
            assert (await client.request('overview',target=('service',None)))['ok']
        else:
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


@pytest.mark.asyncio
async def test_request_aliases_cannot_amplify_allocation_before_validation(tmp_path):
    import tracemalloc
    from src.operator.client import ClientError
    client=client_type()(tmp_path/'none')
    repeated='x'*8192;params={'unexpected':[repeated]*1000}
    tracemalloc.start()
    try:
        with pytest.raises(ClientError) as error:
            await client.request('overview',params=params)
        _,peak=tracemalloc.get_traced_memory()
        assert error.value.code=='invalid_request' and peak<1024*1024
    finally:tracemalloc.stop();await client.close()


@pytest.mark.asyncio
async def test_close_during_connect_cannot_send_a_late_mutation(tmp_path,monkeypatch):
    from src.operator.client import ClientError
    connected=asyncio.Event();release=asyncio.Event();closed=asyncio.Event();frames=[]
    async def receive(reader,writer):
        try:frames.append(await reader.readline())
        finally:writer.close();await writer.wait_closed();closed.set()
    server=await asyncio.start_unix_server(receive,path=str(tmp_path/'test.sock'))
    original=asyncio.open_unix_connection
    async def delayed(*args,**kwargs):
        pair=await original(*args,**kwargs);connected.set();await release.wait();return pair
    monkeypatch.setattr(asyncio,'open_unix_connection',delayed)
    client=client_type()(tmp_path/'test.sock',timeout=.5)
    pending=asyncio.create_task(client.request('stop',target=('service','session')))
    try:
        await asyncio.wait_for(connected.wait(),1)
        await client.close();release.set()
        with pytest.raises(ClientError) as error:await pending
        assert error.value.code=='closed' and not error.value.unknown
        await asyncio.wait_for(closed.wait(),1)
        assert frames==[b''] and client.connection_count==0
    finally:
        release.set();await client.close();await asyncio.gather(pending,return_exceptions=True)
        server.close();await server.wait_closed()


@pytest.mark.parametrize('field,value', [
    ('active_order_count','0'),('position_count',False),('strategy_count',-1),
    ('live_execution_supported',True),('observe_only','yes'),('recent_findings',['not a row']),
    ('recent_events',[{}]),('cleanup_unresolved',None),('source','LIVE'),
])
def test_overview_field_types_cannot_be_mistaken_for_valid_state(tmp_path,field,value):
    from src.operator.client import decode_reply,ClientError
    service=service_type()(valid_config(),output_root=tmp_path/'none')
    request=dict(version=1,request_id='test',op='overview',params={},service_id=None,session_id=None)
    data=service._overview();data[field]=value
    frame=json.dumps(service._envelope(request,data=data)).encode()+b'\n'
    with pytest.raises(ClientError):decode_reply(frame,request)


@pytest.mark.parametrize('field,value', [
    ('execution_mode','LIVE'),('name',{}),('enabled','yes'),
    ('ready',None),('position_count',False),('active_order_count',-1),
])
def test_strategy_row_types_and_execution_mode_are_checked(tmp_path,field,value):
    from src.operator.client import decode_reply,ClientError
    service=service_type()(valid_config(),output_root=tmp_path/'none')
    request=dict(version=1,request_id='test',op='strategies',params={},
        service_id=service.service_id,session_id=None)
    page=service.observations.page(service.observations.strategies.values())
    page['items'][0][field]=value
    frame=json.dumps(service._envelope(request,data=page)).encode()+b'\n'
    with pytest.raises(ClientError):decode_reply(frame,request)


@pytest.mark.asyncio
@pytest.mark.parametrize('op,target,params', [
    ([],None,None),('overview',(),None),('overview',('s',[]),None),
    ('overview',None,[]),('overview',('s',None),{'cursor':[]}),
])
async def test_invalid_local_request_shapes_are_structured_errors(tmp_path,op,target,params):
    from src.operator.client import ClientError
    client=client_type()(tmp_path/'none')
    try:
        with pytest.raises(ClientError) as error:await client.request(op,target=target,params=params)
        assert error.value.code=='invalid_request' and not error.value.unknown
    finally:await client.close()


@pytest.mark.asyncio
async def test_fragmented_large_finding_page_releases_all_payload_reservations(tmp_path):
    from src.operator.client import ClientError
    service=service_type()(valid_config(),output_root=tmp_path/'none')
    rows=[]
    for i in range(50):
        row=service.observations.finding(dict(author='agent',severity='info',message='m'*2048,
            evidence='e'*2048),'test')
        row.update(id=f'f{i:03}',sequence=i+1,session_id='session');rows.append(row)
    page=dict(items=rows,scope='recent',next_cursor=None,has_more=False,
        retained_start=1,retained_end=50,evicted=False)
    async def reply(reader,writer):
        request=json.loads(await reader.readline())
        frame=dict(version=1,request_id=request['request_id'],ok=True,service_id='service',
            session_id='session',revision=1,observed_at=1,data=page)
        raw=json.dumps(frame).encode()+b'\n'
        try:
            for start in range(0,len(raw),4096):
                writer.write(raw[start:start+4096]);await writer.drain();await asyncio.sleep(0)
        finally:writer.close();await writer.wait_closed()
    server=await asyncio.start_unix_server(reply,path=str(tmp_path/'test.sock'))
    client=client_type()(tmp_path/'test.sock')
    try:
        result=await client.request('findings',target=('service','session'))
        assert result['data']['items']==rows
        assert client.payload_highwater>64*1024
        assert client.payload_bytes==client.connection_count==0
    finally:await client.close();server.close();await server.wait_closed()
