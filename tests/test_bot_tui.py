"""Human terminal acceptance through Textual Pilot and actual provider sockets."""
import asyncio
import pytest

from bot_v1_support import eventually,valid_config
from test_bot_service import service_type


def app_type():
    from src.operator.tui import BotApp
    return BotApp


@pytest.mark.asyncio
async def test_idle_tui_is_inert_and_quit_only_detaches(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.overview is not None)
            assert 'idle' in str(app.query_one('#session').content)
            assert 'PAPER' in str(app.query_one('#mode').content)
            assert service.session_id is None and not (tmp_path/'runs').exists()
            await pilot.press('q')
        assert app.return_value==0 and app.client.connection_count==0
        assert service._server.is_serving()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_missing_service_stays_visible_until_operator_quits(tmp_path):
    app=app_type()(tmp_path/'missing.sock')
    async with app.run_test(size=(100,28)) as pilot:
        await eventually(lambda:'Disconnected' in str(app.query_one('#connection').content))
        assert app.overview is None
        await pilot.press('q')
    assert app.return_value==0 and not (tmp_path/'missing.sock').exists()


@pytest.mark.asyncio
async def test_stale_age_changes_while_an_overview_read_is_blocked(tmp_path,monkeypatch):
    from textual.widgets import Static
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    now=[100.0];app=app_type()(service.socket_path,clock=lambda:now[0])
    entered=asyncio.Event();release=asyncio.Event();original=service._execute
    async def block(request):
        if request['op']=='overview':
            entered.set();await release.wait()
        return await original(request)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.overview is not None)
            monkeypatch.setattr(service,'_execute',block)
            now[0]+=1
            await asyncio.wait_for(entered.wait(),2)
            now[0]+=3
            await pilot.pause(.3)
            assert 'STALE' in str(app.query_one('#connection',Static).content)
            assert app.overview['lifecycle']=='idle'
            await pilot.press('q')
        assert app.client.connection_count==0
    finally:
        release.set();await service.close()


@pytest.mark.asyncio
async def test_reconnect_announces_replacement_service_without_starting_it(tmp_path):
    path=tmp_path/'private/control.sock'
    first=service_type()(valid_config(),output_root=tmp_path/'first')
    second=service_type()(valid_config(),output_root=tmp_path/'second')
    await first.start(path);app=app_type()(path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.overview is not None)
            await first.close();await second.start(path)
            await eventually(lambda:app.state.target[0]==second.service_id,timeout=3)
            assert 'Target changed' in str(app.query_one('#notice').content)
            assert second.session_id is None and not (tmp_path/'second').exists()
            await pilot.press('q')
    finally:
        await first.close();await second.close()


@pytest.mark.asyncio
async def test_unavailable_overview_counts_display_unknown_instead_of_zero(tmp_path,monkeypatch):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    original=service._overview
    def unavailable():
        view=original();view.update(active_order_count=None,position_count=None,
            observation_available=False,reason='projection unavailable')
        return view
    monkeypatch.setattr(service,'_overview',unavailable)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.overview is not None)
            content=str(app.query_one('#health').content)
            assert 'unknown' in content and 'projection unavailable' in content
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_poll_cadence_does_not_drift_by_response_latency(tmp_path,monkeypatch):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    now=[100.0];calls=[];original=service._execute
    async def latency(request):
        if request['op']=='overview':
            calls.append(request['op'])
            if len(calls)==1:now[0]+=.25
        return await original(request)
    monkeypatch.setattr(service,'_execute',latency)
    await service.start(tmp_path/'private/control.sock')
    app=app_type()(service.socket_path,clock=lambda:now[0])
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.overview is not None)
            now[0]=101.0
            app.poll()  # The next one-second timer tick, not one second after reply.
            await eventually(lambda:len(calls)>=2)
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_strategy_detail_matches_real_fills_and_renders_findings_literally(tmp_path):
    from src.operator.client import RuntimeClient
    service=service_type()(smoke=True,output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    operator=RuntimeClient(service.socket_path)
    await operator.request('start',target=(service.service_id,None))
    await eventually(lambda:service.session_task.done())
    target=(service.service_id,service.session_id)
    await operator.request('publish_finding',target=target,params={
        'name':'smoke','author':'agent','severity':'warning',
        'message':'[bold]literal diagnostic[/bold]','evidence':'inert/path.py:4'})
    app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            assert app.state.page['items'][0]['execution_mode']=='PAPER'
            app.query_one('#records').focus()
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy=='smoke' and app.state.page is not None)
            await pilot.press('tab','tab')
            await eventually(lambda:app.state.collection=='fills' and app.state.page is not None)
            assert app.state.page['items']==list(service.observations.fills)
            assert 'recent' in str(app.query_one('#collection').content)
            await pilot.press('tab','tab')
            await eventually(lambda:app.state.collection=='findings' and app.state.page is not None)
            assert '[bold]literal diagnostic[/bold]' in str(app.query_one('#record_details').content)
            assert 'inert/path.py:4' in str(app.query_one('#record_details').content)
            await pilot.press('escape')
            await eventually(lambda:app.state.strategy is None)
            await pilot.press('q')
    finally:
        await operator.close();await service.close()


@pytest.mark.asyncio
async def test_strategy_pages_and_disabled_modes_remain_visible(tmp_path):
    from bot_v1_support import strategy_config
    entries=[strategy_config(f's{i:02}',enabled=i!=51) for i in range(52)]
    config=valid_config(strategies=entries)
    config['runtime']['limits'].update(max_strategies=64,max_positions=1,max_active_orders=8)
    config['strategy_portfolios'].pop('s51')
    service=service_type()(config,output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            assert len(app.state.page['items'])==50
            await pilot.press('n')
            await eventually(lambda:app.state.page is not None and len(app.state.page['items'])==2)
            row=app.state.page['items'][-1]
            assert row['name']=='s51' and not row['enabled'] and row['execution_mode'] is None
            content=str(app.query_one('#record_details').content)
            await pilot.press('b')
            await eventually(lambda:app.state.page is not None and len(app.state.page['items'])==50)
            assert service.session_id is None
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_confirmed_start_pause_resume_stop_use_real_session_controls(tmp_path):
    from test_bot_service import controlled_service
    service,feeds,_=controlled_service(tmp_path)
    service.config['runtime'].update(startup_timeout_seconds=2,shutdown_grace_seconds=2)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    async def seed_books():
        await asyncio.gather(*(feed.started.wait() for feed in feeds))
        for feed in feeds:await feed.push_book()
    market_task=asyncio.create_task(seed_books())
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.fresh(app.clock()))
            await pilot.press('s')
            assert 'new' in str(app.screen.query_one('#confirm_warning').content).lower()
            await pilot.press(*'start')
            assert app.screen.query_one('#confirm_action').value=='start'
            await pilot.press('enter')
            await eventually(lambda:all(feed.started.is_set() for feed in feeds))
            await eventually(lambda:app.overview['lifecycle']=='ready',timeout=3)
            await pilot.press('p')
            await eventually(lambda:app.overview['operator_state']=='paused',timeout=3)
            assert 'existing orders' in str(app.query_one('#control_note').content)
            await pilot.press('r')
            await eventually(lambda:app.overview['operator_state']=='running',timeout=3)
            await pilot.press('x',*'stop','enter')
            await eventually(lambda:service.session_task.done())
            await eventually(lambda:app.overview['lifecycle']=='stopped',timeout=3)
            assert len(list((tmp_path/'runs').iterdir()))==1
            await pilot.press('q')
        assert service._server.is_serving()
    finally:
        market_task.cancel()
        await asyncio.gather(market_task,return_exceptions=True)
        await service.close()


@pytest.mark.asyncio
async def test_cancelled_and_replaced_confirmation_never_starts_again(tmp_path):
    from src.operator.client import RuntimeClient
    service=service_type()(smoke=True,output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    operator=RuntimeClient(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.fresh(app.clock()))
            await pilot.press('s',*'bjnk')
            assert app.screen.query_one('#confirm_action').value=='bjnk'
            await pilot.press('escape')
            assert service.session_id is None and not (tmp_path/'runs').exists()
            await pilot.press('s')
            assert app.confirmation is not None
            await operator.request('start',target=(service.service_id,None))
            await eventually(lambda:app.confirmation is None,timeout=3)
            await pilot.press('enter')
            assert len(list((tmp_path/'runs').iterdir()))==1
            await pilot.press('q')
    finally:await operator.close();await service.close()


@pytest.mark.asyncio
async def test_lost_control_reply_stays_unknown_and_is_never_retried(tmp_path,monkeypatch):
    from src.operator.client import RuntimeClient,ClientError
    from test_bot_service import controlled_service
    service,feeds,_=controlled_service(tmp_path)
    service.config['runtime'].update(startup_timeout_seconds=2,shutdown_grace_seconds=2)
    await service.start(tmp_path/'private/control.sock')
    operator=RuntimeClient(service.socket_path)
    await operator.request('start',target=(service.service_id,None))
    await eventually(lambda:all(feed.started.is_set() for feed in feeds))
    for feed in feeds:await feed.push_book()
    app=app_type()(service.socket_path);original=app.client.request;stops=[]
    async def lose(op,**kwargs):
        result=await original(op,**kwargs)
        if op=='stop':
            stops.append(kwargs['target'])
            raise ClientError('connection_lost',unknown=True)
        return result
    monkeypatch.setattr(app.client,'request',lose)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.overview is not None)
            await pilot.press('x',*'stop','enter')
            await eventually(lambda:bool(app.state.unknown))
            await eventually(lambda:app.overview['lifecycle']=='stopped',timeout=3)
            assert len(stops)==1 and 'unknown' in str(app.query_one('#notice').content).lower()
            await pilot.press('escape')
            assert not app.state.unknown
            await pilot.press('q')
    finally:await operator.close();await service.close()


@pytest.mark.asyncio
async def test_blocked_observation_does_not_block_confirmed_stop_or_detach(tmp_path,monkeypatch):
    from textual.events import Key, Paste
    from src.operator.client import RuntimeClient
    from test_bot_service import controlled_service
    service,feeds,_=controlled_service(tmp_path)
    service.config['runtime'].update(startup_timeout_seconds=2,shutdown_grace_seconds=2)
    await service.start(tmp_path/'private/control.sock');operator=RuntimeClient(service.socket_path)
    await operator.request('start',target=(service.service_id,None))
    await eventually(lambda:all(feed.started.is_set() for feed in feeds))
    for feed in feeds:await feed.push_book()
    app=app_type()(service.socket_path)
    original=service._execute;entered=asyncio.Event();release=asyncio.Event()
    async def blocked(request):
        if request['op']=='overview':entered.set();await release.wait()
        return await original(request)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.overview is not None)
            monkeypatch.setattr(service,'_execute',blocked)
            await asyncio.wait_for(entered.wait(),2)
            # Pilot.press waits twice per key for process-wide CPU idleness;
            # the real in-process risk loop prevents that. Send the same public
            # input events and synchronize on visible state instead of idleness.
            app.post_message(Key('x','x'))
            await eventually(lambda:app.confirmation is not None and len(app.screen_stack)>1
                and app.screen.is_mounted)
            field=app.screen.query_one('#confirm_action')
            field.post_message(Paste('stop'))
            await eventually(lambda:field.value=='stop')
            app.post_message(Key('enter','\r'))
            await eventually(lambda:service.session_task.done())
            assert not release.is_set() and app.state.pending is not None
            await pilot.press('q')
        assert app.client.connection_count==0
    finally:
        release.set();await operator.close();await service.close()


@pytest.mark.asyncio
async def test_late_collection_reply_cannot_replace_a_new_tab(tmp_path,monkeypatch):
    from src.operator.client import RuntimeClient
    service=service_type()(smoke=True,output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock');operator=RuntimeClient(service.socket_path)
    await operator.request('start',target=(service.service_id,None))
    await eventually(lambda:service.session_task.done())
    app=app_type()(service.socket_path);original=service._execute
    entered=asyncio.Event();release=asyncio.Event()
    async def delayed(request):
        result=await original(request)
        if request['op']=='strategy' and request['params'].get('collection')=='fills':
            entered.set();await release.wait()
        return result
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy=='smoke' and app.state.page is not None)
            await pilot.press('tab')
            await eventually(lambda:app.state.collection=='positions' and app.state.page is not None)
            monkeypatch.setattr(service,'_execute',delayed)
            await pilot.press('tab');await asyncio.wait_for(entered.wait(),2)
            await pilot.press('tab');release.set()
            await eventually(lambda:app.state.page is not None)
            assert app.state.collection=='events'
            assert app.state.page['items'] and all('action' in row for row in app.state.page['items'])
            await pilot.press('q')
    finally:release.set();await operator.close();await service.close()


@pytest.mark.asyncio
async def test_large_recent_findings_stay_bounded_and_disclose_eviction(tmp_path,request):
    from src.operator.client import RuntimeClient
    from src.paper_trading.runtime_config import retained_bytes
    service=service_type()(smoke=True,output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock');operator=RuntimeClient(service.socket_path)
    await operator.request('start',target=(service.service_id,None))
    await eventually(lambda:service.session_task.done())
    # Populate the copied projection; artifact durability has separate real-I/O tests.
    for _ in range(150):
        row=service.observations.finding(dict(author='agent',severity='warning',name='smoke',
            message='m'*2048,evidence='e'*2048),'fixture')
        row['session_id']=service.session_id;service.observations.accept_finding(row)
    app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy=='smoke' and app.state.page is not None)
            await pilot.press('shift+tab')
            await eventually(lambda:app.state.collection=='findings' and app.state.page is not None)
            assert len(app.state.page['items'])==50 and app.state.page['evicted']
            assert 'evicted True' in str(app.query_one('#collection').content)
            model=retained_bytes([app.state,app._visible_rows])
            reserve=2*(384*1024+512*1024)
            request.node.user_properties.extend([
                ('owned_model_bytes',model),('wire_payload_highwater_bytes',app.client.payload_highwater),
                ('stream_decode_reserve_bytes',reserve),('total_budget_bytes',8*1024*1024),
                ('connection_highwater',app.client.highwater)])
            assert model+app.client.payload_highwater+reserve<8*1024*1024
            assert app.client.highwater<=2
            await pilot.press('q')
        assert app.client.payload_bytes==app.client.connection_count==0
    finally:await operator.close();await service.close()


@pytest.mark.asyncio
async def test_small_terminal_requires_resize_and_restores_a_usable_layout(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(80,20)) as pilot:
            assert app.query_one('#resize').display and not app.query_one('#body').display
            await pilot.resize_terminal(100,28)
            await eventually(lambda:app.state.page is not None)
            assert not app.query_one('#resize').display and app.query_one('#body').display
            table=app.query_one('#records')
            assert table.region.height>=5
            assert sum(column.get_render_width(table) for column in table.columns.values())<=table.content_size.width
            assert app.query_one('#detail_scroll').region.bottom<=27
            target=app.state.target
            await pilot.resize_terminal(60,15)
            await pilot.press('s')
            assert app.confirmation is None and app.state.target==target and service.session_id is None
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_monochrome_ascii_display_keeps_original_strategy_identity(tmp_path,monkeypatch):
    from bot_v1_support import strategy_config
    monkeypatch.setenv('NO_COLOR','1')
    service=service_type()(valid_config(strategies=[strategy_config('α')]),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    app=app_type()(service.socket_path,ascii_mode=True)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            assert app.no_color and app.query_one('#body').styles.border_top[0]=='ascii'
            assert '\\u03b1' in str(app.query_one('#records').get_row_at(0)[0])
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy=='α' and app.state.page is not None)
            assert '\\u03b1' in str(app.query_one('#collection').content)
            await pilot.press('s')
            assert app.screen.query_one('#confirm_action').styles.border_top[0]=='ascii'
            await pilot.press('escape')
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_identity_confirmation_and_control_notices_are_sanitized_without_retargeting(tmp_path,monkeypatch):
    from src.operator.client import ClientError
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    service.service_id='α api_key=SYNTHETIC_DISPLAY_VALUE'
    original=service._execute
    hostile='[bold]literal[/bold] \x1b[2J\x1b]0;title\x07 api_key=SYNTHETIC_RECORD_VALUE'
    async def hostile_reply(request):
        data=await original(request)
        if request['op']=='overview':data['reason']=hostile
        if request['op']=='strategies':data['items'][0]['reason']=hostile
        return data
    monkeypatch.setattr(service,'_execute',hostile_reply)
    await service.start(tmp_path/'private/control.sock')
    app=app_type()(service.socket_path,ascii_mode=True)
    entered=asyncio.Event();release=asyncio.Event();seen=[]
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            identity=str(app.query_one('#identity').content)
            assert 'SYNTHETIC_DISPLAY_VALUE' not in identity and '\\u03b1' in identity
            rendered='\n'.join(str(app.query_one(selector).content) for selector in ('#health','#record_details'))
            rendered+=str(app.query_one('#records').get_row_at(0)[4])
            assert '[bold]literal[/bold]' in rendered
            assert 'SYNTHETIC_RECORD_VALUE' not in rendered and '\x1b' not in rendered and '\x07' not in rendered
            await pilot.press('s')
            dialog='\n'.join(str(widget.content) for widget in app.screen.query('Static'))
            assert 'SYNTHETIC_DISPLAY_VALUE' not in dialog and dialog.isascii()
            assert 'whole session' in dialog
            await pilot.press('escape')
            raw_target=app.state.target
            async def lost_reply(op,*,target=None,params=None):
                seen.append((op,target));entered.set();await release.wait()
                raise ClientError('timeout',unknown=True)
            monkeypatch.setattr(app.client,'request',lost_reply)
            monkeypatch.setattr(app,'poll',lambda:None)
            worker=app.send_control('start',raw_target)
            await asyncio.wait_for(entered.wait(),2)
            assert 'SYNTHETIC_DISPLAY_VALUE' not in str(app.query_one('#notice').content)
            release.set();await worker.wait()
            assert seen==[('start',raw_target)] and raw_target[0]==service.service_id
            assert 'Outcome unknown' in str(app.query_one('#notice').content)
            assert 'SYNTHETIC_DISPLAY_VALUE' not in str(app.query_one('#notice').content)
            app.state.unknown='';app.state.pending=('pause',raw_target)
            app.state.target=('replacement',None)
            app.reconcile_control();app.render_status()
            assert 'SYNTHETIC_DISPLAY_VALUE' not in str(app.query_one('#notice').content)
            await pilot.press('q')
    finally:
        release.set();await service.close()
