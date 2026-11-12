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
