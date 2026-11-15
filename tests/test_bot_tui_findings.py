"""Bounded retained finding traversal, without changing provider pagination."""
import pytest
from test_bot_service import service_type
from test_bot_tui import app_type
from bot_v1_support import eventually,valid_config


@pytest.mark.asyncio
async def test_retained_findings_are_newest_first_across_all_provider_pages(tmp_path):
    from src.operator.tui_findings import read_findings
    from src.operator.client import RuntimeClient
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    service.session_id='fixture'
    for index in range(150):
        row=service.observations.finding(dict(author='agent',severity=('info','warning','error')[index%3],
            name='one',message=f'Finding {index}',evidence='inert/path.py:4'),'fixture')
        row['session_id']=service.session_id;service.observations.accept_finding(row)
    await service.start(tmp_path/'private/control.sock');client=RuntimeClient(service.socket_path)
    try:
        result=await read_findings(client,(service.service_id,service.session_id))
        assert len(result['items'])==128 and result['evicted']
        assert [row['sequence'] for row in result['items']]==list(range(150,22,-1))
        assert client.connection_count==0
    finally:await client.close();await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault',['moving_window','repeated_cursor','too_many'])
async def test_incomplete_finding_traversal_never_becomes_an_empty_success(fault):
    from src.operator.tui_findings import read_findings
    from src.operator.client import ClientError
    class Replies:
        calls=0
        async def request(self,*args,**kwargs):
            self.calls+=1
            return dict(revision=1,data=dict(items=[dict(id=f'finding-{self.calls}',sequence=self.calls)],
                has_more=True,next_cursor='same' if fault=='repeated_cursor' else str(self.calls),
                retained_start=self.calls if fault=='moving_window' else 1,retained_end=128,evicted=False))
    client=Replies()
    with pytest.raises(ClientError):await read_findings(client,('service','session'))
    assert client.calls<=3


@pytest.mark.asyncio
async def test_structured_diagnostics_separate_findings_events_and_full_record_details(tmp_path):
    from textual.widgets import DataTable
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    service.session_id='fixture'
    for index,severity in enumerate(('info','warning','error')):
        row=service.observations.finding(dict(author='agent',severity=severity,name='one',
            message='[bold]literal[/bold] '+severity,evidence='inert/path.py:4'),'fixture')
        row['session_id']=service.session_id;service.observations.accept_finding(row)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(140,50)) as pilot:
            table=app.query_one('#diagnostic_records',DataTable)
            await eventually(lambda:table.row_count==3)
            assert [str(table.get_row_at(i)[0]) for i in range(3)]==['ERROR','WARNING','INFO']
            assert str(table.get_row_at(0)[0].style)=='#ef9696'
            assert '3' in str(app.query_one('#diagnostic_status').content)
            await pilot.click('#diag-events')
            assert app.query_one('#diagnostic_tabs').active=='diag-events'
            await pilot.click('#diag-details')
            assert app.query_one('#detail_scroll').display and not table.display
            await pilot.click('#diag-findings')
            assert table.display and not app.query_one('#detail_scroll').display
            table.focus();await pilot.press('enter')
            assert app.diagnostics_expanded and app.state.strategy is None
            assert '[bold]literal[/bold] error' in str(app.query_one('#diagnostic_details').content)
            assert 'inert/path.py:4' in str(app.query_one('#diagnostic_details').content)
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_severity_filters_cover_the_entire_retained_window_and_main_finding_pages(tmp_path):
    from textual.widgets import DataTable
    from src.operator.tui_diagnostics import Diagnostics
    service=service_type()(valid_config(),output_root=tmp_path/'runs');service.session_id='fixture'
    for index in range(150):
        severity='info' if index<100 else 'warning' if index<125 else 'error'
        row=service.observations.finding(dict(author='agent',severity=severity,name='one',
            message=f'{severity} {index}'),'fixture')
        row['session_id']=service.session_id;service.observations.accept_finding(row)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(140,50)) as pilot:
            diagnostic=app.query_one(Diagnostics);table=app.query_one('#diagnostic_records',DataTable)
            await eventually(lambda:table.row_count==128)
            await pilot.click('#filter-alerts')
            assert table.row_count==50
            assert '50 of 128' in str(app.query_one('#diagnostic_status').content)
            await pilot.click('#filter-warning')
            assert table.row_count==25
            await pilot.click('#filter-info')
            assert table.row_count==103
            await pilot.click('#filter-all')
            assert table.row_count==128
            app.query_one('#records').focus();await pilot.press('enter','shift+tab')
            await eventually(lambda:app.state.collection=='findings' and app.state.page is not None)
            assert app.state.page['items'][0]['sequence']==150
            await pilot.click('#filter-alerts')
            assert len(app.state.page['items'])==50 and not app.state.page['has_more']
            await pilot.click('#filter-info');await pilot.click('#filter-warning');await pilot.click('#filter-error')
            assert table.row_count==78 and len(app.state.page['items'])==50 and app.state.page['has_more']
            assert app.state.page['items'][0]['sequence']==100
            app.query_one('#records').focus();await pilot.press('n')
            await eventually(lambda:app.state.page is not None and len(app.state.page['items'])==28)
            await pilot.click('#filter-alerts')
            assert app.state.cursor is None and len(app.state.page['items'])==50
            assert diagnostic.severities=={'warning','error'}
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_filter_empty_states_and_keyboard_presets_remain_distinct(tmp_path):
    from src.operator.tui_diagnostics import Diagnostics
    service=service_type()(valid_config(),output_root=tmp_path/'runs');service.session_id='fixture'
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            diagnostic=app.query_one(Diagnostics)
            await eventually(lambda:diagnostic.history is not None)
            assert 'No retained findings' in str(app.query_one('#diagnostic_status').content)
            app.action_expand_diagnostics();await pilot.pause()
            await pilot.press('shift+tab','enter')
            assert diagnostic.severities=={'warning','error'}
            row=service.observations.finding(dict(author='agent',severity='info',name='one',message='Only info'),'fixture')
            row['session_id']=service.session_id;service.observations.accept_finding(row)
            await eventually(lambda:diagnostic.history is not None and len(diagnostic.history['items'])==1)
            assert 'No matching findings' in str(app.query_one('#diagnostic_status').content)
            await pilot.click('#filter-all')
            assert len(diagnostic.rows)==1
            await pilot.click('#filter-info');await pilot.click('#filter-warning');await pilot.click('#filter-error')
            assert not diagnostic.rows and 'No severities selected' in str(app.query_one('#diagnostic_status').content)
            await pilot.press('q')
    finally:await service.close()


def test_absent_diagnostic_time_displays_unknown():
    from src.operator.tui_findings import finding_cell
    from src.operator.tui_state import label
    assert str(finding_cell(label,'timestamp',None))=='unknown'


@pytest.mark.asyncio
async def test_long_strategy_name_cannot_hide_the_active_view(tmp_path):
    from textual.geometry import Region
    from bot_v1_support import strategy_config
    name='α'+'long'*30
    service=service_type()(valid_config(strategies=[strategy_config(name)]),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path,ascii_mode=True)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy==name and app.state.page is not None)
            heading=app.query_one('#view_title')
            visible=heading.render_lines(Region(0,0,heading.content_size.width,1))[0].text
            assert 'ORDERS' in visible and visible.isascii()
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_late_diagnostic_history_cannot_replace_a_new_strategy_scope(tmp_path,monkeypatch):
    import asyncio
    from src.operator.tui_diagnostics import Diagnostics
    service=service_type()(valid_config(),output_root=tmp_path/'runs');service.session_id='fixture'
    row=service.observations.finding(dict(author='agent',severity='error',message='Global only'),'fixture')
    row['session_id']=service.session_id;service.observations.accept_finding(row)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    original=app.client.request;old_entered=asyncio.Event();old_release=asyncio.Event()
    new_entered=asyncio.Event();new_release=asyncio.Event()
    async def blocked(op,*,target=None,params=None):
        if op=='findings':
            if params.get('name') is None:old_entered.set();await old_release.wait()
            else:new_entered.set();await new_release.wait()
        return await original(op,target=target,params=params)
    monkeypatch.setattr(app.client,'request',blocked)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await asyncio.wait_for(old_entered.wait(),2)
            await pilot.press('enter')
            diagnostic=app.query_one(Diagnostics)
            assert app.state.strategy=='one' and diagnostic.context[1]=='one'
            old_release.set();await asyncio.wait_for(new_entered.wait(),2)
            assert diagnostic.history is None
            new_release.set();await eventually(lambda:diagnostic.history is not None)
            assert diagnostic.history['items']==[]
            await pilot.press('q')
    finally:
        old_release.set();new_release.set();await service.close()


@pytest.mark.asyncio
async def test_expanded_diagnostics_keep_control_notices_and_scope_visible(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy=='one' and app.state.page is not None)
            await pilot.press('f2')
            app.state.unknown='Outcome unknown: pause; no automatic retry.'
            app.render_status()
            assert app.query_one('#notice').display
            assert 'Outcome unknown' in str(app.query_one('#notice').content)
            assert 'Strategy: one' in str(app.query_one('#diagnostic_divider').content)
            await pilot.press('q')
    finally:await service.close()


@pytest.mark.asyncio
async def test_j_and_k_scroll_the_focused_full_finding_message(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs');service.session_id='fixture'
    row=service.observations.finding(dict(author='agent',severity='warning',name='one',
        message='diagnostic detail '*90,evidence='inert/path.py:4'),'fixture')
    row['session_id']=service.session_id;service.observations.accept_finding(row)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            table=app.query_one('#diagnostic_records')
            await eventually(lambda:table.row_count==1)
            table.focus();await pilot.press('enter')
            scroll=app.query_one('#diagnostic_detail_scroll')
            await eventually(lambda:app.focused is scroll and scroll.max_scroll_y>0)
            await pilot.press('j')
            assert scroll.scroll_y>0
            await pilot.press('k')
            assert scroll.scroll_y==0
            await pilot.press('q')
    finally:await service.close()
