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
