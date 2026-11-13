"""Textual no-selection events must never index an observation list."""
import pytest
from textual.widgets import DataTable

from bot_v1_support import eventually,valid_config
from test_bot_service import service_type
from test_bot_tui import app_type
from src.operator.tui_diagnostics import Diagnostics


@pytest.mark.asyncio
@pytest.mark.parametrize('pane',['main','diagnostics'])
@pytest.mark.parametrize('populated',[False,True])
async def test_no_selection_highlight_and_refresh_are_safe(tmp_path,monkeypatch,pane,populated):
    from textual.coordinate import Coordinate
    service=service_type()(valid_config(),output_root=tmp_path/'runs');service.session_id='fixture'
    if populated:
        row=service.observations.finding(dict(author='fixture',severity='info',message='One finding'),'fixture')
        row['session_id']=service.session_id;service.observations.accept_finding(row)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            if pane=='main' and not populated:
                await pilot.press('enter')
                await eventually(lambda:app.state.strategy=='one' and app.state.page is not None)
            await eventually(lambda:app._read_worker.is_finished)
            monkeypatch.setattr(app,'poll',lambda:None)
            table=app.query_one('#records' if pane=='main' else '#diagnostic_records',DataTable)
            assert table.row_count==int(populated)
            # Recreate the exact framework state/event in the supplied traceback.
            # Keyboard clamping differs; no claim about the user's preceding keys.
            table.set_reactive(DataTable.cursor_coordinate,Coordinate(-1,0))
            assert table.cursor_row==-1
            table.post_message(DataTable.RowHighlighted(table,-1,None))
            await pilot.pause()
            if pane=='main':
                app.render_details()
                assert 'SELECTED RECORD' not in str(app.query_one('#record_details').content)
                app.render_records()
            else:
                app.query_one(Diagnostics).render_selected()
                assert 'No selected diagnostic record' in str(app.query_one('#diagnostic_details').content)
                app.query_one(Diagnostics).render_diagnostics()
            # Empty views remain harmless under normal keyboard input as well.
            table.focus();await pilot.press('down','end')
            await pilot.press('q')
        assert app.return_value==0 and service._server.is_serving()
    finally:await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('index',[-1,999])
async def test_invalid_selection_events_do_not_open_a_strategy_or_diagnostic(tmp_path,index):
    service=service_type()(valid_config(),output_root=tmp_path/'runs');service.session_id='fixture'
    row=service.observations.finding(dict(author='fixture',severity='info',message='One finding'),'fixture')
    row['session_id']=service.session_id;service.observations.accept_finding(row)
    await service.start(tmp_path/'private/control.sock');app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            diagnostic=app.query_one(Diagnostics)
            await eventually(lambda:app.state.page is not None and len(diagnostic.rows)==1)
            table=app.query_one('#records',DataTable)
            app.on_data_table_row_selected(DataTable.RowSelected(table,index,None))
            assert app.state.strategy is None
            finding_table=app.query_one('#diagnostic_records',DataTable)
            diagnostic.on_data_table_row_selected(DataTable.RowSelected(finding_table,index,None))
            assert not app.diagnostics_expanded
            await pilot.press('q')
    finally:await service.close()
