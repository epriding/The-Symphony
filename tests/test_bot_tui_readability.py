"""Readability acceptance using real local providers and Textual input."""
import pytest

from bot_v1_support import eventually,valid_config
from test_bot_service import service_type
from test_bot_tui import app_type


@pytest.mark.asyncio
async def test_strategy_heading_and_visible_tabs_follow_keyboard_and_mouse(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(100,28)) as pilot:
            await eventually(lambda:app.state.page is not None)
            assert 'ALL STRATEGIES' in str(app.query_one('#view_title').content)
            assert not app.query_one('#view_tabs').display
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy=='one' and app.state.page is not None)
            assert 'Strategy: one' in str(app.query_one('#view_title').content)
            assert 'ORDERS' in str(app.query_one('#view_title').content)
            assert app.query_one('#view_tabs').display
            await pilot.press('tab')
            await eventually(lambda:app.state.collection=='positions' and app.state.page is not None)
            assert 'POSITIONS' in str(app.query_one('#view_title').content)
            assert app.query_one('#view_tabs').active=='view-positions'
            await pilot.click('#view-fills')
            await eventually(lambda:app.state.collection=='fills' and app.state.page is not None)
            assert 'FILLS' in str(app.query_one('#view_title').content)
            assert 'recent' in str(app.query_one('#collection').content)
            await pilot.press('escape')
            await eventually(lambda:app.state.strategy is None)
            assert 'ALL STRATEGIES' in str(app.query_one('#view_title').content)
            assert not app.query_one('#view_tabs').display
            await pilot.press('q')
    finally:await service.close()
