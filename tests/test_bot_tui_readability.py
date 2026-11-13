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


@pytest.mark.asyncio
async def test_diagnostics_split_resize_expand_and_restore_keep_navigation(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    app=app_type()(service.socket_path)
    try:
        async with app.run_test(size=(140,50)) as pilot:
            await eventually(lambda:app.state.page is not None)
            panel=app.query_one('#diagnostics')
            assert panel.size.height>=10
            original=panel.size.height
            await pilot.press(']')
            assert panel.size.height>original
            await pilot.press('[')
            assert abs(panel.size.height-original)<=1
            divider=app.query_one('#diagnostic_divider')
            y=divider.region.y
            await pilot.mouse_down('#diagnostic_divider')
            await pilot.hover(offset=(divider.region.x+2,y-3))
            await pilot.mouse_up(offset=(divider.region.x+2,y-3))
            assert panel.size.height>original
            app.query_one('#records').focus()
            await pilot.press('enter')
            await eventually(lambda:app.state.strategy=='one' and app.state.page is not None)
            target=app.state.target
            before=panel.size.height
            await pilot.press('f2')
            assert not app.query_one('#records').display and panel.size.height>before
            assert 'restore' in str(divider.content).lower()
            app.render_records()
            assert not app.query_one('#view_tabs').display
            await pilot.press('escape')
            assert app.query_one('#records').display and app.state.strategy=='one'
            assert app.state.target==target and service.session_id is None
            await pilot.resize_terminal(60,15)
            assert not app.query_one('#body').display
            await pilot.resize_terminal(100,28)
            assert app.query_one('#body').display and panel.region.bottom<=app.screen.size.height-1
            await pilot.press('q')
    finally:await service.close()
