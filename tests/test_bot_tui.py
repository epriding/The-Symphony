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
