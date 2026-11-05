"""Public paper-bot facade; imports never start transport or private accounts."""
from .paper_trading.runtime import BotRunner
from .paper_trading.runtime_config import load_bot_config


async def replay_bot(capture_directory, output_directory):
    from .paper_trading.replay import replay_runtime
    return await replay_runtime(capture_directory, output_directory)


def run_bot():
    from main import main
    return main()
