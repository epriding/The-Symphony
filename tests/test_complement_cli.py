import json
from pathlib import Path
import pytest
from src.bot import BotRunner,load_bot_config
from bot_v1_support import ControlledFeed,VENUES

CONFIG=Path(__file__).resolve().parents[1]/'examples/complement_config.example.json'


def test_example_registers_a_rolling_paper_strategy_without_manual_market_ids(tmp_path):
    config=load_bot_config(CONFIG)
    feeds=[ControlledFeed(v,markets=[]) for v in VENUES]
    runner=BotRunner(config,output_directory=tmp_path/'configured',feeds=feeds)
    assert runner.slots[0].entry['factory']=='immediate-complement-v1'
    assert callable(runner.factories['immediate-complement-v1'])
    assert config['starting_balances']['PAPER']=={v:25 for v in VENUES}
    assert config['risk']['PAPER']['daily_loss_limit_pct']==10
    assert all(not opts['markets'] for opts in config['feeds'].values())
    assert all(v['outbound']['seconds']>0 for v in config['execution']['venues'].values())


@pytest.mark.parametrize('key,value', [('min_net_profit_per_pair',1),('max_pair_spend','nan'),
    ('entry_timeout',True),('recovery_attempts',0),('max_quote_skew',-1),('max_candidates',10001),('typo_setting',1)])
def test_invalid_strategy_settings_fail_before_transport_activation(tmp_path,key,value):
    config=json.loads(CONFIG.read_text())
    config['strategies'][0]['settings'][key]=value
    feeds=[ControlledFeed(v,markets=[]) for v in VENUES]
    with pytest.raises(ValueError):BotRunner(config,output_directory=tmp_path/'invalid',feeds=feeds)
    assert not any(f.metadata_reads or f.started.is_set() for f in feeds)
