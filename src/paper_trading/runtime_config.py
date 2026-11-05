"""Pure validation and conversion for the paper bot configuration."""
from copy import deepcopy
import json
import math
from pathlib import Path
from dataclasses import fields, is_dataclass
from enum import Enum
import sys

from .models import Delay, FeeSchedule, VenueRules, PaperExecutionConfig
from ..utilities.classes import TradingConfig, RiskConfig

TIMINGS = ('startup_timeout_seconds', 'strategy_timeout_seconds',
           'risk_interval_seconds', 'shutdown_grace_seconds')
LIMITS = ('max_strategies', 'max_markets', 'max_active_orders', 'max_positions',
          'max_intents_per_callback', 'max_hot_history_records',
          'max_archive_cache_records', 'max_event_bytes', 'memory_budget_bytes')
COMPATIBILITY_LIMITS = dict(zip(LIMITS, (8, 8, 64, 32, 8, 16, 8, 262144, 67108864)))


def _public_queue_limit(markets):
    # Market-data and trade subscriptions can both reply before stream draining.
    return 2 * len(markets)


def _public_startup_buffer_bytes(config):
    if config['runtime']['source'] != 'public':
        return 0
    queue_slots = sum(_public_queue_limit(options['markets'])
                      for venue, options in config['feeds'].items() if venue == 'POLYMARKET_US')
    # Reserve wire-text storage plus decoded queue/parser working copies.
    return queue_slots * config['runtime']['limits']['max_event_bytes'] * 8


def retained_bytes(value):
    """Measure supported retained containers, including dataclass payloads.

    This admission measure covers runtime-owned state. Arbitrary third-party
    allocations remain outside the cooperative in-process strategy contract.
    """
    seen = set()
    def size(item):
        identity = id(item)
        if identity in seen:
            return 0
        seen.add(identity)
        result = sys.getsizeof(item)
        if isinstance(item, Enum):
            return result
        if isinstance(item, dict):
            return result + sum(size(key) + size(child) for key, child in item.items())
        if isinstance(item, (list, tuple, set, frozenset)) or type(item).__name__ == 'deque':
            return result + sum(size(child) for child in item)
        if is_dataclass(item) and not isinstance(item, type):
            return result + sum(size(getattr(item, field.name)) for field in fields(item))
        return result
    return size(value)


def json_bytes(value):
    def check(item, depth=0):
        if depth > 64:
            raise ValueError('Configuration nesting exceeds 64 levels')
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise ValueError('JSON object keys must be strings')
            for child in item.values():
                check(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                check(child, depth + 1)
        elif item is not None and not isinstance(item, (str, int, float, bool)):
            raise ValueError('Configuration must contain JSON values')
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError('Configuration numbers must be finite')
    check(value)
    try:
        return json.dumps(value, allow_nan=False, separators=(',', ':')).encode()
    except (TypeError, ValueError, RecursionError) as error:
        raise ValueError('Configuration must contain finite JSON values') from error


def positive(value, name, *, integer=False):
    try:
        valid = (not isinstance(value, bool) and isinstance(value, (int, float))
                 and math.isfinite(value) and value > 0
                 and (not integer or isinstance(value, int)))
    except OverflowError:
        valid = False
    if not valid:
        raise ValueError(f'{name} must be a positive finite ' + ('integer' if integer else 'number'))


def trading_config(raw):
    """Convert the existing paper sections without activating any resources."""
    config = TradingConfig()
    try:
        config.strategy_portfolios = deepcopy(raw['strategy_portfolios'])
        config.paper_starting_balances = deepcopy(raw['starting_balances'])
        config.market_overrides = deepcopy(raw.get('market_overrides', {}))
        for name, settings in raw.get('risk', {'PAPER': {}}).items():
            config.risk_configs[name] = RiskConfig(**settings)
            config.risk_configs[name].validate()
        venues = {}
        for exchange, values in raw['execution']['venues'].items():
            settings = deepcopy(values)
            settings['fees'] = FeeSchedule(**settings['fees'])
            settings['market_fee_overrides'] = {
                key: FeeSchedule(**fees) for key, fees in settings.get('market_fee_overrides', {}).items()}
            for delay in ('outbound', 'acknowledgement', 'execution_report', 'cancel'):
                settings[delay] = Delay(**settings[delay])
            venues[exchange] = VenueRules(exchange=exchange, **settings)
        execution = {key: deepcopy(value) for key, value in raw['execution'].items() if key != 'venues'}
        if 'processing_delay' in execution:
            execution['processing_delay'] = Delay(**execution['processing_delay'])
        config.paper_execution = PaperExecutionConfig(venues, **execution)
    except (KeyError, TypeError, ArithmeticError, AttributeError) as error:
        raise ValueError(f'Invalid paper configuration: {error}') from error
    return config


def validate_bot_config(value, *, require_runtime=False):
    if not isinstance(value, dict):
        raise ValueError('Configuration must be an object')
    encoded = json_bytes(value)
    raw = deepcopy(value)
    config = trading_config(raw)
    if 'runtime' not in raw and not require_runtime:
        return raw
    supported = {'strategy_portfolios', 'starting_balances', 'risk', 'execution',
                 'feeds', 'market_overrides', 'strategies', 'runtime'}
    if set(raw) - supported:
        raise ValueError('Unsupported bot configuration keys; execution is paper-only')
    if not isinstance(raw.get('strategy_portfolios'), dict):
        raise ValueError('strategy_portfolios must be a route mapping')
    balances = raw.get('starting_balances')
    if not isinstance(balances, dict):
        raise ValueError('starting_balances must be a paper-portfolio mapping')
    for portfolio, amounts in balances.items():
        if portfolio == 'LIVE' or portfolio not in config.risk_configs or not isinstance(amounts, dict):
            raise ValueError('Starting balances require a configured paper portfolio')
        for venue, amount in amounts.items():
            try:
                valid_amount = (not isinstance(amount, bool) and isinstance(amount, (int, float))
                                and math.isfinite(amount) and amount >= 0)
            except OverflowError:
                valid_amount = False
            if venue not in config.paper_execution.venues or not valid_amount:
                raise ValueError('Starting balances require known venues and finite nonnegative amounts')
    runtime = raw.get('runtime')
    if not isinstance(runtime, dict) or set(runtime) != {'source', 'observe_only', 'limits', *TIMINGS}:
        raise ValueError('runtime must contain the specified source, observation, timing and limits keys')
    if runtime['source'] not in ('public', 'synthetic') or type(runtime['observe_only']) is not bool:
        raise ValueError('Invalid runtime source or observe_only')
    for key in TIMINGS:
        positive(runtime[key], key)
    limits = runtime['limits']
    if not isinstance(limits, dict) or set(limits) != set(LIMITS):
        raise ValueError('runtime.limits must contain all specified limits')
    for key in LIMITS:
        positive(limits[key], key, integer=True)
    entries = raw.get('strategies')
    if not isinstance(entries, list) or len(entries) > limits['max_strategies']:
        raise ValueError('strategies must be a bounded list')
    feeds = raw.get('feeds')
    if not isinstance(feeds, dict) or not feeds:
        raise ValueError('At least one configured feed is required')
    identities = set()
    for venue, options in feeds.items():
        if venue not in raw['execution']['venues'] or venue not in ('POLYMARKET_US', 'KALSHI'):
            raise ValueError(f'Unknown public venue {venue}')
        if not isinstance(options, dict) or not isinstance(options.get('markets'), list) or not options['markets']:
            raise ValueError(f'Explicit markets required for {venue}')
        credential_keys = ({'key_id_env', 'private_key_env'} if venue == 'KALSHI'
                           else {'key_id_env', 'secret_key_env'})
        if set(options) - {'markets', 'read_interval', 'market_rules', *credential_keys}:
            raise ValueError('Public feeds accept only subscriptions, rules, timing and credential environment names')
        if any(key in options and (not isinstance(options[key], str) or not options[key])
               for key in credential_keys):
            raise ValueError('Public-feed credential environment names must be nonempty strings')
        positive(options.get('read_interval'), 'read_interval')
        for market in options['markets']:
            if not isinstance(market, str) or not market or f'{venue}:{market}' in identities:
                raise ValueError('Market identities must be nonempty and unique')
            identities.add(f'{venue}:{market}')
    if len(identities) > limits['max_markets']:
        raise ValueError('Configured markets exceed max_markets')
    enabled, names, settings_size = [], set(), 0
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {'name', 'factory', 'enabled', 'required_markets', 'settings'}:
            raise ValueError('Invalid strategy entry keys')
        name = entry['name']
        if (not isinstance(name, str) or not name or len(name) > 128 or name in names
                or not isinstance(entry['factory'], str) or not entry['factory']
                or type(entry['enabled']) is not bool):
            raise ValueError('Strategy names/factories must be valid and names unique')
        names.add(name)
        required = entry['required_markets']
        if (not isinstance(required, list) or not required
                or any(not isinstance(key, str) or key not in identities for key in required)
                or len(set(required)) != len(required)):
            raise ValueError(f'Strategy {name} requires an unsubscribed market')
        if not isinstance(entry['settings'], dict):
            raise ValueError('Strategy settings must be an object')
        size = len(json_bytes(entry['settings']))
        if size > limits['max_event_bytes']:
            raise ValueError('Strategy settings exceed max_event_bytes')
        settings_size += size
        if entry['enabled']:
            enabled.append(entry)
            route = raw['strategy_portfolios'].get(name)
            if route != 'PAPER' or route not in raw['starting_balances']:
                raise ValueError(f'Strategy {name} requires an explicit funded PAPER route')
            if entry['factory'] == 'smoke-v1' and runtime['source'] != 'synthetic':
                raise ValueError('smoke-v1 is restricted to synthetic data')
    if runtime['observe_only']:
        if enabled or runtime['source'] != 'public':
            raise ValueError('Observation requires public source and no enabled strategies')
    elif not enabled:
        raise ValueError('A non-observation run requires an enabled strategy')
    # Include source/recording/context copies and reserve finite admission/cleanup headroom.
    required_bytes = (retained_bytes(raw) * 4 + settings_size * 2 + limits['max_event_bytes'] * 16
                      + _public_startup_buffer_bytes(raw)
                      + limits['max_active_orders'] * 4096 + limits['max_positions'] * 4096)
    if required_bytes > limits['memory_budget_bytes']:
        raise ValueError('Configuration and bounded working copies exceed memory_budget_bytes')
    return raw


def load_bot_config(path):
    try:
        path = Path(path)
        # A finite predecode compatibility ceiling; runtime budgets are checked below.
        with path.open('rb') as source:
            payload = source.read(16 * 1024 * 1024 + 1)
        if len(payload) > 16 * 1024 * 1024:
            raise ValueError('Configuration exceeds 16 MiB')
        raw = json.loads(payload)
    except (OSError, ValueError, TypeError, RecursionError) as error:
        raise ValueError(f'Cannot load bot configuration: {error}') from error
    return validate_bot_config(raw)


def validate_output_directory(directory):
    path = Path(directory)
    if path.exists():
        raise ValueError('Output directory must be new; existing artifacts are never overwritten')
    parent = path.parent
    while not parent.exists():
        parent = parent.parent
    if not parent.is_dir():
        raise ValueError('Output directory parent must be a directory')
    return path
