"""Detail sequence numbers cannot depend on unrelated archive flushes."""
import gzip
import hashlib
import json
from pathlib import Path

import pytest

from src.paper_trading.history import HistoryStore
from src.paper_trading.replay import replay_runtime


def write_interleaved(directory, operations):
    store = HistoryStore(directory)
    try:
        def append(conn):
            for stream, identity in operations:
                store._append(conn, stream, {'identity': identity})
            conn.commit()
        store._call(append)
        references = store.detail_references()
        return references, [json.loads(x) for x in
            (directory/'order_timings.jsonl').read_text().splitlines()]
    finally:
        store.close()


def test_detail_order_is_local_to_stream_and_ignores_other_stream_flushes(tmp_path):
    a, rows_a = write_interleaved(tmp_path/'a', [
        ('order_timings','first'), ('unrelated','diagnostic'),
        ('markouts','quote'), ('order_timings','second')])
    b, rows_b = write_interleaved(tmp_path/'b', [
        ('order_timings','first'), ('order_timings','second'),
        ('unrelated','diagnostic'), ('markouts','quote')])
    assert rows_a == rows_b == [
        {'identity':'first','accounting_order':1},
        {'identity':'second','accounting_order':2}]
    assert a == b


@pytest.mark.asyncio
async def test_saved_legacy_failure_replays_to_identical_version2_detail_streams(tmp_path):
    fixture = json.loads(gzip.decompress((Path(__file__).parent /
        'fixtures/paper_trading/history_order_capture.json.gz').read_bytes()))
    source = tmp_path/'source'
    source.mkdir()
    (source/'manifest.json').write_text(json.dumps(fixture['manifest']))
    (source/'inputs.jsonl').write_text(fixture['inputs'])
    source_bytes = (source/'inputs.jsonl').read_bytes()
    first = await replay_runtime(source, tmp_path/'first')

    async def pacing(record):
        import asyncio
        await asyncio.sleep(0)

    second = await replay_runtime(source, tmp_path/'second', pacing=pacing)
    assert first['history']['schema_version'] == second['history']['schema_version'] == 2
    assert first['history'] == second['history']
    for kind, original_rows in fixture['details'].items():
        # v2 explicitly defines accounting_order as this stream's ordinal.
        # Every other recorded field, including correction and cost data,
        # remains checked against the original failing capture.
        expected = [dict(row, accounting_order=i+1) for i,row in enumerate(original_rows)]
        raw = (tmp_path/f'first/{kind}.jsonl').read_bytes()
        actual = [json.loads(x) for x in raw.splitlines()]
        assert actual == expected
        reference = next(row for row in first['history']['details'] if row['kind']==kind)
        assert reference['sha256'] == hashlib.sha256(raw).hexdigest()
    assert (source/'inputs.jsonl').read_bytes() == source_bytes
