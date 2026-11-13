"""Bounded retained finding reads for the human client; no protocol extensions."""
from datetime import datetime,timezone
from rich.text import Text

from .client import ClientError

FINDING_LIMIT=128
SEVERITIES=('info','warning','error')
SEVERITY_COLORS={'info':'#8bded0','warning':'#edc47c','error':'#ef9696'}


async def read_findings(client,target,name=None) -> dict:
    params={'limit':50}
    if name is not None:params['name']=name
    rows=[];ids=set();cursors=set();window=None
    for _ in range(3):
        reply=await client.request('findings',target=target,params=params)
        page=reply['data']
        observed=tuple(page[key] for key in ('retained_start','retained_end','evicted'))
        if window is not None and window!=observed:
            raise ClientError('history_changed')
        window=observed
        for row in page['items']:
            if row['id'] in ids:raise ClientError('history_changed')
            ids.add(row['id']);rows.append(row)
        if len(rows)>FINDING_LIMIT:raise ClientError('limit_exceeded')
        if not page['has_more']:
            return dict(items=sorted(rows,key=lambda row:row['sequence'],reverse=True),
                retained_start=window[0],retained_end=window[1],evicted=window[2],
                revision=reply['revision'])
        cursor=page['next_cursor']
        if cursor in cursors:raise ClientError('invalid_response')
        cursors.add(cursor);params['cursor']=cursor
    raise ClientError('limit_exceeded')


def finding_time(value) -> str:
    if value is None:return 'unknown'
    try:
        return datetime.fromtimestamp(value,timezone.utc).strftime('%H:%M:%S')
    except (TypeError,ValueError,OverflowError,OSError):
        return str(value)


def finding_cell(display,field,value) -> Text:
    text=display(finding_time(value) if field=='timestamp' else
        'session-wide' if field=='strategy' and value is None else value)
    return Text(text.upper() if field=='severity' else text,
        style=SEVERITY_COLORS.get(value,'') if field=='severity' else '',
        overflow='crop',no_wrap=True)
