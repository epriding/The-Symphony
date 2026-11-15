"""Separate bounded diagnostic tables and inert selected-record text."""
from textual.app import ComposeResult
from textual.containers import Horizontal,Vertical,VerticalScroll
from textual.widgets import Button,Checkbox,DataTable,Static,Tab,Tabs

from .tui_findings import SEVERITIES,finding_cell

DIAGNOSTIC_VIEWS=('findings','events','details')


class Diagnostics(Vertical):
    def __init__(self):
        super().__init__(id='diagnostics')
        self.mode='findings'
        self.context=None
        self.history=None
        self.failure=None
        self.rows=[]
        self.severities=set(SEVERITIES)

    def compose(self) -> ComposeResult:
        yield Tabs(Tab('Findings',id='diag-findings'),Tab('Events',id='diag-events'),
            Tab('Record details',id='diag-details'),id='diagnostic_tabs')
        with Horizontal(id='finding_filters'):
            for severity in SEVERITIES:
                checkbox=Checkbox(severity.title(),True,id='filter-'+severity,compact=True)
                if self.app.ascii_mode:
                    checkbox.BUTTON_LEFT='['
                    checkbox.BUTTON_RIGHT=']'
                yield checkbox
            yield Button('All',id='filter-all',compact=True)
            yield Button('Warnings + Errors',id='filter-alerts',compact=True)
        yield Static('Loading retained findings',id='diagnostic_status',markup=False)
        yield DataTable(id='diagnostic_records',cursor_type='row',zebra_stripes=True)
        with VerticalScroll(id='diagnostic_detail_scroll',can_focus=True):
            yield Static('Select a diagnostic record',id='diagnostic_details',markup=False)
        with VerticalScroll(id='detail_scroll',can_focus=True):
            yield Static('Waiting for a collection',id='record_details',markup=False)

    def set_context(self,context) -> None:
        if context!=self.context:
            self.context=context
            self.history=None
            self.failure=None
            self.render_diagnostics()

    def show_mode(self,mode: str) -> None:
        self.mode=mode
        tabs=self.query_one('#diagnostic_tabs',Tabs)
        if tabs.active!='diag-'+mode:tabs.active='diag-'+mode
        self.render_diagnostics()

    def on_tabs_tab_activated(self,event: Tabs.TabActivated) -> None:
        if event.tabs.id=='diagnostic_tabs':
            event.stop()
            if event.tabs.active==event.tab.id and self.mode!=event.tab.id[5:]:
                self.show_mode(event.tab.id[5:])

    def on_resize(self) -> None:
        if self.query('#diagnostic_detail_scroll'):
            self.query_one('#diagnostic_detail_scroll').display=self.mode!='details' and self.size.height>=(11 if self.mode=='findings' else 10)

    @property
    def filter_label(self) -> str:
        return 'All' if len(self.severities)==3 else ', '.join(
            value.title() for value in SEVERITIES if value in self.severities) or 'None'

    def filtered_findings(self) -> list:
        return [row for row in (self.history or {}).get('items',[])
            if row['severity'] in self.severities]

    def on_checkbox_changed(self,event: Checkbox.Changed) -> None:
        if not event.checkbox.id.startswith('filter-'):return
        event.stop()
        self.severities={severity for severity in SEVERITIES
            if self.query_one('#filter-'+severity,Checkbox).value}
        self.render_diagnostics()
        self.app.apply_finding_filter(reset=True)

    def on_button_pressed(self,event: Button.Pressed) -> None:
        if event.button.id not in ('filter-all','filter-alerts'):return
        event.stop()
        self.severities=set(SEVERITIES if event.button.id=='filter-all' else ('warning','error'))
        for severity in SEVERITIES:
            self.query_one('#filter-'+severity,Checkbox).value=severity in self.severities
        self.render_diagnostics()
        self.app.apply_finding_filter(reset=True)

    def render_status(self) -> None:
        if self.mode=='details':
            status='Selected main record and full session fields / scroll for more'
        elif self.mode=='events':
            status=f'{len(self.rows)} recent overview events / bounded overview subset'
        elif self.failure:
            status='Findings unavailable / '+self.failure
            if self.history is not None:status+=' / previous retained view'
        elif self.history is None:
            status='Loading retained findings / not an empty result'
        else:
            count=len(self.rows)
            total=len(self.history['items'])
            status=f'{count} of {total} retained / Filter: {self.filter_label} / newest first'
            if not total:status+=' / No retained findings'
            elif not self.severities:status+=' / No severities selected'
            elif not count:status+=' / No matching findings'
            status+=f' / evicted {self.history["evicted"]}'
        if not self.app.state.fresh(self.app.clock()):status+=' / STALE'
        self.app.update_text('#diagnostic_status',status)

    def render_diagnostics(self) -> None:
        table=self.query_one('#diagnostic_records',DataTable)
        index=table.cursor_row
        selected=self.rows[index]['id'] if 0<=index<len(self.rows) else None
        if self.mode=='findings':
            self.rows=self.filtered_findings()
            columns=('Severity','Time (UTC)','Strategy','Author','Summary')
            fields=('severity','timestamp','strategy','author','message')
        else:
            self.rows=list((self.app.overview or {}).get('recent_events',[])) if self.mode=='events' else []
            columns=('Severity','Time (UTC)','Strategy','Event','Summary')
            fields=('severity','timestamp','strategy','action','message')
        table.clear(columns=True)
        for title,width in zip(columns,(9,10,16,12,34)):table.add_column(title,width=width)
        for row in self.rows:
            table.add_row(*(finding_cell(self.app.text,field,row.get(field)) for field in fields),key=row['id'])
        chosen=next((i for i,row in enumerate(self.rows) if row['id']==selected),0)
        if self.rows:table.move_cursor(row=chosen,animate=False)
        table.display=self.mode!='details'
        self.query_one('#finding_filters').display=self.mode=='findings'
        self.query_one('#detail_scroll').display=self.mode=='details'
        self.on_resize()
        self.render_selected()
        self.render_status()

    def render_selected(self) -> None:
        index=self.query_one('#diagnostic_records',DataTable).cursor_row
        content='No selected diagnostic record'
        if 0<=index<len(self.rows):
            content='\n'.join(f'{self.app.text(key)}: {self.app.text(value)}'
                for key,value in self.rows[index].items())
        self.app.update_text('#diagnostic_details',content)

    def on_data_table_row_highlighted(self,event: DataTable.RowHighlighted) -> None:
        if event.data_table.id=='diagnostic_records':
            event.stop();self.render_selected()

    def on_data_table_row_selected(self,event: DataTable.RowSelected) -> None:
        if event.data_table.id=='diagnostic_records':
            event.stop()
            if not 0<=event.cursor_row<len(self.rows):return
            if not self.app.diagnostics_expanded:self.app.action_expand_diagnostics()
            self.app.call_after_refresh(self.query_one('#diagnostic_detail_scroll').focus)
