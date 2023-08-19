import csv
import io

from textual.app import App, ComposeResult

from textual.widgets import DataTable, Welcome

import sqlalchemy
from sqlalchemy import sql
import sqlean as sqlite3
import duckdb


engine_memory = sqlalchemy.create_engine('sqlite://')

with engine_memory.connect() as c:
    c.execute(sql.text("ATTACH DATABASE '/home/phrrngtn/socrata.db3' as socrata;"))



def foo():
    con = duckdb.connect(database=':memory:')
    con.execute('SELECT * FROM   duckdb_views()')

class TableApp(App):
    def compose(self) -> ComposeResult:
        yield DataTable('t1')
        yield DataTable('t2')

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        q = sql.text("SELECT type, name, sql FROM socrata.sqlite_master")

        with engine_memory.connect() as c:
            r = c.execute(q)
            table.add_columns(*r.mappings().keys())
            table.add_rows(r.fetchall())
        table.focus()

def main():
    app = TableApp()
    app.run()

