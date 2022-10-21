import csv
import io

from textual.app import App, ComposeResult
from textual.widgets import DataTable

import sqlalchemy
import pysqlite3

from pysqlite3 import dbapi2 as sqlite

engine_memory = sqlalchemy.create_engine('sqlite://',module=pysqlite3)
engine_memory.execute("ATTACH DATABASE '/home/phrrngtn/socrata.db3' as socrata");

q = "SELECT * FROM socrata.sqlite_master"

class TableApp(App):
    def compose(self) -> ComposeResult:
        yield DataTable()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        r = engine_memory.execute(q)
        table.add_columns(*r.keys())
        table.add_rows(r)
        table.focus()

def main():
    print("hello from rule4.cli.main")
    app = TableApp()
    app.run()
