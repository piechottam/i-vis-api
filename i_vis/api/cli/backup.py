import os

import click
import pandas as pd
from flask.cli import AppGroup

from i_vis.core.db import engine

from ..utils import backup_registry


app_group = AppGroup("backup", short_help="Manage backups.")


@app_group.command("create", short_help="Create backup.")
@click.argument("directory", nargs=1, default="/tmp")
def create(directory: str) -> None:
    for model in backup_registry:
        tname = model.__tablename__
        fname = f"{tname}.tsv"
        df = pd.read_sql_tname(table_name=tname, con=engine)
        df.to_csv(os.path.join(directory, fname), index=False, sep="\t")


@app_group.command("restore", short_help="Restore backup: 'fname.tsv' -> 'table'.")
@click.argument("directory", nargs=1, required=True)
def restore(directory: str) -> None:
    confirm: str = "YES"
    choice = input(
        f"This will replace your database with content from files in '{directory}': {confirm}/no ?"
    )
    if choice != confirm:
        return

    tnames = {model.__tablename__ for model in backup_registry}
    for fname in os.listdir(directory):
        root = os.path.splitext(fname)[0]
        potential_tname = os.path.basename(root)
        if potential_tname and potential_tname in tnames:
            df = pd.read_csv(fname, sep="\t")
            df.to_sql(name=potential_tname, con=engine, if_exists="replace")
