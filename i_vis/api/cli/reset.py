from logging import getLogger
from typing import Sequence

import click
from flask.cli import AppGroup
from sqlalchemy.sql import text

from i_vis.core.db import metadata

from .. import session
from ..models import UpdateStatus
from ..plugin import BasePlugin
from .utils import validate_plugins

logger = getLogger()

app_group = AppGroup("reset", short_help="Reset I-VIS.")
CONFIRM: str = "YES"


@app_group.command("db", short_help="Reset database.")
def reset_db() -> None:
    choice = input(f"Reset database? DELETE EVERYTHING? {CONFIRM}/no ")
    if choice == CONFIRM:
        session.commit()
        session.execute(text("""SET FOREIGN_KEY_CHECKS = 0;"""))
        drop_tbls = "DROP TABLE IF EXISTS " + ", ".join(tbl for tbl in metadata.tables)
        session.execute(text(drop_tbls))
        session.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))

        result = session.execute(text("""SHOW TABLES"""))
        tbls = ", ".join(tuple(row[0] for row in result))
        if tbls:
            msg = f"Unknown tables were NOT deleted: {tbls}"
            logger.warning(msg)
        session.commit()


@app_group.command("plugins", short_help="Reset current plugins.")
@click.argument("plugin", nargs=-1, callback=validate_plugins)
def reset_plugins(plugin: Sequence[BasePlugin]) -> None:
    s = "\n".join(f"* {p.name}" for p in plugin)

    choice = input(f"Reset plugins:\n{s}\nType {CONFIRM}/no: ")
    if choice != CONFIRM:
        return

    for p in plugin:
        if p.version.current.is_known:
            p.updater.remove_files(p.version.current)
            p.updater.remove_tables(p.version.current)
            assert p.updater.current is not None
            p.updater.current.status = UpdateStatus.DELETED
    session.commit()
    metadata.create_all()
