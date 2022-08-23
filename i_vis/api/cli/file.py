import os
from typing import Sequence, TYPE_CHECKING

import click
from flask.cli import AppGroup
from tabulate import tabulate

from i_vis.core.file_utils import md5, size as file_size

from .utils import (
    validate_plugins,
    validate_file_resid,
)
from .. import session
from ..resource import res_registry, ResourceId
from ..plugin_exceptions import MissingPluginModel

if TYPE_CHECKING:
    from ..plugin import BasePlugin

app_group = AppGroup("file", short_help="Show files.")


def format_diff(db_: str, fs: str) -> str:
    if db_ != fs:
        return f"{str(db_)}<DB::FS>{str(fs)}"
    return f"{str(db_)}"


@app_group.command("list", short_help="Show file info (local and DB).")
@click.argument("plugin", nargs=-1, callback=validate_plugins)
@click.option("--omit-md5", is_flag=True, default=False, help="Omit md5 calculation.")
def print_list(plugin: Sequence["BasePlugin"], omit_md5: bool) -> None:
    table = []
    for plugin_ in plugin:
        try:
            _ = plugin_.updater.model
        except MissingPluginModel as e:
            print(e.msg)
            return

        for plugin_update in plugin_.updater.model.updates:
            for fname, file_update in plugin_update.file_updates.items():
                row = {}
                version = plugin_.version.update_to_version(plugin_update)
                if len(plugin) > 1:
                    row["pname"] = plugin_.name
                row["version"] = str(version)
                row["fname"] = fname
                qname = file_update.qname
                if os.path.exists(qname):
                    row["size"] = format_diff(
                        str(file_update.size), str(file_size(qname))
                    )
                    if not omit_md5:
                        assert file_update.md5 is not None
                        row["md5"] = format_diff(file_update.md5, md5(qname))
                else:
                    assert file_update.size is not None
                    row["size"] = format_diff(str(file_update.size), str("0"))
                row["updated"] = str(file_update.updated_at)
                table.append(row.values())
    headers = ["Version", "Filename", "Size", "MD5", "Updated"]
    if len(plugin) > 1:
        headers = ["Plugin"] + headers
    print(tabulate(table, headers=headers, tablefmt="plain"))


@app_group.command(
    "force", short_help="Assume local file is correct and update pending update."
)
@click.argument("res-id", nargs=1, callback=validate_file_resid)
def force(rid: ResourceId) -> None:
    try:
        resource = res_registry[rid]
    except KeyError:
        print("Unknown resource: {rid}.")
        return
    if not os.path.isfile(resource.qname):
        print("File not found: {resource.qfname}.")
        return

    if not resource.plugin.updater.model.pending_update:
        print("No pending update plugin.")
        return

    plugin_update = resource.plugin.updater.model.pending_update
    for file_update in plugin_update.file_updates.values():
        if file_update.name == resource.name:
            file_update.update()
            session.add(file_update)
            session.commit()
            print("File info have been updated.")
            return
    plugin_update.add(resource)
    print("File info have been added.")
