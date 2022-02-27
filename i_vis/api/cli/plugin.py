"""
CLI for plugin(s)
"""

import json
import shutil
from logging import getLogger
from typing import Any, MutableMapping, MutableSequence, Sequence

import click
from flask import current_app
from flask.cli import AppGroup
from sqlalchemy import null
from tabulate import tabulate
from tqdm import tqdm

from i_vis.core.file_utils import create_dir
from i_vis.core.config import add_i_vis

from .utils import (
    validate_plugin,
    validate_plugins,
    validate_plugin_version,
    validate_run,
)
from .. import config_meta, db
from ..models import (
    Plugin as PluginModel,
    PluginUpdate,
    UpdateStatus,
    PluginVersion as PluginVersion,
)
from ..plugin_exceptions import (
    NewPluginVersion,
    MissingPluginModel,
    MissingAnyVersion,
    LatestUrlRetrievalError,
    MissingCurrentVersion,
)
from ..plugin import (
    BasePlugin,
    CoreType,
    DataSource,
    register_tasks,
    post_register,
    register_upgrade_tasks,
    register_update_tasks,
)
from ..task.base import TaskType, Run
from ..task.workflow import Builder as WorkflowBuilder
from ..utils import short_desc

logger = getLogger()

app_group = AppGroup("plugin", short_help="Show plugin relevant info(s).")


# pylint: disable=too-many-branches
@app_group.command("fix", short_help="Check and fix current plugins.")
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
@click.option(
    "-t",
    "--perform-table-check",
    help="Compare DB vs. file system (slow)",
    is_flag=True,
    default=False,
)
@click.option(
    "-w",
    "--workers",
    help="Number of workers to use",
    type=int,
    default=1,
)
@click.option(
    "-c",
    "--cores",
    help="Number of cores to use",
    type=int,
    default=1,
)
@click.option(
    "-T",
    "--trust-local-files",
    help="Trust local files (DANGEROUS).",
    is_flag=True,
    default=False,
)
def fix(
    ignore_upgrade_check: bool,
    trust_local_files: bool,
    perform_table_check: bool,
    workers: int,
    cores: int,
) -> None:
    """Try to fix problems with plugins"""
    config_meta.register_core_variable(
        "PERFORM_TABLE_CHECK", required=False, default=str(perform_table_check)
    )
    config_meta.register_core_variable(
        "TRUST_LOCAL", required=False, default=str(trust_local_files)
    )
    add_i_vis("WORKERS", workers)
    add_i_vis("CORES", workers)

    plugins = BasePlugin.instances()
    for plugin in plugins:
        try:
            # no current and pending version
            if not plugin.version.current.is_known:
                if not plugin.updater.pending:
                    msg = "There is no current or a pending version."
                    msg += "run update and upgrade or"
                    msg += "disable (beware of dependencies)"
                    plugin.logger.warning(msg)
            else:
                create_dir(plugin.dir.current)

            if plugin.version.pending is not None:
                # plugin.warning("Trying to resume")
                try:
                    register_upgrade_tasks(
                        [plugin], check_upgrade=not ignore_upgrade_check
                    )
                except NewPluginVersion as e:
                    plugin.logger.error(e, exc_info=True)
                    return
                create_dir(plugin.dir.by_version(plugin.version.pending))
            else:
                # plugin.logger.warning("Trying to resume install or fix existing")
                if (
                    plugin.updater.current
                    and plugin.updater.current.status != UpdateStatus.INSTALLED
                ):
                    plugin.logger.warning("Invalid plugin status - trying to fix")
                try:
                    register_update_tasks([plugin])
                except NewPluginVersion as e:
                    plugin.logger.error(e, exc_info=True)
                    return
            if not plugin.task_builder.offered_resources().consistent:
                plugin.logger.warning(
                    "Offered resources are out of sync - trying to fix by re-runing workflow"
                )

        except MissingPluginModel:
            plugin.logger.error("Missing plugin model - run update or disable")
            return

    post_register()

    wfbuilder = WorkflowBuilder(plugins=plugins)
    for plugin in plugins:
        wfbuilder.add_plugin(plugin)
    workflow = wfbuilder.build()
    workflow.run(run=Run.DEFAULT, workers=workers, cores=cores)


@app_group.command("status", short_help="Status of plugins.")
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
@click.option(
    "-C",
    "--ignore-consistency-check",
    help="Do not test consistency of file system and DB.",
    is_flag=True,
    default=False,
)
@click.option(
    "-D",
    "--ignore-disabled",
    help="Do not show disabled plugins.",
    is_flag=True,
    default=False,
)
def status(
    ignore_upgrade_check: bool,
    ignore_consistency_check: bool,
    ignore_disabled: bool,
) -> None:
    """Print status of plugins"""
    table = []

    pnames = list(CoreType.pnames(not ignore_disabled)) + list(
        DataSource.pnames(not ignore_disabled)
    )
    for pname in pnames:
        plugin = BasePlugin.get(pname)
        plugin_status = plugin.status(ignore_upgrade_check, ignore_consistency_check)
        row: MutableMapping[str, Any] = dict(plugin_status)
        row["info"] = ",".join(plugin_status["info"])
        table.append(row.values())
    headers = ("Type", "Plugin", "Installed", "Pending", "Latest", "Info")
    print(tabulate(table, headers=headers, tablefmt="plain"))


@app_group.command("updates", short_help="Updates for data sources(s).")
@click.argument("data-source", nargs=-1, callback=validate_plugins)
def print_updates(data_source: Sequence[DataSource]) -> None:
    """Print (local) updates for plugin(s)"""
    table = []
    for data_source_ in data_source:
        try:
            plugin_model = data_source_.updater.model
        except MissingPluginModel as e:
            print(e.msg)
            return

        for plugin_update in plugin_model.updates:
            row = {}
            if len(data_source) > 1:
                row["pname"] = data_source_.name
            row["version"] = plugin_update.plugin_version.version_str
            row["status"] = str(plugin_update.status.value)
            row["installed_at"] = str(plugin_update.installed_at)
            row["files"] = str(len(plugin_update.file_updates))
            row["tables"] = str(len(plugin_update.table_updates))
            table.append(row.values())
    headers = ["Version", "Status", "Installed", "Files", "Tables"]
    if len(data_source) > 1:
        headers = ["Plugin"] + headers
    print(tabulate(table, headers=headers, tablefmt="plain"))


@app_group.command("tasks", short_help="Show tasks.")
@click.argument("plugin", nargs=-1, callback=validate_plugins)
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
def print_tasks(
    plugin: Sequence["BasePlugin"],
    ignore_upgrade_check: bool,
) -> None:
    """Print tasks of plugin(s)"""
    table = []
    try:
        register_tasks(plugin, check_upgrade=not ignore_upgrade_check)
    except MissingPluginModel as e:
        logger.error(e.message)
        return
    except MissingAnyVersion as e:
        logger.error(e.message)
        return
    post_register()
    for p in plugin:
        row = {"pname": p.name}

        tasks = p.task_builder.tasks()
        for task in tasks:
            row["task_id"] = str(task.tid)
            row["task_type"] = task.type.value
            table.append(dict(row).values())
        if not tasks:
            row["task"] = ""
            row["task_type"] = ""
            table.append(row.values())
    headers = ["Plugin", "Task", "Type"]
    print(tabulate(table, headers=headers, tablefmt="plain"))


# pylint: disable=too-many-branches
@app_group.command("versions", short_help="Available versions for plugin(s).")
@click.argument("plugin", nargs=-1, callback=validate_plugins)
def print_versions(plugin: Sequence["BasePlugin"]) -> None:
    """Print (remote) versions of plugin(s)"""
    table = []
    for p in plugin:
        try:
            _ = p.updater.model
        except MissingPluginModel as e:
            p.logger.warning(e.msg)
            continue

        for plugin_version in db.session.query(PluginVersion).filter_by(
            plugin_name=p.name
        ):
            row = {}
            if len(plugin) > 1:
                row["pname"] = p.name
            row["version"] = plugin_version.version_str
            row["installable"] = "yes" if plugin_version.installable else "no"
            row["created_at"] = str(plugin_version.created_at)
            info: MutableSequence[str] = []
            if str(p.version.pending) == plugin_version.version_str:
                info.append("pending")
            elif str(p.version.current) == plugin_version.version_str:
                info.append("current")
            if p.updater.model.frozen:
                info.append("frozen")
            row["info"] = ",".join(info)
            if len(plugin) == 1:
                row["url"] = ""
                table.append(row.values())
            else:
                table.append(row.values())
    headers = ["Version", "Installable", "Checked", "Info"]
    if len(plugin) > 1:
        headers = ["Plugin"] + headers
    else:
        headers = headers + ["Url"]
    print(tabulate(table, headers=headers, tablefmt="plain"))


@app_group.command(
    "choose",
    short_help="Choose version of plugin to install - will be pending.",
)
@click.argument("plugin", nargs=1, callback=validate_plugin)
@click.argument("version", nargs=1, callback=validate_plugin_version)
def choose(plugin: "BasePlugin", plugin_version: PluginVersion) -> None:
    """Choose a remotely available and installable version of a plugin"""
    try:
        _ = plugin.updater.model
    except MissingPluginModel as e:
        plugin.logger.error(e.msg)
        return

    if plugin.updater.model.frozen:
        plugin.logger.error("Plugin version frozen - unfreeze it first")
        return

    if str(plugin.version.current) == plugin_version.version_str:
        plugin.logger.info("Plugin version already installed")
        return

    if not plugin_version.installable:
        plugin.logger.error(
            "Plugin version cannot be choosen. Use update, upgrade to change version"
        )
        return

    plugin_update = db.session.query(PluginUpdate).get(plugin_version.id).first()
    if not plugin_update:
        plugin_update = plugin.updater.model.create_update(plugin_version)
    plugin.updater.model.pending_update_id = plugin_update.plugin_version_id
    db.session.add(plugin.updater.model)
    db.session.commit()
    plugin.logger.info("New version of plugin choosen")


@app_group.command(
    "freeze",
    short_help="Freeze installed version.",
)
@click.argument("plugin", nargs=1, callback=validate_plugin)
@click.option("-i", "--invert", is_flag=True, default=False, help="Unfreeze plugin.")
def freeze(plugin: "BasePlugin", invert: bool) -> None:
    """Freeze or unfreeze a specific installed version of a plugin"""
    try:
        _ = plugin.updater.model
    except MissingPluginModel as e:
        plugin.logger.error(e.msg)
        return

    if not plugin.version.current.is_known:
        plugin.logger.error("There is no current version - nothing to freeze")
        return

    assert plugin.updater.current is not None
    current_update_status = plugin.updater.current.status
    if current_update_status != UpdateStatus.INSTALLED:
        plugin.logger.error(
            f"Invalid plugin status: {current_update_status}. "
            "Cannot freeze invalid state - try to fix"
        )
        return

    if plugin.updater.model.frozen:
        if invert:
            msg = "Plugin version unfrozen"
            plugin.updater.model.frozen = False
        else:
            msg = "Plugin version already frozen"
    else:
        if invert:
            msg = "Plugin already unfrozen"
        else:
            plugin.updater.model.frozen = True
            msg = "Plugin version frozen"
    db.session.commit()
    print(msg)


@app_group.command("update", short_help="Update latest version info of plugin(s).")
@click.argument("plugin", nargs=-1, callback=validate_plugins)
def update(plugin: Sequence[BasePlugin]) -> None:
    """Check remote versions and update local of plugin(s)"""
    # order of plugins important:
    # first non plugins then i_vis
    # core components depend on data sources!!!

    pb = tqdm(total=len(plugin), unit=" plugin", unit_scale=False, leave=False)

    prefix = "Checking "
    width = max(len(p.name) for p in plugin) + len(prefix)
    for p in plugin:
        p.register_tasks()

        pb.set_description(short_desc("Checking", p.name, width=width))
        # retrieve latest version from remote source
        latest_version = p.latest_version
        if not latest_version.is_known:
            p.logger.warning("Could not retrieve remote version")
            pb.update(n=1)
            continue

        try:
            # check if plugin model exists
            _ = p.updater.model
            # check if remote latest version is locally known
            plugin_version = (
                db.session.query(PluginVersion)
                .filter_by(plugin_name=p.name, version_str=str(latest_version))
                .first()
            )
            if plugin_version is not None:
                pb.update(n=1)
                continue  # version already known
        except MissingPluginModel:
            # create new plugin model
            plugin_model = PluginModel(name=p.name)
            db.session.add(plugin_model)
            db.session.commit()

        try:
            p.updater.add_latest_version(latest_version)
        except LatestUrlRetrievalError as e:
            p.logger.error(e.msg)
            return
        db.session.commit()
        pb.update(n=1)


def _create_pending_updates(plugins: Sequence["BasePlugin"]) -> Sequence["BasePlugin"]:
    """Create pending updates for plugin(s)"""

    plugins_need_work: MutableSequence["BasePlugin"] = []
    for plugin in plugins:
        try:
            _ = plugin.updater.model
        except MissingPluginModel as e:
            plugin.logger.error(e.msg)
            return []

        if not plugin.version.local_latest.is_known:
            plugin.logger.error("Latest remote version not available")
            return []

        if plugin.updater.model.frozen:
            plugin.logger.info(f"Version frozen to {plugin.version.current}")

        if plugin.version.is_uptodate:
            plugin.logger.info("No new version available")
            continue

        if not plugin.updater.pending:
            plugin_version = (
                db.session.query(PluginVersion)
                .filter_by(
                    plugin_name=plugin.name,
                    version_str=str(plugin.version.local_latest),
                )
                .first()
            )
            plugin_update = plugin.updater.model.create_update(plugin_version)

            if not plugin.version.current.is_known:
                msg = "Install version: %s "
            else:
                msg = "Upgrade to version: %s "
        else:
            if plugin.version.pending != plugin.version.local_latest:
                msg = "Cancel upgrade to version: %s"
                plugin.logger.info(msg, plugin.version.pending)
                plugin.updater.pending.status = UpdateStatus.FAILED
                db.session.commit()

                plugin_version = (
                    db.session.query(PluginVersion)
                    .filter_by(
                        plugin_name=plugin.name,
                        version_str=str(plugin.version.local_latest),
                    )
                    .first()
                )
                plugin_update = plugin.updater.model.create_update(plugin_version)
                msg = "Upgrade to version: %s"
            else:
                plugin_update = plugin.updater.pending
                if not plugin.version.current.is_known:
                    msg = "Resume install to version: %s "
                else:
                    msg = "Resume upgrade to version: %s "

        version_str = plugin_update.plugin_version.version_str
        version = plugin.version.str_to_version(version_str)
        plugin.logger.info(msg, version)
        create_dir(plugin.dir.by_version(version))
        plugins_need_work.append(plugin)

    db.session.commit()
    return plugins_need_work


@app_group.command("upgrade", short_help="Upgrade plugin(s) to latest version.")
@click.argument("plugin", nargs=-1, callback=validate_plugins)
@click.option(
    "--omit-etl",
    is_flag=True,
    default=False,
    help="Just create pending updates DON'T start ETL",
)
@click.option(
    "--only-extract",
    is_flag=True,
    default=False,
    help="Just run tasks that Download data",
)
@click.option(
    "-T",
    "--trust-local-files",
    help="Trust local files (DANGEROUS).",
    is_flag=True,
    default=False,
)
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
@click.option(
    "-w",
    "--workers",
    help="Number of workers to use",
    type=int,
    default=1,
)
@click.option(
    "-c",
    "--cores",
    help="Number of cores per worker to use",
    type=int,
    default=1,
)
@click.option(
    "--run",
    callback=validate_run,
    type=click.Choice(list(Run.values())),
    default="default",
)
# pylint: disable=too-many-locals,too-many-arguments
def upgrade(
    plugin: Sequence["BasePlugin"],
    omit_etl: bool,
    only_extract: bool,
    trust_local_files: bool,
    ignore_upgrade_check: bool,
    workers: int,
    cores: int,
    run: Run,
) -> None:
    """Create pending updates and (optionally) start ETL process of plugin(s)"""
    if omit_etl and only_extract:
        logger.error("Cannot omit etls and only run extract tasks at the same time")
        return

    if trust_local_files:
        config_meta.register_core_variable(
            "TRUST_LOCAL", required=False, default="True"
        )

    add_i_vis("WORKERS", workers)
    add_i_vis("CORES", workers)

    plugins = plugin
    _ = _create_pending_updates(plugins)

    if omit_etl:
        logger.info("Created pending updates and omitted ETL process")
        return

    upgrade_plugins = tuple(p for p in plugins if p.version.pending is not None)
    if not upgrade_plugins:
        logger.info("Nothing to upgrade")

    # register plugin(s) for
    # * upgrade -> new version or
    # * "update" -> existing version
    register_upgrade_tasks(upgrade_plugins, check_upgrade=not ignore_upgrade_check)

    # TODO BasePlugin.instances()
    installed_plugins = tuple(
        p for p in plugins if p not in upgrade_plugins and p.updater.current
    )
    try:
        register_update_tasks(installed_plugins)
    except MissingCurrentVersion as e:
        logger.error(e.message)
        return
    except NewPluginVersion as e:
        logger.error(e.message)
        return
    post_register()

    wfbuilder = WorkflowBuilder(plugins=BasePlugin.instances())
    task_types = None
    if only_extract:
        task_types = set()
        task_types.add(TaskType.EXTRACT)
    for plugin_ in plugin:
        wfbuilder.add_plugin(plugin_, task_types)
    workflow = wfbuilder.build(add_missing=True)
    workflow.run(run, workers, cores)


@app_group.command("variables", short_help="Variables of plugins")
def variables() -> None:
    """Print variables of all enabled plugins"""
    table = []
    for var_name, meta in config_meta.name2var.items():
        row = {}
        name = str(meta.get("pname", "core"))
        row["name"] = name
        row["type"] = str(meta["type"])
        row["var"] = var_name
        if current_app.config.get(var_name, ""):
            row["value"] = str(current_app.config.get(var_name, ""))
        else:
            row["value"] = str(meta.get("default", ""))
        row["required"] = "yes" if meta["required"] else "no"
        row["info"] = (
            "missing"
            if meta["required"] and var_name not in current_app.config
            else "okay"
        )
        table.append(row.values())

    headers = ("Name", "Component", "Variable", "Value", "Required", "Info")
    print(tabulate(table, headers=headers, tablefmt="plain"))


@app_group.command(
    "details", short_help="Show details for plugin version. Format: plugin::version"
)
@click.argument("plugin-version", nargs=1, callback=validate_plugin_version)
def details(plugin_version: "PluginVersion") -> None:
    assert plugin_version.plugin_name
    plugin = BasePlugin.get(plugin_version.plugin_name)

    if plugin.updater.has_model and plugin.updater.working:
        plugin.register_tasks()

    if hasattr(plugin, "remote_latest_version"):
        remote_lastest_version = str(getattr(plugin_version, "remote_latest_version"))
    else:
        remote_lastest_version = "NA"

    plugin_meta = {
        "pname": plugin.name,
        "fullname": plugin.meta.fullname,
        "url": plugin.meta.url,
        "type": plugin.type,
        "current_version": str(plugin.version.current),
        "local_latest_version": str(plugin.version.local_latest),
        "remote_latest_version": remote_lastest_version,
    }

    print(
        json.dumps(
            plugin_meta,
            indent=2,
        )
    )


# pylint: disable=too-many-branches
@app_group.command("reset", short_help="Reset plugin. Format: plugin::version")
@click.argument("plugin-version", nargs=1, callback=validate_plugin_version)
@click.option("--deep", is_flag=True, default=False, help="Remove all data")
@click.option("--omit-db", is_flag=True, default=False, help="Don't reset DB")
@click.option("--omit-files", is_flag=True, default=False, help="Don't reset files")
def reset(
    plugin_version: PluginVersion, deep: bool, omit_db: bool, omit_files: bool
) -> None:
    plugin_update = plugin_version.plugin_update
    assert plugin_version.plugin_name
    plugin = BasePlugin.get(plugin_version.plugin_name)

    if omit_db and omit_files:
        plugin.logger.error("Cannot not omit DB AND files - something has to ch")
        return

    assert plugin_version.version_str
    version = plugin.version.str_to_version(plugin_version.version_str)

    _choice = "YES"
    choice = input(f"Reset plugin {plugin.name} version {version}? {_choice}/Quit ")
    if choice != _choice:
        plugin.logger.info("Aborted")
        return

    if deep:
        if not omit_files:
            plugin.updater.remove_files(version)
            if plugin_update:
                version_dir = plugin.dir.by_update(plugin_update)
                if version_dir:
                    shutil.rmtree(version_dir)
        if not omit_db:
            plugin.updater.remove_tables(version)

        if plugin_update:
            if (
                plugin.updater.pending
                and plugin.updater.model.pending_update_id == plugin_update.id
            ):
                plugin.updater.model.pending_update_id = null()
                db.session.add(plugin.updater.model)
            if (
                plugin.updater.current
                and plugin.updater.model.current_update_id == plugin_update.id
            ):
                plugin.updater.model.current_update_id = null()
                db.session.add(plugin.updater.model)
            db.session.delete(plugin_version.plugin_update)
            db.session.commit()
        db.session.delete(plugin_version)
        db.session.commit()
    else:
        if not omit_files:
            plugin.updater.remove_files(version)
        if not omit_db:
            plugin.updater.remove_tables(version)
        if plugin_update:
            plugin_update.status = UpdateStatus.DELETED
    db.session.commit()

    what = []
    if not omit_db:
        what.append("DB")
    if not omit_files:
        what.append("files")

    plugin.logger.info(f"Reset: {','.join(what)}")
