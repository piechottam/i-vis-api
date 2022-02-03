import re
from typing import Any, cast, Optional, Sequence, Set

import click
from click.core import Context
from flask.cli import with_appcontext

from .. import db
from ..models import PluginVersion, User
from ..resource import ResourceId, res_registry as res_registry
from ..task.base import TaskType, Run
from ..plugin import BasePlugin, DataSource
from ..plugin_exceptions import UnknownPlugin, WrongPluginType


REGEX = r"^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$"


@with_appcontext
def get_plugin_version(pname: str, version_str: str) -> Optional[PluginVersion]:
    plugin_version = (
        db.session.query(PluginVersion)
        .filter_by(plugin_name=pname, version_str=version_str)
        .first()
    )

    if not plugin_version:
        return None

    return cast("PluginVersion", plugin_version)


def valid_mail(s: str) -> bool:
    return re.search(REGEX, s) is not None


# pylint: disable=unused-argument
def validate_file_resid(ctx: Context, param: Any, value: Any) -> ResourceId:
    try:
        rid = ResourceId.from_str(value)
        return rid
    except NotImplementedError as e:
        raise click.BadParameter(f"Invalid ResId: {value}") from e


# pylint: disable=unused-argument
def validate_plugin_version(ctx: Context, param: Any, value: Any) -> PluginVersion:
    try:
        pname, version_str = value.split("::")
        plugin_version = get_plugin_version(pname, version_str)
        if plugin_version is None:
            raise click.BadParameter(f"Unknown plugin version: {value}")
        return cast(PluginVersion, plugin_version)
    except ValueError as e:
        raise click.BadParameter(f"Unknown plugin version: {value}") from e


@with_appcontext
# pylint: disable=unused-argument
def validate_plugin(ctx: Context, param: Any, value: Any) -> "BasePlugin":
    try:
        return BasePlugin.get(value)
    except UnknownPlugin as e:
        raise click.BadParameter(f"Could not select plugin: {value}") from e


@with_appcontext
# pylint: disable=unused-argument
def validate_data_source(ctx: Context, param: Any, value: Any) -> "DataSource":
    try:
        return DataSource.get(value)
    except (UnknownPlugin, WrongPluginType) as e:
        raise click.BadParameter(f"Could not select data source: {value}") from e


@with_appcontext
# pylint: disable=unused-argument
def validate_plugins(ctx: Context, param: Any, values: Any) -> Sequence["BasePlugin"]:
    if not values:
        return BasePlugin.instances()

    return tuple(validate_plugin(ctx, param, pname) for pname in values)


@with_appcontext
# pylint: disable=unused-argument
def validate_data_sources(
    ctx: Context, param: Any, values: Any
) -> Sequence["DataSource"]:
    if not values:
        return DataSource.instances()

    return tuple(validate_data_source(ctx, param, pname) for pname in values)


# pylint: disable=unused-argument
def validate_rid(ctx: Context, param: Any, value: Any) -> ResourceId:
    try:
        rid = ResourceId.from_str(value)
        _ = res_registry[rid]
        return rid
    except KeyError as e:
        raise click.BadParameter(f"Unknown res-id: {value}") from e


# pylint: disable=unused-argument
def validate_run(ctx: Context, param: Any, value: Any) -> Run:
    try:
        return cast(Run, Run.from_str(value))
    except ValueError as e:
        raise click.BadParameter(f"Could not select run: {value}") from e
    except NotImplementedError as e:
        raise click.BadParameter(f"Could not select run: {value}") from e


# pylint: disable=unused-argument
def validate_runs(ctx: Context, param: Any, values: Any) -> Set[Run]:
    return set(validate_run(ctx, param, value) for value in values)


def validate_graph_format(ctx: Context, param: Any, value: Any) -> str:
    if value in ("dot", "make"):
        return str(value)
    raise click.BadParameter(f"Unknown graph format: {value}")


# pylint: disable=unused-argument
def validate_task_type(ctx: Context, param: Any, value: Any) -> TaskType:
    try:
        return cast(TaskType, TaskType.from_str(value))
    except ValueError as e:
        raise click.BadParameter(f"Could not select task-type: {value}") from e
    except NotImplementedError as e:
        raise click.BadParameter(f"Could not select task-type: {value}") from e


# pylint: disable=unused-argument
def validate_task_types(ctx: Context, param: Any, values: Any) -> Set[TaskType]:
    if not values:
        return set(TaskType)
    return set(validate_task_type(ctx, param, value) for value in values)


@with_appcontext
def validate_unique_username(param: Any, value: Any) -> str:
    if User.load_by_name(value):
        raise click.BadParameter(f"Username already exists: {value}")
    return str(value)


@with_appcontext
def validate_unique_mail(param: Any, value: Any) -> str:
    if User.load_by_mail(value):
        raise click.BadParameter(f"Mail already exists: {value}")
    return str(value)
