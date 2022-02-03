"""
CLI task(s)
"""

import re
from logging import getLogger
from typing import Optional, Set, Tuple

import click
from flask.cli import AppGroup
from tabulate import tabulate

from .utils import validate_task_types, validate_graph_format
from ..config_utils import disabled_pnames
from ..plugin import BasePlugin, register_tasks
from ..plugin_exceptions import UnknownPlugin
from ..task.base import TaskType, Run
from ..task.workflow import Workflow, Builder as WorkflowBuilder
from ..resource import Resources

app_group = AppGroup("task", short_help="Show task relevant info(s).")
logger = getLogger()


@app_group.command("graph", short_help="Show graph of workflow.")
@click.option(
    "-t",
    "--task-type",
    type=click.Choice(list(TaskType.values()), case_sensitive=False),
    callback=validate_task_types,
    help="One of ETL or all.",
    multiple=True,
)
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
@click.option(
    "-f",
    "--graph-format",
    help="Formant of graph.",
    callback=validate_graph_format,
    default="dot",
)
def graph(
    task_type: Set[TaskType], ignore_upgrade_check: bool, graph_format: str
) -> None:
    """Show dependency graph or workflow - Makefile style"""
    register_tasks(BasePlugin.instances(), check_upgrade=not ignore_upgrade_check)
    workflow, _ = _workflow(task_type)
    if graph_format == "make":
        workflow.pprint()
    elif graph_format == "dot":
        dsk = workflow.dask_graph
        print("digraph G {")
        regex = re.compile(r"[:\-.]+")
        for target, s in dsk.items():
            if isinstance(s, str):
                continue

            target = re.sub(regex, "_", target)
            # dask graph format: s[0] is the task
            if isinstance(s, tuple):
                for required_target in s[1:]:
                    required_target = re.sub(regex, "_", str(required_target))
                    print(f" {required_target} -> {target};")
        print("}")


def _workflow(
    task_types: Optional[Set[TaskType]] = None,
) -> Tuple[Workflow, WorkflowBuilder]:
    plugins = BasePlugin.instances()
    wfbuilder = WorkflowBuilder(plugins=plugins)
    for plugin in plugins:
        wfbuilder.add_plugin(plugin=plugin, task_types=task_types)
    if task_types and len(task_types) != len(TaskType):
        workflow = wfbuilder.build(add_missing=True)
    else:
        workflow = wfbuilder.build()
    return workflow, wfbuilder


@app_group.command(
    "check", short_help="Check if there are any updates - what-if-analysis"
)
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
def check(ignore_upgrade_check: bool) -> None:
    """Check which task gets triggered - what-if-analysis"""
    register_tasks(BasePlugin.instances(), check_upgrade=not ignore_upgrade_check)
    workflow, wfbuilder = _workflow()
    resources = workflow.run(Run.PRETEND, workers=1, cores=1)

    table = []
    for res in resources:
        row = {}
        task = wfbuilder.task_by_rid(res.rid)
        row["task_id"] = str(task.tid)
        row["run"] = "yes" if res.dirty else "no"
        table.append(row.values())
    headers = ("Task", "Run")
    print(tabulate(table, headers=headers, tablefmt="plain"))


@app_group.command("details", short_help="Show details for task")
@click.argument("task-id", nargs=1)
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
def details(task_id: str, ignore_upgrade_check: bool) -> None:
    """Check which task gets triggered - what-if-analysis"""
    try:
        pname, task_type, _ = task_id.split("::")
        if task_type not in TaskType.values():
            raise ValueError
        if pname in disabled_pnames():
            msg = f"Cannot collect task info. Corresponding plugin '{pname}' disabled"
            logger.warning(msg)
            return
        plugins = BasePlugin.instances()
        register_tasks(plugins, check_upgrade=not ignore_upgrade_check)
        for plugin in plugins:
            task_types = {TaskType.from_str(task_type)}
            for task in plugin.task_builder.tasks(task_types):
                if task_id == str(task.tid):
                    print(f"Task:\t\t{task_id}")
                    print(f"Plugin:\t\t{plugin.name}")
                    print(f"Type:\t\t{task.type.value}")
                    print(
                        f"Offers:\t\t{','.join(str(offered.rid) for offered in task.offered)}"
                    )
                    print(
                        f"Requires:\t{','.join(str(required_rid) for required_rid in task.required_rids)}"
                    )
                    return
        logger.error("Task not found")
    except ValueError:
        logger.error(
            "Invalid task format (must be: plugin_name::(extract|transform|load)::extra)"
        )
        return


@app_group.command("run", short_help="Run task")
@click.argument("task-id", nargs=1)
@click.option(
    "-I",
    "--ignore-upgrade-check",
    help="Do not request latest version to check upgrade.",
    is_flag=True,
    default=False,
)
def run(task_id: str, ignore_upgrade_check: bool) -> None:
    """Check which task gets triggered - what-if-analysis"""
    pname = ""
    try:
        pname, task_type, _ = task_id.split("::")
        if pname in disabled_pnames():
            msg = f"Cannot run task from disabled plugin: '{pname}'"
            logger.error(msg)
            return
        if task_type not in TaskType.values():
            raise ValueError
    except ValueError:
        logger.error("Task could not be found")
    try:
        plugin = BasePlugin.get(pname)
    except UnknownPlugin as e:
        logger.error(e.msg)
        return

    register_tasks([plugin], check_upgrade=not ignore_upgrade_check)
    task = {str(task.tid): task for task in plugin.task_builder.tasks()}.get(task_id)
    if not task:
        logger.error("Unknown task")
        return

    context = Resources()
    task(context)
