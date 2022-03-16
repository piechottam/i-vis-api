"""Manages ETL process  - checks requirements and starts tasks"""
import logging
from typing import (
    Any,
    Dict,
    Mapping,
    cast,
    MutableMapping,
    MutableSet,
    MutableSequence,
    NewType,
    Optional,
    Set,
    Sequence,
    Union,
    Tuple,
    TYPE_CHECKING,
)

from functools import cached_property
import sys
from flask import current_app
from dask.callbacks import Callback
from dask import get as dask_single_get
from dask.distributed import Client
from tqdm import tqdm

from .. import pp
from ..config_utils import disabled_pnames
from ..models import UpdateStatus
from ..resource import ResourceId, Resources, File
from .base import Task, TaskType, Run
from ..df_utils import json_io

if TYPE_CHECKING:
    from ..plugin import BasePlugin

logger = logging.getLogger()
SpecialTarget = NewType("SpecialTarget", str)
Target = str
DEFAULT_TARGET: Target = SpecialTarget("i-vis-db")

_PluginMapper = MutableMapping[str, "BasePlugin"]
_TaskMapper = MutableMapping[str, "Task"]
_TargetMapper = MutableMapping[str, Target]


class TQDMProgressBar(Callback):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            start=self.__start, pretask=self.__pretask, posttask=self.__posttask
        )
        self.tqdm = None
        self.tqdm_opts = kwargs

    def __start(self, dsk: Mapping[str, Tuple[Any]]) -> None:
        assert self.tqdm is not None
        self.tqdm = tqdm(total=len(dsk), **self.tqdm_opts, leave=False)

    # pylint: disable=unused-argument
    def __pretask(self, key: str, dsk: Mapping[str, Any], state: Any) -> None:
        assert self.tqdm is not None
        self.tqdm.set_description_str(key)

    # pylint: disable=unused-argument
    # pylint: disable=too-many-arguments
    def __posttask(
        self,
        key: str,
        result: Any,
        dsk: Mapping[str, Tuple[Any]],
        state: Any,
        worker_id: Any,
    ) -> None:
        assert self.tqdm is not None
        self.tqdm.update(1)


class Workflow:
    def __init__(
        self,
        rules: Mapping[Target, "_Rule"],
    ) -> None:
        self._rules = rules

    @cached_property
    def dask_graph(
        self,
    ) -> Dict[Target, Union[Any, Tuple[Task, Sequence[Target]]]]:
        """Dask compatible graph representation"""

        graph: Dict[Target, Union[Any, Tuple[Task, Sequence[Target]]]] = {}
        for rule in self._rules.values():
            graph.update(rule.build())
        return graph

    def pprint(self) -> None:
        """Print workflow"""
        pp.pprint(self.dask_graph)

    def run(self, run: Run, workers: int, cores: int) -> "Resources":
        """Execute workflow"""

        with TQDMProgressBar(position=0, unit="tasks"):
            graph = self.dask_graph
            graph[Builder.value_target("run")] = run

            self.check_required("i-vis-db")
            self.check_cycle("i-vis-db")

            if workers == 1:
                result = dask_single_get(graph, DEFAULT_TARGET)
            else:
                client = Client(
                    n_workers=workers,
                    threads_per_worker=cores,
                    processes=True,
                    silence_logs=False,
                )
                result = client.get(graph, DEFAULT_TARGET)
            logger.info("Finished workflow")
        return cast("Resources", result)

    def check_required(self, target: str) -> None:
        if not isinstance(self.dask_graph[target], tuple):
            return

        _, *required_targets = self.dask_graph[target]
        for required_target in required_targets:
            if required_target not in self.dask_graph:
                raise Exception(
                    f"Missing requirement: {target} requires {required_target}"
                )

    # TODO change to non-recursive
    def check_cycle(
        self, target: str, path_targets: Optional[MutableSequence[str]] = None
    ) -> None:
        path_targets = path_targets or []

        if isinstance(self.dask_graph[target], tuple):
            _, *required_targets = self.dask_graph[target]
            path_targets.append(target)
            for required_target in required_targets:
                if required_target in path_targets:
                    raise Exception(
                        f"Cycle {required_target}: {' -> '.join(path_targets)}"
                    )

                self.check_cycle(cast(str, required_target), list(path_targets))


class _Rule:
    def __init__(
        self, target: Target, task: Optional["Task"] = None, value: Optional[Any] = None
    ):
        self.target = target
        self._task = task
        self._value = value
        self._required_targets: MutableSet[Target] = set()

    @property
    def task(self) -> Optional["Task"]:
        return self._task

    @property
    def required_targets(self) -> Set[Target]:
        return set(self._required_targets)

    def add_required_target(self, target: Target) -> "_Rule":
        self._required_targets.add(target)
        return self

    def build(self) -> Mapping[Target, Union[Tuple["Task", Set[Target]], Any]]:
        if self._value:
            return {self.target: self._value}

        return {
            self.target: (self.task, *list(target for target in self.required_targets))
        }


class Builder:
    def __init__(
        self,
        plugins: Sequence["BasePlugin"],
    ) -> None:
        self.name2plugin = {plugin.name: plugin for plugin in plugins}

        self._tid2pname: MutableMapping[str, str] = {}
        self._task_mapper: _TaskMapper = {}
        self._target_mapper: _TargetMapper = {}
        self._init_mapper(list(self.name2plugin.values()))

        self._rules: MutableMapping[Target, _Rule] = {
            DEFAULT_TARGET: _Rule(DEFAULT_TARGET, task=FinishWorkflow()),
            Builder.value_target("run"): _Rule(
                Builder.value_target("run"), value=Run.PRETEND
            ),
            Builder.value_target("config"): _Rule(
                Builder.value_target("config"), value=current_app.config
            ),
        }

    @staticmethod
    def value_target(name: str) -> Target:
        return Target(f"value::{name}")

    def build(self, add_missing: bool = False) -> Workflow:
        rules = dict(self._rules)

        if add_missing:
            missing = []
            for target, rule in rules.items():
                for required_target in rule.required_targets:
                    if required_target not in rules:
                        missing.append(required_target)
            for target in missing:
                pname = self._tid2pname[target]
                plugin = self.name2plugin[pname]
                offered_res = plugin.task_builder.tid2task[target].offered
                rules[target] = _Rule(target, value=offered_res)
        return Workflow(rules)

    def _init_mapper(self, plugins: Sequence["BasePlugin"]) -> None:
        """Inits task-resource mappings"""
        for plugin in plugins:
            for task in plugin.task_builder.tasks():
                self._tid2pname[str(task.tid)] = plugin.name
                for offered in task.offered:
                    self._task_mapper[str(offered.rid)] = task
                    self._target_mapper[str(offered.rid)] = str(task.tid)

    @staticmethod
    def _add_values(rule: _Rule) -> None:
        for value in set([Builder.value_target("run"), Builder.value_target("config")]):
            rule.add_required_target(value)

    def add_plugin(
        self,
        plugin: "BasePlugin",
        task_types: Optional[Set["TaskType"]] = None,
    ) -> "Builder":
        plugin_rid = str(File.link(plugin.name, "plugin_info.json"))
        plugin_target = self._target_mapper[str(plugin_rid)]
        plugin_task = self._task_mapper[str(plugin_rid)]
        plugin_rule = self._rules.setdefault(
            plugin_target, _Rule(plugin_target, task=plugin_task)
        )
        self._add_values(plugin_rule)
        self._rules[DEFAULT_TARGET].add_required_target(plugin_target)
        for task in plugin.task_builder.tasks(task_types):
            self._add_task(task, plugin_rule)
        return self

    def _add_task(self, task: "Task", plugin_rule: _Rule) -> "Builder":
        task_target = str(task.tid)
        if task_target == plugin_rule.target:
            return self

        plugin_rule.add_required_target(task_target)
        task_rule = self._rules.setdefault(task_target, _Rule(task_target, task=task))
        self._add_values(task_rule)
        for rid in task.required_rids:
            try:
                task_rule.add_required_target(self._target_mapper[str(rid)])
            except KeyError:
                msg = (
                    f"Workflow cannot run. Task '{str(task.tid)}' requires '{str(rid)}'"
                )
                if rid.pname in disabled_pnames():
                    msg = msg + f" (Plugin '{rid.pname}' is disabled)"
                logger.error(msg)
                sys.exit(1)
        return self

    def task_by_rid(self, rid: ResourceId) -> "Task":
        return self._task_mapper[str(rid)]


class UpdatePlugin(Task):
    def __init__(self, pname: str) -> None:
        out_file = File(pname, "plugin_info.json", io=json_io)
        super().__init__(offers=[out_file])
        self.task_types = {TaskType.EXTRACT, TaskType.TRANSFORM, TaskType.LOAD}

    def post_register(self) -> None:
        for task in self.plugin.task_builder.tasks():
            for offered in task.offered:
                if offered.rid != self.out_res.rid:
                    try:
                        self.required_rids.add(offered.rid)
                    except Exception as e:
                        for task_ in self.plugin.task_builder.tasks():
                            print(task.tid)
                            task_.required_rids.pprint()
                        breakpoint()

    def _do_work(self, context: "Resources") -> None:
        plugin = self.plugin
        update = False
        if not plugin.updater.current and plugin.updater.pending:  # install
            self._install()
            update = True
        elif plugin.updater.current and plugin.updater.pending:  # upgrade
            self._upgrade()
            update = True
        elif plugin.updater.current and not plugin.updater.pending:  # fix current
            update = self._fix()

        if not update and not self.required_rids.to_resources(context).dirty:
            return

        with open(self.out_res.qname, "w", encoding="utf8") as f_out:
            f_out.write(str(plugin.version.current))
        self.out_res.dirty = True
        self.out_res.update_db()

    def requires_work(self, context: Resources) -> bool:
        return (
            len(self.required_rids) > 0
            and super().requires_work(context)
            or not self.offered.consistent()
            or not self.plugin.installed
        )

    def _install(self) -> None:
        assert self.plugin.updater.pending is not None

        if self.plugin.task_builder.offered_resources(self.task_types).consistent():
            self.plugin.updater.pending.status = UpdateStatus.INSTALLED
            self.plugin.logger.info("Installed")
        else:
            self.plugin.updater.pending.status = UpdateStatus.ONGOING
            self.plugin.logger.error("Install Failed")
        self.plugin.updater.model.current_update_id = (
            self.plugin.updater.model.pending_update_id
        )
        self.plugin.updater.model.pending_update_id = None

    def _upgrade(self) -> None:
        assert (
            self.plugin.updater.current is not None
            and self.plugin.updater.pending is not None
        )

        if self.plugin.updater.current.status == UpdateStatus.ONGOING:
            self.plugin.updater.current.status = UpdateStatus.FAILED

        if self.plugin.task_builder.offered_resources(self.task_types).consistent():
            self.plugin.logger.info("Upgraded")
            self.plugin.updater.pending.status = UpdateStatus.INSTALLED
        else:
            self.plugin.logger.error("Upgrade failed")
            self.plugin.updater.pending.status = UpdateStatus.ONGOING

        self.plugin.updater.model.current_update_id = (
            self.plugin.updater.model.pending_update_id
        )
        self.plugin.updater.model.pending_update_id = None

    def _fix(self) -> bool:
        assert self.plugin.updater.current is not None

        inconsistent = self.plugin.task_builder.offered_resources(
            self.task_types
        ).inconsistent()
        if not inconsistent:
            if (
                self.plugin.task_builder.tasks_done
                or self.plugin.updater.current.status != UpdateStatus.INSTALLED
            ):
                self.plugin.updater.current.status = UpdateStatus.INSTALLED
                self.plugin.logger.info("Fixed")
                return True
        else:
            self.plugin.updater.current.status = UpdateStatus.ONGOING
            self.plugin.logger.error(
                f"Failed - inconsistent: {','.join(str(res.rid) for res in inconsistent)}",
            )
            return True

        return False

    @property
    def type(self) -> TaskType:
        return TaskType.UPDATE


class FinishWorkflow(Task):
    def __init__(self) -> None:
        super().__init__(pname="i_vis", offers=[])

    def _do_work(self, context: Resources) -> None:
        if context.dirty:
            self.logger.info("At least one task finished without an error")
        else:
            self.logger.info("No task was executed")

    @property
    def type(self) -> "TaskType":
        return TaskType.FINISH
