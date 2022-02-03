from typing import Optional, Set, Sequence

from itertools import chain

from ..plugin import BasePlugin
from ..task.base import Task, TaskType


def get_tasks(
    plugins: Optional[Sequence[BasePlugin]] = None,
    task_types: Optional[Set[TaskType]] = None,
) -> Sequence[Task]:
    if not plugins:
        plugins = BasePlugin.instances()
    if not task_types:
        task_types = set(task_type for task_type in TaskType)
    tasks = [plugin.task_builder.tasks(task_types) for plugin in plugins]
    return list(chain.from_iterable(tasks))
