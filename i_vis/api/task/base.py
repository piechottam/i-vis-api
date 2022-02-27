"""
Tasks are grouped into the following categories:
* extract,
* transform, and
* load.

At least one *load* task is required to be defined within a plugin.
Other tasks are optional.

Under each tag an arbitrary number of tasks can be added.
Just make sure to use the correct task category!
Each task should be assigned an unique ID, e.g.:

```yaml
def task_def():
  return {
    ETL.extract: {
      extract1: {...}
      extract2: {...}
    },
  }
```

Two *extract* tasks with distinct IDs have been added: *extract1* and *extract2*.

#### extract

Each *extract* requires:
* desc,
* type, and
* download.

Values under *opts* are optional and depend on *type*.
Check respective class for additional options.

```yaml
<extract_task_id>:
  desc: Retrieve data # use some descriptive message
  type: [HTTP|HTTPS|FTP|FTPS] #  available protocols to use
  download: # list of URLs to download
    - URL1
    - REMOTE_FNAME: LOCAL_FNAME # rename remove file when downloading
    - ...
    - URLn
  opts:
    user: <user>
    pass: <pass>
    port: <port>
    [...] # check the respective extract class for additional opt(ion)s
```

To rename a remote file reference by some url `URL1` after downloading to `LOCAL_FNAME`,
change the item under *download* from `URL1` to `URL1: LOCAL_FNAME`.

### *dep*

The *dep* tag defines the dependency structure for the plugin.
The implicit dependency structure that comes from the tasks defined under *task* is defined as is depicted in the
following Makefile where the recipe part has been omitted:
"""

from typing import (
    cast,
    Optional,
    Sequence,
    MutableSet,
    TYPE_CHECKING,
    Any,
    Tuple,
)
from datetime import datetime, timedelta
from enum import Enum
from logging import getLogger, LoggerAdapter
from functools import cache, cached_property

from types import SimpleNamespace

from flask import current_app, Flask
from flask.config import Config
from dask.distributed import get_client, get_worker

from i_vis.core.utils import EnumMixin
from i_vis.core.config import variable_name

from ..resource import (
    is_obsolete,
    Resource,
    ResourceId,
    ResourceIds,
    Resources,
    MissingResourceError,
    File,
    Parquet,
    Table,
)
from ..config_utils import get_config

if TYPE_CHECKING:
    from ..plugin import BasePlugin

logger = getLogger("i_vis.api")


class Run(EnumMixin, Enum):
    DEFAULT = "default"
    PRETEND = "pretend"
    UNCONDITIONAL = "unconditional"
    FORCE = "force"


class TaskType(EnumMixin, Enum):
    """Processes of ETL to classify tasks."""

    FINISH = "finish"
    UPDATE = "update-plugin"

    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"


class Status(EnumMixin, Enum):
    INIT = "init"
    WORKING = "working"
    ERROR = "error"
    DONE = "done"


class TaskId:
    def __init__(
        self, ttype: TaskType, pname: str, tname: str, offers: Sequence[ResourceId]
    ) -> None:
        self.type = ttype
        self.pname = pname
        self.tname = tname
        self.tid = to_tid(ttype, pname, tname, offers)

    def __hash__(self) -> int:
        return hash(self.tid)

    def __str__(self) -> str:
        return self.tid


def to_tid(
    ttype: TaskType, pname: str, tname: str, offers: Sequence[ResourceId]
) -> str:
    """Create a qualified resource id (resid)
    args:
        ttype (Type): task type
        pname (str): plugin name
        tname (String): task name
    returns:
        task identifier (String)
    """
    _ = ttype
    names = ",".join(rid.name for rid in offers)
    ids = [pname, tname]
    return "::".join([id_ for id_ in ids if id_]).replace(" ", "_") + "->" + names


class Task:
    """Captures a task in I-VIS"""

    # unique tid for tasks
    _task_ids: MutableSet[TaskId] = set()
    # dask + flask workaround
    _app: Optional[Flask] = None

    @classmethod
    def _register(cls, tid: TaskId, _register: bool) -> TaskId:
        if tid in cls._task_ids:
            raise ValueError("Duplicate task id")

        if _register:
            cls._task_ids.add(tid)
        return tid

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        offers: Sequence[Resource],
        requires: Optional[Sequence[ResourceId]] = None,
        name_args: Optional[Sequence[str]] = None,
        _register: bool = True,
        pname: str = "",
    ) -> None:
        if offers:
            pnames = set(offered.rid.pname for offered in offers)
            if pname:
                pnames.add(pname)
            if len(pnames) != 1:
                raise ValueError(
                    f"There must be exactly one plugin reference in the output: '"
                    f"{','.join(list(pnames))}'"
                )

        if not pname:
            pname = self.extract_pname(offers, requires)
        name = type(self).__name__
        if name_args:
            name += ":" + ",".join(name_args)
        self.tid = Task._register(
            TaskId(self.type, pname, name, [offered.rid for offered in offers]),
            _register,
        )

        if not requires:
            requires = []

        # what does the task offer, once it finishes
        # what does the task require to start working
        self._resources = SimpleNamespace(
            offered=Resources(offers),
            required_rids=ResourceIds(requires),
        )

        self.status = Status.INIT
        self._time = SimpleNamespace(started=0.0, finished=-1.0)

        self.logger = LoggerAdapter(getLogger("i_vis.api"), {"detail": self.tid})
        self._config = current_app.config

    def valid(self) -> None:
        pass

    @staticmethod
    def extract_pname(
        offers: Sequence[Resource],
        requires: Optional[Sequence[ResourceId]] = None,
        **kwargs: Any,
    ) -> str:
        if "pname" in kwargs:
            return cast(str, kwargs["pname"])

        if offers:
            res = offers[0]
            return res.rid.pname

        if requires:
            rid = requires[0]
            return rid.pname

        raise ValueError("Missing plugin")

    @property
    def plugin(self) -> "BasePlugin":
        from ..plugin import BasePlugin

        return BasePlugin.get(self.tid.pname)

    def post_register(self) -> None:
        """Executed, after all tasks have been registered."""

    def _do_work(self, context: Resources) -> None:
        raise NotImplementedError(self.tid)

    @staticmethod
    def _variables(*args: Any) -> Tuple["Resources", "Run", "Config"]:
        context = Resources()
        run = Run.PRETEND
        config: "Config" = Config("")
        if args:
            for arg in args:
                if isinstance(arg, Config):
                    config = arg
                elif isinstance(arg, Resources):
                    context.update_all(arg)
                elif isinstance(arg, Run):
                    run = arg
                else:
                    print(f"Unknown arg: {type(arg)}")
        return context, run, config

    @staticmethod
    def _restore_app(config: "Config") -> None:
        if Task._app:
            return

        from .. import db

        if Task._app:
            return

        Task._app = Flask(__name__)
        Task._app.config.from_mapping(config)
        db.init_app(Task._app)

        with Task._app.app_context():
            from ..plugin import BasePlugin, CoreType, DataSource

            # register core and plugins - order important!
            CoreType.import_plugins()
            DataSource.import_plugins()

            # register tasks
            for plugin in BasePlugin.instances():
                plugin.register_tasks()

    def _evaluate(self, run: Run, context: Resources) -> None:
        if run in [Run.FORCE, Run.UNCONDITIONAL] or self.requires_work(context):
            self._start()
            if run is not Run.PRETEND:
                self._do_work(context)
            else:
                for offered in self.offered:
                    offered.dirty = True
            self._finish()

    @property
    def running_dask(self) -> bool:
        # check if we are running in distributed mode
        try:
            _ = get_client()
            _ = get_worker()
            return True
        except ValueError:
            return False

    def __call__(self, *args: Any, **kwargs: Any) -> Resources:
        # lock resources
        self._resources.required_rids.close()

        context, run, config = self._variables(*args)

        if self.running_dask:
            self._restore_app(config)
            app = Task._app
            assert app is not None
        else:
            app = current_app

        with app.app_context():
            try:
                self._evaluate(run, context)
            except MissingResourceError as e:
                self.set_error(e)
                self.logger.exception(e)
            except Exception as general_e:  # pylint: disable=broad-except
                self.set_error(general_e)
                self.logger.error("Unrecoverable error - ETL stopped")
            finally:
                from .. import db

                db.session.flush()
                SystemExit()

        return Resources(self.offered)

    @property
    def type(self) -> "TaskType":
        """Type of task: Extract, Transform, Load, ..."""
        raise NotImplementedError

    @property
    def done(self) -> bool:
        return self.status == Status.DONE

    @property
    def offered(self) -> Resources:
        return cast(Resources, self._resources.offered)

    @property
    def required_rids(self) -> ResourceIds:
        return cast(ResourceIds, self._resources.required_rids)

    def _start(self) -> None:
        self.logger.debug("Started")
        self._time.started = datetime.now().timestamp()
        self.status = Status.WORKING

    def set_error(self, e: Optional["Exception"] = None) -> None:
        if e:
            self.logger.exception(e)
        self.status = Status.ERROR

    @cache
    def required_dirty(self, context: Resources) -> ResourceIds:
        dirty = ResourceIds()
        for rid in self._resources.required_rids:
            if context[rid].dirty:
                dirty.add(rid)
        return dirty

    @cache
    def offered_obsolete(self, context: Resources) -> Resources:
        obsolete = Resources()
        for required_rid in self.required_rids:
            for offered in self.offered:
                if is_obsolete(context[required_rid], offered, self.logger):
                    obsolete.add(offered)
        return obsolete

    @cached_property
    def offered_insonsistent(self) -> ResourceIds:
        rids = ResourceIds()
        for offered in self._resources.offered:
            if (
                get_config().get(variable_name("TRUST_LOCAL"), False)
                and offered.exists()
            ):
                self.logger.info(f"Trusting local {offered.in_rid}")
                offered.update_db()
            if offered.inconsistent:
                rids.add(offered.in_rid)
        return rids

    def requires_work(self, context: Resources) -> bool:
        self.logger.info("Checking if work is required...")

        # do work if requirements have changed or
        # if there are no requirements defined

        for required_rid in self._resources.required_rids:
            try:
                res = context[required_rid]
                if res.dirty:
                    self.logger.debug(
                        f"Rebuild because {res.type} '{res.name}' is dirty"
                    )
                    return True
            except MissingResourceError as e:
                self.logger.exception(e)
                raise e

        for offered in self.offered:
            if (
                get_config().get(variable_name("TRUST_LOCAL"), False)
                and offered.exists()
            ):
                self.logger.info(f"Trusting local {offered.rid}")
                offered.update_db()
            if offered.inconsistent:
                self.logger.debug(f"Rebuild because {offered.rid} is inconsistent")
                return True

        # dirty res vs. obsolete:
        # dirty: set internally by i_vis (to simulate obsolete)
        # obsolete: determined externally
        for required_rid in self.required_rids:
            for offered in self.offered:
                if is_obsolete(context[required_rid], offered, self.logger):
                    self.logger.debug(
                        f"Rebuild {offered.rid} because {required_rid} changed"
                    )
                    return True

        self.logger.debug("Does not require work")
        return False

    def _finish(self) -> None:
        self._time.finished = datetime.now().timestamp()
        self.status = Status.DONE
        elapsed_sec = abs(int(self.elapsed))
        duration = timedelta(seconds=elapsed_sec)
        self.logger.info("Finished after %s", str(duration))

    @property
    def elapsed(self) -> float:
        """Running time of task.

        Returns:
            Running time of task or -1.0 it task did not start yet.
        """

        return float(self._time.finished) - float(self._time.started)

    def __hash__(self) -> int:
        return hash(self.tid)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Task):
            return False

        return self.tid == other.tid

    @property
    def pname(self) -> str:
        """Plugin this task belongs to."""

        return self.tid.pname

    @property
    def in_rid(self) -> ResourceId:
        if len(self.required_rids) == 1:
            return next(self.required_rids.__iter__())

        rid = getattr(self, "_in_rid")
        if rid:
            return cast(ResourceId, rid)

        raise AttributeError

    @property
    def out_res(self) -> Resource:
        if len(self.offered) == 1:
            return next(self.offered.__iter__())

        res = getattr(self, "_out_res")
        if res:
            return cast(Resource, res)

        raise AttributeError

    @property
    def out_file(self) -> File:
        file = self.out_res
        if not isinstance(file, File):
            raise TypeError

        return file

    @property
    def out_parquet(self) -> Parquet:
        parquet = self.out_res
        if not isinstance(parquet, Parquet):
            raise TypeError

        return parquet

    @property
    def out_table(self) -> Table:
        table = self.out_res
        if not isinstance(table, Table):
            raise TypeError

        return table
