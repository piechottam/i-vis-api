"""Utilities"""
import datetime
from typing import (
    cast,
    Any,
    Callable,
    Iterable,
    Iterator,
    Optional,
    Tuple,
    Type,
    MutableSequence,
    MutableMapping,
    Mapping,
    Union,
    TYPE_CHECKING,
)
from itertools import islice
from functools import cached_property
import logging

from flask import request
from distributed import worker

from .config_utils import get_config

if TYPE_CHECKING:
    from . import db
    from .task.base import TaskId
    from logging import Logger, LoggerAdapter

API_URL_PREFIX = "/api"

# container for models marked for backup
backup_registry: MutableSequence[Any] = []
I_VIS_Logger = Union["Logger", "LoggerAdapter"]  # pylint: disable=invalid-name


def to_str(obj: Any, sep: str = ";") -> str:
    if isinstance(obj, (list, tuple)):
        return sep.join(obj)

    return str(obj)


# pylint: disable=C0103
def getLogger(obj: Optional[Any] = None) -> I_VIS_Logger:
    if obj:
        logger = getattr(obj, "logger")
        if logger:
            return cast(I_VIS_Logger, logger)

    # TODO dask
    try:
        _ = worker.get_worker()
    except ValueError:
        return cast(logging.Logger, worker.logger)

    return logging.getLogger()


def clean_query_args(query_args: Mapping[str, Any]) -> MutableMapping[str, Any]:
    new_query_args = {
        key: value for key, value in request.args.items() if key in query_args
    }
    new_query_args.pop("token", None)
    return new_query_args


class BaseUrl:
    def __init__(
        self,
        callback: Callable[[], str],
        static: bool = True,
        latest: bool = False,
        args: Union[None, Callable[[], Mapping[str, Any]], Mapping[str, Any]] = None,
    ):
        self._callback = callback
        self.static = static
        self.version_specific = not latest
        self._args = args

    def __str__(self) -> str:
        return self.value

    @cached_property
    def value(self) -> str:
        return self._callback()

    @cached_property
    def args(self) -> Mapping[str, Any]:
        if callable(self._args):
            return self._args()

        return self._args if self._args else {}


class DefaultUrl(BaseUrl):
    def __init__(self, url: str) -> None:
        super().__init__(callback=lambda: url)


class DynamicUrl(BaseUrl):
    def __init__(self, callback: Callable[[], str], **kwargs: Any) -> None:
        super().__init__(callback, static=False, **kwargs)


class VariableUrl(BaseUrl):
    def __init__(self, var_name: str, **kwargs: Any) -> None:
        super().__init__(callback=lambda: str(get_config().get(var_name)), **kwargs)


def register_backup(model: Type["db.Model"]) -> Type["db.Model"]:
    """Mark models for backup.

    Args:
        model:

    Returns:
        Unmodified model for backup.
    """

    backup_registry.append(model)
    return model


def tqdm_desc(tid: "TaskId", time: Optional[datetime.datetime] = None) -> str:
    if time is None:
        time = datetime.datetime.now()
    date_time = time.strftime("%d/%m/%y %H:%M:%S")
    return f"{date_time} [i_vis.api: {tid}]"


def short_desc(*strs: str, sep: str = " ", width: int = 20) -> str:
    desc = sep.join(strs)
    desc = (desc[: (width - 3)] + "...") if len(desc) > width else desc
    return desc.ljust(width)


# from https://stackoverflow.com/questions/24527006/split-a-generator-into-chunks-without-pre-walking-it
def chunker(it: Iterable[Any], n: int) -> Iterator[Any]:
    def helper(helper_it: Iterable[Any]) -> Callable[[], Tuple[Any, ...]]:
        return lambda: tuple(islice(helper_it, n))

    return iter(helper(iter(it)), ())


def join(*p: str, sep: str = "/") -> str:
    return f"{sep}{sep.join(list(p))}"


def api_prefix(
    *p: str, api_url_prefix: str = API_URL_PREFIX, api_version: Optional[str] = None
) -> str:
    if api_version is None:
        from . import api_spec

        api_version = api_spec.spec.version

    return api_url_prefix + join(api_version, *p)


def to_uri(s: str) -> str:
    return s.replace("_", "-")


def from_uri(s: str) -> str:
    return s.replace("-", "_")
