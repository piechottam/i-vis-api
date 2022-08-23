import os
from abc import ABC
from builtins import staticmethod
from itertools import chain
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Iterable,
    Iterator,
    MutableMapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import pandas as pd
from sqlalchemy import inspect
from tabulate import tabulate
from tqdm import tqdm

from . import Base, engine

if TYPE_CHECKING:
    from .db_utils import ResDescMixin
    from .df_utils import AnyDataFrame, DataFrameIO
    from .models import ResourceUpdate
    from .plugin import BasePlugin
    from .utils import ivis_logger


ResourceType = str


def resource_types() -> Sequence[ResourceType]:
    return tuple(cls.__name__ for cls in Resource.__subclasses__())


class MissingResourceError(Exception):
    def __init__(self, rid: "ResourceId") -> None:
        Exception.__init__(self)
        self.rid = rid

    def __str__(self) -> str:
        return self.message.format(rid=str(self.rid))

    @property
    def message(self) -> str:
        return "Resource does not exist: '{rid}'"

    @property
    def msg(self) -> str:
        return "Resource does not exist"


class MissingPluginUpdateError(Exception):
    pass


class ResourceId:
    def __init__(self, rtype: ResourceType, pname: str, name: str) -> None:
        self.rtype = rtype
        self.pname = pname
        self.name = name
        self.__rid = create_rid(self.rtype, self.pname, self.name)

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return self.__rid

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, ResourceId) and str(self) == str(other)

    @staticmethod
    def from_str(s: str) -> "ResourceId":
        rtype, pname, name = s.split("::")
        if rtype not in resource_types():
            raise ValueError(f"Unknown resource type: '{rtype}'")
        return ResourceId(rtype, pname, name)

    def exists(self) -> bool:
        return self in res_registry

    def get(self) -> "Resource":
        return res_registry[self]


def create_rid(rtype: ResourceType, pname: str, name: str) -> str:
    """Create a qualified resource id (resid)
    args:
        rtype (ResType): resource type
        pname (str): plugin name
        name (String): resource name
    returns:
        resource identifier (String)
    """

    ids = [rtype, pname, name]
    return "::".join([id_ for id_ in ids if id_])


class ResourceBuilder:
    def __init__(self, pname: str) -> None:
        self.pname = pname

    def file(
        self,
        fname: str,
        io: Optional["DataFrameIO"] = None,
        desc: Optional["ResourceDesc"] = None,
    ) -> "File":
        return File(pname=self.pname, fname=fname, io=io, desc=desc)

    def parquet(
        self,
        path: str,
        io: "DataFrameIO",
        desc: Optional["ResourceDesc"] = None,
    ) -> "Parquet":
        return Parquet(pname=self.pname, path=path, io=io, desc=desc)

    def table(
        self,
        tname: str,
        io: Optional["DataFrameIO"] = None,
        desc: Optional["ResourceDesc"] = None,
    ) -> "Table":
        return Table(pname=self.pname, tname=tname, io=io, desc=desc)

    def table_from_model(
        self,
        model: Type["ResDescMixin"],
        io: Optional["DataFrameIO"] = None,
    ) -> "Table":
        return self.table(
            tname=inspect(model).local_table.name,
            io=io,
            desc=model.get_res_desc(),
        )


T = TypeVar("T")


class Container(Generic[T]):
    def __init__(self, items: Optional[Iterable[T]] = None):
        self._closed = False
        self._items: MutableMapping[ResourceId, T] = {}
        if items:
            self.add_all(items)

    def clear(self) -> None:
        self._closed = False
        self._items.clear()

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True

    def add(self, item: T, check: bool = True) -> T:
        if self.closed:
            raise Exception("Container closed")

        key = self._rid(item)
        if check and key in self._items:
            raise KeyError(f"Duplicate: '{str(key)}'")

        self._items[key] = item
        return item

    def update(self, item: T) -> T:
        return self.add(item, False)

    def add_all(self, items: Iterable[T], check: bool = False) -> None:
        for item in items:
            self.add(item, check)

    def update_all(self, items: Iterable[T]) -> None:
        self.add_all(items, False)

    def __iter__(self) -> Iterator[T]:
        """Iterator over items"""

        return iter(self._items.values())

    def __len__(self) -> int:
        """Number of items"""
        return len(self._items)

    def __contains__(self, other: Any) -> bool:
        return other in self._items

    def __getitem__(self, rid: ResourceId) -> T:
        try:
            return self._items[rid]
        except KeyError as e:
            raise MissingResourceError(rid) from e

    def _rid(self, item: T) -> ResourceId:
        raise NotImplementedError

    def pprint(self) -> None:
        for item in self:
            print(str(item))


class ResourceIds(Container[ResourceId]):
    def _rid(self, item: ResourceId) -> ResourceId:
        return item

    def to_resources(self, context: Optional["Resources"] = None) -> "Resources":
        if context is not None:
            return Resources([context[rid] for rid in self])

        return Resources([rid.get() for rid in self])


class ResourceDesc:
    def __init__(self, cols: Sequence[str]):
        self._cols = cols

    @property
    def cols(self) -> Sequence[str]:
        return list(self._cols)

    def unmatched_res(self, res: "Resource") -> Tuple[Sequence[str], Sequence[str]]:
        df = res.read()
        return self.unmatched_df(df)

    def check_res(self, res: "Resource") -> bool:
        unmatched_in_ann, unmatched_in_res = self.unmatched_res(res)
        return not unmatched_in_ann and not unmatched_in_res

    def check_df(self, df: "AnyDataFrame") -> bool:
        unmatched_in_ann, unmatched_in_df = self.unmatched_df(df)
        return not unmatched_in_ann and not unmatched_in_df

    def unmatched_df(self, df: "AnyDataFrame") -> Tuple[Sequence[str], Sequence[str]]:
        df_cols = set(df.columns.to_list())
        ann_cols = set(self._cols)

        return (
            tuple(str(v) for v in ann_cols - df_cols),
            tuple(str(v) for v in df_cols - ann_cols),
        )

    @staticmethod
    def from_model(model: "Base") -> "ResourceDesc":
        return ResourceDesc(
            cols=[column.name for column in getattr(model, "__table__").c],
        )


class Resource(ABC):
    """File or Table provided by plugins"""

    def __init__(
        self,
        pname: str,
        name: str,
        io: Optional["DataFrameIO"] = None,
        desc: Optional["ResourceDesc"] = None,
        _register: bool = True,
    ):
        for key, value in zip(["plugin", "name"], [pname, name]):
            if not value:
                raise ValueError(f"'{key}' must have a value")

        self.rid = ResourceId(self.get_type(), pname, name)
        self.io = io
        self.desc = desc

        self.dirty = False

        if _register:
            res_registry.add(self)

    def read(self, **kwargs: Any) -> Union["AnyDataFrame", Iterator[pd.DataFrame]]:
        """Read resource to DataFrame"""

        if not self.exists():
            raise MissingResourceError(self.rid)

        assert self.io is not None
        return self.io.read(self, **kwargs)

    def read_full(self) -> pd.DataFrame:
        df = self.read()
        assert self.io is not None
        return self.io.to_full(df)

    def save(self, df: "AnyDataFrame", **kwargs: Any) -> None:
        assert self.io is not None
        self.io.write(self, df, **kwargs)

    def pprint(self) -> str:
        return str(self.rid)

    @property
    def empty(self) -> bool:
        return not self.exists()

    def exists(self) -> bool:
        raise NotImplementedError(str(self.rid))

    @classmethod
    def get_type(cls) -> ResourceType:
        return ResourceType(cls.__name__)

    @classmethod
    def link(cls, pname: str, name: str) -> ResourceId:
        return ResourceId(cls.get_type(), pname, name)

    @property
    def plugin(self) -> "BasePlugin":
        from .plugin import BasePlugin

        return BasePlugin.get(self.rid.pname)

    @property
    def working_update(self) -> Optional["ResourceUpdate"]:
        plugin_update = self.plugin.updater.working
        if plugin_update is None:
            raise MissingPluginUpdateError(self.plugin.name)

        return plugin_update.resource_updates_by_type(self.get_type()).get(self.name)

    @property
    def consistent(self) -> bool:
        return not self.inconsistent

    @property
    def inconsistent(self) -> bool:
        self.plugin.logger.info(
            f"Checking Consistency of {self.get_type()}: '{self.name}'..."
        )
        return (
            not self.exists()
            or self.working_update is None
            or not self.working_update.check()[0]
        )

    def update_db(self, **kwargs: Any) -> "ResourceUpdate":
        plugin_update = self.plugin.updater.working
        assert plugin_update is not None
        return plugin_update.add(resource=self, **kwargs)

    @property
    def name(self) -> str:
        """Table or file name"""
        return self.rid.name

    @property
    def qname(self) -> str:
        raise NotImplementedError


def qname(res: Resource) -> str:
    assert res.plugin.updater.working is not None
    plugin_update = res.plugin.updater.working
    return os.path.join(res.plugin.dir.by_update(plugin_update), res.name)


class File(Resource):
    """File resource representation"""

    def __init__(
        self,
        pname: str,
        fname: str,
        io: Optional["DataFrameIO"] = None,
        desc: Optional["ResourceDesc"] = None,
    ) -> None:
        super().__init__(
            pname=pname,
            name=fname,
            io=io,
            desc=desc,
        )

    def exists(self) -> bool:
        return os.path.exists(self.qname)

    @property
    def qname(self) -> str:
        """Qualified name of file"""

        return qname(self)


class Parquet(Resource):
    def __init__(
        self,
        pname: str,
        path: str,
        io: "DataFrameIO",
        desc: Optional["ResourceDesc"] = None,
    ) -> None:
        super().__init__(
            pname=pname,
            name=path,
            io=io,
            desc=desc,
        )

    @staticmethod
    def format_fname(fname: str) -> str:
        if fname.endswith(".parquet"):
            return fname

        return fname + ".parquet"

    def exists(self) -> bool:
        return os.path.exists(self.qname)

    @property
    def qname(self) -> str:
        """Qualified name of parquet file"""

        return qname(self)


class Table(Resource):
    """Table resource representation"""

    def __init__(
        self,
        pname: str,
        tname: str,
        io: Optional["DataFrameIO"] = None,
        desc: Optional[ResourceDesc] = None,
    ) -> None:
        if not io:
            from .df_utils import PandasDataFrameIO

            opts: MutableMapping[str, Any] = {}
            opts.setdefault("con", engine)
            io = PandasDataFrameIO(read_callback="read_sql_table", read_opts=opts)
        super().__init__(pname=pname, name=tname, io=io, desc=desc)

    def exists(self) -> bool:
        with engine.connect() as conn:
            return engine.dialect.has_table(conn, getattr(self.model, "__tablename__"))

    @property
    def model(self) -> Base:
        for mapper in Base.registry.mappers:
            if mapper.class_.__tablename__ == self.name:
                return cast(Base, mapper.class_)

        raise ValueError()

    @property
    def qname(self) -> str:
        return self.name

    @property
    def tname(self) -> str:
        return self.name

    def save(self, df: "AnyDataFrame", **kwargs: Any) -> None:
        raise NotImplementedError


class Resources(Container[Resource]):
    """Container for resources"""

    @property
    def dirty(self) -> bool:
        """Any resources changed?"""
        for res in self:
            if res.dirty:
                return True
        return False

    def filter_dirty(self) -> "Resources":
        """Get resources that changed"""
        return Resources([res for res in self if res.dirty])

    def get(self, rtype: ResourceType) -> "Resources":
        return Resources(res for res in self if res.get_type() == rtype)

    def table(self, rid: ResourceId) -> "Table":
        res = self[rid]
        assert res.get_type() == Table.get_type()
        return cast("Table", res)

    def file(self, rid: ResourceId) -> "File":
        res = self[rid]
        assert res.get_type() == File.get_type()
        return cast("File", res)

    def parquet(self, rid: ResourceId) -> "Parquet":
        res = self[rid]
        assert res.get_type() == Parquet.get_type()
        return cast("Parquet", res)

    def set_dirty(self) -> None:
        """Mark all resources as changed"""

        for res in self:
            res.dirty = True

    def consistent(self, **kwargs: Any) -> bool:
        """Check if data and DB consistency"""
        return not self.inconsistent(**kwargs)

    def inconsistent(self, **kwargs: Any) -> Sequence[Resource]:
        if kwargs:
            resources = tqdm(self, **kwargs)
        else:
            resources = self
        return [res for res in resources if res.inconsistent]

    def _rid(self, item: Resource) -> ResourceId:
        return item.rid

    def pprint(self) -> None:
        table = []
        for res in self:
            row = {
                "rid": str(res.rid),
                "name": res.name,
                "qname": res.qname,
            }

            if res.exists():
                actual_status = "present"
            else:
                actual_status = "missing"

            try:
                if res.working_update is not None:
                    db_status = "present"
                    if db_status == actual_status:
                        if res.working_update.check()[0]:
                            db_status = "ok"
                            actual_status = "ok"
                        else:
                            db_status = "inconsistent"
                            actual_status = "inconsistent"
                else:
                    db_status = "missing"
            except MissingPluginUpdateError:
                db_status = "missing"

            row["db"] = db_status
            row["actual"] = actual_status
            row["dirty"] = "yes" if res.dirty else "no"
            table.append(row.values())
        headers = ["ResId", "name", "qname", "db", "actual", "dirty"]
        print(tabulate(table, headers=headers, tablefmt="plain"))

    def get_table(self, tname: str) -> Optional["Table"]:
        for tbl_res in self.get(Table.get_type()):
            if tbl_res.model.__tablename__ == tname:  # type: ignore
                return cast(Table, tbl_res)

        return None


res_registry: "Resources" = Resources()


def merge(*resources: Resources) -> Resources:
    new_resources = {res.rid: res for res in chain.from_iterable(resources)}
    return Resources(iter(new_resources.values()))


def is_obsolete(
    pre: Resource,
    target: Resource,
    logger: Optional["ivis_logger"] = None,
) -> bool:
    if logger is None:
        logger = getLogger()

    if not target.working_update:
        logger.debug("'target.working_update' does not exist")
        return True
    target_datetime = target.working_update.updated_at
    if not target_datetime:
        logger.debug("'target_datetime' does not exist")
        return True

    assert pre.working_update is not None
    pre_datetime = pre.working_update.updated_at
    assert pre_datetime is not None
    obs = pre_datetime > target_datetime
    if obs:
        logger.debug("'pre_datetime' > 'target_datetime'")
    return obs
