from copy import copy, deepcopy
from functools import cached_property, lru_cache
from inspect import isclass
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    ItemsView,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)

import pandas as pd
from inflection import underscore
from marshmallow.fields import Field
from pandas import DataFrame
from sqlalchemy import Column, ForeignKey, Text
from sqlalchemy.orm import backref, relationship

from i_vis.core.config import get_ivis
from i_vis.core.file_utils import url2fname
from i_vis.core.utils import class_name

from . import Base, df_utils
from .config_utils import CHUNKSIZE
from .db_utils import (
    RAW_DATA_PK,
    HarmonizedData,
    HarmonizedDataMixin,
    RawData,
    RawDataMixin,
    get_part,
    harmonized_tname,
    raw_tname,
)
from .df_utils import RAW_DATA_FK, DataFrameIO
from .plugin import BasePlugin, CoreType, UpdateManager
from .resource import Parquet
from .terms import Term, TermType, TermTypes
from .utils import BaseUrl

if TYPE_CHECKING:
    from .resource import ResourceId
    from .task.base import Task

IvisDtype = Union[str, type]
ColumnModifier = Union[
    Mapping[str, Any],
    Callable[[pd.Series], pd.Series],
    Tuple[Callable[[pd.Series], pd.Series], IvisDtype],
]
# DataFrameModifier = Callable[[DataFrame, Union[str, Tuple[str, ...]]], DataFrame]
DataFrameModifier = Union[
    Callable[[DataFrame], DataFrame], Callable[[DataFrame, str], DataFrame]
]
Formatter = Callable[[Any], str]


class MissingResourceDesc(Exception):
    def __init__(self, rid: "ResourceId") -> None:
        super().__init__(f"Missing {rid}")
        self.rid = rid


class IllegalColumnError(Exception):
    pass


class UnknownOptionError(Exception):
    def __init__(self, option: str) -> None:
        super().__init__(f"Unknown option {option}")
        self.option = option


class Container:
    """container for etl"""

    def __init__(self) -> None:
        self._part2etl: MutableMapping[str, "ETL"] = {}
        self._pname2etl: MutableMapping[str, MutableSequence["ETL"]] = {}

    def clear(self) -> None:
        """

        Returns:

        """
        self._part2etl.clear()
        self._pname2etl.clear()

    def register(self, etl: "ETL") -> None:
        """Register an ETL

        Args:
            etl:

        Returns:

        """

        self._part2etl[etl.part] = etl
        self._pname2etl.setdefault(etl.pname, []).append(etl)

    def by_pname(self, pname: str) -> Sequence["ETL"]:
        """Get ETL by plugin name.

        Args:
            pname: Plugin name.

        Returns:
            Sequence of ETLs associated with ``pname``.
        """

        return self._pname2etl.get(pname, [])

    def by_part(self, part: str) -> Optional["ETL"]:
        """Get ETL by part.

        Returns:

        """

        return self._part2etl.get(part, None)

    def by_core_type(self, core_type: "CoreType") -> Sequence["ETL"]:
        """ETLs by core type

        Args:
            core_type:

        Returns:

        """

        return tuple(
            etl for etl in self._part2etl.values() if core_type in etl.core_types
        )

    def by_core_types(
        self, *core_types: "CoreType", strict: bool = False
    ) -> Sequence["ETL"]:
        etls = []
        for etl in self._part2etl.values():
            intersected = set(core_types).intersection(etl.core_types)
            if intersected:
                if strict:
                    if len(intersected) == len(core_types):
                        etls.append(etl)
                else:
                    etls.append(etl)
        return etls


etl_registry: Container = Container()


def extract_columns(opts: type, error: bool = False) -> "ColumnContainer":
    """Extract columns from a opts class"""
    name2column: MutableMapping[str, BaseColumn] = {}
    for name, value in vars(opts).items():
        if isinstance(value, BaseColumn):
            name2column[name] = value
        elif error:
            raise IllegalColumnError(f"Unknown option: {name}")
    return ColumnContainer(name2column)


class RawOpts:
    """Stores options for raw columns"""

    def __init__(self, opts: type) -> None:
        self.raw_columns = extract_columns(opts)


# options supported by extract
EXTRACT_OPTS_ATTRS = (
    "url",
    # "info",
    "add_id",
    "rid",
    "out_fname",
    "unpack",
    "io",
    "Raw",
)


def is_opts(name: str) -> bool:
    """Check if an option.

    Args:
        name: String to check.

    Returns:
        True if name is an option, False otherwise.
    """

    return cast(bool, name and not name.startswith("_"))


# pylint: disable=too-many-instance-attributes
class ExtractOpts:
    """Stores options for extract"""

    def __init__(self, opts: type) -> None:
        # check only supported options are present
        for attr in vars(opts):
            if is_opts(attr) and attr not in EXTRACT_OPTS_ATTRS:
                raise UnknownOptionError(attr)

        self.url: Optional[BaseUrl] = getattr(opts, "url", None)

        # save under fname
        self._out_fname = str(getattr(opts, "out_fname", ""))

        # or use existing i-vis resource
        self.rid: Optional["ResourceId"] = getattr(opts, "rid", None)

        # if not self.rid and not self.url and not self.info:
        #    raise Exception("Neither 'url', 'info', or 'rid' are set")

        if not self.rid and not self.url:
            raise Exception("Neither 'url' nor 'rid' are set.")

        if self.rid and self.url:
            raise Exception("'url' and 'rid' cannot be set at the same time.")

        # unpack?
        self.unpack: Optional[str] = getattr(opts, "unpack", None)

        # how to read as data frame
        self.io: Optional[DataFrameIO] = getattr(opts, "io", None)

        self.raw_columns = None
        if hasattr(opts, "Raw"):
            self.raw_columns = extract_columns(getattr(opts, "Raw"))

        self._add_id = getattr(opts, "add_id", None)

    @cached_property
    def add_id(self) -> Optional[Mapping[str, Any]]:
        if self._add_id is None:
            return None

        add_id_opts = {
            "col": RAW_DATA_PK,
            "read_opts": {"chunksize": get_ivis("CHUNKSIZE", CHUNKSIZE)},
        }
        if isinstance(self._add_id, bool):
            pass
        elif isinstance(self._add_id, Mapping):
            for key, value in self._add_id.items():
                if key == "col":
                    add_id_opts.setdefault("col", value)
                else:
                    read_opts = cast(
                        MutableMapping[str, Any],
                        add_id_opts.setdefault("read_opts", {}),
                    )
                    read_opts[key] = value
        else:
            raise ValueError

        return add_id_opts

    @cached_property
    def out_fname(self) -> str:
        if self._out_fname:
            return self._out_fname

        if self.url:
            url = self.get_url()
            return url2fname(url)

        raise AttributeError("'out_fname' does not exits.")

    def get_url(self) -> str:
        if not self.url:
            raise AttributeError("Missing 'url'.")

        return str(self.url)


# options supported by transform
TRANSFORM_OPTS_ATTRS = (
    "task",
    "Raw",
    "io",
)


class TransformOpts:
    """Stores options for transform."""

    def __init__(self, opts: type) -> None:
        self.core_type2harm_modifier: MutableMapping[
            "CoreType", "HarmonizerModifier"
        ] = {}
        names = set()
        for core_type in CoreType.instances():
            try:
                _ = core_type.short_name
            except AttributeError:
                pass

            name = core_type.short_name.replace("-", "_")
            if not hasattr(opts, name):
                continue

            modifier = getattr(opts, name)
            if modifier:
                self.core_type2harm_modifier[core_type] = modifier
                names.add(name)

        # check only supported options are present
        short_names = [
            CoreType.get(pname).short_name
            for pname in CoreType.pnames(include_disabled=True)
        ]
        for attr in vars(opts):
            if (
                not attr.startswith("__")
                and attr not in TRANSFORM_OPTS_ATTRS
                and attr not in names
                and attr not in short_names
            ):
                raise UnknownOptionError(attr)

        self.task = getattr(opts, "task", None)

        self.raw_columns = None
        if hasattr(opts, "Raw"):
            self.raw_columns = extract_columns(getattr(opts, "Raw"))
        self.io = getattr(opts, "io", None)


LOAD_OPTS_ATTRS = ("create_table",)


class LoadOpts:
    """Stores options for transform."""

    def __init__(self, opts: type) -> None:
        # check only supported options are present
        for attr in vars(opts):
            if not attr.startswith("__") and attr not in LOAD_OPTS_ATTRS:
                raise UnknownOptionError(attr)

        self.create_table = getattr(opts, "create_table", True)


class Modifier:
    def __init__(
        self,
        task: Type["Task"],
        task_opts: Optional[Mapping[str, Any]] = None,
        new_columns: Optional[Mapping[str, "BaseColumn"]] = None,
    ) -> None:
        self.task_opts = task_opts or {}
        self.task = task
        new_columns = new_columns or {}
        self.columns = ColumnContainer(new_columns)


class HarmonizerModifier:
    def __init__(
        self,
        modifier: DataFrameModifier,
        **name2column: "BaseColumn",
    ) -> None:
        self.modifier = modifier
        for name in name2column:
            if not name.startswith("i_vis_"):
                raise ValueError("Column name should be prefixed with 'i_vis_': ", name)
        self.columns = ColumnContainer(name2column)

    @property
    def dask_meta(self) -> Mapping[str, Any]:
        return {col: column.dtype for col, column in self.columns.items()}


def get_part_name(name: str) -> str:
    if name == "Spec":
        name = ""

    return underscore(name)


class ETLSpecMeta(type):
    # pylint: disable=bad-mcs-classmethod-argument
    def __new__(
        mcs, name: str, bases: Tuple[type, ...], attrs: Mapping[str, Any]
    ) -> "ETLSpecMeta":
        new_attrs: Dict[str, Any] = {
            "part_name": attrs.get("part_name", get_part_name(name)),
        }
        for attr in ("create", "get_part_name", "Extract", "Transform", "Raw"):
            if attrs.get(attr, None) is not None:
                new_attrs[attr] = attrs[attr]

        if len(bases) > 1:
            raise Exception("Inheritance not supported")

        try:
            extract = attrs.get("Extract")
            if extract:
                new_attrs["extract_opts"] = ExtractOpts(extract)
        except UnknownOptionError as e:
            raise RuntimeError(
                f"Unknown option '{e.option}' in class '{name}' in Extract"
            ) from e

        try:
            transform = attrs.get("Transform")
            if transform:
                new_attrs["transform_opts"] = TransformOpts(transform)
        except UnknownOptionError as e:
            raise RuntimeError(
                f"Unknown option '{e.option}' in class '{name}' Transform"
            ) from e

        raw = attrs.get("Raw")
        if raw:
            new_attrs["raw_opts"] = RawOpts(attrs["Raw"])

        return type.__new__(mcs, name, bases, new_attrs)


class ETLSpec(metaclass=ETLSpecMeta):
    part_name: str = ""

    @classmethod
    def create(cls, pname: str) -> "ETL":
        raw_columns = None
        raw_opts = getattr(cls, "raw_opts", None)
        if raw_opts:
            raw_columns = raw_opts.raw_columns

        extract_opts = getattr(cls, "extract_opts", None)
        if extract_opts:
            if extract_opts.raw_columns:
                if raw_columns:
                    raise Exception("Raw can only be defined once")
                raw_columns = extract_opts.raw_columns

        transform_opts = getattr(cls, "transform_opts", None)
        if transform_opts:
            if transform_opts.raw_columns:
                if raw_columns:
                    raise Exception("Raw can only be defined once")
                raw_columns = transform_opts.raw_columns

        load_opts = getattr(cls, "load_opts", None)

        if not raw_columns:
            raise Exception("Missing description of Raw data")

        return ETL(
            pname=pname,
            part_name=get_part_name(cls.__name__),
            extract_opts=extract_opts,
            transform_opts=transform_opts,
            load_opts=load_opts,
            raw_columns=raw_columns,
        )


class CoreTypeHarmDesc:
    """Description of core type column harmonization"""

    def __init__(
        self,
        core_type: "CoreType",
        harm_modifier: Optional[HarmonizerModifier] = None,
    ) -> None:
        self.core_type = core_type
        self.harm_modifier = harm_modifier

    # TODO
    def raw_columns(self) -> "ColumnContainer":
        """All relevant columns for harmonization - before transformation"""
        pass  # TODO

    def harm_columns(self) -> "ColumnContainer":
        """All relevant columns for harmonization - after transformation"""
        pass  # TODO


class ETL:
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        pname: str,
        part_name: str,
        raw_columns: "ColumnContainer",
        extract_opts: Optional[ExtractOpts] = None,
        transform_opts: Optional[TransformOpts] = None,
        load_opts: Optional[LoadOpts] = None,
        _register: bool = True,
    ) -> None:
        self.pname = pname
        self.part_name = part_name
        self.part = get_part(pname, part_name)

        if _register:
            etl_registry.register(self)

        # store opts
        self.extract_opts = extract_opts
        self.transform_opts = transform_opts
        self.load_opts = load_opts
        self.raw_columns = raw_columns
        # create/register mapped classes
        self._create_models()

    @property
    def raw_data_path(self) -> str:
        return Parquet.format_fname("_" + self.part)

    @property
    def raw_data_fname(self) -> str:
        return "_" + self.part + ".tsv"

    def __getstate__(self) -> Any:
        state = dict(self.__dict__)
        # reset cached properties and LRU-> don't pickle
        del_keys = []
        for key, value in state.items():
            if isinstance(value, cached_property):
                del_keys.append(key)
            elif hasattr(value, "clear_cache"):
                value.clear_cache()
        for key in del_keys:
            del state[key]
        return state

    @cached_property
    def _plugin(self) -> "BasePlugin":
        return BasePlugin.get(self.pname)

    @cached_property
    def _updater(self) -> UpdateManager:
        return self._plugin.updater

    @cached_property
    def column_modifier_columns(self) -> "ColumnContainer":
        name2column = {
            df_utils.i_vis_mod(name): column.modified()
            for name, column in self.raw_columns.items()
            if column.modifier
        }
        return ColumnContainer(name2column)

    @cached_property
    def modifier_task_columns(self) -> "ColumnContainer":
        """Columns added via a task modifier"""

        if not self.transform_opts:
            return ColumnContainer({})

        task = self.transform_opts.task
        if not task:
            return ColumnContainer({})

        return cast(ColumnContainer, task.columns)

    @cached_property
    def all_raw_columns(self) -> "ColumnContainer":
        """Raw columns and columns added by task modifier"""

        return merge_column_container(self.raw_columns, self.modifier_task_columns)

    def harm_desc(self, core_type: "CoreType") -> "CoreTypeHarmDesc":
        """Mapping entities to harmonization descriptions"""

        if self.transform_opts is None:
            harm_modified = {}
        else:

            core_type2harm_modified = self.transform_opts.core_type2harm_modifier

        self.transform_opts

        # extract "raw" columns that are mapped to an core type and store in container
        core_types = self.all_raw_columns.core_types

        for core_type in core_types:
            raw_columns = self.all_raw_columns.columns(core_type)
            name2column = {}
            if core_type2harm_modified:
                harm_modified = core_type2harm_modified.get(core_type)
                if harm_modified:
                    name2column.update(dict(harm_modified.columns.items()))
            for name, raw_column in raw_columns.items():
                if not raw_column.modifier:
                    continue

                name2column[df_utils.i_vis_mod(name)] = raw_column.modified()

        return {
            core_type: CoreTypeHarmDesc(core_type, raw_columns, harm_columns)
            for core_type in self.core_types
        }

    @cached_property
    def core_type2name2column(
        self,
    ) -> Mapping["CoreType", Mapping[str, "BaseColumn"]]:
        core_type2name2column: MutableMapping[
            "CoreType", MutableMapping[str, "BaseColumn"]
        ] = {}

        # extract "raw" columns that are mapped to an core type and store in container
        for column_container in (self.raw_columns, self.modifier_task_columns):
            for core_type, cols in column_container.core_type2cols.items():
                for col in cols:
                    name2column = core_type2name2column.setdefault(core_type, {})
                    if col in name2column:
                        raise KeyError(f"Duplicate {col}")
                    name2column[col] = column_container[col]

        # extract columns from core type modifiers and store in container
        for core_type, core_type_modifier in self.core_type2modifier.items():
            for name, column in core_type_modifier.columns.items():
                name2column = core_type2name2column.setdefault(core_type, {})
                if name in name2column:
                    raise KeyError(f"Duplicate {name}")
                name2column[name] = column
        return core_type2name2column

    @cached_property
    def core_type2columns(self) -> Mapping["CoreType", "ColumnContainer"]:
        return {
            core_type: ColumnContainer(name2column)
            for core_type, name2column in self.core_type2name2column.items()
        }

    # TODO remove
    # @cached_property
    # def core_type2modifier(self) -> Mapping[CoreType, "HarmonizerModifier"]:
    #    """Mapping entities to modifiers with new columns"""
    #    if self.transform_opts is None:
    #        return {}

    #    return self.transform_opts.core_type2harm_modifier

    @cached_property
    def core_types(self) -> Set[CoreType]:
        """All contained entities"""

        return set(self.core_type2harm_desc.keys())

    @cached_property
    def raw_model(self) -> Type[RawData]:
        """Model for raw data"""

        def harmonized_data(self_: Any) -> Mapping[str, Any]:
            return {
                core_type_.clean_name: getattr(
                    self_, f"harmonized_{core_type_.clean_name}"
                )
                for core_type_ in self.core_types
            }

        def processed_data(self_: Any) -> Mapping[str, Any]:
            return {
                core_type_.clean_name: getattr(
                    self_, f"i_vis_raw_{core_type_.clean_name}"
                )
                for core_type_ in self.core_types
            }

        # class name for mapped class
        name = class_name(self.pname, self.part_name)
        # base classes for mapped class
        bases = [RawDataMixin, Base]
        # attributes for base class
        attrs = {
            "__tablename__": raw_tname(self.pname, self.part_name),
            "get_etl": classmethod(lambda cls: self),
            "harmonized_data": property(harmonized_data),
            "processed_data": property(processed_data),
        }
        for core_type in self.core_types:
            attrs[f"i_vis_raw_{core_type.blueprint_name.replace('-', '_')}"] = Column(
                Text
            )

        # container for all raw columns
        column_container = merge_column_container(
            self.raw_columns, self.modifier_task_columns
        )
        # expose (materialize in DB) relevant columns
        table_args: MutableSequence[Any] = []
        for col in column_container.exposed_cols:
            exposed_info = column_container[col].exposed_info
            assert exposed_info is not None
            attrs[col] = exposed_info.db_column
            if exposed_info.table_args:
                table_args.append(table_args)
        if table_args:
            attrs["__table_args__"] = tuple(table_args)

        return cast(
            Type[RawData],
            type(
                name,
                tuple(base for base in bases),
                attrs,
            ),
        )

    @lru_cache
    def harmonized_model(self, core_type: "CoreType") -> Type[HarmonizedData]:
        """CoreType specific mapped class for harmonized data"""
        # base classes for mapped class
        bases = [HarmonizedDataMixin, Base]
        # add core type specific mixin
        if core_type in self.core_types:
            bases.append(core_type.harm_meta.db_mixin)
        if len(bases) <= 1:
            raise Exception("")

        # class name for mapped class
        name = class_name(self.pname, self.part_name, core_type.short_name)

        # pylint: disable=E1101
        raw_tablename = getattr(self.raw_model, "__tablename__")

        # attributes for mapped class
        attrs: Dict["str", Any] = {
            "__tablename__": harmonized_tname(self.pname, core_type, self.part_name),
            RAW_DATA_FK: Column(
                ForeignKey(f"{raw_tablename}.i_vis_id"), nullable=False, index=True
            ),
            "raw_data": relationship(
                self.raw_model,
                backref=backref(
                    f"harmonized_{core_type.blueprint_name.replace('-', '_')}",
                    lazy="joined",
                ),
            ),
            "get_core_type": classmethod(lambda cls: core_type),
            "get_etl": classmethod(lambda cls: self),
        }

        return cast(
            Type[HarmonizedData],
            type(
                name,
                tuple(bases),
                attrs,
            ),
        )

    def _create_models(self) -> Tuple[Any, Any]:
        """Helper - register mapped classes"""
        return self.raw_model, self.core_type2model

    @cached_property
    def core_type2model(self) -> Mapping["CoreType", Type[HarmonizedData]]:
        """Mapping entities to DB mapped classes"""

        return {
            core_type: self.harmonized_model(core_type) for core_type in self.core_types
        }


class ColumnContainer:
    def __init__(self, name2column: Mapping[str, "BaseColumn"]) -> None:
        self._name2column = dict(name2column)

    def col_by_closest_term(self, query_term: TermType) -> Sequence[str]:
        cols = set()
        for name, column in self._name2column.items():
            for term in column.terms:
                if not isclass(term) and term.__class__ == query_term:
                    cols.add(name)
                elif term.is_parent(query_term):
                    cols.add(name)

        return tuple(cols)

    def __getitem__(self, key: str) -> "BaseColumn":
        return self._name2column[key]

    def keys(self) -> Sequence[str]:
        return list(self._name2column.keys())

    def items(self) -> ItemsView[str, "BaseColumn"]:
        return self._name2column.items()

    @property
    def cols(self) -> Sequence[str]:
        return list(self._name2column.keys())

    def core_types_by_col(self, col: str) -> Sequence["CoreType"]:
        core_types = []
        for term in self[col].terms:
            if Term.harmonize(term):
                core_types.append(term.get_core_type())
        return core_types

    @cached_property
    def core_type2cols(self) -> Mapping["CoreType", Sequence[str]]:
        core_type2cols_: MutableMapping["CoreType", MutableSequence[str]] = {}
        for col in self._name2column.keys():
            for core_type in self.core_types_by_col(col):
                core_type2cols_.setdefault(core_type, []).append(col)
        return core_type2cols_

    @cached_property
    def columns(self, core_type: "CoreType") -> "ColumnContainer":
        return ColumnContainer({col: self._name2column.get(col) for col in self.core_type2cols.get(core_type, [])}

    @cached_property
    def col2targets(self) -> Mapping[str, Sequence[str]]:
        col2targets: MutableMapping[str, MutableSequence[str]] = {}
        for core_type in self.core_types:
            for col in self.core_type2cols.get(core_type, []):
                for target in core_type.harm_meta.targets:
                    targets = col2targets.get(col, [])
                    if target not in targets:
                        targets.append(target)
        return col2targets

    @property
    def core_types(self) -> Sequence[CoreType]:
        return list(self.core_type2cols.keys())

    @property
    def exposed_cols(self) -> Sequence[str]:
        return [
            name for name, column in self._name2column.items() if column.exposed_info
        ]


def merge_column_container(*column_containers: "ColumnContainer") -> "ColumnContainer":
    name2column: MutableMapping[str, "BaseColumn"] = {}
    for column_container in column_containers:
        for name, column in column_container.items():
            if name in name2column:
                raise ValueError(f"Duplicate: {name}")
            name2column[name] = column
    return ColumnContainer(name2column)


class ExposeInfo:
    def __init__(
        self,
        db_column: Column[Any],
        fields: Optional[Mapping[str, Type[Field]]] = None,
        table_args: Optional[Tuple[Any]] = None,
    ):
        self.db_column = db_column
        self.fields = fields
        self.table_args = table_args


class BaseColumn:
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        dtype: Optional[IvisDtype] = None,
        terms: Optional[TermTypes] = None,
        modifier: Optional[ColumnModifier] = None,
        formatter: Optional[Formatter] = None,
        exposed_info: Optional[ExposeInfo] = None,
    ) -> None:
        if not dtype:
            dtype = str

        self._terms: MutableSequence[TermType] = []
        if terms:
            if isinstance(terms, Term):
                self._terms.append(terms)
            else:
                self._terms.extend(terms)

        self._mod_type: Optional[IvisDtype] = dtype
        if modifier:
            self.modifier: Callable[[pd.Series], pd.Series]
            if isinstance(modifier, dict):

                def helper(s: pd.Series) -> pd.Series:
                    return cast(pd.Series, s.str.split(**modifier))

                self.modifier = helper
            elif isinstance(modifier, tuple):
                self.modifier = modifier[0]
                self._mod_type = modifier[1]
            elif callable(modifier):
                self.modifier = modifier
        else:
            self._mod_type = None

        self.formatter = formatter
        self.exposed_info = exposed_info
        self.dtype = dtype

    def modified(self) -> "BaseColumn":
        return BaseColumn(
            dtype=self.mod_dtype,
            terms=list(self.terms),
            formatter=deepcopy(self.formatter),
            exposed_info=copy(self.exposed_info),
        )

    @property
    def mod_dtype(self) -> Optional[IvisDtype]:
        if not self.modifier:
            return None

        return self._mod_type

    def copy(self) -> "BaseColumn":
        return BaseColumn(
            dtype=self.dtype,
            terms=list(self._terms),
            modifier=deepcopy(self.modifier),
            formatter=deepcopy(self.formatter),
            exposed_info=copy(self.exposed_info),
        )

    @property
    def terms(self) -> Set[TermType]:
        return set(self._terms)

    @property
    def expose(self) -> bool:
        return self.exposed_info is not None

    @property
    def harmonized_to(self) -> Sequence[str]:
        return [term.get_core_type().name for term in self.terms if term.harmonize]


class Exposed(BaseColumn):
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        exposed_info: ExposeInfo,
        dtype: Optional[IvisDtype] = None,
        terms: Optional[TermTypes] = None,
        modifier: Optional[ColumnModifier] = None,
        formatter: Optional[Formatter] = None,
    ) -> None:
        super().__init__(
            dtype=dtype,
            terms=terms,
            modifier=modifier,
            formatter=formatter,
            exposed_info=exposed_info,
        )


class Simple(BaseColumn):
    def __init__(
        self,
        dtype: Optional[IvisDtype] = None,
        terms: Optional[TermTypes] = None,
        modifier: Optional[ColumnModifier] = None,
        fomatter: Optional[Formatter] = None,
    ) -> None:
        super().__init__(
            dtype=dtype, terms=terms, modifier=modifier, formatter=fomatter
        )
