import gzip
import os
import shutil
from typing import (
    Any,
    Tuple,
    Dict,
    cast,
    Mapping,
    MutableMapping,
    Sequence,
    Optional,
    TYPE_CHECKING,
    Callable,
    Union,
    Generator,
    Iterator,
)
from zipfile import ZipFile
from abc import ABC
from itertools import chain
from functools import cached_property, partial

import dask
from dask import dataframe as dd
import pandas as pd
import xmltodict
import pyarrow as pa
from cyvcf2 import VCF

from i_vis.core.config import get_ivis

from .base import Task, TaskType
from ..df_utils import (
    sort_prefixed,
    RAW_DATA_FK,
    loc,
    ParquetIO,
    DataFrameIO,
    convert_to_parquet,
    clean_df,
)
from .. import df_utils
from ..resource import File, Parquet
from ..db_utils import RAW_DATA_PK
from ..utils import to_str


if TYPE_CHECKING:
    from ..resource import Resources, Resource, ResourceDesc, ResourceId, ResourceIds
    from ..etl import ColumnContainer, CoreTypeHarmDesc
    from ..plugin import CoreType
    from ..df_utils import AnyDataFrame


# pylint: disable=W0223
class Transform(Task, ABC):
    @property
    def type(self) -> TaskType:
        return TaskType.TRANSFORM


class BaseModifier(Transform, ABC):
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        in_rid: "ResourceId",
        out_fname: str,
        io: DataFrameIO,
        desc: Optional["ResourceDesc"] = None,
        **kwargs: Any,
    ) -> None:
        out_res = io.create(in_rid.pname, name=out_fname, desc=desc)
        super().__init__(requires=[in_rid], offers=[out_res], **kwargs)


class Unpack(Transform):
    def __init__(
        self,
        in_file_id: "ResourceId",
        out_files: Sequence[File],
        **kwargs: Any,
    ) -> None:
        super().__init__(
            offers=out_files,
            requires=[in_file_id],
            **kwargs,
        )

    def _do_work(self, context: "Resources") -> None:
        in_file = context.file(self.in_rid)
        _, extension = os.path.splitext(in_file.name)
        if extension == ".zip":
            unzip(in_file, self.offered)
        elif extension in (".gz", ".gzip"):
            assert len(self.offered) == 1
            gzip_(in_file, cast(File, self.out_res))
        else:
            raise ValueError(f"Unknown suffix: {extension}")

        for res in self.offered:
            res.dirty = True
            res.update_db()


class BuildDict(Transform):
    # pylint: disable=too-many-instance-attributes
    def __init__(
        self,
        in_rids: "ResourceIds",
        entities: File,
        names: File,
        target_id: str,
        max_name_length: int = 255,
        max_type_length: int = 255,
        id_prefix: str = "",
        main_in_rid: Optional["ResourceId"] = None,
    ):
        requires = []
        if main_in_rid:
            requires.append(main_in_rid)

        super().__init__(
            requires=requires,
            offers=[
                entities,
                names,
            ],
        )

        self.main_in_rid = main_in_rid
        self.in_rids = in_rids

        self.entities = entities
        self.names = names
        self.target_id = target_id
        self.max_name_length = max_name_length
        self.max_type_length = max_type_length
        self.id_prefix = id_prefix

    def post_register(self) -> None:
        for rid in self.in_rids:
            if rid not in self.required_rids:
                self.required_rids.add(rid)
        self.in_rids.close()

    def _do_work(self, context: "Resources") -> None:
        dfs = [
            context[in_rid].read_full()
            for in_rid in self.required_rids
            if in_rid != self.main_in_rid
        ]
        df = pd.concat(dfs, ignore_index=True).drop_duplicates()

        # if main file provided - filter ids that are unknown to main
        if self.main_in_rid:
            df_main = context[self.main_in_rid].read_full().set_index(self.target_id)
            df = pd.concat([df_main, df], ignore_index=True).drop_duplicates()
            df = df.set_index(self.target_id)
            df = df[df_main.index]
            breakpoint()

        # filter entities with id = name
        is_id = df[self.target_id] == df["name"]
        if is_id.any():
            df = df[~is_id]
            self.logger.info(f"{sum(is_id)} removed {self.target_id}")

        # names that are TOO long
        too_long_name = cast(pd.Series, df["name"].str.len() > self.max_name_length)
        if too_long_name.any():
            df = df[~too_long_name]
            self.logger.info(
                f"{sum(too_long_name)} removed too long (>{self.max_name_length}) name(s)"
            )
        # types that are TOO long
        if "type" in df.columns:
            too_long_type = cast(pd.Series, df["type"].str.len() > self.max_type_length)
            if too_long_type.any():
                df = df[~too_long_type]
                self.logger.info(
                    f"{sum(too_long_type)} removed too long (>{self.max_type_length}) type(s)"
                )

        df = sort_prefixed(df, self.target_id, self.id_prefix)

        # store entities
        entities = pd.DataFrame({self.target_id: df[self.target_id].unique()})
        entities = df_utils.add_id(entities, "id")

        self.logger.info(f"{len(entities)} unique {self.target_id} entities")
        self.entities.save(entities, logger=self.logger)

        # remove entries that don't have a name
        df = clean_df(df)

        # store transformed names
        # aggregate plugins for future use
        agg = {
            "data_sources": ",".join,
        }
        cols = [self.target_id, "name", "data_sources"]
        if "type" in df.columns:
            agg["type"] = ",".join
            cols.append("type")
        df_names = (
            df[cols]
            .drop_duplicates()
            .groupby([self.target_id, "name"], as_index=False)
            .agg(agg)
        )
        df_names = df_utils.add_id(df_names, col="id")
        self.logger.info(f"{len(df_names)} unique {self.target_id} <-> raw names")
        self.names.save(
            df_names[["id"] + cols],
            logger=self.logger,
        )


def unzip(in_file: File, out_files: "Resources") -> None:
    paths = [out_file.qname for out_file in out_files]
    if len(paths) > 1:
        path = os.path.commonpath(paths)
    else:
        path = os.path.dirname(paths[0])

    with ZipFile(in_file.qname, "r") as zip_obj:
        for out_file in out_files:
            zip_obj.extract(member=out_file.name, path=path)


def gzip_(in_file: File, out_res: Union[File, Parquet]) -> None:
    with gzip.open(in_file.qname, "rb") as f_in:
        with open(out_res.qname, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)


class Process(Transform):
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        out_res: "Resource",
        in_rid: Optional["ResourceId"] = None,
        **kwargs: Any,
    ) -> None:
        requires = []
        if in_rid is not None:
            requires.append(in_rid)

        super().__init__(offers=(out_res,), requires=requires, **kwargs)

        self._out_res = out_res

    def _do_work(self, context: "Resources") -> None:
        if self.in_rid is not None:
            in_res = context[self.in_rid]
            df = in_res.read()
        else:
            df = pd.DataFrame()
        if isinstance(df, dd.DataFrame):
            df = df.map_partitions(self._process, context=context)
        elif isinstance(df, pd.DataFrame):
            df = self._process(df, context)
        elif isinstance(df, Iterator):
            df = pd.concat(df)  # type: ignore
            df = self._process(df, context)

        if self.out_res.desc is not None:
            df = loc(df, self.out_res)

        self.out_res.save(df, logger=self.logger)
        # if isinstance(df, dd.DataFrame) and len(df.partitions) > 1:
        #    tmp_dir = os.path.join(self.out_res.qname + "-dask", "*.tsv")
        #    fnames = df.to_csv(tmp_dir, **self._save_opts)
        #    _ = merge_files(in_fnames=fnames, out_fname=self.out_res.qname)
        #    shutil.rmtree(tmp_dir)
        # else:
        #    df.to_csv(self.out_res.qname, **self._save_opts)

    # dask_process
    # pd_process
    def _process(self, df: "AnyDataFrame", context: "Resources") -> "AnyDataFrame":
        raise NotImplementedError(self.tid)


class HarmonizeRawData(Transform):
    """Harmonize data"""

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        in_rid: "ResourceId",
        harm_desc2files: Mapping["CoreTypeHarmDesc", Tuple[File, File]],
        out_res: "Resource",
        raw_columns: "ColumnContainer",
        **kwargs: Any,
    ) -> None:
        requires = [in_rid]
        self._in_rid = in_rid
        offers = [out_res] + list(chain.from_iterable(harm_desc2files.values()))
        self._out_res = out_res

        core_types = [harm_desc.core_type for harm_desc in harm_desc2files.keys()]
        kwargs["name_args"] = (core_type.short_name for core_type in core_types)
        super().__init__(
            offers=offers,
            requires=requires,
            **kwargs,
        )
        self.raw_columns = raw_columns
        self.harm_files = {
            harm_desc.core_type: file
            for harm_desc, (file, _) in harm_desc2files.items()
        }
        self.not_harm_files = {
            harm_desc.core_type: file
            for harm_desc, (_, file) in harm_desc2files.items()
        }
        self.harm_descs = {
            harm_desc.core_type: harm_desc for harm_desc in harm_desc2files.keys()
        }

        for core_type in core_types:
            core_type.register_harmonize_raw_data_task(self)

    # pylint: disable=too-many-locals
    def _do_work(self, context: "Resources") -> None:
        if not self.harm_files:
            self.logger.info("Nothing to harmonize.")

        core_types = [
            core_type for core_type in self.harm_files.keys() if core_type.installed
        ]

        in_file = context[self.in_rid]
        df = in_file.read()
        if isinstance(df, pd.DataFrame):
            self._do_pandas_work(df, core_types)
        else:
            cores = int(get_ivis("CORES"))
            if cores > 1:
                with dask.config.set(
                    {
                        "scheduler": "processes",
                        "num_workers": cores,
                        "multiprocessing.context": "fork",
                    }
                ):
                    self._do_dask_work(df, core_types)
            else:
                self._do_dask_work(df, core_types)

        for core_type in core_types:
            for files in (self.harm_files, self.not_harm_files):
                file = files[core_type]
                file.update_db()
                file.dirty = True

    # pylint: disable=too-many-locals
    def _do_dask_work(self, raw_data: dd, core_types: Sequence["CoreType"]) -> None:
        breakpoint()
        for core_type in core_types:
            harmonizer = core_type.harmonizer
            desc = self.harm_descs[core_type]

            # column modifier
            for col in self.raw_columns.core_type2cols.get(core_type, []):
                if self.raw_columns[col].modifier:
                    raw_data = raw_data.map_partitions(
                        self.raw_columns[col].modify, col=col
                    )

            # dataframe modifier
            harm_modifier = desc.harm_modifier
            if harm_modifier is not None:
                raw_data = raw_data.map_partitions(harm_modifier.modifier)

            cols = list(desc.cols)
            raw_data = raw_data[[RAW_DATA_PK] + cols].rename(
                columns={RAW_DATA_PK: RAW_DATA_FK}
            )
            result = raw_data.map_partitions(harmonizer.harmonize, cols=cols)
            harmonized = result.map_partitions(harmonizer.harmonized)
            not_harmonized = result.map_partitions(harmonizer.not_harmonized)
            targets = list(core_type.harm_meta.targets)
            harmonized = harmonized[[RAW_DATA_FK] + targets]

            harmonized.map_partitions(sort_by_targets, targets=targets, meta=harmonized)

            objs = (
                (harmonized, self.harm_files),
                (not_harmonized, self.not_harm_files),
            )
            for df_, files in objs:
                file = files[core_type]
                file.save(df_)

    def _do_pandas_work(
        self, raw_data: pd.DataFrame, core_types: Sequence["CoreType"]
    ) -> None:
        # TODO add exposed data
        raw_table_data = pd.DataFrame(
            columns=["i_vis_raw_" + core_type.clean_name for core_type in core_types],
            index=raw_data.index.copy(),
            dtype="str",
        ).fillna("")
        for core_type in core_types:
            df = raw_data.copy()
            harmonizer = core_type.harmonizer
            desc = self.harm_descs[core_type]

            # column modifier
            for col in self.raw_columns.core_type2cols.get(core_type, []):
                if self.raw_columns[col].modifier:
                    df = self.raw_columns[col].modify(df=df, col=col)

            # dataframe modifier
            harm_modifier = desc.harm_modifier
            if harm_modifier is not None:
                df = harm_modifier.modifier(df)

            cols = list(desc.cols)
            raw_table_data["i_vis_raw_" + core_type.clean_name] = df[cols].apply(
                lambda x: ";".join(set(map(to_str, filter(None, x)))), axis=1
            )
            df.reset_index(inplace=True)
            breakpoint()
            df = df[[RAW_DATA_PK] + cols].rename(columns={RAW_DATA_PK: RAW_DATA_FK})
            result = harmonizer.harmonize(df=df, cols=cols)
            is_harmonized = harmonizer.is_harmonized(result)
            harmonized = result[is_harmonized]
            not_harmonized = result[~is_harmonized]
            targets = list(core_type.harm_meta.targets)
            harmonized = harmonized[[RAW_DATA_FK] + targets]
            harmonized = harmonized.sort_values(
                by=[RAW_DATA_FK] + targets, ignore_index=True
            )
            # remove duplicate mappings
            if core_type.harmonizer.deduplicate:
                harmonized = harmonized.drop_duplicates(
                    subset=[RAW_DATA_FK] + targets,
                    ignore_index=True,
                )

            objs = (
                (harmonized, self.harm_files),
                (not_harmonized, self.not_harm_files),
            )
            for df_, files in objs:
                file = files[core_type]
                file.save(df_, logger=self.logger)
        self.out_res.save(raw_table_data, logger=self.logger)


def sort_by_targets(df: "AnyDataFrame", targets: Sequence[str]) -> "AnyDataFrame":
    return df.sort_values(by=[RAW_DATA_FK] + list(targets), ignore_index=True)


class ConvertXML(BaseModifier):
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        in_rid: "ResourceId",
        getter: Callable[[Dict[Any, Any]], Dict[Any, Any]],
        io: DataFrameIO,
        out_fname: str,
        to_opts: Optional[Mapping[str, Any]] = None,
        add_id: bool = False,
        **kwargs: Any,
    ) -> None:
        kwargs["io"] = io
        super().__init__(
            in_rid,
            out_fname=out_fname,
            **kwargs,
        )

        self.getter = getter
        self.to_opts = to_opts or {}
        self.add_id = add_id

    def _do_work(self, context: "Resources") -> None:
        in_file = context.file(self.in_rid)
        with open(in_file.qname, "r", encoding="utf8") as in_f:
            xml = in_f.read()
            dt = xmltodict.parse(xml)
            del xml
            df = pd.DataFrame(self.getter(dt))
            if self.add_id:
                df = df_utils.add_pk(df)
                df.set_index(RAW_DATA_PK, drop=True, inplace=True)
            self.out_res.save(df, logger=self.logger, **self.to_opts)


class ConvertToParquet(Transform):
    def __init__(
        self,
        in_file_id: "ResourceId",
        out_parquet: "Parquet",
        col: str = RAW_DATA_PK,
        **kwargs: Any,
    ):
        super().__init__(requires=[in_file_id], offers=[out_parquet], **kwargs)
        self.col = col

    def _do_work(self, context: "Resources") -> None:
        in_file = context.file(self.in_rid)
        _ = convert_to_parquet(in_file, cast(Parquet, self.out_res), col=self.col)
        self.out_res.update_db()
        self.out_res.dirty = True


# pylint: disable=too-many-arguments
def to_sequence(
    obj: Any,
    variant: Any,
    create_sequence: type,
    empty: Optional[Any] = None,
    get_count: Optional[Callable[[Any], int]] = None,
    convert: Optional[Callable[[Any], Sequence[Any]]] = None,
) -> Sequence[Any]:
    count = 1
    if get_count is not None:
        count = get_count(variant)
    if obj is None:
        return cast(Sequence[Any], count * create_sequence((empty,)))

    if count == 1 and not isinstance(obj, create_sequence):
        return cast(Sequence[Any], create_sequence((obj,)))

    if convert:
        return convert(obj)

    return cast(Sequence[Any], obj)


class Parse(ABC):
    def __init__(self, id_: str, number: str, type_: str):
        self.id = id_
        self.convert: Optional[Callable[[Any, Any], Any]] = None

        if number == "0":
            self.convert = to_bool
            self.data_type = Parser.TYPE2DATA_TYPE[type_]
            self.to_dtype = Parser.TYPE2DTYPE[type_]
        elif number == "1":
            self.data_type = Parser.TYPE2DATA_TYPE[type_]
            self.to_dtype = Parser.TYPE2DTYPE[type_]
        elif number in ("R", "G"):
            self.data_type = Parser.TYPE2DATA_TYPE["String"]
            self.to_dtype = "object"
        elif number in "A":
            self.data_type = Parser.TYPE2DATA_TYPE[type_]
            self.to_dtype = "object"
            _convert_kwargs: MutableMapping[str, Any] = {
                "get_count": lambda variant: len(variant.ALT),
                "create_sequence": tuple,
            }
            if type_ == "String":
                _convert_kwargs["empty"] = ""
                _convert_kwargs["convert"] = lambda s: tuple(
                    str(y) for y in s.split(",")
                )
            if self.key == "alt":
                _convert_kwargs["create_sequence"] = list
                _convert_kwargs["convert"] = None
                self.data_type = pa.list_(Parser.TYPE2DATA_TYPE[type_])

            self.convert = partial(to_sequence, **_convert_kwargs)
        else:
            self.data_type = pa.list_(Parser.TYPE2DATA_TYPE[type_])
            self.to_dtype = "object"

            def helper(obj: Any) -> Any:
                if not isinstance(obj, tuple):
                    return (obj,)

                return obj

            self.convert = partial(
                to_sequence,
                create_sequence=tuple,
                empty="" if type_ == "String" else None,
                convert=helper,
            )

        self.number = number
        self.type = type_

    def parse(self, variant: Any) -> Any:
        raise NotImplementedError

    @cached_property
    def key(self) -> str:
        raise NotImplementedError


class ParseInfo(Parse):
    def parse(self, variant: Any) -> Any:
        value = variant.INFO.get(self.id)
        if self.convert:
            value = self.convert(value, variant)
        return value

    @cached_property
    def key(self) -> str:
        return "info_" + self.id.lower()


class ParseMain(Parse):
    def parse(self, variant: Any) -> Any:
        value = getattr(variant, self.id)
        if self.convert:
            value = self.convert(value, variant)
        return value

    @cached_property
    def key(self) -> str:
        return self.id.lower()


def to_bool(obj: Any, _: Any) -> bool:
    return obj is not None


class Parser:

    TYPE2DTYPE = {
        "Flag": "bool",
        "Float": "Float64",
        "Integer": "Int64",
        "String": "object",
    }
    TYPE2DATA_TYPE = {
        "Flag": pa.bool_(),
        "Float": pa.float64(),
        "Integer": pa.int64(),
        "String": pa.string(),
    }

    def __init__(
        self,
        qfname: str,
        buffer_size: int = 10000,
        info_parser: Optional[
            Mapping[str, Callable[[pd.DataFrame, "Parser"], Any]]
        ] = None,
    ) -> None:
        self.qfname = qfname
        self.variants = VCF(qfname)
        self.buffer_size = buffer_size
        self.info_parser = info_parser or {}

    @cached_property
    def header(self) -> Mapping[str, Mapping[str, Any]]:
        result: MutableMapping[str, Any] = {}
        for header_line in self.variants.header_iter():
            try:
                result.setdefault(header_line["HeaderType"], {})[
                    header_line["ID"]
                ] = header_line
            except KeyError:
                pass
        return result

    @property
    def header_types(self) -> Sequence[str]:
        return list(self.header.keys())

    @property
    def header_info(self) -> Mapping[str, Any]:
        return self.header.get("INFO", {})

    @property
    def header_filter(self) -> Mapping[str, Any]:
        return self.header.get("FILTER", {})

    @property
    def header_format(self) -> Mapping[str, Any]:
        return self.header.get("FORMAT", {})

    def schema(self, df: pd.DataFrame) -> pa.Schema:
        schema_ = {
            RAW_DATA_PK: pa.int64(),
        }
        for col in df.columns:
            if col in self.key2parse:
                schema_[col] = self.key2parse[col].data_type
            elif col == "i_vis_alt":
                schema_[col] = pa.string()
            else:
                raise ValueError("Unknown column.")

        return pa.schema(schema_)

    @property
    def to_dataframe(self) -> Generator[pd.DataFrame, None, None]:
        key2value: MutableMapping[str, Any] = {}

        variant_count = 1
        for i, variant in enumerate(self.variants, start=1):
            for raw_key in ("CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER"):
                key = raw_key.lower()
                value = self.key2parse[key].parse(variant)
                key2value.setdefault(key, []).append(value)

            for id_ in self.header_info.keys():
                key = "info_" + id_.lower()
                value = self.key2parse[key].parse(variant)
                if self.info_parser.get(id_):
                    value = self.info_parser[id_](value, self)
                key2value.setdefault(key, []).append(value)

            if i % self.buffer_size == 0:
                df = self._to_dataframe(key2value, variant_count)
                variant_count += len(df)
                key2value.clear()
                yield df

        if key2value:
            df = self._to_dataframe(key2value, variant_count)
            variant_count += len(df)
            key2value.clear()
            yield df

    def _to_dataframe(
        self, key2value: Mapping[str, Sequence[Any]], variant_count: int
    ) -> pd.DataFrame:
        df = pd.DataFrame(
            {
                key: pd.Series(values, dtype=self.key2parse[key].to_dtype)
                for key, values in key2value.items()
            },
        )
        df["i_vis_alt"] = df["alt"].copy()
        cols = [
            "info_" + header_info["ID"].lower()
            for header_info in self.header_info.values()
            if header_info["Number"] == "A"
        ]
        df = df.explode(column=cols + ["i_vis_alt"], ignore_index=True)  # type: ignore
        df[RAW_DATA_PK] = pd.Series(
            range(variant_count, variant_count + len(df) + 1), dtype="int64"
        )
        df.set_index(RAW_DATA_PK, inplace=True, drop=True)
        return df

    @cached_property
    def key2parse(self) -> Mapping[str, Parse]:
        key2parse_: MutableMapping[str, Parse] = {
            parse.key: parse
            for parse in (
                ParseMain("CHROM", "1", "String"),
                ParseMain("POS", "1", "Integer"),
                ParseMain("ID", "1", "String"),
                ParseMain("REF", "1", "String"),
                ParseMain("ALT", "A", "String"),
                ParseMain("QUAL", "1", "Float"),
                ParseMain("FILTER", "1", "String"),
            )
        }

        for id_, info in self.header_info.items():
            parse = ParseInfo(id_, info["Number"], info["Type"])
            key2parse_[parse.key] = parse
        # TODO
        # FORMAT
        # SAMPLE
        return key2parse_


ProcessVCF = Callable[[pd.DataFrame, Parser], pd.DataFrame]


class MergeRawData(Transform):
    def __init__(self, in_res_ids: Sequence["ResourceId"], out_file: "File") -> None:
        super().__init__(offers=[out_file], requires=in_res_ids)

    def _do_work(self, context: "Resources") -> None:
        dfs = [context[in_rid].read() for in_rid in self.required_rids]
        df = dd.concat(dfs, axis=1)
        self.out_res.save(df, logger=self.logger)


class VCF2Dataframe(Transform):
    def __init__(
        self,
        in_rid: "ResourceId",
        desc: "ResourceDesc",
        process: Optional[ProcessVCF] = None,
        io: Optional[ParquetIO] = None,
    ) -> None:
        path = "_" + in_rid.name
        out_parquet = Parquet(in_rid.pname, path=path, io=io or ParquetIO(), desc=desc)
        super().__init__(offers=[out_parquet], requires=[in_rid])
        self.process = process

    def _do_work(self, context: "Resources") -> None:
        in_file = context.file(self.in_rid)
        parser = Parser(in_file.qname)

        schema = None
        _to_parquet_kwargs = {
            "path": self.out_res.qname,
            "engine": "pyarrow",
            "compression": "gzip",
        }
        for i, df in enumerate(parser.to_dataframe):
            if self.process:
                df = self.process(df, parser)
            dd.from_pandas(df, npartitions=1)
            if i == 0:
                schema = parser.schema(df)
                dd.to_parquet(df, overwrite=True, schema=schema, **_to_parquet_kwargs)
            else:
                dd.to_parquet(df, append=True, schema=schema, **_to_parquet_kwargs)
