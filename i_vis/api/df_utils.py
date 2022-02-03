"""Tools for pandas data frames"""
from typing import (
    Any,
    Callable,
    cast,
    Hashable,
    Iterator,
    Optional,
    Sequence,
    Mapping,
    TYPE_CHECKING,
    Union,
)
from abc import ABC
from functools import partial
from dask import dataframe as dd

import numpy as np
import pandas as pd
from inflection import underscore
from tqdm import tqdm

from i_vis.core.db_utils import i_vis_col
from i_vis.core.config import get_ivis

from .utils import getLogger
from .db_utils import PK
from .resource import File, Parquet


if TYPE_CHECKING:
    from .resource import Resource, ResourceDesc
    from .etl import DataFrameModifier
    from .utils import I_VIS_Logger


PD_READ_OPTS = {
    "sep": "\t",
}
PD_TO_CSV_OPTS = {
    "sep": "\t",
    "index": False,
}


RAW_DATA_ID = i_vis_col("raw_data_id")


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Clean data frame

    Remove duplicates and empty rows with empty cells
    """

    return df.drop_duplicates().replace("", np.nan).dropna()


AnyDataFrame = Union[pd.DataFrame, dd.DataFrame]


class DataFrameIterator(Iterator[pd.DataFrame]):
    def __init__(
        self,
        dfs: Iterator[pd.DataFrame],
        in_res: "Resource",
        normalize: bool,
        check: bool,
        logger: Optional["I_VIS_Logger"] = None,
    ) -> None:
        self.dfs = dfs
        self.in_res = in_res
        self.normalize = normalize
        self.check = check
        self.logger = logger

    def __iter__(self) -> "DataFrameIterator":
        return self

    def __next__(self) -> pd.DataFrame:
        df = next(self.dfs)
        df = DataFrameIO.check_df(df, self.in_res, self.normalize, self.check)
        self.check = False
        return df


class DataFrameIO(ABC):
    def __init__(
        self,
        read_callback: str,
        to_callback: Optional[str] = None,
        read_opts: Optional[Mapping[str, Any]] = None,
        to_opts: Optional[Mapping[str, Any]] = None,
        normalize: Optional[bool] = True,
        check: Optional[bool] = True,
        logger: Optional["I_VIS_Logger"] = None,
        **kwargs: Any,
    ):
        self._read_callback = read_callback
        self._to_callback = to_callback
        #
        self.__normalize = normalize
        self.__check = check
        self.__logger = logger or getLogger()
        #
        read_opts = dict(read_opts) if read_opts else {}
        to_opts = dict(to_opts) if to_opts else {}
        if kwargs:
            read_opts.update(kwargs)
            to_opts.update(kwargs)
        self._read_opts = read_opts
        self._to_opts = to_opts

    def read(
        self,
        in_res: "Resource",
        normalize: Optional[bool] = None,
        check: Optional[bool] = None,
        logger: Optional["I_VIS_Logger"] = None,
        **kwargs: Any,
    ) -> Union[pd.DataFrame, dd.DataFrame, Iterator[pd.DataFrame]]:
        logger = self._logger(logger)
        df = self._read_df(in_res, logger, **kwargs)

        normalize = self._normalize(normalize) or False
        check = self._check(check) or False

        if isinstance(df, (pd.DataFrame, dd.DataFrame)):
            return self.check_df(df, in_res, normalize, check, logger)

        return DataFrameIterator(df, in_res, normalize, check, logger)

    @staticmethod
    def check_df(
        df: "AnyDataFrame",
        in_res: "Resource",
        normalize: bool,
        check: bool,
        logger: Optional["I_VIS_Logger"] = None,
    ) -> "AnyDataFrame":
        if normalize:
            df = normalize_columns(df)
        if check:
            if in_res.desc is not None:
                check_columns(
                    df, cols=in_res.desc.cols, name=in_res.name, logger=logger
                )
            elif logger:
                msg = f"Cannot check columns. '{in_res.name}' is missing a resource description."
                logger.warning(msg)
        return df

    def _read_df(
        self, in_res: "Resource", logger: Optional["I_VIS_Logger"] = None, **kwargs: Any
    ) -> Union[AnyDataFrame, Iterator[pd.DataFrame]]:
        raise NotImplementedError

    def write(
        self,
        out_res: "Resource",
        df: Union[pd.DataFrame, dd.DataFrame],
        logger: Optional["I_VIS_Logger"] = None,
        **kwargs: Any,
    ) -> None:
        self._write_df(out_res, df, logger, **kwargs)
        out_res.update_db()
        out_res.dirty = True
        if out_res.empty and logger:
            logger.warning(f"DataFrame from '{out_res.name}' is empty.")

    def _write_df(
        self,
        out_res: "Resource",
        df: AnyDataFrame,
        logger: Optional["I_VIS_Logger"] = None,
        **kwargs: Any,
    ) -> None:
        raise NotImplementedError

    def _normalize(self, normalize: Optional[bool]) -> Optional[bool]:
        if normalize is not None:
            return normalize
        return self.__normalize

    def _check(self, check: Optional[bool]) -> Optional[bool]:
        if check is not None:
            return check
        return self.__check

    def _logger(self, logger: Optional["I_VIS_Logger"]) -> Optional["I_VIS_Logger"]:
        if logger is not None:
            return logger
        return self.__logger

    def create(
        self, pname: str, name: str, desc: Optional["ResourceDesc"] = None
    ) -> "Resource":
        raise NotImplementedError

    @staticmethod
    def to_full(df: Union[Iterator[pd.DataFrame], AnyDataFrame]) -> pd.DataFrame:
        if isinstance(df, pd.DataFrame):
            return df

        if isinstance(df, dd.DataFrame):
            return cast(pd.DataFrame, df.compute())

        return pd.concat(list(df), ignore_index=True)


class PandasDataFrameIO(DataFrameIO):
    def _read_df(
        self, in_res: "Resource", logger: Optional["I_VIS_Logger"] = None, **kwargs: Any
    ) -> Union[pd.DataFrame, Iterator[pd.DataFrame]]:
        read_opts = dict(self._read_opts)
        if kwargs:
            read_opts.update(kwargs)
        if self._read_callback in ("read_json", "read_excel"):
            read_opts.pop("chunksize", None)

        df = getattr(pd, self._read_callback)(in_res.qname, **read_opts)
        if isinstance(df, pd.DataFrame):
            return df

        return cast(Iterator[pd.DataFrame], df)

    def _write_df(
        self,
        out_res: "Resource",
        df: AnyDataFrame,
        logger: Optional["I_VIS_Logger"] = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(df, dd.DataFrame):
            df = df.compute()

        to_opts = dict(self._to_opts)
        if kwargs:
            to_opts.update(kwargs)
        assert self._to_callback is not None
        getattr(df, self._to_callback)(out_res.qname, **to_opts)

    def create(
        self, pname: str, name: str, desc: Optional["ResourceDesc"] = None
    ) -> "Resource":
        return File(pname, fname=name, io=self, desc=desc)


class DaskDataFrameIO(DataFrameIO):
    def _read_df(
        self, in_res: "Resource", logger: Optional["I_VIS_Logger"] = None, **kwargs: Any
    ) -> dd.DataFrame:
        read_opts = dict(self._read_opts)
        if kwargs:
            read_opts.update(kwargs)
        df = getattr(pd, self._read_callback)(in_res.qname, **kwargs)
        return cast(dd.DataFrame, df)

    def _write_df(
        self,
        out_res: "Resource",
        df: AnyDataFrame,
        logger: Optional["I_VIS_Logger"] = None,
        **kwargs: Any,
    ) -> None:
        if isinstance(df, pd.DataFrame):
            df = dd.from_pandas(df, npartitions=1)

        # TODO
        #    if to == "to_csv":
        #        kwargs.setdefault("sep", "\t")
        #        kwargs.setdefault("index", False)
        #        if isinstance(df, dd.DataFrame):
        #            tmp_dir = os.path.join(out_res.qname + "-dask", "*.tsv")
        #            fnames = df.to_csv(tmp_dir, **kwargs)
        #            _ = merge_files(in_fnames=fnames, out_fname=out_fname)
        #            shutil.rmtree(tmp_dir)
        #        elif isinstance(df, DataFrame):
        #            df.to_csv(out_fname, **kwargs)
        #        else:
        #            raise ValueError

        #    elif to == "to_parquet":
        #        df.to_parquet(out_fname, overwrite=True, **kwargs)
        #    else:
        #        getattr(df, to)(out_fname, **kwargs)

        to_opts = dict(self._to_opts)
        if kwargs:
            to_opts.update(kwargs)
        assert self._to_callback is not None
        getattr(df, self._to_callback)(out_res.qname, **to_opts)

    def create(
        self, pname: str, name: str, desc: Optional["ResourceDesc"] = None
    ) -> "Resource":
        if self._read_callback == "read_parquet":
            return Parquet(pname, path=name, io=self, desc=desc)

        return File(pname, fname=name, io=self, desc=desc)


class TsvIO(PandasDataFrameIO):
    def __init__(self, **kwargs: Any) -> None:
        kwargs["read_callback"] = "read_csv"
        kwargs["to_callback"] = "to_csv"
        kwargs["sep"] = "\t"
        kwargs.setdefault("to_opts", {}).setdefault("index", False)
        super().__init__(**kwargs)


class ParquetIO(DaskDataFrameIO):
    def __init__(self, **kwargs: Any) -> None:
        kwargs["read_callback"] = "read_parquet"
        kwargs["to_callback"] = "to_parquet"
        kwargs["engine"] = "pyarrow"
        super().__init__(**kwargs)


parquet_io = ParquetIO()

tsv_io = TsvIO()
csv_io = PandasDataFrameIO(read_callback="read_csv", to_callback="to_csv")
json_io = PandasDataFrameIO(read_callback="read_json", to_opts={"indent": 2})


def normalize_columns_helper(s: str) -> str:
    return underscore(s.strip()).replace(" ", "_")


def normalize_columns(df: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    df = df.rename(columns={"GRCh": "grch"}, **kwargs)
    columns = (
        df.columns.str.replace(r"s\(s\)$", "s", regex=True)
        .str.replace("([A-Z]+)s$", "\\1S", regex=True)
        .str.replace("([^A-Z]+)([A-Z]+)", "\\1_\\2", regex=True)
        .str.replace(r"\(s\)", "s", regex=True)
        .str.replace(r"/", "_", regex=True)
        .str.replace(r"\(|\)", " ", regex=True)
        .str.replace("[@#]+", "", regex=True)
        .str.lower()
    )
    df.columns = columns

    # manual cast because of: https://github.com/python/mypy/issues/8293
    df = df.rename(
        columns=cast(Callable[[Hashable], Hashable], normalize_columns_helper),
        **kwargs,
    )
    df.columns = df.columns.str.replace("_+", "_", regex=True)
    return df


def has_split(s: pd.Series, sep: str) -> bool:
    return hasattr(s, "str") and (s.str.count(sep).gt(0).any())


def split_on_demand(s: pd.Series, sep: str) -> pd.Series:
    if has_split(s, sep):
        return cast(pd.Series, s.str.split(sep))

    return s


def complex_explode(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    inv_cols = df.columns.difference(cols)

    tqdm.pandas(desc="Complex explode", unit="columns", leave=False)
    splited = df.loc[:, cols].progress_apply(partial(split_on_demand, sep=";"))
    lengths = splited.applymap(len).progress_apply(lambda x: x.unique(), axis=1)
    repeats = lengths.progress_apply(lambda x: x[0])
    repeated = np.apply_along_axis(
        partial(np.repeat, repeats=repeats), 0, df.loc[:, inv_cols].to_numpy()
    )
    expanded = np.apply_along_axis(np.concatenate, 0, splited.to_numpy())
    old_cols = df.columns.copy()
    new_cols = df.columns[inv_cols].tolist() + df.columns[inv_cols].tolist()
    df = pd.DataFrame(data=np.column_stack((repeated, expanded)), columns=new_cols)
    df = df[old_cols]
    return df


# make sure df[col] is sorted ascending
def add_id(
    df: pd.DataFrame, col: str, start: int = 1, overwrite: bool = False
) -> pd.DataFrame:
    if not overwrite and col in df.columns:
        df[col] = df[col].duplicated().cumsum() + start
    else:
        df.loc[:, [col]] = pd.Series(range(start, start + len(df)))
    df.loc[:, [col]] = df.loc[:, [col]].astype("Int64", copy=False)
    return df


def add_pk(df: pd.DataFrame, overwrite: bool = False, start: int = 1) -> pd.DataFrame:
    if overwrite or PK not in df.columns:
        df = add_id(df, col=PK, start=start)
    return df


class MissingColumnError(Exception):
    def __init__(self, col: str) -> None:
        Exception.__init__(self)
        self.col = col

    def __str__(self) -> str:
        return self.message.format(col=self.col)

    @property
    def message(self) -> str:
        return "Missing column '{col}'"

    @property
    def msg(self) -> str:
        return "Missing column"


def missing_columns(df: AnyDataFrame, cols: Sequence[str]) -> Sequence[str]:
    return [col for col in cols if col not in df.columns]


def new_columns(df: AnyDataFrame, cols: Sequence[str]) -> Sequence[str]:
    return [col for col in df.columns if col not in cols]


def check_columns(
    df: AnyDataFrame,
    cols: Sequence[str],
    name: str,
    hard: bool = False,
    logger: Optional["I_VIS_Logger"] = None,
) -> None:
    if logger is None:
        logger = getLogger()
    check_new_columns(df, cols=cols, name=name, logger=logger)
    check_missing_column(df, cols=cols, name=name, logger=logger, hard=hard)


# res: "Resource",
def check_missing_column(
    df: AnyDataFrame,
    cols: Sequence[str],
    name: str,
    logger: "I_VIS_Logger",
    hard: bool = True,
) -> None:
    for col in missing_columns(df, cols):
        msg = f"{name} IS MISSING column '{col}'"
        if hard:
            logger.error(msg + ".")
            raise MissingColumnError(col)
        msg += " (trying to continue)"
        logger.warning(msg)


def check_new_columns(
    df: AnyDataFrame,
    cols: Sequence[str],
    name: str,
    logger: "I_VIS_Logger",
) -> None:
    new_cols = new_columns(df, cols)
    if new_cols:
        unknown_cols = ",".join(f"'{col}'" for col in cols)
        msg = f"{name} HAS UNKNOWN column(s) {unknown_cols}"
        logger.warning(msg)


def loc(df: AnyDataFrame, res: "Resource") -> AnyDataFrame:
    desc = res.desc
    assert desc is not None
    cols = [col for col in desc.cols if col in df.columns]
    return cast(AnyDataFrame, df.loc[:, cols])


def sort_prefixed(df: pd.DataFrame, col: str, prefix: str) -> pd.DataFrame:
    new_index = (
        df[col]
        .str.replace(f"^{prefix}", "", regex=True)
        .astype("int")
        .sort_values()
        .index
    )
    return df.reindex(index=new_index).reset_index(drop=True)


def convert_to_parquet(
    in_file: "File",
    out_parquet: "Parquet",
    col: str,
    func: Optional["DataFrameModifier"] = None,
) -> int:
    line_count = 0
    assert in_file.io is not None
    dfs = in_file.io.read(
        in_file, check=True, normalize=True, chunksize=get_ivis("CHUNKSIZE", 25e4)
    )
    for df in dfs:
        if func:
            df = func(df)
            if df.empty:
                continue
        df_len = len(df)
        df.insert(
            0,
            col,
            pd.Series(range(line_count + 1, line_count + 1 + df_len), dtype="Int64"),
        )
        df.set_index(col, inplace=True)

        df = dd.from_pandas(df, npartitions=1)
        opts = {}
        if line_count == 0:
            opts["overwrite"] = True
        else:
            opts["append"] = True
        out_parquet.save(df, **opts)
        line_count += df_len
    return line_count
