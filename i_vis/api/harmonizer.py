import os
from typing import Any, Optional, Sequence, TYPE_CHECKING, List, cast
import pandas as pd
from pandas import DataFrame, Series

from .df_utils import normalize_columns
from .preon import PreonModified

if TYPE_CHECKING:
    from .preon import QueryResult


class QueryOpts:
    def __init__(
        self,
        progress: bool = True,
        prefix: str = "_",
        **kwargs: Any,
    ) -> None:
        self.progress = progress
        self.prefix = prefix
        self.kwargs = kwargs


class Harmonizer:
    def _init(self) -> None:
        raise NotImplementedError

    def clear(self) -> None:
        raise NotImplementedError

    # pylint: disable=unused-argument
    def harmonize(
        self,
        df: DataFrame,
        cols: Sequence[str],
        opts: Optional[QueryOpts] = None,
        **kwargs: Any,
    ) -> DataFrame:
        """

        Args:
            df:
            cols:
            opts:

        Returns:

        """

        raise NotImplementedError

    def query(self, query_name: str, **kwargs: Any) -> Optional["QueryResult"]:
        raise NotImplementedError

    @property
    def deduplicate(self) -> bool:
        raise NotImplementedError

    def is_harmonized(self, result: DataFrame) -> Series:
        raise NotImplementedError

    def harmonized(self, result: DataFrame) -> DataFrame:
        return result[self.is_harmonized(result)]

    def not_harmonized(self, result: DataFrame) -> DataFrame:
        return result[~self.is_harmonized(result)]


class SimpleHarmonizer(Harmonizer):
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        target: str,
        match_types: Sequence[str],
        fname: str,
        id_col: str,
        name_col: str,
    ) -> None:
        """

        Args:
            target:
            match_types:
            fname:
            id_col:
            name_col:
        """

        super().__init__()

        self.target = target
        self.fname = fname

        self.match_types = match_types
        self.id_col = id_col
        self.name_col = name_col

        self._preon: Optional[PreonModified] = None

    @property
    def preon(self) -> PreonModified:
        if self._preon is None:
            self._preon = PreonModified(match_types=self.match_types)
            self._init()
        return self._preon

    def _init(self) -> None:
        mapping = pd.read_csv(
            os.path.join(self.fname),
            sep="\t",
        )

        assert self._preon is not None
        self._preon.fit(
            names=mapping[self.name_col].to_list(),
            ids=mapping[self.id_col].to_list(),
        )

    def clear(self) -> None:
        self._preon = None

    @property
    def deduplicate(self) -> bool:
        return True

    # move to kwargs
    # in_res: Optional["Resource"] = None,
    # harm_desc: Optional["CoreTypeHarmDesc"] = None,

    def dask_meta(self, static_cols: List[str], opts: QueryOpts) -> DataFrame:
        return DataFrame(
            columns=static_cols
            + [
                opts.prefix + self.target + "_query",
                opts.prefix + self.target + "_found_names",
                self.target,
                opts.prefix + self.target + "_meta_info",
                opts.prefix + "col",
            ]
        )

    @staticmethod
    def static_cols(query_df: DataFrame, query_cols: Sequence[str]) -> List[str]:
        return cast(List[str], query_df.columns.difference(query_cols).to_list())

    def harmonize(
        self,
        df: DataFrame,
        cols: Sequence[str],
        opts: Optional[QueryOpts] = None,
        **kwargs: Any,
    ) -> DataFrame:
        opts = QueryOpts()
        results = []
        static_cols = self.static_cols(df, cols)
        for query_col in cols:
            df_tmp = (
                df[static_cols + [query_col]]
                .explode(query_col)
                .dropna(subset=[query_col])
            )

            query = df_tmp[query_col].drop_duplicates().reset_index(drop=True)
            result = DataFrame.from_records(
                query.apply(self.preon.query, **kwargs).to_list(),
                columns=["Found Names", "Found Name IDs", "Meta Info"],
            )
            result["Query Name"] = query

            result = normalize_columns(result)
            result = result.rename(
                columns={
                    "found_name_ids": opts.prefix + self.target,
                    "found_names": opts.prefix + self.target + "_found_names",
                    "meta_info": opts.prefix + self.target + "_meta_info",
                }
            )
            result = (
                df_tmp.merge(
                    result, left_on=query_col, right_on="query_name", how="left"
                )
                .drop(columns=["query_name"])
                .dropna(subset=[query_col])
                .explode(opts.prefix + self.target)
                .explode(opts.prefix + self.target)
                .reset_index(drop=True)
            )
            result = result.rename(
                columns={
                    query_col: opts.prefix + self.target + "_query",
                    opts.prefix + self.target: self.target,
                }
            )
            result[opts.prefix + "col"] = query_col
            results.append(result)
        result = pd.concat(results).reset_index(drop=True)
        return result

    def is_harmonized(self, result: DataFrame) -> Series:
        if self.target in result.columns:
            return ~result[self.target].isna()
        return Series()

    def is_empty(self, result: DataFrame, opts: QueryOpts) -> Series:
        col = opts.prefix + self.target + "_query"
        if col in result.columns:
            return result[col].isna()
        return Series()

    def harmonized_cols(
        self,
        query_df: DataFrame,
        result: DataFrame,
        query_cols: Sequence[str],
        opts: QueryOpts,
    ) -> Sequence[str]:
        meta_cols = [col for col in result.columns if col.startswith(opts.prefix)]
        return self.static_cols(query_df, query_cols) + [self.target] + meta_cols

    def not_harmonized_cols(
        self, query_df: DataFrame, query_cols: Sequence[str], opts: QueryOpts
    ) -> Sequence[str]:
        return self.static_cols(query_df, query_cols) + [
            opts.prefix + self.target + "_query",
            opts.prefix + "col",
        ]

    def query(
        self,
        query_name: str,
        **kwargs: Any,
    ) -> Optional["QueryResult"]:
        return self.preon.query(
            query_name,
            **kwargs,
        )
