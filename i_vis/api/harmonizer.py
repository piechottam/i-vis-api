import os
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, cast

import pandas as pd
from pandas import DataFrame

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

    def dask_meta(self, opts: Optional[QueryOpts] = None) -> Mapping[str, Any]:
        raise NotImplementedError


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

    def dask_meta(
        self,
        opts: Optional[QueryOpts] = None,
    ) -> Mapping[str, Any]:
        if not opts:
            opts = QueryOpts()
        return {
            opts.prefix + self.target + "_query": "str",
            opts.prefix + self.target + "_found_names": "object",
            self.target: "str",
            opts.prefix + self.target + "_meta_info": "object",
            opts.prefix + "col": "str",
        }

    def harmonize(
        self,
        df: DataFrame,
        cols: Sequence[str],
        opts: Optional[QueryOpts] = None,
        **kwargs: Any,
    ) -> DataFrame:
        opts = QueryOpts()
        results = []
        for col in cols:
            df_tmp = df[[col]].explode(col).dropna(subset=[col])

            query = df_tmp[col].drop_duplicates().reset_index(drop=True)
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
                df_tmp.merge(result, left_on=col, right_on="query_name", how="left")
                .drop(columns=["query_name"])
                .dropna(subset=[col])
                .explode(opts.prefix + self.target)
                .explode(opts.prefix + self.target)
                .reset_index(drop=True)
            )
            result = result.rename(
                columns={
                    col: opts.prefix + self.target + "_query",
                    opts.prefix + self.target: self.target,
                }
            )
            result.loc[:, opts.prefix + "col"] = col
            results.append(result)
        result = pd.concat(results).reset_index(drop=True)
        result.loc[:, "_harmonized"] = ~result[self.target].isna()
        return result

    def query(
        self,
        query_name: str,
        **kwargs: Any,
    ) -> Optional["QueryResult"]:
        return self.preon.query(
            query_name,
            **kwargs,
        )


def get_harmonized(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    return cast(pd.DataFrame, df.loc[df["_harmonized"], cols])


def get_not_harmonized(df: pd.DataFrame, cols: Sequence[str]) -> pd.DataFrame:
    return cast(pd.DataFrame, df.loc[~df["_harmonized"], cols])
