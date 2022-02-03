from typing import (
    Any,
    cast,
    MutableSequence,
    MutableMapping,
    Optional,
    Sequence,
    Union,
    Tuple,
    MutableSet,
    TYPE_CHECKING,
)
import collections
import collections.abc
import re
import time
from itertools import chain

import jellyfish
import numpy as np
import pandas as pd
from nltk import ngrams
from pandas import DataFrame, Series
from tqdm import tqdm

from .df_utils import normalize_columns

if TYPE_CHECKING:
    pass

TRANSFORM_REGEX = re.compile(r"[^a-zA-z0-9]+")
# filter drug_names: PREFIX111 and 111
FILTER_REGEX = re.compile("^[a-zA-Z]*[0-9]+")

Plugins = MutableSet[str]
Strs = Union[Series, Sequence[Optional[str]]]
QueryResult = Tuple[Sequence[str], Sequence, MutableMapping]
ExtendedQueryResult = Tuple[Sequence[str], Sequence[Sequence[str]], MutableMapping]


def transform_name(name: str) -> Optional[str]:
    """Normalize name"""
    if not isinstance(name, str):
        return None

    return re.sub(TRANSFORM_REGEX, "", name).lower()


# Modified preon
class PreonModified:
    def __init__(
        self,
        name_type: str = "Unknown",
        name_id_type: str = "Unknown",
        match_types: Optional[Sequence[str]] = None,
    ):
        # core type -> relation type, ids
        # self._core_types: MutableMapping[str, Tuple[str, str]] = {}
        # name -> type, -> plugin, id
        self._names: MutableMapping[str, Tuple[str, MutableMapping[str, Plugins]]] = {}
        # id -> type, plugins
        self._ids: MutableMapping[str, Tuple[str, Plugins]] = {}

        self._name_type = name_type
        self._name_id_type = name_id_type

        if not match_types:
            self._match_types: Sequence[str] = [
                "direct",
                "exact",
                "substring",
                "partial",
            ]
        else:
            self._match_types = match_types

    def fit(
        self, names: Sequence[str], ids: Sequence[str], transform_ids: bool = True
    ) -> None:
        transformed_names = [transform_name(name) for name in names]
        if transform_ids:
            transformed_names = transformed_names + [transform_name(id_) for id_ in ids]
            transformed_ids = 2 * list(ids)
            self.add(transformed_names, transformed_ids, "Unknown")
        else:
            self.add(transformed_names, ids, "Unknown")

    # pylint: disable=too-many-arguments,too-many-locals
    def add(
        self,
        names: Strs,
        name_ids: Strs,
        plugins: Union[str, Strs],
        name_types: Optional[Strs] = None,
        name_id_types: Optional[Strs] = None,
    ) -> "PreonModified":
        assert len(names) == len(name_ids)
        for values in (plugins, name_types, name_id_types):
            if (
                values is not None
                and not isinstance(values, str)
                and isinstance(values, collections.Iterable)
                and isinstance(values, collections.abc.Sized)
                and len(values) > 1
            ):
                assert len(names) == len(values)

        if not name_types:
            name_types = len(names) * [self._name_type]
        if not name_id_types:
            name_id_types = len(name_ids) * [self._name_id_type]
        if isinstance(plugins, str):
            plugins = len(names) * [plugins]

        rows = zip(names, name_ids, plugins, name_types, name_id_types)
        for name, name_id, plugin, name_type, name_id_type in rows:
            # name
            if name is not None:
                old_name_type, name_ids_ = self._names.setdefault(name, (name_type, {}))
                name_ids_.setdefault(name_id, set()).add(plugin)
                assert old_name_type == name_type

            # name id
            old_name_id_type, id_plugins = self._ids.setdefault(
                name_id, (name_id_type, set())
            )
            id_plugins.add(plugin)
            assert old_name_id_type == name_id_type

        return self

    def _get_query_result(
        self, found_names: Sequence[str], meta_info: MutableMapping[str, Any]
    ) -> QueryResult:
        found_names = cast(Sequence[Any], np.unique(found_names).tolist())
        name_ids = [
            np.unique(list(self._names[found_name][1].keys())).tolist()
            for found_name in found_names
        ]
        return found_names, name_ids, meta_info

    def _get_extended_query_result(
        self, found_names: Sequence[str], meta_info: MutableMapping[str, Any]
    ) -> QueryResult:
        found_names = cast(Sequence[Any], np.unique(found_names).tolist())

        meta_info["name_type"] = []
        meta_info["name_plugins"] = []

        name_ids = []
        for found_name in found_names:
            name_type, name_ids_ = self._names[found_name]
            name_ids.append(list(name_ids_.keys()))
            meta_info["name_type"].append(name_type)
            meta_info["name_plugins"].append(list(name_ids_.values()))
        return found_names, name_ids, meta_info

    def direct(
        self,
        raw_query: str,
        transformed_query: str,
        extended_meta: bool = False,
        **_kwargs: Any,
    ) -> Optional[QueryResult]:
        if raw_query not in self._ids:
            return None

        meta_info: MutableMapping[str, Any] = {"match_type": "direct"}
        if extended_meta:
            name_id_type, plugins = self._ids[transformed_query]
            meta_info["name_id_type"] = [name_id_type]
            meta_info["name_id_plugins"] = [plugins]
            return [], [[raw_query]], meta_info
        return [], [raw_query], meta_info

    # pylint: disable=unused-argument
    def exact(
        self,
        raw_query: str,
        transformed_query: str,
        extended_meta: bool = False,
        **_: Any,
    ) -> Optional[QueryResult]:
        if transformed_query not in self._names:
            return None

        # try to find the trivial match
        meta_info = {"match_type": "exact"}

        if extended_meta:
            return self._get_extended_query_result([transformed_query], meta_info)
        return self._get_query_result([transformed_query], meta_info)

    # pylint: disable=unused-argument,too-many-arguments
    def substring(
        self,
        raw_query: str,
        transformed_query: str,
        n_grams: int = 1,
        sep: str = " ",
        extended_meta: bool = False,
    ) -> Optional[QueryResult]:
        substrings = raw_query.split(sep)

        for i_gram in range(2, n_grams + 1):
            grams = ngrams(substrings, i_gram)
            grams = [sep.join(gram) for gram in grams]
            substrings.extend(grams)

        matches = []

        for token in substrings:
            _token = transform_name(token)
            if not _token:
                continue
            if _token in self._names:
                matches.append(_token)

        if len(matches) == 0:
            return None

        # try to find trivial substring match
        meta_info = {"match_type": "substring"}

        if extended_meta:
            return self._get_extended_query_result(matches, meta_info)
        return self._get_query_result(matches, meta_info)

    def filtered_partial(
        self, transformed_query: str, **kwargs: Any
    ) -> Optional[QueryResult]:
        if re.match(FILTER_REGEX, transformed_query):
            return None

        return self.partial(transformed_query=transformed_query, **kwargs)

    def partial(
        self,
        raw_query: str,
        transformed_query: str,
        threshold: float = 0.2,
        extended_meta: bool = False,
    ) -> Optional[QueryResult]:
        names = list(self._names)
        distances = []

        # calculate distances for query string
        for name in names:
            dist = jellyfish.levenshtein_distance(transformed_query, name) / max(
                len(transformed_query), len(name)
            )
            distances.append(dist)

        distance_array = np.array(distances)

        if np.min(distances) > threshold:
            return None

        meta_info = {
            "match_type": "partial",
            "edit_distance": np.min(distance_array),
        }
        names_idx = np.isin(distance_array, meta_info["edit_distance"])
        name_array = np.array(names)

        if extended_meta:
            return self._get_extended_query_result(
                name_array[names_idx].tolist(),
                meta_info,
            )
        return self._get_query_result(
            name_array[names_idx].tolist(),
            meta_info,
        )

    def query(
        self,
        query_name: str,
        match_types: Optional[Sequence[str]] = None,
        extended_meta: bool = False,
        **kwargs: Any,
    ) -> QueryResult:
        if not match_types:
            match_types = self._match_types

        transformed_query = transform_name(query_name)
        if not transformed_query:
            return [], [], {}

        for _match_type in match_types:
            func = getattr(self, _match_type)
            result = func(
                raw_query=query_name,
                transformed_query=transformed_query,
                extended_meta=extended_meta,
                **kwargs.get(_match_type, {}),
            )
            if result is not None:
                return cast(QueryResult, result)

        return [], [], {}

    def query_all(
        self, query: Series, progress: bool = True, **query_args: Any
    ) -> DataFrame:
        if progress:
            unit = query_args.get("target", "entity")
            tqdm.pandas(desc="Harmonizing", unit=unit, leave=False)
            res = query.progress_apply(self.query, **query_args)
        else:
            res = query.apply(self.query, **query_args)

        df = DataFrame.from_records(
            res.to_list(),
            columns=["Found Names", "Found Name IDs", "Meta Info"],
        )
        df["Query Name"] = query
        return df

    # pylint: disable=invalid-name
    # noinspection PyPep8Naming
    def transform(
        self, X: Sequence[str], verbose: int = 0, **query_args: Any
    ) -> DataFrame:
        results: MutableSequence[
            Tuple[
                str,
                Sequence[str],
                Sequence[str],
                str,
                Optional[float],
                float,
            ],
        ] = []

        for name in tqdm(X, disable=verbose < 1):
            query_time = time.process_time()
            result = self.query(name, **query_args)
            query_time = time.process_time() - query_time

            if not result:
                results.append((name, [], [], "none", None, query_time))
                continue

            found_names, found_ids, meta_info = result
            results.append(
                (
                    name,
                    found_names,
                    found_ids,
                    meta_info["match_type"],
                    meta_info.get("edit_distance", None),
                    query_time,
                )
            )

        df = pd.DataFrame.from_records(
            results,
            columns=[
                "Name",
                "Found Names",
                "Found Name IDs",
                "Match Type",
                "Edit Distance",
                "Query Time",
            ],
        )
        return df

    # noinspection PyPep8Naming
    def evaluate(self, X, y, verbose=0, **query_args):  # type: ignore
        df = self.transform(X, verbose=verbose, **query_args)

        correct_matches = []
        for name_ids, found_ids in zip(y, df["Found Name IDs"].tolist()):
            correct_matches.append(
                any(
                    name_id in list(chain.from_iterable(found_ids))
                    for name_id in name_ids
                )
            )

        df["Correct Match"] = correct_matches
        df["Name IDs"] = y

        return df[
            [
                "Name",
                "Found Names",
                "Name IDs",
                "Found Name IDs",
                "Match Type",
                "Correct Match",
                "Edit Distance",
                "Query Time",
            ]
        ]

    def assess(
        self,
        query: Series,
        expected: Series,
        progress: bool = True,
        extended_meta: bool = True,
        **query_args: Any,
    ) -> DataFrame:
        df = self.query_all(
            query, progress=progress, extended_meta=extended_meta, **query_args
        )

        df = normalize_columns(df)

        new_df = DataFrame({"query": query, "expected": expected})

        df = pd.concat([new_df, df], axis=1)

        prediction = []
        for expected_name_id, found_name_ids in zip(
            expected, df["found_name_ids"].tolist()
        ):
            found_name_ids = list(chain.from_iterable(found_name_ids))
            if not expected_name_id:
                if found_name_ids:
                    prediction.append("FP")
                else:
                    prediction.append("TN")
            elif expected_name_id in found_name_ids:
                prediction.append("TP")
            elif not found_name_ids:
                prediction.append("FN")
            else:
                prediction.append("FP")
        df["prediction"] = prediction

        return df
