from typing import Any, Optional, Sequence, cast

import pandas as pd
from pandas import DataFrame, Series

from ...harmonizer import Harmonizer, QueryOpts
from ...preon import QueryResult
from ..variant_utils import (
    AA1_REGEX,
    AAC_REGEX,
    HGVS_DESC_TYPE,
    HGVS_LIKE_REGEX,
    REF_VERSION_REGEX,
    VARIANT_DESC_LEN,
    VARIANT_LEN,
    VARIANT_REF_LEN,
    ModifyFlag,
    harmonize_aa1_to_aa3,
)


# noinspection PyAbstractClass
class Simple(Harmonizer):
    def _init(self) -> None:
        pass

    @property
    def input(self) -> str:
        return "hgvs"

    @staticmethod
    def clean(s: Series) -> Series:
        """

        Args:
            s:

        Returns:

        """
        invalid = s.apply(str).str.len().lt(VARIANT_LEN)
        if invalid.any():
            s[invalid] = ""
        return s

    @property
    def deduplicate(self) -> bool:
        return False

    @staticmethod
    def _harm_hgvs(
        variant_names: DataFrame,
        is_hgvs: Series,
        col: str,
        static_cols: Sequence[str],
    ) -> DataFrame:
        hgvs_names = variant_names.loc[is_hgvs, col]
        hgvs_names = split_hgvs(hgvs_names)
        hgvs_names["hgvs"] = variant_names.loc[is_hgvs, col]

        # filter too long variants
        too_long_ref = cast(Series, hgvs_names["ref"].str.len() > VARIANT_REF_LEN)
        if too_long_ref.any():
            hgvs_names = hgvs_names[~too_long_ref]

        too_long_desc = cast(Series, hgvs_names["desc"].str.len() > VARIANT_DESC_LEN)
        if too_long_desc.any():
            hgvs_names = hgvs_names[~too_long_desc]

        hgvs_names["modify_flags"] = 0
        hgvs_names[static_cols] = variant_names.loc[is_hgvs, static_cols]

        p_type = hgvs_names["desc_type"].eq("p")
        if p_type.any():
            is_aa1 = hgvs_names["desc"].str.match(AA1_REGEX)
            combined = is_aa1 & p_type
            if combined.any():
                hgvs_names.loc[combined, "desc"] = harmonize_aa1_to_aa3(
                    hgvs_names.loc[combined, "desc"]
                )
                hgvs_names["modify_flags"] += ModifyFlag.AA1_TO_AA3

        return cast(DataFrame, hgvs_names)

    @staticmethod
    def _process_unknown_type(hgvs_names: DataFrame) -> Series:
        unknown_type = hgvs_names["desc_type"].eq("")
        if unknown_type.any():
            is_acc = hgvs_names.loc[unknown_type, "desc"].str.match(AAC_REGEX)
            if is_acc.any():
                combined = unknown_type & is_acc
                hgvs_names.loc[combined, "desc_type"] = "p"
                hgvs_names.loc[combined, "modify_flags"] += ModifyFlag.GUESSED_DESC_TYPE
        unknown_type = hgvs_names["desc_type"].eq("")
        return cast(Series, unknown_type)

    # pylint: disable=too-many-locals
    def harmonize(
        self,
        df: DataFrame,
        cols: Sequence[str],
        opts: Optional[QueryOpts] = None,
        **kwargs: Any
    ) -> DataFrame:
        # TODO index
        static_cols = df.columns.difference(cols).to_list()

        results = []
        unknown_desc_type = []
        not_hgvs = []

        for col in cols:
            variant_names = df[static_cols + [col]].dropna().explode(col)

            # filter too long variants
            too_long_var = cast(Series, variant_names[col].str.len().gt(VARIANT_LEN))
            if too_long_var.any():
                variant_names = variant_names[~too_long_var]

            is_hgvs = variant_names[col].str.match(HGVS_LIKE_REGEX)
            if is_hgvs.any():
                hgvs_names = self._harm_hgvs(variant_names, is_hgvs, col, static_cols)
                results.append(hgvs_names)

                unknown_type = self._process_unknown_type(hgvs_names)
                if unknown_type.any():
                    # TODO why this?
                    breakpoint()
                    unknown_desc_type.append(hgvs_names[unknown_type])

            if ~is_hgvs.any():
                not_hgvs.append(variant_names.loc[~is_hgvs, [col]])

        if results:
            df = pd.concat(results, ignore_index=True)
            df["unmapped_type"] = ""
        else:
            df = DataFrame()

        if unknown_desc_type:
            unmapped = pd.concat(unknown_desc_type, ignore_index=True)
            unmapped["unmapped_type"] = "unknown_desc_type"
            df = pd.concat([df, unmapped], ignore_index=True)

        if not_hgvs:
            unmapped = pd.concat(not_hgvs, ignore_index=True)
            unmapped["unmapped_type"] = "not_hgvs"
            df = pd.concat([df, unmapped], ignore_index=True)

        df["harmonized"] = df["unmapped_type"].neq("")
        return df

    def clear(self) -> None:
        pass

    def query(self, query_name: str, **kwargs: Any) -> Optional[QueryResult]:
        raise NotImplementedError


def parse_desc(desc: Series) -> DataFrame:
    desc_types = desc.str.extract(HGVS_DESC_TYPE)
    desc_types.columns = ["desc_type", "desc"]
    return cast(DataFrame, desc_types)


def parse_ref(ref: Series) -> DataFrame:
    df = DataFrame({"ref": ref, "ref_version": len(ref) * ""})
    if ref.str.match(REF_VERSION_REGEX).any():
        df = (
            ref.str.split(r"\.", expand=True)
            .rename(
                columns={
                    0: "ref",
                    1: "ref_version",
                }
            )
            .fillna("")
        )
    return df


def split_hgvs(variants: Series) -> DataFrame:
    hgvs_vars = variants.str.split(":", expand=True).rename(
        columns={
            0: "ref",
            1: "desc",
        }
    )
    #
    return pd.concat(
        [
            parse_desc(hgvs_vars["desc"]),
            parse_ref(hgvs_vars["ref"]),
        ],
        axis=1,
    )


def clean_hgvs(variants: Series) -> Series:
    hgvs_vars = split_hgvs(variants)
    desc_type = hgvs_vars["type"]
    has_desc_type = desc_type.ne()
    if has_desc_type.any():
        desc_type[has_desc_type] = desc_type[has_desc_type] + "."
    return hgvs_vars["ref"] + ":" + desc_type + hgvs_vars["raw_desc"]
