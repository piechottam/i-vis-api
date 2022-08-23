from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence, Tuple, Type, cast

from pandas import DataFrame, Series
from sqlalchemy import func, literal, or_, tuple_

from . import config_meta, session
from .core_types import variant_utils
from .core_types.gene import meta as gene_meta
from .core_types.variant import meta as variant_meta
from .etl import ETL
from .plugin import CoreType

if TYPE_CHECKING:
    from sqlalchemy.orm import Query

    from .core_types.variant.models import HarmonizedVariant
    from .db_utils import RawData


LIMIT = 2
_LIMIT_VAR = config_meta.register_core_variable(
    "RETRIEVE_LIMIT", required=False, default=LIMIT
)


def parse_core_types_args(
    core_types: Sequence[CoreType], query_args: Mapping[str, Any]
) -> Mapping[CoreType, Mapping[str, Any]]:
    return {
        core_type: core_type.harm_meta.parse_args(query_args)
        for core_type in core_types
    }


def parse_exposed_args(etl: ETL, query_args: Mapping[str, Any]) -> Mapping[str, Any]:
    exposed_args = {}
    exposed_cols = set(etl.raw_columns.exposed_cols)
    for key, value in query_args.items():
        if key in exposed_cols:
            exposed_args[key] = value
    return exposed_args


def parts_by_non_hgvs(
    raw_model: Type["RawData"],
    exposed2arg: Optional[Mapping[str, Any]] = None,
    core_type2kwargs: Optional[Mapping["CoreType", Mapping[str, Any]]] = None,
    db_query: Optional["Query[Any]"] = None,
) -> "Query[Any]":
    exposed2arg = exposed2arg or {}
    core_type2kwargs = core_type2kwargs or {}

    etl = raw_model.get_etl()
    if not db_query:
        db_query = session.query(raw_model)

    if exposed2arg:
        db_query = db_query.filter_by(**exposed2arg)
    if core_type2kwargs:
        variant_core_type = CoreType.get_by_target("hgvs")
        for core_type, key2values in core_type2kwargs.items():
            if core_type == variant_core_type:
                continue

            model = etl.core_type2model[core_type]
            db_query = db_query.join(model)
            for key, value in key2values.items():
                if isinstance(value, list):
                    db_query = db_query.filter(getattr(model, key).in_(value))
                else:
                    db_query = db_query.filter(getattr(model, key) == value)
    return db_query


def etl_by_non_hgvs(
    etl: ETL,
    core_type: CoreType,
    exposed2kwargs: Optional[Mapping[str, Any]] = None,
    core_type2kwargs: Optional[Mapping["CoreType", Mapping[str, Any]]] = None,
) -> "Query[Any]":
    harmonized_model = etl.harmonized_model(core_type)
    target = core_type.harm_meta.target
    target_column = getattr(harmonized_model, target)
    db_query = session.query(
        literal(etl.part).label("part"),
        target_column.label(target),
        func.count(target_column).label(f"{core_type.short_name}_count"),
    ).group_by(target_column)

    # TODO add exposed args
    print(f"{core_type2kwargs=}")
    if core_type2kwargs:
        variant_core_type = CoreType.get_by_target("hgvs")
        for core_type_, key2values in core_type2kwargs.items():
            if core_type == variant_core_type:
                continue

            model = etl.core_type2model[core_type_]
            if model != harmonized_model:
                db_query = db_query.join(
                    model, harmonized_model.i_vis_raw_data_id == model.i_vis_raw_data_id
                )
            for key, value in key2values.items():
                if isinstance(value, list):
                    db_query = db_query.filter(getattr(model, key).in_(value))
                else:
                    db_query = db_query.filter(getattr(model, key) == value)
    return db_query


DEFAULT_HGVS_RESULT_MATCH: Sequence[variant_utils.MatchFlag] = [
    variant_utils.MatchFlag.RAW,
    variant_utils.MatchFlag.HGVS1,
]


def _process_query(hgvs: Sequence[str]) -> DataFrame:
    from .core_types.variant.harmonizer import split_hgvs

    query = split_hgvs(Series(hgvs))
    query["raw"] = hgvs
    return query


def _get_match_obs(
    hgvs_harm_model: Type["HarmonizedVariant"], query: DataFrame
) -> Sequence[Tuple[variant_utils.MatchFlag, Any, Any]]:

    return (
        (variant_utils.MatchFlag.RAW, hgvs_harm_model.hgvs, query["raw"]),
        (variant_utils.MatchFlag.REF, hgvs_harm_model.ref, query["ref"]),
        (
            variant_utils.MatchFlag.REF_VERSION,
            hgvs_harm_model.ref_version,
            query["ref_version"],
        ),
        (
            variant_utils.MatchFlag.DESC_TYPE,
            hgvs_harm_model.desc_type,
            query["desc_type"],
        ),
        (variant_utils.MatchFlag.DESC, hgvs_harm_model.desc, query["desc"]),
    )


def _harmonize_ref(etl: "ETL", query: DataFrame) -> None:
    gene_core_type = CoreType.get(gene_meta.name)
    harmonized_genes = etl.core_type2model[gene_core_type]
    # harmonize REFerence
    result = gene_core_type.harmonizer.harmonize(df=query, cols=["ref"])
    # TODO add extra columns hgnc_id -> list of harmonized
    breakpoint()


def _process_hgvs_result_match(
    hgvs_result_match: int, hgvs_harm_model: Type["HarmonizedVariant"], query: DataFrame
) -> Any:
    harm_model_cols = []
    query_cols = []
    for match_flag, harm_model_col, query_col in _get_match_obs(hgvs_harm_model, query):
        if match_flag & hgvs_result_match:
            harm_model_cols.append(harm_model_col)
            query_cols.append(query_col)
    if len(harm_model_cols) == 1:
        return harm_model_cols[0] == query_cols[0]

    if len(harm_model_cols) > 1:
        return tuple_(*harm_model_cols) == zip(*query_cols)

    raise ValueError("Cannot be < 1 - wrong universe...")


# pylint: disable=too-many-arguments
def parts_by_hgvs(
    raw_model: Type["RawData"],
    hgvs: Sequence[str],
    hgvs_query_modify: int = variant_utils.ModifyFlag.NONE,
    hgvs_result_modify: int = variant_utils.ModifyFlag.NONE,
    hgvs_result_matches: Optional[Sequence[variant_utils.MatchFlag]] = None,
    db_query: Optional["Query[Any]"] = None,
) -> "Query[Any]":
    query = _process_query(hgvs)
    hgvs_result_matches = hgvs_result_matches or list(DEFAULT_HGVS_RESULT_MATCH)
    etl = raw_model.get_etl()

    hgvs_core_type = CoreType.get(variant_meta.name)
    hgvs_harm_model = cast(
        Type["HarmonizedVariant"], etl.core_type2model[hgvs_core_type]
    )

    if hgvs_query_modify & variant_utils.ModifyFlag.MAP_REF_TO_HGNC_ID:
        _harmonize_ref(etl, query)  # TODO use this query

    if not db_query:
        db_query = session.query(raw_model)
    db_query = db_query.join(hgvs_harm_model)

    or_conds = []
    for hgvs_result_match in hgvs_result_matches:
        or_conds.append(
            _process_hgvs_result_match(hgvs_result_match, hgvs_harm_model, query)
        )

    db_query = db_query.filter(or_(*or_conds))
    if hgvs_result_modify:
        db_query.filter(hgvs_harm_model.flags & hgvs_result_modify)
    return db_query


def raw_data(raw_model: Type["RawData"], db_query: "Query[Any]") -> "Query[Any]":
    return session.query(raw_data).filter(raw_model.i_vis_id.in_(db_query))
