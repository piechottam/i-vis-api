from typing import (
    Any,
    Callable,
    cast,
    Union,
    MutableMapping,
    MutableSequence,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Type,
)
import itertools

from flask import url_for
from flask.views import MethodView
from sqlalchemy import union_all

from i_vis.core.blueprint import (
    Blueprint,
    DictPager,
    DictWrapper,
    QueryPager,
    QueryWrapper,
)

# pylint: disable=unused-import
from .. import db, ma
from .. import arguments, schemas
from ..plugin import CoreType, DataSource
from ..etl import etl_registry
from ..query_utils import (
    # TODO parts_by_non_hgvs,
    # TODO parts_by_hgvs,
    parse_core_types_args,
    parse_exposed_args,
    etl_by_non_hgvs,
)
from ..utils import clean_query_args

if TYPE_CHECKING:
    from ..harmonizer import QueryResult

GetCallback = Callable[[Any], Any]


def format_by_target(core_type: CoreType, name: str) -> str:
    """Format endpoint name"""
    return name.format(target=core_type.harm_meta.target)


def register_list(
    core_type_name: str,
    blp: Blueprint,
    endpoint: str = "list",
) -> Tuple[str, GetCallback]:
    core_type = CoreType.get(core_type_name)

    @blp.arguments(arguments.Restricted, location="query")
    @blp.response(200, schemas.response("CoreType", schemas.CoreTypeDesc()))
    def get(_self: Any, _query_args: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            schemas.ENVELOPE: core_type,
            "links": {
                "self": url_for(f"{core_type.blueprint_name}.list"),
                "collection": url_for("api.core-types"),
            },
        }

    get.__doc__ = f"""Show details counts for '{core_type.short_name}'

        ---
        """

    generic = type("Generic", (MethodView,), {"get": get})
    return endpoint, cast(
        GetCallback,
        blp.route("", endpoint=endpoint)(generic),
    )


def register_stats(
    core_type_name: str,
    blp: Blueprint,
    endpoint: str = "stats",
) -> Tuple[str, GetCallback]:
    core_type = CoreType.get(core_type_name)

    @blp.arguments(arguments.Restricted, location="query")
    @blp.response(
        200,
        schemas.response(
            "CoreTypeCount",
            schemas.CoreTypeCount,
            many=True,
        ),
    )
    # pylint: disable=unused-argument
    def get(_self: Any, *_args: Any, **_kwargs: Any) -> Any:
        return {
            schemas.ENVELOPE: generic_stats(core_type),
            "links": {
                "self": url_for(f"{core_type.blueprint_name}.stats"),
                # "collection": url_for(f"{core_type.blueprint_name}.show"),
            },
        }

    get.__doc__ = f"""Show counts for '{core_type.short_name}s' in integrated parts

        ---
        """

    generic = type("Generic", (MethodView,), {"get": get})
    return endpoint, cast(GetCallback, blp.route("/stats", endpoint=endpoint)(generic))


# pylint: disable=too-many-arguments
def register_browse(
    core_type_name: str,
    blp: Blueprint,
    model: db.Model,
    schema: ma.Schema,
    column: db.Column,
    endpoint: str = "",
) -> Tuple[str, GetCallback]:
    core_type = CoreType.get(core_type_name)

    endpoint = endpoint or "browse-" + core_type.harm_meta.target

    @blp.arguments(
        arguments.paginated(
            core_type.short_name,
            dict(core_type.harm_meta.default_fields),
        ),
        location="query",
    )
    @blp.response(
        200,
        schemas.paginated(
            core_type.short_name.title(),
            schema,
            many=True,
        ),
    )
    @blp.keyset_paginate(QueryPager())
    def get(_self: Any, query_args: Mapping[str, Any], **_kwargs: Any) -> Any:
        args = dict(query_args)
        args.pop("token", None)
        self_args = dict(args)
        args.pop("current_id", None)
        collection_args = dict(args)

        def helper(next_id: int) -> str:
            collection_args["next"] = next_id
            return url_for(
                f"{core_type.blueprint_name}.{endpoint}", **{"current_id": next_id}
            )

        return QueryWrapper(
            column,
            db.session.query(model),
            links={
                "self": url_for(f"{core_type.blueprint_name}.{endpoint}", **self_args),
                "collection": url_for(
                    f"{core_type.blueprint_name}.{endpoint}", **collection_args
                ),
                "next": helper,
            },
        )

    get.__doc__ = f"""Browse '{core_type.short_name}s' of integrated parts

    ---
    """

    generic = type("Generic", (MethodView,), {"get": get})
    return endpoint, cast(
        GetCallback,
        blp.route(
            f"/{core_type.harm_meta.target}", endpoint=endpoint, strict_slashes=False
        )(generic),
    )


def register_show(
    core_type_name: str,
    blp: Blueprint,
    model: Type[Any],
    schema: ma.Schema,
    target: str,
    field: ma.Field,
    endpoint: str,
) -> Tuple[str, GetCallback]:
    core_type = CoreType.get(core_type_name)

    @blp.arguments(arguments.Restricted, location="query")
    @blp.arguments(ma.Schema.from_dict({target: field}, name=""), location="view_args")
    @blp.response(200, schema)
    def get(
        _self: Any,
        _query_args: Mapping[str, Any],
        view_args: Mapping[str, Any],
        **_kwargs: Any,
    ) -> Any:
        return db.session.query(model).get(view_args[target])

    get.__doc__ = f"""Show specific '{core_type.short_name}' using '{target}'

    ---
    """

    generic = type("Generic", (MethodView,), {"get": get})
    return endpoint, cast(
        GetCallback,
        blp.route(
            f"/{target.removesuffix('_id').replace('_', '-')}/<{target}>",
            endpoint=endpoint,
        )(generic),
    )


def register_harmonizer(
    core_type_name: str, blp: Blueprint, endpoint: str = "harmonizer"
) -> Tuple[str, GetCallback]:
    core_type = CoreType.get(core_type_name)

    @blp.arguments(arguments.harmonizer(), location="query")
    @blp.response(
        200,
        schemas.response(
            core_type_name.title() + "HarmonizerResult",
            schemas.harmonizer_result(core_type_name),
            many=True,
        ),
    )
    def get(_self: Any, query_args: MutableMapping[str, Any]) -> Mapping[str, Any]:
        query_args.pop("token", None)
        return {
            schemas.ENVELOPE: generic_map(
                core_type,
                query_args["query"],
                query_args["match_type"],
                # FUTURE remove comment but format response query_args["extended_meta"],
            ),
            "links": {
                "self": url_for(
                    f"{core_type.blueprint_name}.{endpoint}",
                    **clean_query_args(query_args),
                ),
            },
        }

    get.__doc__ = f"""Harmonize '{core_type.short_name}(s)' to '{",".join(core_type.harm_meta.targets)}'

    ---
    """

    generic = type("Generic", (MethodView,), {"get": get})
    return endpoint, cast(
        GetCallback, blp.route("/harmonizer", endpoint=endpoint)(generic)
    )


def register_explorer(
    core_type_name: str, blp: Blueprint, endpoint: str = "explorer"
) -> Tuple[str, GetCallback]:
    core_type = CoreType.get(core_type_name)

    # pylint: disable=too-many-locals
    @blp.arguments(
        arguments.paginated(core_type_name, arguments.explorer_fields(core_type_name)),
        location="query",
    )
    @blp.response(200, schemas.explorer(core_type))
    @blp.keyset_paginate(DictPager())
    def get(_self: Any, query_args: Mapping[str, Any]) -> DictWrapper:
        print(f"{query_args=}")

        query_core_types = set(parse_core_types_args(CoreType.instances(), query_args))
        query_core_types.add(core_type)

        results: MutableMapping[str, Any] = {}
        db_queries: MutableSequence[db.Query] = []
        for part in query_args["part"]:
            etl = etl_registry.by_part(part)
            assert etl is not None
            if not query_core_types.issubset(etl.core_types):
                continue

            exposed_args = parse_exposed_args(etl, query_args)
            core_type2kwargs = parse_core_types_args(list(etl.core_types), query_args)
            db_query = etl_by_non_hgvs(etl, core_type, exposed_args, core_type2kwargs)
            print("\n")
            print(f"{str(db_query)=}")
            db_queries.append(db_query)

        for part, target_value, count in db.session.execute(
            union_all(*db_queries)
        ).all():
            etl = etl_registry.by_part(part)
            assert etl is not None

            exposed_args = parse_exposed_args(etl, query_args)
            core_type2kwargs = parse_core_types_args(list(etl.core_types), query_args)
            kwargs = {}
            for core_type_ in etl.core_types:
                for key, values in clean_query_args(
                    core_type2kwargs.get(core_type_, {})
                ).items():
                    kwargs[key] = values
            part_info = {
                "count": count,
                "links": {
                    "parts": url_for(f"{etl.pname}.browse-{part}", **kwargs),
                },
            }
            results.setdefault(
                target_value,
                {
                    "id": target_value,
                    f"{core_type.harm_meta.target}": target_value,
                    "type": core_type.short_name,
                    "links": {
                        "related": url_for(
                            f"{core_type.blueprint_name}.show-{core_type.harm_meta.target}",
                            **{core_type.harm_meta.target: target_value},
                        )
                    },
                },
            ).setdefault("parts", {}).setdefault(part, part_info)

        def helper(next_id: int) -> str:
            query_kwargs = clean_query_args(query_args)
            query_kwargs["next"] = next_id
            return url_for(
                f"{core_type.blueprint_name}.{endpoint}",
                **query_kwargs,
            )

        links: MutableMapping[str, Union[str, Callable[[int], str]]] = {
            "self": url_for(
                f"{core_type.blueprint_name}.{endpoint}", **clean_query_args(query_args)
            ),
            "next": helper,
        }
        return DictWrapper(results=results, links=links)

    get.__doc__ = f"""Show specific '{core_type.short_name}' using '{core_type.harm_meta.target}'

        ---
        """

    generic = type("Generic", (MethodView,), {"get": get})
    return endpoint, cast(
        GetCallback,
        blp.route("/explorer", endpoint=endpoint)(generic),
    )


def generic_stats(core_type: "CoreType") -> Sequence[Mapping[str, int]]:
    stats = []
    for etl in etl_registry.by_core_type(core_type):
        data_source = DataSource.get(etl.pname)
        if not data_source or not data_source.installed:
            continue
        assert data_source.updater.current is not None

        harmonized_model = etl.core_type2model[core_type]
        harmonized_table_update = data_source.updater.current.table_updates[
            harmonized_model.__tablename__
        ]
        harmonized = harmonized_table_update.row_count

        if core_type.harm_meta.target is not None:
            target_attrs = [getattr(harmonized_model, core_type.harm_meta.target)]
        else:
            target_attrs = [
                getattr(harmonized_model, target)
                for target in core_type.harm_meta.targets
            ]
        q = db.session.query(harmonized_model)
        for target_attr in target_attrs:
            q = q.with_entities(target_attr)
        q = q.distinct()
        unique = q.count()

        stats.append(
            {
                "core_type_name": core_type.blueprint_name,
                "data_source": data_source.name,
                "part": etl.part,
                "harmonized": harmonized,
                "unique": unique,
            }
        )

    return stats


def generic_map(
    core_type: "CoreType",
    queries: Sequence[str],
    match_type: Optional[Sequence[str]] = None,
    extended_meta: bool = False,
) -> Sequence[Mapping[str, Any]]:
    results = []
    if isinstance(queries, str):
        queries = [queries]
    else:
        queries = list(set(queries))
    for query in queries:
        harm_res = core_type.harmonizer.query(
            query, match_type=match_type, extended_meta=extended_meta
        )
        result = _process_harm_res(query, core_type, harm_res)
        results.append(result)
    return results


def _process_harm_res(
    query: str, core_type: "CoreType", harm_res: Optional["QueryResult"]
) -> Mapping[str, Any]:
    matches: MutableSequence[MutableMapping[str, Any]] = []
    result = {
        "query": query,
        "matches": matches,
        "match_type": None,
    }
    if not harm_res:
        return result

    harm_meta = core_type.harm_meta
    matched_names, matched_targets, meta_info = harm_res
    result_mapping = {
        getattr(gene, harm_meta.target): gene
        for gene in db.session.query(core_type.model)
        .filter(
            getattr(
                core_type.model,
                harm_meta.target,
            ).in_(itertools.chain.from_iterable(matched_targets))
        )
        .all()
    }

    result["match_type"] = meta_info["match_type"]
    for matched_name, matched_targets_ in zip(matched_names, matched_targets):
        for matched_target in matched_targets_:
            matches.append(
                {
                    "matched_name": matched_name,
                    f"{harm_meta.target}": matched_target,
                    "links": {
                        "related": url_for(
                            f"{core_type.blueprint_name}.show-{harm_meta.target}",
                            **{harm_meta.target: matched_target},
                        ),
                    },
                }
            )
    return result_mapping


def get_links(_schema: ma.Schema, core_type_name: str, obj: Any) -> Mapping[str, Any]:
    core_type = CoreType.get(core_type_name)
    target = core_type.harm_meta.target
    return {
        "self": url_for(
            f"{core_type.blueprint_name}.show-{target}",
            **{
                target: getattr(obj, target),
            },
        ),
        "collection": url_for(f"{core_type.blueprint_name}.browse-{target}"),
    }


def create_name2endpoint(
    services: Sequence[Tuple[str, GetCallback]], names: Optional[Sequence[str]] = None
) -> Mapping[str, str]:
    names = names or [""] * len(services)
    assert len(services) == len(names)
    return {name or endpoint: endpoint for name, (endpoint, _) in zip(names, services)}
