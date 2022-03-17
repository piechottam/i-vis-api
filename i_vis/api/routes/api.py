from typing import (
    Any,
    cast,
    Mapping,
    Type,
    TYPE_CHECKING,
)

from flask.helpers import url_for
import flask
from flask.views import MethodView

# noinspection PyProtectedMember
from psutil import (
    cpu_percent,
    disk_usage,
    getloadavg,
    sensors_temperatures,
    virtual_memory,
    _common,
)

from i_vis.core.blueprint import Blueprint, QueryWrapper, QueryPager
from i_vis.core.login import admin_required

from .. import VERSION as I_VIS_VERSION, api_spec, db
from .. import arguments, schemas
from ..plugin import CoreType, DataSource
from ..query_utils import (
    parts_by_non_hgvs,
    parse_exposed_args,
    parse_core_types_args,
    parts_by_hgvs,
)
from ..terms import Term
from ..utils import API_URL_PREFIX, api_prefix, clean_query_args

if TYPE_CHECKING:
    from ..etl import ETL

api_blp = Blueprint(
    "api",
    __name__,
    url_prefix="",
    description="Services for API version(s)",
)


def api_version_desc(
    code_version: str,
    api_version: str = api_spec.spec.version,
) -> Mapping[str, Any]:
    return {
        "code_version": code_version,
        "api_version": api_version,
        "links": {
            "self": url_for("api.latest-version"),
            "collection": url_for("api.list-versions"),
            "terms": url_for("api.terms"),
            "dashboard": url_for("api.dashboard"),
            "core_types": url_for("api.core-types"),
            "data_sources": url_for("api.data-sources"),
        },
    }


@api_blp.route(API_URL_PREFIX, **{"endpoint": "list-versions"}, strict_slashes=False)
class ApiVersions(MethodView):
    @api_blp.arguments(arguments.Restricted, location="query")
    @api_blp.response(
        200, schemas.response("ApiVersions", schemas.ApiVersion, many=True)
    )
    # pylint: disable=no-self-use
    def get(self, _query_args: Any, **_kwargs: Any) -> Mapping[str, Any]:
        """List available API versions.

        Returns available API and current I-VIS build version info.
        ---
        """
        return {
            schemas.ENVELOPE: [api_version_desc(str(I_VIS_VERSION))],
            "links": {
                "self": url_for("api.list-versions"),
                "swagger": "/api/swagger",
                "redoc": "/api/redoc",
                "latest": url_for("api.latest-version"),
            },
        }


@api_blp.route(api_prefix(), **{"endpoint": "latest-version"}, strict_slashes=False)
@api_blp.route(
    api_prefix(), **{"endpoint": f"show-{api_spec.spec.version}"}, strict_slashes=False
)
class ApiVersion(MethodView):
    @api_blp.arguments(arguments.Restricted, location="query")
    @api_blp.response(200, schemas.response("ApiVersion", schemas.ApiVersion))
    # pylint: disable=unused-argument,no-self-use
    def get(
        self,
        _query_args: Any,
        **_kwargs: Any,
    ) -> Any:
        """Show specific API versions

        Returns specific API and I-VIS build version info.
        ---
        """

        return {
            schemas.ENVELOPE: api_version_desc(str(I_VIS_VERSION)),
            "links": {"self": url_for("api.show-v0")},
        }


@api_blp.route(api_prefix("terms"), **{"endpoint": "terms"})
class Terms(MethodView):
    @api_blp.arguments(arguments.Restricted, location="query")
    @api_blp.response(200, schemas.response("Terms", schemas.Terms))
    # pylint: disable=unused-argument,no-self-use
    def get(self, _query_args: Mapping[str, Any]) -> Mapping[str, Any]:
        """List all terms

        Returns all I-VIS terms used for describing raw data columns.
        ---
        """

        return {
            schemas.ENVELOPE: cast(Type[Term], Term),
            "links": {
                "self": url_for("api.terms"),
                "latest": url_for("api.latest-version"),
            },
        }


def machine_status() -> Mapping[str, Any]:
    disk = disk_usage("/")
    mem = virtual_memory()
    return {
        "disk": {
            "total": _common.bytes2human(disk.total),
            "used": _common.bytes2human(disk.used),
            "available": _common.bytes2human(disk.free),
        },
        "mem": {"total": _common.bytes2human(mem.total), "used": f"{mem.percent}%"},
        "cpu_usage": cpu_percent(percpu=True),
        "load": getloadavg(),
        "temperatures": sensors_temperatures(),
    }


@api_blp.route(api_prefix("dashboard"), **{"endpoint": "dashboard"})
# pylint: disable=unused-argument
class Dashboard(MethodView):
    @admin_required
    @api_blp.arguments(arguments.Restricted, location="query")
    @api_blp.response(200, schemas.response("MachineStatus", schemas.MachineStatus))
    # pylint: disable=unused-argument,no-self-use
    def get(self, _query_args: Mapping[str, Any]) -> Mapping[str, Any]:
        """Show summary of resources

        Returns a summary of resource usage of the machine that provides I-VIS API instance.
        ---
        """

        return {
            schemas.ENVELOPE: machine_status(),
            "links": {
                "self": url_for("api.dashboard"),
                "latest": url_for("api.latest-version"),
            },
        }


@api_blp.route(
    api_prefix("data-sources"), **{"endpoint": "data-sources"}, strict_slashes=False
)
class DataSources(MethodView):
    @api_blp.arguments(arguments.Restricted, location="query")
    @api_blp.response(
        200, schemas.response("DataSources", schemas.DataSourceDesc, many=True)
    )
    # pylint: disable=unused-argument,no-self-use
    def get(self, _query_args: Mapping[str, Any]) -> Mapping[str, Any]:
        data_sources = [
            data_source
            for data_source in DataSource.instances()
            if data_source.installed
        ]

        return {
            schemas.ENVELOPE: data_sources,
            "links": {
                "self": url_for("api.data-sources"),
                "latest": url_for("api.latest-version"),
            },
        }


@api_blp.route(
    api_prefix("core-types"), **{"endpoint": "core-types"}, strict_slashes=False
)
class CoreTypes(MethodView):
    @api_blp.arguments(arguments.Restricted, location="query")
    @api_blp.response(
        200, schemas.response("CoreTypes", schemas.CoreTypeDesc, many=True)
    )
    # pylint: disable=unused-argument,no-self-use
    def get(self, _query_args: Mapping[str, Any]) -> Mapping[str, Any]:
        core_types = [
            core_type for core_type in CoreType.instances() if core_type.installed
        ]

        return {
            schemas.ENVELOPE: core_types,
            "links": {
                "self": url_for("api.core-types"),
                "latest": url_for("api.latest-version"),
            },
        }


ds_bp = flask.Blueprint("ds", __name__)


def _create_data_source_blp(data_source: DataSource) -> Blueprint:
    pname = data_source.name
    data_source_blp = Blueprint(
        pname,
        __name__,
        url_prefix=api_prefix("data-sources", pname),
        description=f"Operations on integrated parts from plugin {pname}",
    )
    ds_bp.register_blueprint(data_source_blp)
    return data_source_blp


def _create_data_source_view(
    data_source: DataSource, data_source_blp: Blueprint
) -> Type[MethodView]:
    @data_source_blp.route("", **{"endpoint": "show"}, strict_slashes=False)
    class _DataSource(MethodView):
        _pname = data_source.name

        @data_source_blp.arguments(arguments.Restricted, location="query")
        @data_source_blp.response(
            200, schemas.response("DataSource", schemas.DataSourceDesc)
        )
        # pylint: disable=no-self-use
        def get(
            self,
            _query_args: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Mapping[str, Any]:
            return {
                schemas.ENVELOPE: DataSource.get(self._pname),
                "links": {
                    "self": url_for(f"{self._pname}.show"),
                    "collection": url_for("api.data-sources"),
                },
            }

    return _DataSource


def _create_part_browse_view(
    data_source: DataSource, data_source_blp: Blueprint, etl: "ETL"
) -> Type[MethodView]:
    part = etl.part

    @data_source_blp.route(
        f"/{part}", **{"endpoint": f"browse-{part}"}, strict_slashes=False
    )
    class _Parts(MethodView):
        @data_source_blp.arguments(
            arguments.paginated(part, arguments.generic_part_fields(part)),
            location="query",
        )
        @data_source_blp.response(
            200, schemas.paginated("PartsData", schemas.part_data(), many=True)
        )
        @data_source_blp.keyset_paginate(QueryPager())
        # pylint: disable=no-self-use
        def get(
            self,
            query_args: Mapping[str, Any],
            **_kwargs: Any,
        ) -> QueryWrapper:
            exposed_args = parse_exposed_args(etl, query_args)
            core_type2kwargs = parse_core_types_args(list(etl.core_types), query_args)
            raw_model = etl.raw_model

            variant_core_type = CoreType.get_by_target("hgvs")
            if (
                variant_core_type
                and variant_core_type in core_type2kwargs
                and core_type2kwargs[variant_core_type].get(
                    variant_core_type.harm_meta.target, None
                )
            ):
                db_query = parts_by_hgvs(
                    raw_model, **core_type2kwargs[variant_core_type]
                )
            else:
                db_query = db.session.query(raw_model)
            db_query = parts_by_non_hgvs(
                raw_model,
                exposed2arg=exposed_args,
                core_type2kwargs=core_type2kwargs,
            )

            args = clean_query_args(query_args)

            args.pop("token", None)
            self_args = dict(args)
            args.pop("current_id", None)
            collection_args = dict(args)

            def helper(next_id: int) -> str:
                collection_args["next"] = next_id
                return url_for(
                    f"{data_source.name}.browse-{part}",
                    **collection_args,
                )

            return QueryWrapper(
                column=etl.raw_model.i_vis_id,
                query=db_query,
                links={
                    "self": url_for(
                        f"{data_source.name}.browse-{part}",
                        **self_args,
                    ),
                    "collection": url_for(
                        f"{data_source.name}.browse-{part}",
                        **collection_args,
                    ),
                    "next": helper,
                },
            )

    return _Parts


def _create_part_show_view(
    data_source: DataSource, data_source_blp: Blueprint, etl: "ETL"
) -> Type[MethodView]:
    part = etl.part

    @data_source_blp.route(f"/{part}/<id>", **{"endpoint": f"show-{part}"})
    class _Part(MethodView):
        @data_source_blp.arguments(arguments.Restricted, location="query")
        @data_source_blp.arguments(arguments.RawData, location="view_args")
        @data_source_blp.response(
            200, schemas.response("PartData", schemas.part_data())
        )
        # pylint: disable=no-self-use
        def get(
            self,
            _query_args: Mapping[str, Any],
            view_args: Mapping[str, Any],
            **_kwargs: Any,
        ) -> Any:
            raw_model = etl.raw_model
            return {
                schemas.ENVELOPE: db.session.query(raw_model).get(view_args["id"]),
                "links": {
                    "self": url_for(
                        f"{data_source.name}.show-{part}",
                        **{"id": view_args["id"]},
                    ),
                    "collection": url_for(f"{data_source.name}.browse-{part}"),
                },
            }

    return _Part


def list_parts() -> None:
    for data_source in DataSource.instances():
        data_source_blp = _create_data_source_blp(data_source)
        _data_source_view = _create_data_source_view(data_source, data_source_blp)
        for etl in data_source.etl_manager.part2etl.values():
            _parts_view = _create_part_browse_view(data_source, data_source_blp, etl)
            _part_view = _create_part_show_view(data_source, data_source_blp, etl)
        api_spec.register_blueprint(data_source_blp)
