from functools import cache
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Type, Union

from flask import request
from flask.helpers import url_for
from inflection import underscore
from marshmallow import EXCLUDE

from i_vis.api import fields, ma, terms
from i_vis.api.plugin import CoreType, DataSource
from i_vis.core import blueprint
from i_vis.core.utils import class_name

if TYPE_CHECKING:
    from i_vis.api.etl import ETL, RawData

_SCHEMA_CACHE: MutableMapping[str, ma.Schema] = {}

ENVELOPE = "data"


def _name(name: str, nested_schema: ma.Schema) -> str:
    return name + "-" + repr(nested_schema)


def cache_schema(name: str, nested_schema: ma.Schema, schema: ma.Schema) -> ma.Schema:
    _SCHEMA_CACHE[_name(name, nested_schema)] = schema
    return schema


def cached_schema(name: str, nested_schema: ma.Schema) -> ma.Schema:
    name_ = _name(name, nested_schema)
    return _SCHEMA_CACHE[name_]


class IdMixin:
    id = fields.StrMethod("get_id")
    type = fields.StrMethod("get_type")


class LinksMixin:
    links = fields.StrDictMethod("get_links")


class _Base(ma.Schema):
    class Meta:
        ordered = True
        unknown = EXCLUDE

    links = ma.Dict(keys=ma.Str(), values=ma.Str())


class _Response(_Base):
    pass


def response(name: str, nested_schema: ma.Schema, **kwargs: Any) -> ma.Schema:
    try:
        return cached_schema(name, nested_schema)
    except KeyError:
        pass

    new_fields = {
        f"{ENVELOPE}": ma.Nested(nested_schema, **kwargs),
    }
    return cache_schema(
        name,
        nested_schema,
        _Response.from_dict(new_fields, name=class_name(name, "Response")),
    )


class PagerInfo(ma.Schema):
    pages = ma.Int()
    current_id = fields.IntMethod("get_current_id")
    next_id = ma.Int()
    size = fields.IntMethod("get_size")
    max_size = fields.IntMethod("get_max_size")
    total = ma.Int()

    @staticmethod
    def get_current_id(data: blueprint.PagerInfo) -> int:
        return data.pagination_params.current_id

    @staticmethod
    def get_size(data: blueprint.PagerInfo) -> int:
        return data.pagination_params.size

    @staticmethod
    def get_max_size(data: blueprint.PagerInfo) -> int:
        return data.pagination_params.max_size


class _Paginated(_Response):
    pager_info = ma.Nested(PagerInfo)


def paginated(name: str, nested_schema: ma.Schema, **kwargs: Any) -> ma.Schema:
    try:
        return cached_schema(name, nested_schema)
    except KeyError:
        pass

    new_fields = {
        ENVELOPE: ma.Nested(nested_schema, **kwargs),
    }
    return cache_schema(
        name,
        nested_schema,
        _Paginated.from_dict(new_fields, name=class_name(name, "PaginatedResponse")),
    )


class ApiVersion(_Base, IdMixin):
    """Schema for API version"""

    api_version = ma.String()
    code_version = ma.String()
    created = ma.Date()

    @staticmethod
    def get_id(data: Mapping[str, Any]) -> str:
        return str(data["api_version"])

    @staticmethod
    def get_type(_data: Mapping[str, Any]) -> str:
        return "api_version"


class MemoryUsage(ma.Schema, IdMixin):
    total = ma.Str()
    used = ma.Str()

    @staticmethod
    def get_id(_data: Mapping[str, Any]) -> str:
        return "memory_usage"

    @staticmethod
    def get_type(_data: Mapping[str, Any]) -> str:
        return "machine_property"


class DiskUsage(ma.Schema):
    total = ma.Str()
    used = ma.Str()
    available = ma.Str()


class Temperature(ma.Schema):
    label = ma.Str()
    current = ma.Float()
    high = ma.Float()
    critical = ma.Float()


class MachineStatus(IdMixin, _Base):
    """Schema for machine status"""

    disk = ma.Nested(DiskUsage())
    mem = ma.Nested(MemoryUsage())
    cpu_usage = ma.List(ma.Float())
    load = ma.List(ma.Float())
    temperatures = ma.Dict(keys=ma.Str, values=ma.Nested(Temperature, many=True))

    @staticmethod
    def get_id(_data: Mapping[str, Any]) -> str:
        return request.host_url

    @staticmethod
    def get_type(_data: Mapping[str, Any]) -> str:
        return "machine_status"

    @staticmethod
    def get_links(_data: Mapping[str, Any]) -> Mapping[str, Any]:
        return {
            "self": url_for("api.dashboard"),
        }


class Term(ma.Schema):
    name = ma.Str()
    normalized_to = ma.Str(missing=None)

    @staticmethod
    def get_id(data: terms.Term) -> str:
        return data.get_name()

    @staticmethod
    def get_type(_data: terms.Term) -> str:
        return "term"


class Terms(Term):
    children = ma.Nested("self", many=True)


class ColumnDesc(ma.Schema):
    terms = ma.Nested("Term", many=True)
    jsonify = ma.Bool(missing=False)
    expose = ma.Bool(missing=False)


class PartDesc(LinksMixin, IdMixin, _Base):
    raw_columns = ma.Dict(keys=ma.Str(), values=ma.Nested(ColumnDesc))
    modifier_task_columns = ma.Dict(keys=ma.Str(), values=ma.Nested(ColumnDesc))

    @staticmethod
    def get_id(data: "ETL") -> str:
        return data.part

    @staticmethod
    def get_type(_data: "ETL") -> str:
        return "part_desc"

    @staticmethod
    def get_links(data: "ETL") -> Mapping[str, Any]:
        return {
            "collection": url_for(f"{data.pname}.browse-{data.part}"),
        }


class PluginVersion(ma.Schema):
    current = ma.Str()
    local_latest = ma.Str()


class DataSourceDesc(LinksMixin, IdMixin, _Base):
    url = fields.StrFunction(lambda data_source: data_source.meta.url)
    version = ma.Nested(PluginVersion)
    parts = ma.Dict(keys=ma.Str(), values=ma.Nested(PartDesc))

    @staticmethod
    def get_id(data: DataSource) -> str:
        return data.name

    @staticmethod
    def get_type(data: DataSource) -> str:
        return underscore(data.type() + "_desc").replace(" ", "_")

    @staticmethod
    def get_links(data: DataSource) -> Mapping[str, Any]:
        return {
            "self": url_for(f"{data.name}.show"),
        }


class CoreTypeDesc(LinksMixin, IdMixin, _Base):
    version = ma.Pluck(PluginVersion, "current")
    normalized_to = ma.Str()

    @staticmethod
    def get_id(data: "CoreType") -> str:
        return data.name

    @staticmethod
    def get_type(data: "CoreType") -> str:
        return underscore(data.type() + "_desc").replace(" ", "_")

    @staticmethod
    def get_links(data: "CoreType") -> Mapping[str, Any]:
        return {
            "self": url_for(f"{data.blueprint_name}.list"),
        }


@cache
def harmonized_data() -> ma.Schema:
    new_fields: MutableMapping[str, Union[ma.Field, Type[ma.Field]]] = {}
    for core_type in CoreType.instances():
        new_fields[core_type.clean_name] = ma.Nested(
            core_type.harm_meta.schema, many=True
        )

    return ma.Schema.from_dict(new_fields, name="HarmonizedData")


@cache
def processed_data() -> ma.Schema:
    new_fields: MutableMapping[str, Union[ma.Field, Type[ma.Field]]] = {}
    for core_type in CoreType.instances():
        new_fields[core_type.clean_name] = ma.Str()
    return ma.Schema.from_dict(new_fields, name="ProcessedData")


@cache
def part_data() -> Type[ma.Schema]:
    class PartData(_Base, IdMixin):
        data_source = fields.StrFunction(lambda part_data_: part_data_.get_etl().pname)
        part = ma.Str()
        raw_data = ma.Dict(keys=ma.Str(), values=ma.Str())
        processed_data = ma.Nested(processed_data())
        harmonized_data = ma.Nested(harmonized_data())

        @staticmethod
        def get_id(data: "RawData") -> str:
            return str(data.i_vis_id)

        @staticmethod
        def get_type(data: "RawData") -> str:
            return data.get_etl().part

        @staticmethod
        def get_data_source(data: "RawData") -> str:
            return data.get_etl().pname

    return PartData


class PartCount(_Base):
    counts = ma.Str()


@cache
def explorer_result(core_type: CoreType) -> ma.Schema:
    new_fields = {
        # "id": ma.Str(),
        core_type.harm_meta.target: ma.Str(),
        "type": ma.Str(),
        "parts": ma.Dict(keys=ma.Str(), values=ma.Nested(PartCount)),
    }

    return _Base.from_dict(
        new_fields,
        name=class_name(core_type.short_name, "ExplorerResult"),
    )


@cache
def explorer(core_type: CoreType) -> ma.Schema:
    new_fields = {
        ENVELOPE: ma.Dict(
            keys=ma.Str(),
            values=ma.Nested(explorer_result(core_type)),
        )
    }
    return _Paginated.from_dict(
        new_fields,
        name=class_name(
            core_type.short_name,
            "ExplorerPaginatedResult",
        ),
    )


class CoreTypeCount(_Base, IdMixin):
    data_source = ma.Str()
    part = ma.Str()
    harmonized = ma.Int()
    unique = ma.Int()

    @staticmethod
    def get_id(data: Mapping[str, Any]) -> str:
        return str(data["part"])

    @staticmethod
    def get_type(data: Mapping[str, Any]) -> str:
        core_type_name = data["core_type_name"]
        return f"{core_type_name}_count"


@cache
def harmonizer_match(core_type_name: str) -> ma.Schema:
    core_type = CoreType.get(core_type_name)
    new_fields = {
        "matched_name": ma.Str(),
        core_type.harm_meta.target: ma.Str(),
    }
    return _Base.from_dict(
        new_fields, name=class_name(core_type_name, "HarmonizerMatch")
    )


@cache
def harmonizer_result(core_type_name: str) -> ma.Schema:
    new_fields = {
        "query": ma.Str(),
        "matches": ma.Nested(harmonizer_match(core_type_name), many=True),
        "match_type": ma.Str(),
    }
    return _Base.from_dict(
        new_fields, name=class_name(core_type_name, "HarmonizerResult")
    )
