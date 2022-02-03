from functools import cache
from typing import (
    cast,
    Any,
    Optional,
    Mapping,
    MutableMapping,
    Union,
    Type,
    TYPE_CHECKING,
    Sequence,
)
from marshmallow import RAISE, validates_schema, ValidationError
from marshmallow.validate import ContainsOnly, Length, Range
from webargs.fields import DelimitedList

from i_vis.core.utils import class_name
from i_vis.core.blueprint import MAX_SIZE, CURRENT_ID, SIZE

from i_vis.api import ma, fields
from i_vis.api.plugin import CoreTypeField, CoreType, default_field
from i_vis.api.etl import etl_registry

if TYPE_CHECKING:
    pass


class Restricted(ma.Schema):
    class Meta:
        ordered = True
        unknown = RAISE

    token = fields.Token()


class _Paginated(Restricted):
    pass


def paginated(
    prefix: str,
    new_fields: Mapping[str, Union[CoreTypeField, Type[CoreTypeField]]],
    current_id_: int = CURRENT_ID,
    size_: int = SIZE,
    max_size_: int = MAX_SIZE,
) -> _Paginated:
    new_fields_ = {
        "current_id": ma.Integer(missing=current_id_),
        "size": ma.Integer(
            missing=size_,
            validate=Range(min=1, max=max_size_),
        ),
        "max_size": ma.Constant(max_size_),
    }
    new_fields_.update(new_fields)

    return cast(
        _Paginated,
        _Paginated.from_dict(
            new_fields_, name=class_name(prefix, "PaginatedParameters")
        ),
    )


class RawData(ma.Schema):
    id = ma.Int(required=True, validate=lambda v: v > 0)


@cache
def generic_part_fields(part: str) -> ma.Schema:
    """Argument schema for querying a part of a plugin"""

    etl = etl_registry.by_part(part)
    new_fields: MutableMapping[str, Union[ma.Field, Type[ma.Field]]] = {}
    if etl:
        # add fields for harmonized columns
        for core_type_ in etl.core_types:
            new_fields.update(dict(core_type_.harm_meta.default_fields))
        # add fields for exposed columns
        for col in etl.all_raw_columns.exposed_cols:
            column = etl.all_raw_columns[col]
            assert column.exposed_info is not None
            exposed_info = column.exposed_info
            if exposed_info.fields:
                new_fields.update(exposed_info.fields)
    return new_fields


def restricted(
    new_fields: MutableMapping[
        str, Union[ma.Field, Type[ma.Field], CoreTypeField, Type[CoreTypeField]]
    ],
    schema_name: str = "",
) -> ma.Schema:
    """Argument schema for querying a part of a plugin"""

    schema_name = schema_name or "Parameters"
    return Restricted.from_dict(new_fields, name=schema_name)


def core_type_fields(*core_types: CoreType) -> MutableMapping[str, Any]:
    new_fields = {}
    for core_type_ in core_types:
        target = core_type_.harm_meta.target
        for name in core_type_.harm_meta.fields:
            if target == name:
                new_fields[name] = DelimitedList(
                    core_type_.harm_meta.default_fields[name],
                    many=True,
                    validate=Length(1, 5),
                )
            else:
                new_fields[name] = default_field(core_type_.harm_meta.fields[name])
    return new_fields


@cache
def explorer_fields(core_type_name: str) -> ma.Str:
    etls = etl_registry.by_core_types(CoreType.get(core_type_name))
    parts = [etl.part for etl in etls]
    core_types = {core_type_ for etl in etls for core_type_ in etl.core_types}

    new_fields = core_type_fields(*tuple(core_types))
    new_fields["part"] = DelimitedList(
        ma.Str(),
        validate=ContainsOnly(parts),
        default=parts,
        missing=parts,
    )
    return new_fields


@cache
def search() -> ma.Schema:
    etls = etl_registry.by_core_types(*CoreType.instances())
    parts = [etl.part for etl in etls]
    core_types = {core_type_ for etl in etls for core_type_ in etl.core_types}

    new_fields = core_type_fields(*tuple(core_types))
    new_fields["part"] = DelimitedList(
        ma.Str(),
        validate=ContainsOnly(parts),
        default=parts,
        missing=parts,
    )
    schema = Restricted.from_dict(new_fields)

    @validates_schema
    def validate_schema(_self: Any, data: Mapping[str, Any], **_kwargs: Any) -> None:
        missing = [field_name for field_name in new_fields if not data[field_name]]
        if len(missing) == len(new_fields):
            ValidationError(
                f"At least one field needs to have values: {','.join(missing)}"
            )

    setattr(schema, "validate_schema", validate_schema)
    return cast(Restricted, schema)


@cache
def harmonizer(
    new_fields: Optional[MutableMapping[str, ma.Field]] = None,
    match_type: Optional[Sequence["str"]] = None,
) -> Restricted:
    if match_type is None:
        match_type = ["direct", "exact", "substring"]
    if not new_fields:
        new_fields = {}
    new_fields["query"] = DelimitedList(
        ma.String(validate=Length(3, 100)),
        required=True,
        validate=Length(1, 100),
    )
    new_fields["match_type"] = DelimitedList(
        ma.String(),
        validate=ContainsOnly(match_type),
        missing=match_type,
    )
    # FUTURE add but needs formatting in response new_fields["extended_meta"] = fields.Bool(missing=False)
    return cast(Restricted, Restricted.from_dict(new_fields))
