from typing import Any, Sequence, Set

from flask import current_app, has_app_context
from marshmallow import fields
from marshmallow.exceptions import ValidationError

from i_vis.core.constants import API_TOKEN_LENGTH

from i_vis.api import api_spec
from i_vis.api.etl import etl_registry
from i_vis.api.models import User
from i_vis.api.plugin import is_plugin, CoreType


def missing_part(core_types: Set[CoreType]) -> Sequence[str]:
    missing = set()
    for core_type in core_types:
        for etl in etl_registry.by_core_type(core_type):
            if not etl:
                continue
            missing.add(etl.part)
            continue
    return list(missing)


class PluginField(fields.Str):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(validate=is_plugin, **kwargs)


def valid_token(token: str) -> None:
    if current_app.config.get("TESTING", False):
        return

    if len(token) != API_TOKEN_LENGTH:
        raise ValidationError("Invalid token.")
    if not User.load_by_token(token):
        raise ValidationError("Unknown token.")


class Token(fields.Str):
    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("required", has_app_context() and not current_app.testing)
        super().__init__(validate=valid_token, **kwargs)


class IntMethod(fields.Method):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class StrMethod(fields.Method):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class StrFunction(fields.Function):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


class StrDictMethod(fields.Method):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)


api_spec.register_field(Token, fields.Str)
api_spec.register_field(StrMethod, fields.Str)
api_spec.register_field(StrFunction, fields.Str)
api_spec.register_field(IntMethod, fields.Int)
api_spec.register_field(StrDictMethod, fields.Dict)
