from i_vis.core.blueprint import Blueprint

from . import meta

from ..utils import (
    register_list,
    register_stats,
    register_explorer,
)

from ...plugin import CoreType
from ...utils import api_prefix

blp = Blueprint(
    CoreType.get_blueprint_name(meta),
    __name__,
    url_prefix=api_prefix("core-types", meta.api_prefix),
    description="Operations on HGVS",
)


def register() -> None:
    _ = (
        register_list(
            core_type_name=meta.name,
            blp=blp,
        ),
        register_stats(
            core_type_name=meta.name,
            blp=blp,
        ),
        register_explorer(
            core_type_name=meta.name,
            blp=blp,
            endpoint="explore",
        ),
    )
