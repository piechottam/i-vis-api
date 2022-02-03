from i_vis.core.blueprint import Blueprint

from . import meta
from .models import CancerType, CancerTypeSchema
from .plugin import DOidField
from ..utils import (
    register_list,
    register_browse,
    register_show,
    register_explorer,
    register_harmonizer,
    register_stats,
)
from ...utils import api_prefix
from ...plugin import CoreType

blp = Blueprint(
    CoreType.get_blueprint_name(meta),
    __name__,
    url_prefix=api_prefix("core-types", meta.api_prefix),
    description="Operations on cancer types",
)


def register() -> None:
    # not needed
    # _, _, name2endpoint = register_operations_list(
    #     core_type_name=meta.name,
    #     blp=blp,
    # )
    core_type = CoreType.get(meta.name)
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
        ),
        register_harmonizer(
            core_type_name=meta.name,
            blp=blp,
        ),
        register_browse(
            core_type_name=meta.name,
            blp=blp,
            model=CancerType,
            schema=CancerTypeSchema,
            column=CancerType.do_id,
        ),
        register_show(
            core_type_name=meta.name,
            blp=blp,
            model=CancerType,
            schema=CancerTypeSchema,
            target=core_type.harm_meta.target,
            field=DOidField(),
            endpoint=f"show-{core_type.harm_meta.target}",
        ),
    )

    # not needed
    # name2endpoint.update(create_name2endpoint(services))
