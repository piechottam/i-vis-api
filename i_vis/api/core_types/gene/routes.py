from i_vis.core.blueprint import Blueprint

from . import meta
from .plugin import HGNCidField
from .models import Gene, GeneSchema
from ..utils import (
    register_list,
    register_show,
    register_browse,
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
    description="Operations on genes",
)


def register() -> None:
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
            model=Gene,
            schema=GeneSchema,
            column=Gene.hgnc_id,
        ),
        register_show(
            core_type_name=meta.name,
            blp=blp,
            model=Gene,
            schema=GeneSchema,
            target=core_type.harm_meta.target,
            field=HGNCidField(),
            endpoint=f"show-{core_type.harm_meta.target}",
        ),
    )
