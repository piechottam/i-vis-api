import os
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence, Type

from flask_marshmallow.fields import URLFor
from i_vis.core.version import Default as DefaultVersion
from i_vis.core.version import Version

from ... import config_meta, ma

# from ...query import set_ops
from ...config_utils import get_config
from ...df_utils import tsv_io
from ...harmonizer import Harmonizer, SimpleHarmonizer
from ...plugin import CoreType, CoreTypeField, CoreTypeMeta
from ...resource import ResourceId, ResourceIds
from ...task.transform import BuildDict
from . import meta
from .models import (
    CANCER_TYPE_NAME_MAX_LENGTH,
    DO_ID,
    DOID_PREFIX,
    CancerType,
    CancerTypeMixin,
    CancerTypeName,
)

if TYPE_CHECKING:
    from ...terms import TermType

_mapping_rids = ResourceIds()

NAMES_FNAME = "_cancer_types_names.tsv"


def add_do_mapping_rid(rid: ResourceId) -> None:
    _mapping_rids.add(rid)


class DOidField(CoreTypeField):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            validate=lambda value: value.startswith("DOID:"),
            **kwargs,
        )

    @classmethod
    def core_type_name(cls) -> str:
        return meta.name

    # @classmethod
    # def ops(cls) -> Set[Callable[[Any], str]]:
    #    return set_ops


class HarmonizedCancerTypeSchema(ma.Schema):
    do_id = DOidField()
    links = ma.Hyperlinks(
        {
            "related": URLFor(
                "cancer-types.show-do_id",
                values={
                    "do_id": "<do_id>",
                },
            )
        }
    )


_match_type_default = ["direct", "exact", "substring"]
_match_type = config_meta.register_core_variable(
    "TYPE_MATCH_TYPES",
    required=False,
    default=",".join(_match_type_default),
)


class Plugin(CoreType):
    def __init__(self) -> None:
        super().__init__(meta=meta, str_to_version=DefaultVersion.from_str)
        self._merge_cancer_names_task = None

    @property
    def latest_version(self) -> DefaultVersion:
        return DefaultVersion(major=1)

    @cached_property
    def harmonizer(self) -> Harmonizer:
        match_types = (
            get_config().get(_match_type, ",".join(_match_type_default)).split(",")
        )

        fname = os.path.join(self.dir.by_version(self.version.current), NAMES_FNAME)
        return SimpleHarmonizer(
            target=self.harm_meta.target,
            match_types=match_types,
            fname=fname,
            id_col="do_id",
            name_col="name",
        )

    def register_with_terms(self) -> Sequence["TermType"]:
        from ...terms import CancerType as CancerTypeTerm

        CancerTypeTerm.register_core_type(CancerTypeTerm, self)

        return [CancerTypeTerm]

    @property
    def harm_meta(self) -> "CoreTypeMeta":
        return CoreTypeMeta(
            db_mixin=CancerTypeMixin,
            fields={DO_ID: DOidField},
            schema=HarmonizedCancerTypeSchema,
            targets=[DO_ID],
            target=DO_ID,
        )

    def _init_tasks(self) -> None:
        cancer_type_file = self.task_builder.res_builder.file(
            fname="_cancer_types.tsv",
            io=tsv_io,
            desc=CancerType.get_res_desc(),
        )
        cancer_type_names_file = self.task_builder.res_builder.file(
            fname=NAMES_FNAME,
            io=tsv_io,
            desc=CancerType.get_res_desc(),
        )
        # build terms to raw drug name dictionary
        self.task_builder.add_task(
            BuildDict(
                in_rids=_mapping_rids,
                entities=cancer_type_file,
                names=cancer_type_names_file,
                target_id=DO_ID,
                max_name_length=CANCER_TYPE_NAME_MAX_LENGTH,
                id_prefix=DOID_PREFIX,
            )
        )

        # load do cancer type dictionary
        self.task_builder.load(
            in_rid=cancer_type_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=CancerType),
        )

        # load raw chembl to drug name dictionary
        self.task_builder.load(
            in_rid=cancer_type_names_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=CancerTypeName),
        )

    @property
    def _latest_version(self) -> Version:
        return DefaultVersion(major=1)

    @property
    def model(self) -> Type[CancerType]:
        return CancerType
