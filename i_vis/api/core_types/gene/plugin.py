import os
from typing import Any, Sequence, TYPE_CHECKING
from functools import cached_property

from flask_marshmallow.fields import URLFor

from i_vis.core.version import Default as DefaultVersion, Version

from . import meta
from ...config_utils import get_config
from .models import (
    Gene,
    GeneName,
    GeneMixin,
    HGNC_ID,
    HGNC_PREFIX,
    GENE_NAME_MAX_LENGTH,
    GENE_NAME_TYPE_MAX_LENGTH,
)
from ... import config_meta, ma
from ...df_utils import tsv_io
from ...harmonizer import Harmonizer, SimpleHarmonizer
from ...plugin import CoreType, CoreTypeMeta, CoreTypeField
from ...resource import ResourceIds, ResourceId
from ...task.transform import BuildDict

# from ...query import set_ops

if TYPE_CHECKING:
    from ...terms import TermType
    from ... import db

_mapping_rids = ResourceIds()


def add_hgnc_mapping_rid(rid: ResourceId) -> None:
    _mapping_rids.add(rid)


class HGNCidField(CoreTypeField):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            validate=lambda value: value.startswith(HGNC_PREFIX),
            **kwargs,
        )

    @classmethod
    def core_type_name(cls) -> str:
        return meta.name

    # @classmethod
    # def ops(cls) -> Set[Callable[[Any], str]]:
    #    return set_ops


class HarmonizedGeneSchema(ma.Schema):
    hgnc_id = HGNCidField()
    links = ma.Hyperlinks(
        {
            "related": URLFor(
                "genes.show-hgnc_id",
                values={"hgnc_id": "<hgnc_id>"},
            )
        }
    )


_match_type_default = ["direct", "exact"]
_match_type = config_meta.register_plugin_variable(
    meta.name,
    "GENE_MATCH_TYPES",
    required=False,
    default=",".join(_match_type_default),
)


class Plugin(CoreType):
    def __init__(self) -> None:
        super().__init__(meta=meta, str_to_version=DefaultVersion.from_str)

    @property
    def latest_version(self) -> DefaultVersion:
        return DefaultVersion(major=1)

    def register_with_terms(self) -> Sequence["TermType"]:
        from ...terms import GeneName as GeneNameTerm

        GeneNameTerm.register_core_type(GeneNameTerm, self)

        return [GeneNameTerm]

    @cached_property
    def harmonizer(self) -> Harmonizer:
        match_types = (
            get_config().get(_match_type, ",".join(_match_type_default)).split(",")
        )
        fname = os.path.join(
            self.dir.by_version(self.version.current), "gene_names.tsv"
        )
        return SimpleHarmonizer(
            target=self.harm_meta.target,
            match_types=match_types,
            fname=fname,
            id_col="hgnc_id",
            name_col="name",
        )

    @property
    def harm_meta(self) -> "CoreTypeMeta":
        return CoreTypeMeta(
            db_mixin=GeneMixin,
            fields={HGNC_ID: HGNCidField},
            schema=HarmonizedGeneSchema,
            targets=[HGNC_ID],
        )

    def _init_tasks(self) -> None:
        # gene specific data
        genes_file = self.task_builder.res_builder.file(
            fname="_genes.tsv",
            io=tsv_io,
            desc=Gene.get_res_desc(),
        )
        self.task_builder.load(
            in_rid=genes_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=Gene),
        )

        gene_names_file = self.task_builder.res_builder.file(
            fname="_gene_names.tsv",
            io=tsv_io,
            desc=GeneName.get_res_desc(),
        )
        self.task_builder.add_task(
            BuildDict(
                in_rids=_mapping_rids,
                entities=genes_file,
                names=gene_names_file,
                target_id=HGNC_ID,
                max_name_length=GENE_NAME_MAX_LENGTH,
                max_type_length=GENE_NAME_TYPE_MAX_LENGTH,
                id_prefix=HGNC_PREFIX,
            )
        )

        # load gene_names table
        self.task_builder.load(
            in_rid=gene_names_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=GeneName),
        )

    @property
    def _latest_version(self) -> Version:
        return DefaultVersion(major=1)

    @property
    def model(self) -> "db.Model":
        return Gene
