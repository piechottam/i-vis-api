import os
from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence, Type

import pandas as pd
from flask_marshmallow.fields import URLFor
from i_vis.core.version import Default as DefaultVersion

from ... import config_meta, ma
from ...config_utils import get_config
from ...df_utils import tsv_io
from ...harmonizer import Harmonizer, SimpleHarmonizer
from ...plugin import CoreType, CoreTypeField, CoreTypeMeta
from ...resource import File, ResourceId, ResourceIds
from ...task.transform import Transform
from . import meta
from .models import (
    CHEMBL_ID,
    CHEMBL_PREFIX,
    DRUG_NAME_MAX_LENGTH,
    Drug,
    DrugMixin,
    DrugName,
)

# from ...query import set_ops

if TYPE_CHECKING:
    from ...resource import Resources
    from ...terms import TermType

_mapping_rids = ResourceIds()

DRUGS_FNAME = "_drugs.tsv"
DRUG_NAMES_FNAME = "_drug_names.tsv"


def add_chembl_mapping_rid(rid: ResourceId) -> None:
    _mapping_rids.add(rid)


class CHEMBLidField(CoreTypeField):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            validate=lambda value: value.startswith("CHEMBL"),
            **kwargs,
        )

    @classmethod
    def core_type_name(cls) -> str:
        return meta.name

    # @classmethod
    # def ops(cls) -> Set[Callable[[Any], str]]:
    #    return set_ops


class HarmonizedDrugSchema(ma.Schema):
    chembl_id = CHEMBLidField()
    links = ma.Hyperlinks(
        {
            "self": URLFor(
                "drugs.show-chembl_id",
                values={"chembl_id": "<chembl_id>"},
            )
        }
    )


_match_type_default = ["direct", "exact", "substring"]  # , "filtered_partial"
_match_type = config_meta.register_plugin_variable(
    pname=meta.name,
    name="MATCH_TYPES",
    required=False,
    default=",".join(_match_type_default),
)


class Plugin(CoreType):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            str_to_version=DefaultVersion.from_str,
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        return DefaultVersion(major=1)

    def register_with_terms(self) -> Sequence["TermType"]:
        from ...terms import DrugName as DrugNameTerm

        DrugNameTerm.register_core_type(DrugNameTerm, self)

        return [DrugNameTerm]

    @cached_property
    def harmonizer(self) -> Harmonizer:
        match_types = (
            get_config().get(_match_type, ",".join(_match_type_default)).split(",")
        )

        fname = os.path.join(
            self.dir.by_version(self.version.current), DRUG_NAMES_FNAME
        )
        return SimpleHarmonizer(
            target=self.harm_meta.target,
            match_types=match_types,
            fname=fname,
            id_col="chembl_id",
            name_col="name",
        )

    @property
    def harm_meta(self) -> "CoreTypeMeta":
        return CoreTypeMeta(
            db_mixin=DrugMixin,
            fields={CHEMBL_ID: CHEMBLidField},
            schema=HarmonizedDrugSchema,
            targets=[CHEMBL_ID],
        )

    def _init_tasks(self) -> None:
        from ...data_sources.chembl import POST_MERGED_MAPPING_FNAME
        from ...data_sources.chembl import meta as chembl_meta
        from ...task.transform import BuildDict

        drugs_file = self.task_builder.res_builder.file(
            fname=DRUGS_FNAME,
            io=tsv_io,
            desc=Drug.get_res_desc(),
        )
        drug_names_file = self.task_builder.res_builder.file(
            fname=DRUG_NAMES_FNAME,
            io=tsv_io,
            desc=DrugName.get_res_desc(),
        )

        merged_chembl_mapping_file = self.task_builder.res_builder.file(
            "_pre_merged_mappings.tsv",
            io=tsv_io,
            desc=DrugName.get_res_desc(),
        )
        merge_mappings_task = MergeMappings(
            mapping_rids=_mapping_rids, out_file=merged_chembl_mapping_file
        )
        self.task_builder.add_task(merge_mappings_task)

        in_rids = ResourceIds()
        in_rids.add(File.link(chembl_meta.name, POST_MERGED_MAPPING_FNAME))
        # build chembl to drug name dictionary
        build_dict_task = BuildDict(
            in_rids=in_rids,
            entities=drugs_file,
            names=drug_names_file,
            max_name_length=DRUG_NAME_MAX_LENGTH,
            target_id=CHEMBL_ID,
            id_prefix=CHEMBL_PREFIX,
        )
        self.task_builder.add_task(build_dict_task)

        # load chembl drugs dictionary
        self.task_builder.load(
            in_rid=drugs_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=Drug),
        )

        # load raw chembl to drug name dictionary
        self.task_builder.load(
            in_rid=drug_names_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=DrugName),
        )

    @property
    def model(self) -> Type[Drug]:
        return Drug


class MergeMappings(Transform):
    def __init__(self, mapping_rids: ResourceIds, out_file: File) -> None:
        super().__init__(offers=[out_file], requires=[])
        self.mapping_rids = mapping_rids

    def post_register(self) -> None:
        for rid in self.mapping_rids:
            if rid not in self.required_rids:
                self.required_rids.add(rid)
        self.mapping_rids.close()

    def _do_work(self, context: "Resources") -> None:
        dfs = [context[rid].read_full() for rid in self.required_rids]
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates()
        self.out_res.save(df)
