import os
from typing import Any, Sequence, TYPE_CHECKING
from functools import cached_property

from flask_marshmallow.fields import URLFor

from i_vis.core.version import Default as DefaultVersion

from . import meta
from ...config_utils import get_config
from .models import (
    Drug,
    DrugMixin,
    CHEMBL_ID,
    CHEMBL_PREFIX,
    DrugName,
    DRUG_NAME_MAX_LENGTH,
    DRUG_NAME_TYPE_MAX_LENGTH,
)
from ... import config_meta, ma
from ...df_utils import tsv_io
from ...harmonizer import Harmonizer, SimpleHarmonizer
from ...plugin import CoreType, CoreTypeField, CoreTypeMeta
from ...resource import ResourceId, ResourceIds, File

# from ...query import set_ops

if TYPE_CHECKING:
    from ...terms import TermType
    from ... import db

_mapping_rids = ResourceIds()


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

        fname = os.path.join(self.dir.by_version(self.version.current), "names_raw.tsv")
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
        from ...task.transform import BuildDict
        from ...data_sources.chembl import meta as chembl_meta, OTHER_MAPPINGS_FNAME

        drugs_file = self.task_builder.res_builder.file(
            fname="_drugs.tsv",
            io=tsv_io,
            desc=Drug.get_res_desc(),
        )
        names_raw_file = self.task_builder.res_builder.file(
            fname="_names_raw.tsv",
            io=tsv_io,
            desc=DrugName.get_res_desc(),
        )

        # build chembl to raw drug name dictionary
        build_dict_task = BuildDict(
            in_rids=_mapping_rids,
            entities=drugs_file,
            names_raw=names_raw_file,
            max_name_length=DRUG_NAME_MAX_LENGTH,
            max_type_length=DRUG_NAME_TYPE_MAX_LENGTH,
            target_id=CHEMBL_ID,
            id_prefix=CHEMBL_PREFIX,
        )
        # Workaround - ChEMBL does not offer flat file - add ChEMBL entries "on demand"
        build_dict_task.required_rids.add(
            File.link(chembl_meta.name, OTHER_MAPPINGS_FNAME)
        )
        self.task_builder.add_task(build_dict_task)

        # load chembl drugs dictionary
        self.task_builder.load(
            in_rid=drugs_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=Drug),
        )

        # load raw chembl to drug name dictionary
        self.task_builder.load(
            in_rid=names_raw_file.rid,
            table=self.task_builder.res_builder.table_from_model(model=DrugName),
        )

    @property
    def model(self) -> "db.Model":
        return Drug
