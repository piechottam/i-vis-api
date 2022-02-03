import re
from typing import Any, Sequence, TYPE_CHECKING
from functools import cached_property
from webargs import validate
from webargs.fields import DelimitedList

from i_vis.core.version import Default as DefaultVersion, Version

from . import meta
from .models import VariantMixin
from .harmonizer import Harmonizer, Simple as SimpleHarmonizer
from ..variant_utils import HGVS_LIKE_REGEX, ModifyFlag, MatchFlag
from ... import ma
from ...resource import ResourceIds
from ...plugin import CoreType, CoreTypeField, CoreTypeMeta
from ...task.transform import Transform

if TYPE_CHECKING:
    from ...task.transform import HarmonizeRawData
    from ...terms import TermType
    from ...resource import Resources, ResourceId
    from ... import db


_rids = ResourceIds()


def add_rid(rid: "ResourceId") -> None:
    _rids.add(rid)


class HGVSField(CoreTypeField):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            validate=lambda value: re.match(HGVS_LIKE_REGEX, value),
            **kwargs,
        )

    @classmethod
    def core_type_name(cls) -> str:
        return meta.name

    # @classmethod
    # def ops(cls) -> Set[Callable[[Any], str]]:
    #    return set_ops


class HarmonizedVariantSchema(ma.Schema):
    class Meta:
        ordered = True

    hgvs = ma.Str()


class Plugin(CoreType):
    def __init__(self) -> None:
        super().__init__(meta=meta, str_to_version=DefaultVersion.from_str)
        self._store_variants_task = None

    @property
    def latest_version(self) -> DefaultVersion:
        return DefaultVersion(major=1)

    def _init_tasks(self) -> None:
        self.task_builder.add_task(RegisterVariants())

    @property
    def harm_meta(self) -> "CoreTypeMeta":
        return CoreTypeMeta(
            db_mixin=VariantMixin,
            fields={
                "hgvs": HGVSField,
                "hgvs_query_modify": ma.Int(
                    missing=ModifyFlag.GUESSED_DESC_TYPE + ModifyFlag.AA1_TO_AA3,
                    validate=validate.Range(0, sum(ModifyFlag)),
                ),
                "hgvs_result_modify": ma.Int(
                    missing=ModifyFlag.GUESSED_DESC_TYPE + ModifyFlag.AA1_TO_AA3,
                    validate=validate.Range(0, sum(ModifyFlag)),
                ),
                "hgvs_result_matches": DelimitedList(
                    ma.Int(missing=MatchFlag.RAW), validate=validate.Length(1, 5)
                ),
            },
            schema=HarmonizedVariantSchema,
            target="hgvs",
        )

    def register_with_terms(self) -> Sequence["TermType"]:
        from ...terms import HGVS

        HGVS.register_core_type(HGVS, self)

        return [HGVS]

    @cached_property
    def harmonizer(self) -> Harmonizer:
        return SimpleHarmonizer()

    @property
    def _latest_version(self) -> Version:
        return DefaultVersion(major=1)

    @property
    def model(self) -> "db.Model":
        raise NotImplementedError

    def register_harmonize_raw_data_task(self, task: "HarmonizeRawData") -> None:
        for file in task.harm_files.values():
            add_rid(file.rid)


class RegisterVariants(Transform):
    def __init__(self, pname: str = meta.name, **kwargs: Any) -> None:
        super().__init__(pname=pname, offers=[], requires=[], **kwargs)

    def _do_work(self, context: "Resources") -> None:
        # FIXME
        pass
