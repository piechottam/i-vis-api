"""
dbNSFP
------

:name: dbnsfp
:url: `<https://sites.google.com/site/jpopgen/dbNSFP>`_
:required: no
:entities: gene, variant
:access: file download
:credentials: none
"""

from typing import Any
import enum

from marshmallow import fields
from sqlalchemy import Column, Float, Enum

from i_vis.core.utils import EnumMixin
from i_vis.core.version import Default as DefaultVersion
from . import meta
from ...config_utils import get_config
from ... import terms as t
from ...etl import ETLSpec, Simple, Exposed, ExposeInfo
from ...plugin import DataSource
from ...utils import VariableUrl as Url

_URL_VAR = meta.register_variable(
    name="URL",
    default="ftp://dbnsfp:dbnsfp@dbnsfp.softgenetics.com/dbNSFP4.1c.zip",
)
_VERSION = meta.register_variable(name="VERSION", default="4.1c")

CHRS = list(str(i) for i in range(1, 22)) + ["X", "Y", "M"]


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[],  # TODO
            str_to_version=DefaultVersion.from_str,
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        return DefaultVersion.from_str(get_config()[_VERSION])

    def _init_tasks(self) -> None:
        pass
        # from .tasks import read_zipped_df
        # from .models import RawModel

        # tasks: MutableSequence[Task] = []

        # extract_task, archive_file = build_extract_task(
        #    plugin=self, url=self.get_url("archive")
        # )
        # tasks.append(extract_task)

        # files = unpack_files(
        #    plugin=self,
        #    out_fnames=[f"dbNSFP4.1a_variant.chr{chr_}.gz" for chr_ in CHRS],
        #    readers=len(CHRS) * [read_zipped_df],
        #    descriptions=len(CHRS) * [RawModel.get_raw_desc()],
        # )
        # tasks.append(Unpack(pname=NAME, archive=archive_file, out_files=files))

        # TODO transform and load


class LRTPred(EnumMixin, enum.Enum):
    D = "Deleterious"
    N = "Neutral"
    U = "Unknown"


class LRTPredField(fields.String):
    def _validated(self, value: Any) -> str:
        if value not in LRTPred:
            raise self.make_error("invalid", input=value)
        return str(value)


class MetaSVMPred(enum.Enum):
    T = "Tolerated"
    D = "Damaging"


class MetaSVMPredField(fields.Field):
    pass  # TODO


class MetaLRPred(EnumMixin, enum.Enum):
    T = "Tolerated"
    D = "Damaging"


class MetaLRPredField(fields.Field):
    pass  # TODO


class Spec(ETLSpec):
    class Extract:
        url = Url(_URL_VAR)

        class Raw:
            chr = Simple()
            pos_1_based = Simple()
            ref = Simple()
            alt = Simple()
            aaref = Simple()
            aaalt = Simple()
            aapos = Simple()
            genename = Simple(terms=[t.HGNCname()])
            ensembl_geneid = Simple(terms=[t.EnsemblGeneID])
            ensembl_transcript_id = (Simple(terms=[t.EnsemblTranscripID]),)
            ensembl_protein_id = Simple(terms=[t.EnsemblProteinID])
            hgvsc_snpEff = Simple(terms=[t.HGVSc()])
            hgvsp_snpEff = Simple(terms=[t.HGVSp()])
            cds_strand = Simple()
            lrt_score = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(
                        Float(),
                    ),
                    fields={
                        "lrt_score": fields.Float,
                    },
                ),
                terms=[t.PathogenicityScore],
            )
            lrt_converted_rankscore = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(
                        Float(),
                    ),
                    fields={
                        "lrt_converted_rankscore": fields.Float,
                    },
                ),
                terms=[t.PathogenicityScore],
            )
            lrt_pred = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(Enum(LRTPred)),
                    fields={
                        "lrt_pred": LRTPredField,
                    },
                ),
                terms=[t.PathogenicityPrediction],
            )
            lrt_omega = Simple(terms=[t.Pathogenicity])
            meta_svm_score = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(
                        Float(),
                    ),
                    fields={
                        "meta_svm_score": fields.Float,
                    },
                ),
                terms=[t.PathogenicityScore],
            )
            meta_svm_rankscore = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(
                        Float(),
                    ),
                    fields={
                        "meta_svm_rankscore": fields.Float,
                    },
                ),
                terms=[t.PathogenicityScore],
            )
            meta_svm_pred = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(
                        Float(),
                    ),
                    fields={
                        "meta_svm_pred": MetaSVMPredField,
                    },
                ),
                terms=[t.PathogenicityPrediction],
            )
            meta_lr_score = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(Float()),
                    fields={
                        "meta_lr_score": fields.Float,
                    },
                ),
                terms=[t.PathogenicityScore],
            )
            meta_lr_rankscore = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(
                        Float(),
                    ),
                    fields={
                        "meta_lr_rankscore": fields.Float,
                    },
                ),
                terms=[t.PathogenicityScore],
            )
            meta_lr_pred = Exposed(
                exposed_info=ExposeInfo(
                    db_column=Column(
                        Float(),
                    ),
                    fields={
                        "meta_lr_pred": MetaLRPredField,
                    },
                ),
                terms=[t.PathogenicityPrediction],
            )
