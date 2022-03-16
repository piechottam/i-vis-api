"""
CIViC
-----

:name: civic
:url: `<https://civicdb.org/>`_
:required: no
:entities: gene, cancer-type, variant
:access: file download
:credentials: none
"""

import io
from typing import (
    cast,
    Any,
    MutableSequence,
    TYPE_CHECKING,
    Optional,
    List,
    Mapping,
    Union,
    Iterable,
)

import pandas as pd

from i_vis.core.version import Date as DateVersion

from . import meta
from ... import db
from ... import terms as t
from ...df_utils import PandasDataFrameIO, tsv_io
from ...etl import Exposed, Simple, ETLSpec, ExposeInfo
from ...plugin import DataSource
from ...utils import VariableUrl as Url


if TYPE_CHECKING:
    from ...resource import Resource
    from ...utils import I_VIS_Logger

_URL_PREFIX = "https://civicdb.org/downloads/nightly/nightly-"

_VARIANT_SUMMARIES_URL_VAR = meta.register_variable(
    name="VARIANT_SUMMARIES", default=_URL_PREFIX + "VariantSummaries.tsv"
)
_GENE_SUMMARIES_URL_VAR = meta.register_variable(
    name="GENE_SUMMARIES", default=_URL_PREFIX + "GeneSummaries.tsv"
)
_CLINICAL_EVIDENCE_SUMMARIES_URL_VAR = meta.register_variable(
    name="CLINICAL_EVIDENCE_SUMMARIES",
    default=_URL_PREFIX + "ClinicalEvidenceSummaries.tsv",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[GeneSummary, VariantSummary, ClinicalEvidence],
            str_to_version=DateVersion.from_str,
        )

    @property
    def _latest_version(self) -> DateVersion:
        versions = []
        for etl in self.etl_manager.part2etl.values():
            extract_opts = etl.extract_opts
            assert extract_opts is not None
            versions.append(DateVersion.from_url(url=extract_opts.get_url()))
        version_strs = set(str(version) for version in versions)

        if len(version_strs) != 1:
            raise ValueError("No version available.")

        return versions[0]


# This is specific for "VariantSummaries.tsv"
# Last column is incorrectly expanded with "\t" instead of ",".
# The number of columns differs between rows.
# Fix by merging overhanging columns
class CustomIO(PandasDataFrameIO):
    def __init__(self, read_opts: Optional[Mapping[str, Any]] = None) -> None:
        if not read_opts:
            read_opts_ = {}
        else:
            read_opts_ = dict(read_opts)
        read_opts_.setdefault("sep", "\t")

        super().__init__(read_callback="read_csv", read_opts=read_opts_)

    def _read_df(
        self, in_res: "Resource", logger: Optional["I_VIS_Logger"] = None, **kwargs: Any
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        read_opts = dict(self._read_opts)
        if kwargs:
            read_opts.update(kwargs)
        sep = read_opts["sep"]
        with open(in_res.qname, "r", encoding="utf8") as file:
            fixed = io.StringIO()
            header: MutableSequence[str] = []
            col_index = -1
            for row in file:
                cols = row.split(sep)
                if not header:
                    header.extend(cols)
                    col_index = cols.index("assertion_ids")
                else:
                    cols = self._parse(cols, col_index)
                row = sep.join(cols)
                fixed.write(row)
            fixed.seek(0)
            df = getattr(pd, self._read_callback)(fixed, **read_opts)
            if not isinstance(df, pd.DataFrame):
                return cast(Iterable[pd.DataFrame], df)

            return df

    @staticmethod
    def _parse(cols: List[str], col_index: int) -> List[str]:
        ids = cols[col_index]
        if ids:
            ids_len = len(ids.split(","))
            # fmt: off
            urls = ",".join(cols[col_index + 1:col_index + 1 + ids_len])
            del cols[col_index + 1:col_index + 1 + ids_len]
            # fmt: on
            cols.insert(col_index + 1, urls)
        return cols


class GeneSummary(ETLSpec):
    class Extract:
        url = Url(_GENE_SUMMARIES_URL_VAR, latest=True)
        io = tsv_io
        add_id = True

        class Raw:
            # TODO
            gene_id = Exposed(
                exposed_info=ExposeInfo(
                    db_column=db.Column(
                        db.Integer(),
                        # nullable=False,
                        index=True,
                        unique=True,
                    )
                )
            )
            gene_civic_url = Simple(terms=[t.Gene])
            name = Simple(terms=[t.HGNCname()])  # name or symbol
            entrez_id = Simple()  # EntrezId
            description = Simple()  # general
            last_review_date = Simple(terms=[t.UpdatedDate])


class VariantSummary(ETLSpec):
    class Extract:
        url = Url(_VARIANT_SUMMARIES_URL_VAR, latest=True)
        io = CustomIO()
        add_id = True

        class Raw:
            variant_id = Simple()
            # TODO
            #         expose=ExposeInfo(
            #             db_JSON=db.JSON(
            #                 db.Integer,
            #                 # nullable=False,
            #                 index=True,
            #                 unique=True,
            #             )
            #         ),
            #     )
            variant_civic_url = Simple()
            gene = Simple(terms=[t.HGNCname()])
            entrez_id = Simple()
            variant = Simple(terms=[t.VariantName])
            summary = Simple()
            variant_groups = Simple()
            chromosome = Simple()
            start = Simple()
            stop = Simple()
            reference_bases = Simple()
            variant_bases = Simple()
            representative_transcript = Simple()
            ensembl_version = Simple()
            reference_build = Simple()
            chromosome2 = Simple()
            start2 = Simple()
            stop2 = Simple()
            representative_transcript2 = Simple()
            variant_types = Simple()
            hgvs_expressions = Simple(terms=[t.HGVS()], modifier={"pat": ","})
            last_review_date = Simple()
            civic_variant_evidence_score = Simple()
            allele_registry_id = Simple()
            clinvar_ids = Simple()
            variant_aliases = Simple()
            assertion_ids = Simple()
            assertion_civic_urls = Simple()


class ClinicalEvidence(ETLSpec):
    class Extract:
        url = Url(_CLINICAL_EVIDENCE_SUMMARIES_URL_VAR, latest=True)
        io = tsv_io
        add_id = True

        class Raw:
            gene = Simple(terms=[t.HGNCsymbol()])
            entrez_id = Simple(terms=[t.GeneName])
            variant = Simple(terms=[t.Variant])
            disease = Simple(terms=[t.CancerType()])
            doid = Simple(terms=[t.CancerType()])
            phenotypes = Simple(terms=[t.Phenotype], modifier={"pat": ","})
            drugs = Simple(terms=[t.DrugName()], modifier={"pat": ","})
            drug_interaction_type = Simple(terms=[t.Drug])
            evidence_type = Simple(terms=[t.EvidenceType])
            evidence_direction = Simple(terms=[t.EvidenceDirection])
            evidence_level = Simple(terms=[t.EvidenceLevel])
            clinical_significance = Simple(terms=[t.ClinicalSignificance])
            evidence_statement = Simple(terms=[t.Evidence])
            citation_id = Simple(terms=[t.Reference])
            source_type = Simple(terms=[t.Reference])
            asco_abstract_id = Simple(terms=[t.ASCO])
            citation = Simple(terms=[t.Reference])
            nct_ids = Simple(terms=[t.NCTid], modifier={"pat": ","})
            rating = Simple()
            evidence_status = Simple(terms=[t.Evidence])
            evidence_id = Simple(terms=[t.Evidence])
            variant_id = Simple()
            # TODO
            #         terms=[t.Variant],
            #         expose=ExposeInfo(
            #             db_JSON=db.JSON(
            #                 db.ForeignKey(internal_fk(RawVariantSummaryModel, fk="variant_id")),
            #                 # nullable=False,
            #             )
            #         ),
            #     )
            gene_id = Simple()
            # TODO
            #         terms=[t.Gene],
            #         expose=ExposeInfo(
            #             db_JSON=db.JSON(
            #                 db.ForeignKey(internal_fk(RawGeneSummaryModel, fk="gene_id")),
            #                 # nullable=False,
            #             )
            #         ),
            #     )
            chromosome = Simple()
            start = Simple()
            stop = Simple()
            reference_bases = Simple()
            variant_bases = Simple()
            representative_transcript = Simple()
            chromosome2 = Simple()
            start2 = Simple()
            stop2 = Simple()
            representative_transcript2 = Simple()
            ensembl_version = Simple()
            reference_build = Simple(terms=[t.Assembly])
            variant_summary = Simple(terms=[t.Variant])
            variant_origin = Simple(terms=[t.Variant])
            last_review_date = Simple(terms=[t.UpdatedDate])
            evidence_civic_url = Simple(terms=[t.Evidence])
            variant_civic_url = Simple(terms=[t.Variant])
            gene_civic_url = Simple(terms=[t.Gene])
