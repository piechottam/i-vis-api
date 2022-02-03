"""
clinvar
-------

:name: clinvar
:url: `<https://www.ncbi.nlm.nih.gov/clinvar>`_
:required: no
:entities: gene, cancer-type, variant
:access: file download
:credentials: none
"""

from pandas import DataFrame

from i_vis.core.version import Date as DateVersion, recent

from . import meta
from ... import terms as t
from ...df_utils import tsv_io, TsvIO, i_vis_col
from ...etl import Simple, ETLSpec
from ...plugin import DataSource
from ...utils import VariableUrl as Url

_URL_PREFIX = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/"

_GENE_SPECIFIC_SUMMARY_URL_VAR = meta.register_variable(
    name="GENE_SPECIFIC_SUMMARY",
    default=_URL_PREFIX + "gene_specific_summary.txt",
)
_VARIANT_SUMMARY_URL_VAR = meta.register_variable(
    name="VARIANT_SUMMARY",
    default=_URL_PREFIX + "variant_summary.txt.gz",
)
_VAR_CITATIONS_URL_VAR = meta.register_variable(
    name="VAR_CITATIONS",
    default=_URL_PREFIX + "var_citations.txt",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[GeneSummaries, VariantSummary, VarCitations],
            str_to_version=DateVersion.from_str,
        )

    @property
    def _latest_version(self) -> DateVersion:
        dates = []
        for etl in self.etl_manager.part2etl.values():
            if etl.extract_opts is None:
                continue

            extract_opts = etl.extract_opts
            if not hasattr(extract_opts, "url"):
                continue

            url = extract_opts.get_url()
            date = DateVersion.from_url(url).to_date()
            dates.append(date)
        return DateVersion(recent(*dates))


#     def _init_tasks(self):
#         from .models import (
#             RawGeneSummaryModel,
#             MappedGeneSummaryModel,
#             RawVariantSummaryModel,
#             MappedVariantSummaryModel,
#             RawVarCitationModel,
#         )
#
#         # download variant summary archive
#         variant_summary_archive_file = self._extractself.get_url(
#             "variant_summary.txt.gz"
#         )
#
#         # unpack archive
#         (variant_summary_file,) = unpack_files(
#             plugin=self,
#             out_fnames=("variant_summary.txt",),
#             readers=(partial(secure_read_df, low_memory=False),),
#             descriptions=(RawVariantSummaryModel.get_raw_desc(),),
#         )
#         unpack_task = Unpack(
#             pname=NAME,
#             archive=variant_summary_archive_file,
#             out_files=(variant_summary_file,),
#         )
#         tasks.append(unpack_task)
#
#         # map and load variant summaries
#         self._map_load(
#             tasks,
#             in_res=variant_summary_file,
#             data_model=RawVariantSummaryModel,
#             mapping_models=MappedVariantSummaryModel,
#             mapping_task=MapRawData,
#             part_name="variant_summaries",
#         )
#
#         # download gene specific summaries
#         extract_gene_task, gene_summaries_file = build_extract_task(
#             plugin=self,
#             url=self.get_url("gene_specific_summary.txt"),
#             reader=partial(secure_read_df, header=1),
#             description=RawGeneSummaryModel.get_raw_desc(),
#         )
#         tasks.append(extract_gene_task)
#
#         # map and load gene summaries
#         self._map_load(
#             tasks,
#             in_res=gene_summaries_file,
#             data_model=RawGeneSummaryModel,
#             mapping_models=MappedGeneSummaryModel,
#             mapping_task=MapRawData,
#             part_name="gene_specific_summary",
#         )
#
#         # download var citations file
#         extract_var_citations_task, var_citations_file = build_extract_task(
#             plugin=self,
#             url=self.get_url("var_citations.txt"),
#             description=RawVarCitationModel.get_raw_desc(),
#             reader=secure_read_df,
#         )
#         tasks.append(extract_var_citations_task)
#
#         # load var citations
#         var_citations_table = Table(
#             plugin=self,
#             tname=RawVarCitationModel.__tablename__,
#             description=RawVarCitationModel.get_tbl_desc(),
#         )
#         tasks.append(
#             Load(
#                 pname=self.name,
#                 file_rid=var_citations_file.rid,
#                 table=var_citations_table,
#                 jsonify=True,
#             )
#         )


# FUTURE imported data breaks FK constraint
class GeneSummaries(ETLSpec):
    class Extract:
        url = Url(_GENE_SPECIFIC_SUMMARY_URL_VAR, latest=True)
        io = TsvIO(read_opts={"header": 1})

        class Raw:
            gene_id = Simple()
            #    expose_col=db.JSON(
            #        db.Integer,
            #        nullable=False,
            #        index=True,
            #        # unique=True,
            #    ),
            # )
            symbol = Simple(terms=[t.HGNCsymbol()])
            total_submissions = Simple()
            total_alleles = Simple()
            submissions_reporting_this_gene = Simple()
            alleles_reported_pathogenic_likely_pathogenic = Simple()
            gene_mim_number = Simple()
            number_uncertain = Simple()
            number_with_conflicts = Simple()


def clean_hgvs(df: DataFrame, col: str) -> DataFrame:
    df.loc[:, i_vis_col(col)] = (
        df[col]
        .str.replace(r"\([^)]+\):", ":", regex=True)
        .str.replace(r"([^:]+):([^ ]+) \(([^)]+)\)", "\\1:\\2", regex=True)
    )
    return df


class VariantSummary(ETLSpec):
    class Extract:
        url = Url(_VARIANT_SUMMARY_URL_VAR, latest=True)
        unpack = "variant_summary.txt"
        io = TsvIO(read_opts={"dtype": str})
        add_id = True

        class Raw:
            variation_id = Simple()
            # TODO
            #    terms=[t.VariantName],
            #    expose_col=db.JSON(
            #        db.Integer,
            #        nullable=False,
            #        index=True,
            #        # unique=True,
            #    ),
            # )
            gene_id = Simple()
            # TODO
            #    terms=[t.GeneName],
            #    expose_col=db.JSON(
            #        db.ForeignKey(internal_fk(RawGeneSummary, fk="gene_id")),
            #        nullable=False,
            #    ),
            # )
            allele_id = Simple()
            type = Simple(terms=[t.VariantType])
            name = Simple(
                terms=[t.HGVS()], modifier=clean_hgvs
            )  # NM_000410.3(HFE):c.989G>T (p.Arg330Met)
            gene_symbol = Simple(terms=[t.HGNCsymbol()])
            hgnc_id = Simple(terms=[t.HGNCid()])  # format: "HGNC:id"
            clinical_significance = Simple(terms=[t.ClinicalSignificance])
            clin_sig_simple = Simple()
            last_evaluated = Simple(terms=[t.UpdatedDate])
            rs_db_snp = Simple(terms=[t.DbSNP])
            nsv_esv_db_var = Simple(terms=[t.DbVar])
            rcv_accession = Simple(terms=[t.RCV])
            phenotype_ids = Simple(terms=[t.Phenotype], modifier={"pat": ","})
            phenotype_list = Simple(terms=[t.Phenotype], modifier={"pat": ","})
            origin = Simple()
            origin_simple = Simple()
            assembly = Simple(
                terms=[t.Assembly]
            )  # e.g.: 'GRCh37'], 'GRCh38', 'na', 'NCBI36
            chromosome_accession = Simple()
            chromosome = Simple()
            start = Simple()
            stop = Simple()
            reference_allele = Simple()
            alternate_allele = Simple()
            cytogenetic = Simple()
            review_status = Simple()
            number_submitters = Simple()
            guidelines = Simple()
            tested_in_gtr = Simple()
            other_ids = Simple(modifier={"pat": ","})
            submitter_categories = Simple()
            position_vcf = Simple(terms=[t.Variant])
            reference_allele_vcf = Simple(terms=[t.Variant])
            alternate_allele_vcf = Simple(terms=[t.Variant])


class VarCitations(ETLSpec):
    class Extract:
        url = Url(_VAR_CITATIONS_URL_VAR, latest=True)
        io = tsv_io
        add_id = True

        class Raw:
            allele_id = Simple()
            variation_id = Simple()
            # TODO
            #    terms=[t.VariantName],
            #    expose_col=db.JSON(
            #        db.ForeignKey(internal_fk(RawVariantSummary, fk="variation_id")),
            #        nullable=False,
            #    ),
            # )
            rs = Simple(terms=[t.DbSNP])
            nsv = Simple(terms=[t.DbVar])
            citation_source = Simple(
                terms=[t.Reference]
            )  # ['PubMed'], 'NCBIBookShelf', 'PubMedCentral']
            citation_id = Simple(terms=[t.PMID])  # see citation_source
