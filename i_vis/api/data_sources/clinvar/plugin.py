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

from typing import cast

from pandas import Series

from i_vis.core.version import Date as DateVersion
from i_vis.core.version import recent

from ... import terms as t
from ...df_utils import TsvIO
from ...etl import ETLSpec, Simple
from ...plugin import DataSource
from ...utils import VariableUrl as Url
from . import meta

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


# FUTURE imported data breaks FK constraint
class GeneSummaries(ETLSpec):
    class Extract:
        url = Url(_GENE_SPECIFIC_SUMMARY_URL_VAR, latest=True)
        io = TsvIO(read_opts={"header": 1, "dtype": "str"})
        add_id = True

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


def clean_hgvs(hgvs: Series) -> Series:
    return cast(
        Series,
        hgvs.str.replace(r"\([^)]+\):", ":", regex=True).str.replace(
            r"([^:]+):([^ ]+) \(([^)]+)\)", "\\1:\\2", regex=True
        ),
    )


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
        io = TsvIO(read_opts={"dtype": str})
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
