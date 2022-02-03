"""
COSMIC
------

:name: cosmic
:url: `<https://cancer.sanger.ac.uk/cosmic>`_
:required: no
:entities: gene, cancer-type, variant
:access: file download
:credentials: username, password
"""

from typing import Callable
import re
import os
from base64 import b64encode

import requests
from pandas import DataFrame

from i_vis.core.constants import GenomeAssembly
from i_vis.core.version import Default as DefaultVersion, by_xpath
from i_vis.core.utils import StatusCode200Error

from . import meta
from ...config_utils import get_config
from ... import terms as t
from ...df_utils import DaskDataFrameIO, i_vis_col, csv_io
from ...etl import Simple, ETLSpec
from ...plugin import DataSource
from ...utils import DynamicUrl

_URL_PREFIX_VAR = meta.register_variable(
    name="URL_PREFIX",
    default="https://cancer.sanger.ac.uk/cosmic/file_download/{assembly}/cosmic/{version}",
)
_USER_VAR = meta.register_variable(name="USER")
_PASS_VAR = meta.register_variable(name="PASS")

_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="https://cancer.sanger.ac.uk/cosmic",
)
_VERSION_XPATH_VAR = meta.register_variable(
    name="VERSION_XPATH",
    default="/html/body/div[1]/article/div[2]/section[1]/h1",
)
_VERSION_PATTERN_VAR = meta.register_variable(
    name="VERSION_PATTERN",
    default="COSMIC v([^v,]+), released .+",
)


def get_auth(user: str = "", password: str = "") -> str:
    user = user or get_config()[_USER_VAR]
    password = password or get_config()[_PASS_VAR]

    s = "{}:{}".format(user, password)
    return b64encode(bytes(s, "utf-8")).decode("utf-8")


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[MutantExport, CGC],
            str_to_version=DefaultVersion.from_str,
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        s = by_xpath(
            url=get_config()[_VERSION_URL_VAR],
            xpath=get_config()[_VERSION_XPATH_VAR],
        )
        # expected format, e.g.: 'COSMIC v90, released 05-SEP-19'
        match = re.search(get_config()[_VERSION_PATTERN_VAR], s)
        if not match:
            raise ValueError("Pattern did not match anything.")

        s = match.group(1)
        return DefaultVersion(major=int(s))


#     def _init_tasks(self):
#
#         # download mutant export archive
#         mutant_export_archive_file = File(
#             plugin=self,
#             fname="mutant_export.tsv.gz",
#         )
#         extract_mutant_task = Delayed(
#             pname=self.name,
#             func=partial(
#                 self.get_url,
#                 name="CosmicMutantExport.tsv.gz",
#             ),
#             out_file=mutant_export_archive_file,
#         )
#         tasks.append(extract_mutant_task)
#
#         # unpack mutant export archive
#         (mutant_export_file,) = unpack_files(
#             plugin=self,
#             out_fnames=("mutant_export.tsv",),
#             readers=(
#                 partial(
#                     secure_read_df,
#                     sep="\t",
#                     encoding="ISO-8859-1",
#                     chunksize=5 * 10 ** 6,
#                     dtype={
#                         "GRCh": "Int64",
#                         "HGNC ID": "Int64",
#                     },
#                 ),
#             ),
#             descriptions=(RawMutantExportModel.get_raw_desc(),),
#         )
#         unpack_task = Unpack(
#             pname=NAME,
#             archive=mutant_export_archive_file,
#             out_files=(mutant_export_file,),
#         )
#         tasks.append(unpack_task)
#
#         # map and load mutant export
#         self._map_load(
#             in_res=mutant_export_file,
#             data_model=RawMutantExportModel,
#             mapping_models=MappedMutantExportModel,
#             mapping_task=MapRawData,
#             part_name="mutant_export",
#         )
#
#         # download gene consensus
#         cancer_gene_consensus_file = File(
#             plugin=self,
#             fname="cancer_gene_census.csv",
#             description=RawCGCModel.get_raw_desc(),
#             reader=partial(secure_read_df, sep=","),
#         )
#         tasks.append(
#             Delayed(
#                 pname=self.name,
#                 func=partial(
#                     self.get_url,
#                     name=cancer_gene_consensus_file.fname,
#                 ),
#                 out_file=cancer_gene_consensus_file,
#             )
#         )
#
#         # map and load cancer consensus
#         self._map_load(
#             in_file=cancer_gene_consensus_file,
#             data_model=RawCGCModel,
#             mapping_models=MappedCGCModel,
#             mapping_task=MapRawData,
#             part_name="cancer_gene_census",
#         )


def url_callback(name: str) -> Callable[[], str]:
    def helper() -> str:
        plugin = DataSource.get(meta.name)

        url = os.path.join(get_config()[_URL_PREFIX_VAR], name).format(
            assembly=GenomeAssembly.GRCH38.value,
            version="v" + str(plugin.version.working),
        )
        auth = get_auth()
        headers = {"Authorization": "Basic " + auth}
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            raise StatusCode200Error(response=r)

        data = r.json()
        return str(data["url"])

    return helper


class CGC(ETLSpec):
    class Extract:
        url = DynamicUrl(url_callback("cancer_gene_census.csv"))
        out_fname = "cancer_gene_census.csv"
        io = csv_io
        add_id = True

        class Raw:
            gene_symbol = Simple(terms=[t.HGNCsymbol()])
            name = Simple()
            genome_location = Simple(terms=[t.GenomePosition("chr:start-end")])
            tier = Simple()
            hallmark = Simple()
            chr_band = Simple()
            somatic = Simple()
            germline = Simple()
            tumour_types_somatic = Simple(terms=[t.CancerType()], modifier={"pat": ","})
            tumour_types_germline = Simple(
                terms=[t.CancerType()], modifier={"pat": ","}
            )
            cancer_syndrome = Simple()
            tissue_type = Simple(terms=[t.Tissue], modifier={"pat": ","})
            molecular_genetics = Simple()  # Dom|Rec
            role_in_cancer = Simple()  # TSG, oncogene, etc.
            mutation_types = Simple(terms=[t.VariantType])
            translocation_partner = Simple()
            other_germline_mut = Simple()
            other_syndrome = Simple()
            synonyms = Simple(terms=[t.GeneName], modifier={"pat": ","})


def clean_gene_name(df: DataFrame, col: str) -> DataFrame:
    # t.HGNCsymbol"_"EnsemblTranscript
    df[i_vis_col(col)] = df[col].str.replace(r"^(.+)_ENST.+", r"\1", regex=True)
    return df


def fix_hgnc_id(df: DataFrame, col: str) -> DataFrame:
    df[i_vis_col(col)] = "HGNC:" + df[col].astype(str)  # type: ignore
    return df


class MutantExport(ETLSpec):
    class Extract:
        url = DynamicUrl(url_callback("CosmicMutantExport.tsv.gz"))
        io = DaskDataFrameIO(
            read_callback="read_csv",
            read_opts={
                "encoding": "ISO-8859-1",
                "dtype": str,
            },
            sep="\t",
        )
        add_id = True

        class Raw:
            gene_name = Simple(
                terms=[t.HGNCsymbol(), t.EnsemblTranscripID], modifier=clean_gene_name
            )
            accession_number = Simple()
            gene_cds_length = Simple(terms=[t.Gene])
            hgnc_id = Simple(terms=[t.HGNCid()], modifier=fix_hgnc_id)
            sample_name = Simple()
            id_sample = Simple()
            id_tumour = Simple()
            primary_site = Simple()
            site_subtype_1 = Simple()
            site_subtype_2 = Simple()
            site_subtype_3 = Simple()
            primary_histology = Simple(terms=[t.CancerType()])
            histology_subtype_1 = Simple(terms=[t.CancerType()])
            histology_subtype_2 = Simple(terms=[t.CancerType()])
            histology_subtype_3 = Simple(terms=[t.CancerType()])
            genome_wide_screen = Simple()
            genomic_mutation_id = Simple()
            legacy_mutation_id = Simple()
            mutation_id = Simple()
            mutation_cds = Simple(terms=[t.VariantType])
            mutation_aa = Simple(terms=[t.VariantType])
            mutation_description = Simple(terms=[t.VariantType])
            mutation_zygosity = Simple()
            loh = Simple()
            grch = Simple(terms=[t.Assembly])
            mutation_genome_position = Simple()
            mutation_strand = Simple()
            snp = Simple(terms=[t.VariantType])
            resistance_mutation = Simple()
            fathmm_prediction = Simple()
            fathmm_score = Simple(terms=[t.FATHMM])
            mutation_somatic_status = Simple(terms=[t.Variant])
            pubmed_pmid = Simple(terms=[t.PMID])
            id_study = Simple()
            sample_type = Simple()
            tumour_origin = Simple(terms=[t.CancerType])  # type
            age = Simple()
            hgvsp = Simple(terms=[t.HGVSp()])
            hgvsc = Simple(terms=[t.HGVSc()])
            hgvsg = Simple(terms=[t.HGVSg()])
