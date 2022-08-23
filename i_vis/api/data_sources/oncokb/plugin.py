"""
OncoKB
------

:name: oncokb
:url: `<https://oncokb.org/>`_
:required: no
:entities: gene, variant
:access: file download
:credentials: API-token
"""

from pandas import DataFrame

from i_vis.core.version import Default as DefaultVersion

from ... import terms as t
from ...df_utils import TsvIO, parquet_io
from ...etl import ETLSpec, HarmonizerModifier, Simple
from ...plugin import DataSource
from ...utils import VariableUrl as Url
from . import meta

_URL_PREFIX = "https://oncokb.org/api/v1/utils/"

_API_TOKEN_VAR = meta.register_variable(
    name="API_TOKEN",
    required=True,
)
# see removed due ro API change
# _VERSION_URL_VAR = meta.register_variable(
#    name="VERSION_URL",
#    default="https://www.oncokb.org/api/v1/info",
# )
_ACTIONABLE_VARIANTS_URL_VAR = meta.register_variable(
    name="ACTIONABLE_VARIANTS",
    default=_URL_PREFIX + "allActionableVariants.txt",
)
_ANNOTATED_VARIANTS_URL_VAR = meta.register_variable(
    name="ANNOTATED_VARIANTS", default=_URL_PREFIX + "allAnnotatedVariants.txt"
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[AnnotatedVariants, ActionableVariant],
            str_to_version=DefaultVersion.from_str,
        )

    # removed due to API change
    # Excerpt from last working output:
    # # This file is generated based on data v1.23 (released on 08/28/2019). Please use with cautious.
    # # We do not suggest using this file for data analysis which usually leads to inaccurate result. Please consider
    # using our APIs or Variant Annotator instead (https://api.oncokb.org/).
    #
    # @property
    # def _latest_version(self) -> DefaultVersion:
    #    headers = {"Authorization": "Bearer " + current_app.config[_API_TOKEN_VAR]}
    #    url = current_app.config[_VERSION_URL_VAR]
    #    r = requests.get(url, headers=headers)
    #    if r.status_code != 200:
    #        raise StatusCode200Error(reponse=r)
    #    data_version = r.json().get("dataVersion", {})
    #    version = data_version.get("version", None)
    #    major, minor = version.split(".")
    #    return DefaultVersion(major, minor)

    @property
    def _latest_version(self) -> DefaultVersion:
        return DefaultVersion(prefix="v", major=1, minor=23)


#     def _init_tasks(self):
#         from .models import (
#             RawAnnotatedVariantsModel,
#             MappedAnnotatedVariantsModel,
#             RawActionableVariantsModel,
#             MappedActionableVariantsModel,
#         )
#
#         # extract annotated variants
#         extract_annotated_vars_task, annotated_vars_file = build_extract_task(
#             plugin=self,
#             url=self.get_url("allAnnotatedVariants.txt"),
#             description=RawAnnotatedVariantsModel.get_raw_desc(),
#             reader=partial(secure_read_df, encoding="ISO-8859-1"),
#         )
#         tasks.append(extract_annotated_vars_task)
#
#         # map and load raw annotated variants
#         self._map_load(
#             tasks,
#             in_res=annotated_vars_file,
#             data_model=RawAnnotatedVariantsModel,
#             mapping_models=MappedAnnotatedVariantsModel,
#             mapping_task=MapRawData,
#             part_name="annotated_vars",
#         )
#
#         # extract raw actionable variants
#         extract_actionable_vars_task, actionable_vars_file = build_extract_task(
#             plugin=self,
#             url=self.get_url("allActionableVariants.txt"),
#             description=RawActionableVariantsModel.get_raw_desc(),
#             reader=secure_read_df,
#         )
#         tasks.append(extract_actionable_vars_task)
#
#         # map and load raw actionable variants
#         self._map_load(
#             tasks,
#             in_res=actionable_vars_file,
#             data_model=RawActionableVariantsModel,
#             mapping_models=MappedActionableVariantsModel,
#             mapping_task=MapRawData,
#             part_name="actionable_vars",
#         )


def make_hgvs_like(df: DataFrame, col: str) -> DataFrame:
    # filter "known" variants
    mask = df["alteration"].str.contains(r"^[A-Z][\d]+[A-Z]")
    df[col] = ""
    df.loc[mask, col] = df.loc[mask, "ref_seq"] + ":" + df.loc[mask, "alteration"]
    return df


class ActionableVariant(ETLSpec):
    class Extract:
        url = Url(_ACTIONABLE_VARIANTS_URL_VAR, latest=True)
        io = TsvIO(
            read_opts={
                "comment": "#",
                "encoding": "ISO-8859-1",
                "skiprows": 3,
            },
            sep="\t",
        )
        add_id = True

        class Raw:
            isoform = Simple()  # t.TranscriptId
            ref_seq = Simple(terms=[t.RefSeqId])
            entrez_gene_id = Simple(terms=[t.EntrezGeneID])
            hugo_symbol = Simple(terms=[t.HGNCsymbol()])
            alteration = Simple(terms=[t.Variant])
            protein_change = Simple()
            cancer_type = Simple(terms=[t.CancerType()])
            level = Simple()
            drugs = Simple(terms=[t.DrugName()])
            pmids_for_drug = Simple(terms=[t.Drug, t.Reference])
            abstracts_for_drug = Simple(terms=[t.Drug, t.Reference])

    class Transform:
        variant = HarmonizerModifier(
            make_hgvs_like, i_vis_hgvs=Simple(terms=[t.HGVS()])
        )
        io = parquet_io


class AnnotatedVariants(ETLSpec):
    class Extract:
        url = Url(_ANNOTATED_VARIANTS_URL_VAR, latest=True)
        io = TsvIO(
            read_opts={
                "comment: " "encoding": "ISO-8859-1",  # ",
                "skiprows": 3,
            }
        )
        add_id = True

        class Raw:
            isoform = Simple()
            ref_seq = Simple(terms=[t.RefSeqId])
            entrez_gene_id = Simple(terms=[t.EntrezGeneID])
            hugo_symbol = Simple(terms=[t.HGNCsymbol()])
            alteration = Simple(terms=[t.Variant])
            protein_change = Simple()
            oncogenicity = Simple()
            mutation_effect = Simple()
            pmids_for_mutation_effect = Simple(terms=[t.Variant, t.Reference])
            abstracts_for_mutation_effect = Simple(terms=[t.Variant, t.Reference])

    class Transform:
        variant = HarmonizerModifier(
            make_hgvs_like, i_vis_hgvs=Simple(terms=[t.HGVS()])
        )
        io = parquet_io
