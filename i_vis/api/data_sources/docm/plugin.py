"""
DoCM
----

:name: docm
:url: `<https://www.docm.info>`_
:required: no
:entities: gene, variant
:access: file download
:credentials: none
"""

from pandas import DataFrame

from i_vis.core.version import Default as DefaultVersion
from . import meta
from ...config_utils import get_config
from ... import terms as t
from ...df_utils import tsv_io, i_vis_col
from ...etl import Simple, ETLSpec, HarmonizerModifier
from ...plugin import DataSource
from ...utils import BaseUrl as Url

_MAJOR = meta.register_variable(
    name="MAJOR",
    default=3,
)
_MINOR = meta.register_variable(
    name="MINOR",
    default=2,
)
_URL = meta.register_variable(
    name="URL",
    default="http://www.docm.info/api/v1/variants.tsv?versions={MAJOR}.{MINOR}",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DefaultVersion.from_str
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        return DefaultVersion(
            major=get_config()[_MAJOR],
            minor=get_config()[_MINOR],
        )


def make_hgvs_like(df: DataFrame) -> DataFrame:
    df[i_vis_col("gene_aa")] = df["gene"] + ":" + df["amino_acid"]
    return df


def fetch_url() -> str:
    major = get_config()[_MAJOR]
    minor = get_config()[_MINOR]
    return str(get_config()[_URL].format(**{"MAJOR": major, "MINOR": minor}))


class Spec(ETLSpec):
    class Extract:
        url = Url(fetch_url)
        io = tsv_io
        add_id = True

        class Raw:
            hgvs = Simple(terms=[t.HGVSc()])
            chromosome = Simple()
            start = Simple()
            stop = Simple()
            read = Simple()
            variant = Simple()  # variant Sequence/bases
            reference_version = Simple(terms=[t.Assembly])  # ['GRCh37']
            gene = Simple(terms=[t.HGNCsymbol()])
            mutation_type = Simple(terms=[t.VariantType])
            amino_acid = Simple(terms=[t.Variant])  # AA
            diseases = Simple(terms=[t.CancerType()], modifier={"pat": ","})
            pubmed_sources = Simple(terms=[t.PMID], modifier={"pat": ","})

    class Transform:
        variant = HarmonizerModifier(
            make_hgvs_like,
            i_vis_gene_aa=Simple(terms=[t.HGVSp()]),
        )
