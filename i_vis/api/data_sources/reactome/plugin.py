"""
Reactome
--------

:name: reactome
:url: `<http://reactome.org/>`_
:required: no
:entities: gene
:access: file download
:credentials: none
"""

from typing import Any, TYPE_CHECKING, Optional

import requests
from pandas import DataFrame

from i_vis.core.file_utils import prefix_fname
from i_vis.core.version import Default as DefaultVersion
from i_vis.core.utils import StatusCode200Error

from . import meta
from ...config_utils import get_config
from ... import terms as t
from ...df_utils import TsvIO
from ...etl import ETLSpec, Simple, Modifier
from ...plugin import DataSource
from ...task.transform import Process
from ...utils import VariableUrl as Url

if TYPE_CHECKING:
    from ...resource import ResourceId, Resources, ResourceDesc
    from ...df_utils import DataFrameIO

_URL_VAR = meta.register_variable(
    name="URL",
    default="http://reactome.org/download/current/Ensembl2Reactome_All_Levels.txt",
)
_VERSION_URL = meta.register_variable(
    name="VERSION_URL",
    default="https://reactome.org/ContentService/data/database/version",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DefaultVersion.from_str
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        url = get_config()[_VERSION_URL]
        r = requests.get(url)
        if r.status_code != 200:
            raise StatusCode200Error(reponse=r)

        major = int(r.text)
        return DefaultVersion(major)


# pylint: disable=too-many-arguments
class RetainHumanEntries(Process):
    def __init__(
        self,
        in_rid: "ResourceId",
        out_fname: str = "",
        io: Optional["DataFrameIO"] = None,
        desc: Optional["ResourceDesc"] = None,
        **kwargs: Any,
    ):
        if not io:
            in_res = in_rid.get()
            io = in_res.io
            assert io is not None
        out_res = io.create(
            in_rid.pname,
            name=out_fname or prefix_fname(in_rid.name, "_filtered"),
            desc=desc,
        )
        super().__init__(
            in_rid=in_rid,
            out_res=out_res,
            **kwargs,
        )

    def _process(self, df: DataFrame, context: "Resources") -> DataFrame:
        self.logger.info(f"{len(df)} total entries loaded")
        human_mask = df["species"].eq("Homo sapiens")
        df = df.loc[human_mask].copy()
        self.logger.info(f"{len(df)} Homo Sapiens related entries retained")
        return df


class Spec(ETLSpec):
    class Extract:
        url = Url(_URL_VAR, latest=True)
        io = TsvIO(
            read_opts={
                "names": [
                    "ensembl_gene_id",
                    "reactome_id",
                    "reactome_url",
                    "pathway_name",
                    "evidence_code",
                    "species",
                ]
            }
        )
        add_id = True

        class Raw:
            ensembl_gene_id = Simple(
                terms=[t.EnsemblGeneID()]
            )  # filtered by species "Homo Sapiens"
            reactome_id = Simple(terms=[t.ReactomeId])
            reactome_url = Simple()
            pathway_name = Simple(terms=[t.Pathway])
            evidence_code = Simple(terms=[t.Evidence])
            species = Simple(terms=[t.Species])

    class Transform:
        task = Modifier(RetainHumanEntries)
