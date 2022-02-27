"""
PMKB
----

:name: pmkb
:url: `<https://pmkb.weill.cornell.edu/>`_
:required: no
:entities: drug, gene, variant
:access: file download
:credentials: none
"""

from typing import Any, TYPE_CHECKING, Optional, Iterable, Union
import pandas as pd

from i_vis.core.version import Default as DefaultVersion

from . import meta
from ... import terms as t
from ...etl import Simple, ETLSpec
from ...plugin import DataSource
from ...utils import VariableUrl as Url
from ...df_utils import PandasDataFrameIO

_URL_VAR = meta.register_variable(
    name="URL",
    default="https://pmkb.weill.cornell.edu/therapies/download.xlsx",
)

if TYPE_CHECKING:
    from ...resource import Resource
    from ...utils import I_VIS_Logger


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DefaultVersion.from_str
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        return DefaultVersion(major=1)


class CustomReader(PandasDataFrameIO):
    def __init__(self) -> None:
        super().__init__(read_callback="read_excel", read_opts={"engine": "openpyxl"})

    def _read_df(
        self, in_res: "Resource", logger: Optional["I_VIS_Logger"] = None, **kwargs: Any
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
        df = super()._read_df(in_res, logger, **kwargs)
        if isinstance(df, pd.DataFrame):
            return self._fix(df)

        return [self._fix(df_) for df_ in df]

    @staticmethod
    def _fix(df: pd.DataFrame) -> pd.DataFrame:
        # change sep from ',' to ';'
        df.iloc[:, 1].str.replace(",", ";", regex=False)
        # merge citations into one column
        citations = df.iloc[:, 7:].apply(lambda x: "; ".join(x.astype(str)), axis=1)
        # retain everything but citation columns
        df = df.iloc[:, :7]
        df["citations"] = citations
        return df


class Spec(ETLSpec):
    class Extract:
        url = Url(_URL_VAR, latest=True)
        io = CustomReader()
        add_id = True

        class Raw:
            gene = Simple(terms=[t.GeneName()])
            tumor_types = Simple(terms=[t.CancerType()], modifier={"pat": ", "})
            tissue_types = Simple(terms=[t.Tissue], modifier={"pat": ", "})
            variants = Simple(terms=[t.Variant], modifier={"pat": ", "})
            tier = Simple()
            pmkb_url = Simple()
            interpretations = Simple()
            citations = Simple(terms=[t.Citation])
