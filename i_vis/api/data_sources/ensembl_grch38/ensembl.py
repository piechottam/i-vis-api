import re
from typing import TYPE_CHECKING, Optional, Type, Sequence

from pandas import DataFrame

from i_vis.core.constants import GenomeAssembly
from i_vis.core.file_utils import read_query
from i_vis.core.version import Default as DefaultVersion, by_xpath

from . import meta, _genome_prefix
from ...plugin import DataSource
from ...task.transform import Process
from ...df_utils import clean_df, sort_prefixed

if TYPE_CHECKING:
    from ...etl import ETLSpec
    from ...resource import Resources


def get_url(fname: str, genome_assembly: GenomeAssembly) -> str:
    q = read_query(fname)
    return f"https://{_genome_prefix(genome_assembly)}ensembl.org/biomart/martservice?query={q}"


class EnsemblPlugin(DataSource):
    def __init__(
        self,
        genome_assembly: GenomeAssembly,
        etl_specs: Optional[Sequence[Type["ETLSpec"]]] = None,
    ):
        self._assembly = genome_assembly
        super().__init__(
            meta=meta,
            etl_specs=etl_specs,
            str_to_version=DefaultVersion.from_str,
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        url = f"http://{_genome_prefix(self._assembly)}ensembl.org/Homo_sapiens/Info/Index"
        xpath = "/html/body/div[1]/div/div[2]/div[2]/div[1]/div[1]/p"
        s = by_xpath(url, xpath)
        patterns = {
            GenomeAssembly.GRCH37: r"Ensembl GRCh37 release ([\d]+) - ",
            GenomeAssembly.GRCH38: r"Ensembl release ([\d]+) -",
        }
        pattern = patterns[self._assembly]
        match = re.search(pattern, s)
        if not match:
            raise ValueError("Pattern did not match anything.")
        return DefaultVersion(
            major=int(match.group(1)), suffix=f"-{self._assembly.value}"
        )


class FilterHgncMapping(Process):
    def _process(self, df: DataFrame, context: "Resources") -> DataFrame:
        # filter hgnc_ids: remove empty rows
        pre = len(df)
        df = df.drop_duplicates().dropna()
        df = clean_df(df)
        post = len(df)
        if post < pre:
            self.logger.info(f"Filtered {post - pre}")

        # melt to type and name
        df = df.melt(id_vars=["hgnc_id"], var_name="type", value_name="name")
        df = sort_prefixed(df, "hgnc_id", "HGNC:")
        return df
