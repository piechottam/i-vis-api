"""
NCBI
----

:name: ncbi
:url: `<https://ncbi.nlm.nih.gov/>`_
:required: no
:entities: gene
:access: file download
:credentials: none
"""

import re
from typing import TYPE_CHECKING

import requests

from i_vis.core.version import Default as DefaultVersion
from i_vis.core.utils import StatusCode200Error

from . import meta
from ... import terms as t
from ...config_utils import get_config
from ...df_utils import TsvIO
from ...etl import ETLSpec, Simple
from ...plugin import DataSource
from ...utils import VariableUrl as Url

if TYPE_CHECKING:
    pass

_URL_VAR = meta.register_variable(
    name="URL",
    default="https://ftp.ncbi.nlm.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz",
)
_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="https://ftp.ncbi.nlm.nih.gov/gene/DATA/README_ensembl",
)
_VERSION_PATTERN_VAR = meta.register_variable(
    name="VERSION_PATTERN",
    default=r"Homo sapiens Annotation Release ([\S]+)",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DefaultVersion.from_str
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        url = str(get_config()[_VERSION_URL_VAR])
        r = requests.get(url)
        if r.status_code != 200:
            raise StatusCode200Error(reponse=r)

        ensembl = r.text
        # expected format: 9606\tHomo sapiens Annotation Release 109\t[...]
        pattern = str(get_config()[_VERSION_PATTERN_VAR])
        match = re.search(pattern, ensembl)
        if not match:
            raise ValueError("Pattern did not match anything.")

        s = match.group(1)
        major = int(str(s))
        return DefaultVersion(major)


# TODO don't use
# class ExtractColumns(BaseModifier):
#    def _do_work(self, context: "Resources") -> None:
#        in_file = context[self.in_rid]
#        df = in_file.read()
#        save_df(df.iloc[:, [2, 3, 5, 6, 9, 11, 12]], self.out_res)
#        self.out_res.update_db()
#        self.out_res.dirty = True


class Spec(ETLSpec):
    class Extract:
        url = Url(_URL_VAR, latest=True)
        io = TsvIO()
        add_id = True

        class Raw:
            # TODO add missing columns
            tax_id = Simple()
            gene_id = Simple(terms=[t.Gene])
            symbol = Simple(terms=[t.HGNCsymbol()])
            locus_tag = Simple()
            synonyms = Simple()
            db_xrefs = Simple()
            chromosome = Simple()
            map_location = Simple()
            description = Simple()
            type_of_gene = Simple()
            symbol_from_nomenclature_authority = Simple()
            full_name_from_nomenclature_authority = Simple()
            nomenclature_status = Simple()
            other_designations = Simple()
            modification_date = Simple()
            feature_typ = Simple()
