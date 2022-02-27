"""
Genenames (HGNC)
----------------

:name: genenames
:url: `<https://www.genenames.org/>`_
:required: no
:entities: gene
:access: biomart, file download
:credentials: none
"""

from datetime import datetime
from typing import TYPE_CHECKING, cast

import requests
from pandas import DataFrame

from i_vis.core.file_utils import read_query
from i_vis.core.version import Date as DateVersion
from i_vis.core.utils import StatusCode200Error

from . import meta
from ... import terms as t
from ...config_utils import get_config
from ...df_utils import tsv_io, add_pk
from ...etl import ETLSpec, Simple
from ...file_utils import qualify_fname
from ...plugin import DataSource
from ...core_types.gene.plugin import add_hgnc_mapping_rid
from ...utils import BaseUrl as Url
from ...resource import File
from ...task.transform import Process

if TYPE_CHECKING:
    from ...resource import Resources

_URL_VAR = meta.register_variable(
    name="URL",
    default="https://biomart.genenames.org/martservice/results",
)
_QUERY_FNAME = meta.register_variable(
    name="QUERY_FNAME",
    default=qualify_fname(__file__, "data_query.xml"),
)
_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="http://rest.genenames.org/info",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DateVersion.from_str
        )

    @property
    def _latest_version(self) -> DateVersion:
        headers = {"Accept": "application/json"}
        r = requests.get(get_config()[_VERSION_URL_VAR], headers=headers, timeout=100)
        if r.status_code != 200:
            raise StatusCode200Error(reponse=r)

        data = r.json()
        last_modified_str = data["lastModified"]
        return DateVersion(
            datetime.strptime(last_modified_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        )

    def _init_tasks(self) -> None:
        from ...core_types.gene.models import gene_name_res_desc

        super()._init_tasks()

        fname = "_mapping.tsv"
        mapping_file = self.task_builder.res_builder.file(
            fname=fname, io=tsv_io, desc=gene_name_res_desc
        )
        add_hgnc_mapping_rid(mapping_file.rid)
        self.task_builder.add_task(
            HgncMapping(
                pname=self.name,
                in_rid=File.link(self.name, "data.tsv"),
                out_res=mapping_file,
            )
        )


def url_callback() -> str:
    url = get_config()[_URL_VAR]
    query_fname = get_config()[_QUERY_FNAME]
    q = read_query(query_fname)
    return f"{url}?query={q}"


class HgncMapping(Process):
    def _process(self, df: DataFrame, context: "Resources") -> DataFrame:
        df = cast(DataFrame, df.convert_dtypes())
        # add FK
        fk = "hgnc_id"
        df[fk] = df["hgnc_id"]
        # gather columns that contain ids/names
        cols_mask = df.columns.str.contains("(?:_symbol|_name|_accession|_id)$")
        cols = df.columns[cols_mask]
        # extract i-vis columns
        id_vars = fk

        df = (
            df.loc[:, cols]
            .melt(
                id_vars=id_vars,
                value_name="name",
                var_name="type",
            )
            .dropna()
            .sort_values(fk)
            .explode("name")
            .dropna()
            .drop_duplicates(ignore_index=True)
            .pipe(add_pk)
        )
        df["data_sources"] = self.out_res.plugin.name
        return df


# e.g.:
DATE_FORMAT = "%Y-%m-%d"


class Spec(ETLSpec):
    class Extract:
        url = Url(url_callback, latest=True)
        out_fname = "data.tsv"
        io = tsv_io
        add_id = True

        class Raw:
            hgnc_id = Simple(terms=[t.HGNCid()])
            status = Simple(terms=[t.Gene])
            approved_symbol = Simple(terms=[t.HGNCsymbol])
            approved_name = Simple(terms=[t.HGNCname])
            alias_symbol = Simple(terms=[t.HGNCsymbol])
            alias_name = Simple(terms=[t.HGNCname])
            previous_symbol = Simple(terms=[t.HGNCsymbol])
            previous_name = Simple(terms=[t.HGNCname])
            chromosome = Simple(terms=[t.Gene])
            chromosome_location = Simple(terms=[t.Gene])
            locus_group = Simple(terms=[t.Gene])
            locus_type = Simple(terms=[t.Gene])
            date_approved = Simple(terms=[t.CreatedDate(date_format=DATE_FORMAT)])
            date_name_changed = Simple(terms=[t.UpdatedDate(date_format=DATE_FORMAT)])
            date_symbol_changed = Simple(terms=[t.UpdatedDate(date_format=DATE_FORMAT)])
            date_modified = Simple(terms=[t.UpdatedDate(date_format=DATE_FORMAT)])
            ensembl_gene_id = Simple(terms=[t.GeneName])
            ncbi_gene_id = Simple(terms=[t.GeneName])
            ucsc_gene_id = Simple(terms=[t.GeneName])
            ref_seq_accession = Simple(terms=[t.GeneName])
            ccds_accession = Simple(terms=[t.GeneName])
            uni_prot_accession = Simple(terms=[t.GeneName])
