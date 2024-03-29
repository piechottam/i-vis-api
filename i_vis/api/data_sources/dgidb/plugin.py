"""
DGIdb
-----

:name: dgidb
:url: `<http://www.dgidb.org>`_
:required: no
:entities: drug, gene
:access: file download
:credentials: none
"""
import datetime
from functools import cache
from typing import TYPE_CHECKING, Any, Callable, Mapping, Sequence, cast

import pandas as pd
from dask import dataframe as dd
from pandas import DataFrame, Series

from i_vis.core.version import Date as DateVersion
from i_vis.core.version import recent

from ... import terms as t
from ...config_utils import get_config
from ...core_types.drug.plugin import add_chembl_mapping_rid
from ...df_utils import tsv_io
from ...etl import ETLSpec, Simple
from ...plugin import DataSource
from ...resource import File
from ...task.transform import Process
from ...utils import BaseUrl as Url
from . import meta

if TYPE_CHECKING:
    from ...resource import ResourceId, Resources

_URL_PREFIX = meta.register_variable(
    name="URL_PREFIX",
    default="http://www.dgidb.org/data/monthly_tsvs/",
)
_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="https://www.dgidb.org/downloads",
)
_VERSION_FORMAT_VAR = meta.register_variable(
    name="VERSION_FORMAT",
    default="%Y-%b",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[Interaction, Drugs],
            str_to_version=DateVersion.from_str,
        )

    @property
    def _latest_version(self) -> DateVersion:
        dates = get_dates()
        format_ = get_config()[_VERSION_FORMAT_VAR]
        versions = [datetime.datetime.strptime(d, format_) for d in dates]
        return DateVersion(recent(*versions))

    def _init_tasks(self) -> None:
        from ...core_types.drug.models import drug_name_res_desc

        super()._init_tasks()
        chembl_mapping_file = self.task_builder.res_builder.file(
            fname="_chembl_mapping.tsv",
            io=tsv_io,
            desc=drug_name_res_desc,
        )
        add_chembl_mapping_rid(chembl_mapping_file.rid)
        self.task_builder.add_task(
            FilterChEMBLMapping(
                File.link(self.name, "drugs.tsv"), out_file=chembl_mapping_file
            )
        )


def get_dates() -> Sequence[str]:
    url = get_config()[_VERSION_URL_VAR]
    tbls = pd.read_html(url, attrs={"id": "tsv_downloads"})
    if len(tbls) > 1:
        raise ValueError("Too many tables found.")

    return cast(Sequence[str], tbls[0]["Date"].to_list())


def clean_drug_names(drug_names: Series) -> Series:
    return cast(Series, drug_names.str.replace("^chembl:", "", regex=True))


class FilterChEMBLMapping(Process):
    def __init__(self, in_rid: "ResourceId", out_file: "File") -> None:
        super().__init__(
            pname=meta.name,
            in_rid=in_rid,
            out_res=out_file,
        )

    def dask_meta(self, df: dd.DataFrame) -> Mapping[str, Any]:
        return {
            "chembl_id": str,
            "name": str,
            "data_sources": str,
        }

    def _process(self, df: DataFrame, context: "Resources") -> DataFrame:
        # FIXME 2x clean - how to apply columns
        # Filter ChEMBL IDs
        chembl_rows = df["concept_id"].str.startswith("chembl:", na=False)
        df = df[chembl_rows].copy()
        # ChEMBL IDs that are use as drug names
        cols = ["drug_claim_name", "drug_name"]
        for col in cols + ["concept_id"]:
            df[col] = clean_drug_names(df[col])
        # retain needed columns
        df = df.loc[:, cols + ["concept_id"]]
        df.rename(columns={"concept_id": "chembl_id"}, inplace=True)
        df = df.melt(
            id_vars="chembl_id",
            value_vars=cols,
            value_name="name",
        )
        df = df[["chembl_id", "name"]].dropna().drop_duplicates()
        df["data_sources"] = self.pname
        self.logger.info(f"{len(df)} ChEMBL IDs retained")
        breakpoint()
        return df


@cache
def url_callback(name: str) -> Callable[[], str]:
    def helper() -> str:
        plugin = DataSource.get(meta.name)
        version = cast(DateVersion, plugin.latest_version)
        s = version.to_date().strftime(get_config()[_VERSION_FORMAT_VAR])
        return str(get_config()[_URL_PREFIX]) + "/" + s + "/" + name

    return helper


class Interaction(ETLSpec):
    class Extract:
        url = Url(url_callback("interactions.tsv"))
        io = tsv_io
        add_id = True

        class Raw:
            gene_name = Simple(terms=[t.HGNCsymbol()])
            gene_claim_name = Simple()
            entrez_id = Simple(terms=[t.EntrezGeneID])
            interaction_claim_source = Simple()
            interaction_types = Simple()
            drug_claim_name = Simple(terms=[t.DrugName()], modifier=clean_drug_names)
            drug_claim_primary_name = Simple(
                terms=[t.DrugName()], modifier=clean_drug_names
            )
            drug_name = Simple(terms=[t.DrugName()], modifier=clean_drug_names)
            drug_concept_id = Simple(terms=[t.DrugName()], modifier=clean_drug_names)
            pmids = Simple(terms=[t.PMID])


class Drugs(ETLSpec):
    class Extract:
        url = Url(url_callback("drugs.tsv"))
        io = tsv_io
        add_id = True

        class Raw:
            drug_claim_name = Simple(terms=[t.DrugName()], modifier=clean_drug_names)
            drug_name = Simple(terms=[t.DrugName()], modifier=clean_drug_names)
            concept_id = Simple(terms=[t.DrugName()], modifier=clean_drug_names)
            drug_claim_source = Simple(terms=[t.Drug])
