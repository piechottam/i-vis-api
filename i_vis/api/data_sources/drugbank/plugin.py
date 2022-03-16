"""
DrugBank
--------

In order to use Drugbank within I-VIS, an user account on Drugbank is required!
To create an account go to https://go.drugbank.com/ and follow the instructions.

MAKE SURE TO CHOOSE THE CORRECT LICENSE FOR DRUGBANK GIVEN YOUR APPLICATION.
I-VIS is intended for academic use only!

For citing Drugbank and other information go to: https://go.drugbank.com/about

:name: drugbank
:url: `<https://www.drugbank.ca>`_
:required: optional (extend drug mapping)
:entities: drug
:access: file download
:credentials: username, password
"""

from typing import Any, Mapping, Sequence, TYPE_CHECKING

import requests
from packaging.version import parse
from pandas import DataFrame, Series
from requests.auth import HTTPBasicAuth

from i_vis.core.version import Default as DefaultVersion
from i_vis.core.utils import StatusCode200Error

from . import meta
from ... import terms as t
from ...config_utils import get_config
from ...core_types.drug.plugin import add_chembl_mapping_rid
from ...df_utils import i_vis_col, tsv_io, flat_parquet_io
from ...etl import ETLSpec, Simple, Modifier
from ...plugin import DataSource
from ...plugin_exceptions import LatestUrlRetrievalError
from ...resource import Parquet
from ...task.transform import Process, ConvertXML
from ...utils import DynamicUrl as Url

if TYPE_CHECKING:
    from ...resource import Resources

_USER_VAR = meta.register_variable(name="USER")
_PASS_VAR = meta.register_variable(name="PASS")

_URL_VAR = meta.register_variable(
    name="URL",
    default="https://go.drugbank.com/releases/{version}/downloads/all-full-database",
)
_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="https://go.drugbank.com/releases.json",
)


def get_request(url: str, username: str, password: str) -> requests.Response:
    return requests.get(
        url, auth=HTTPBasicAuth(username, password), allow_redirects=True
    )


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DefaultVersion.from_str
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        url = get_config()[_VERSION_URL_VAR]
        r = requests.get(url)
        if r.status_code != 200:
            raise StatusCode200Error(reponse=r)

        releases = r.json()
        versions = [parse(release["version"]) for release in releases]
        latest = str(max(versions))
        return DefaultVersion.from_str(latest)

    def _init_tasks(self) -> None:
        super()._init_tasks()

        chembl_mapping_file = self.task_builder.res_builder.file(
            fname="_chembl_mapping.tsv",
            io=tsv_io,
        )
        add_chembl_mapping_rid(chembl_mapping_file.rid)
        self.task_builder.add_task(
            FilterChEMBLMapping(
                in_rid=Parquet.link(self.name, "_" + meta.name + ".parquet"),
                out_res=chembl_mapping_file,
            )
        )


def url_callback() -> str:
    data_source = DataSource.get(meta.name)
    latest_version = data_source.latest_version
    if not latest_version.is_known:
        raise LatestUrlRetrievalError(data_source.name)

    return str(
        get_config()[_URL_VAR].format(version=str(latest_version).replace(".", "-"))
    )


def extract_synonyms(synonyms: Series) -> Sequence[str]:
    if not synonyms:
        return []

    synonyms = synonyms["synonym"]
    if isinstance(synonyms, dict):
        synonyms = [synonyms]
    return [
        synonym["#text"]
        for synonym in synonyms
        if synonym["@language"] in ("german", "english")
    ]


def extract_synonyms_wrapper(df: DataFrame, col: str) -> DataFrame:
    df[i_vis_col(col)] = df[col].apply(extract_synonyms)
    return df


def extract_chembl(external_ids: Series) -> Sequence[str]:
    if not external_ids:
        return []

    external_ids = external_ids["external-identifier"]
    if isinstance(external_ids, dict):
        external_ids = [external_ids]
    result = []
    for external_id in external_ids:
        if (
            external_id["resource"] == "ChEMBL"
            and external_id["identifier"] not in result
        ):
            result.append(external_id["identifier"])
    return result


def extract_chembl_wrapper(df: DataFrame, col: str) -> DataFrame:
    df[i_vis_col(col)] = df[col].apply(extract_chembl)
    return df


class FilterChEMBLMapping(Process):
    def _process(self, df: DataFrame, context: "Resources") -> DataFrame:
        new_df = DataFrame()
        new_df["chembl_id"] = df["external_identifiers"].apply(extract_chembl)
        new_df["name"] = df["name"]
        new_df["synonyms"] = df["synonyms"].apply(extract_synonyms)
        new_df = new_df.dropna(subset=["chembl_id"])
        new_df = new_df.explode("synonyms")
        new_df = new_df.melt(
            id_vars="chembl_id",
            var_name="name_type",
            value_name="name",
        ).drop(columns="name_type")
        new_df = new_df.explode("chembl_id").dropna().drop_duplicates()
        new_df["data_sources"] = meta.name
        return new_df


def url_args_callback() -> Mapping[str, Any]:
    return {
        "auth": (
            get_config()[_USER_VAR],
            get_config()[_PASS_VAR],
        ),
        "allow_redirects": True,
    }


class Spec(ETLSpec):
    class Extract:
        url = Url(url_callback, args=url_args_callback)
        out_fname = "database.zip"
        unpack = "full database.xml"

    class Transform:
        task = Modifier(
            ConvertXML,
            task_opts={
                "out_fname": "_" + meta.name + ".parquet",
                "io": flat_parquet_io,
                "getter": lambda dt: dt["drugbank"]["drug"],
                "add_id": True,
            },
        )

        class Raw:
            type = Simple()
            created = Simple()
            updated = Simple()
            drugbank_id = Simple()
            name = Simple(terms=[t.DrugName()])
            description = Simple()
            cas_number = Simple()
            unii = Simple()
            state = Simple()
            groups = Simple()
            general_references = Simple()
            synthesis_reference = Simple()
            indication = Simple()
            pharmacodynamics = Simple()
            mechanism_of_action = Simple()
            toxicity = Simple()
            metabolism = Simple()
            absorption = Simple()
            half_life = Simple()
            protein_binding = Simple()
            route_of_elimination = Simple()
            volume_of_distribution = Simple()
            clearance = Simple()
            classification = Simple()
            salts = Simple()
            synonyms = Simple(terms=[t.DrugName()], modifier=extract_synonyms_wrapper)
            products = Simple()
            international_brands = Simple()
            mixtures = Simple()
            packagers = Simple()
            manufacturers = Simple()
            prices = Simple()
            categories = Simple()
            affected_organisms = Simple()
            dosages = Simple()
            atc_codes = Simple()
            ahfs_codes = Simple()
            pdb_entries = Simple()
            fda_label = Simple()
            msds = Simple()
            patents = Simple()
            food_interactions = Simple()
            drug_interactions = Simple()
            Sequences = Simple()
            experimental_properties = Simple()
            external_identifiers = Simple(
                terms=[t.ChEMBLid()], modifier=extract_chembl_wrapper
            )
            external_links = Simple()
            pathways = Simple()
            reactions = Simple()
            snp_effects = Simple()
            snp_adverse_drug_reactions = Simple()
            targets = Simple()
            enzymes = Simple()
            carriers = Simple()
            transporters = Simple()
            average_mass = Simple()
            monoisotopic_mass = Simple()
            calculated_properties = Simple()
