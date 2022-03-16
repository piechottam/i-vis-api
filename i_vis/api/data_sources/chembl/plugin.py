"""
ChEMBL
------

No complete dump of ChEMBLy^

:name: chembl
:url: `<https://www.ebi.ac.uk/chembl/>`_
:required: yes
:entities: drug
:access: WEB-Service
:credentials: none
"""

from typing import TYPE_CHECKING, Optional

import re

import pandas as pd
import requests
from pandas import DataFrame
from tqdm import tqdm

from i_vis.core.version import Default as DefaultVersion
from i_vis.core.utils import StatusCode200Error

from . import meta, POST_MERGED_MAPPING_FNAME
from ...config_utils import get_config
from ...df_utils import (
    sort_prefixed,
    add_pk,
    check_new_columns,
    check_missing_column,
    json_io,
    parquet_io,
    tsv_io,
    DataFrameIO,
    update_pk,
)
from ...etl import ETLSpec, Simple
from ...plugin import DataSource
from ...resource import File, Parquet, ResourceId, ResourceDesc
from ...task.extract import Extract
from ...task.transform import Process, Transform
from ...terms import ChEMBLid
from ...utils import chunker, tqdm_desc

if TYPE_CHECKING:
    from ...resource import Resources
    from ...df_utils import AnyDataFrame

_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="https://www.ebi.ac.uk/chembl/api/data/status",
)
_VERSION_PATTERN_VAR = meta.register_variable(
    name="VERSION_PATTERN",
    default=r"ChEMBL_([\d]+)",
)
_TIMEOUT_VAR = meta.register_variable(
    name="TIMEOUT",
    default=100,
)

REGEX = re.compile(r" \((BAN|INN|MI|FDA|JAN|USAN|USP|DCF|NF|, )+\)")


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[Spec],
            str_to_version=DefaultVersion.from_str,
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        url = get_config()[_VERSION_URL_VAR]
        timeout = get_config()[_TIMEOUT_VAR]
        r = requests.get(url, timeout=timeout, params={"format": "json"})
        if r.status_code != 200:
            raise StatusCode200Error(reponse=r)

        version_str = r.json().get("chembl_db_version", "")
        pattern = get_config()[_VERSION_PATTERN_VAR]
        match = re.search(pattern, version_str)
        if not match:
            raise ValueError("Pattern did not match anything.")

        return DefaultVersion(major=int(match.group(1)))

    def _init_tasks(self) -> None:
        from ...core_types.drug import meta as drug_meta
        from ...core_types.drug.models import drug_name_res_desc

        self.etl_manager.init_part2etl()
        etl = self.etl_manager.part2etl[meta.name]

        # retrieve ChEMBL drugs from ChEMBL
        drugs_file = self.task_builder.res_builder.file(
            fname="drugs.json",
            io=json_io,
            desc=ResourceDesc(etl.raw_columns.cols),
        )
        self.task_builder.add_task(
            RetrieveDrugs(out_file=drugs_file),
        )

        # drug_mapping_file = self.task_builder.res_builder.file(
        #    fname="_drug_mappings.tsv", io=tsv_io, desc=drug_name_res_desc
        # )
        # self.task_builder.add_task(
        #    ConvertJSON(in_rid=drugs_file.rid, out_res=drug_mapping_file)
        # )
        # add_chembl_mapping_rid(drug_mapping_file.rid)

        # retrieve non ChEMBL drugs from ChEMBL entries
        merged_entries_file = self.task_builder.res_builder.file(
            fname="_merged_entries.json",
            io=json_io,
            desc=ResourceDesc(etl.raw_columns.cols),
        )
        pre_merged_mapping_file_id = File.link(
            drug_meta.name, "_pre_merged_mappings.tsv"
        )

        self.task_builder.add_task(
            RetrieveOther(
                drugs_file_id=drugs_file.rid,
                pre_merged_mapping_file_id=pre_merged_mapping_file_id,
                merged_entries_file=merged_entries_file,
            ),
        )

        #
        post_merged_mapping_file = self.task_builder.res_builder.file(
            fname=POST_MERGED_MAPPING_FNAME,
            io=tsv_io,
            desc=drug_name_res_desc,
        )
        raw_data_parquet = self.task_builder.res_builder.parquet(
            path="_" + meta.name + ".parquet",
            io=parquet_io,
            desc=ResourceDesc(etl.raw_columns.cols),
        )
        self.task_builder.add_task(
            Merge(
                merged_entries_file_id=merged_entries_file.rid,
                pre_merged_mapping_file_id=pre_merged_mapping_file_id,
                post_merged_mapping_file=post_merged_mapping_file,
                raw_data_parquet=raw_data_parquet,
            )
        )

        out_res, harm_desc2files = self.task_builder.harmonize(
            raw_data_parquet.rid, etl
        )
        self.task_builder.load_raw(out_res, etl)
        self.task_builder.load_harmonized(harm_desc2files, etl=etl)


class RetrieveDrugs(Extract):
    def __init__(self, out_file: "File") -> None:
        super().__init__(offers=[out_file])

    def _do_work(self, context: "Resources") -> None:
        from chembl_webresource_client.new_client import new_client

        # pylint: disable=maybe-no-member
        chembl_drugs = tqdm(
            new_client.drug,
            desc=tqdm_desc(self.tid),
            unit=" drugs",
            leave=False,
        )

        # check columns
        assert self.out_res.desc is not None
        df = DataFrame(chembl_drugs)
        check_new_columns(df, self.out_res.desc.cols, self.out_res.name, self.logger)
        check_missing_column(
            df=df,
            cols=self.out_res.desc.cols,
            name=self.out_res.name,
            logger=self.logger,
            hard=False,
        )

        self.out_res.save(df, logger=self.logger)


class RetrieveOther(Extract):
    """
    Retrieves Information for entries in ChEMBL beyond the ones that are listed as drugs
    """

    def __init__(
        self,
        drugs_file_id: ResourceId,
        merged_entries_file: File,
        pre_merged_mapping_file_id: Optional[ResourceId] = None,
    ) -> None:
        required = [drugs_file_id]
        if pre_merged_mapping_file_id:
            required.append(pre_merged_mapping_file_id)

        super().__init__(
            offers=[merged_entries_file],
            requires=required,
        )

        # input
        # file with existing chembl drug ids
        self.chembl_drugs_file_id = drugs_file_id

        # file with existing other chembl ids
        # expected format: "chembl_id"\t"name"\t"data_source
        self.pre_merged_mapping_file_id = pre_merged_mapping_file_id

    # pylint: disable=too-many-locals
    def _do_work(self, context: "Resources") -> None:
        from chembl_webresource_client.new_client import new_client

        known_drugs = context[self.chembl_drugs_file_id].read_full()
        known_chembl_ids = set(known_drugs["molecule_chembl_id"].to_list())

        if not self.pre_merged_mapping_file_id:
            self.logger.warning("No other sources")
            self.out_res.save(known_drugs, logger=self.logger)
            return

        # expected format: file with one column called "chembl_id"
        other_mapping = context[self.pre_merged_mapping_file_id].read_full()

        # container for chembl ids from other sources
        other_chembl_ids = set(other_mapping["chembl_id"].unique())

        unknown_chembl_ids = other_chembl_ids.difference(known_chembl_ids)
        if not unknown_chembl_ids:
            self.logger.warning("No unknown ChEMBL IDs in other sources")
            self.out_res.save(known_drugs, logger=self.logger)
            return

        self.logger.info(
            f"{len(unknown_chembl_ids)} unknown ChEMBL IDs remain after intersect with ChEMBL drugs"
        )

        chunk_size = 20
        other_chembl_entries = []
        with tqdm(
            total=len(unknown_chembl_ids),
            desc="Retrieving other entries",
            unit=" entries",
            leave=False,
        ) as pb:
            for chunk in chunker(unknown_chembl_ids, chunk_size):
                #  retrieve
                # pylint: disable=maybe-no-member
                results = new_client.molecule.get(list(chunk))
                if results:
                    other_chembl_entries.extend(results)
                pb.update(len(chunk))
        self.logger.info(
            f"Retrieved {len(other_chembl_entries)} ChEMBL IDs from other sources"
        )

        # check columns
        assert self.out_res.desc is not None
        df = DataFrame(other_chembl_entries)
        check_new_columns(df, self.out_res.desc.cols, "Other entries", self.logger)
        check_missing_column(
            df,
            self.out_res.desc.cols,
            "Other entries",
            self.logger,
            hard=False,
        )

        # save merged drugs and other entries
        merged_entries = pd.concat([known_drugs, df], ignore_index=True)
        merged_entries = add_pk(merged_entries)
        merged_entries["chembl_id"] = merged_entries["molecule_chembl_id"]

        self.out_res.save(merged_entries, logger=self.logger)


class ConvertJSON(Process):
    def _process(self, df: "AnyDataFrame", context: "Resources") -> "AnyDataFrame":
        df = DataFrameIO.to_full(df)
        return extract_names(df)


def extract_names(df: DataFrame) -> DataFrame:
    from ...core_types.drug.models import CHEMBL_PREFIX

    new_df = DataFrame()
    new_df["chembl_id"] = df["molecule_chembl_id"]
    new_df["research_codes"] = df["research_codes"]
    new_df["synonyms"] = df["synonyms"]
    for col in ("molecule_synonym",):
        tmp_col = col + "s"
        new_df[col] = df[tmp_col].apply(
            lambda values, key: [v[key] for v in values], key=col
        )

    new_df = (
        new_df.melt(
            id_vars="chembl_id",
            var_name="name_type",
            value_name="name",
        )
        .drop(columns="name_type")
        .explode("name")
        .dropna()
        .drop_duplicates()
        .query("name" + " != ''")
    )
    # remove specific parenthesis - see regex
    new_df["name"] = new_df["name"].str.replace(REGEX, "", regex=True)
    new_df = new_df.drop_duplicates().reset_index(drop=True)

    # nicely sort by chembl_id
    new_df = sort_prefixed(new_df, "chembl_id", CHEMBL_PREFIX)
    new_df["data_sources"] = "chembl"
    return new_df


class Merge(Transform):
    def __init__(
        self,
        merged_entries_file_id: ResourceId,
        pre_merged_mapping_file_id: ResourceId,
        post_merged_mapping_file: File,
        raw_data_parquet: Parquet,
    ) -> None:
        super().__init__(
            offers=[post_merged_mapping_file, raw_data_parquet],
            requires=[merged_entries_file_id, pre_merged_mapping_file_id],
        )
        self.merged_entries_file_id = merged_entries_file_id
        self.pre_merged_mapping_file_id = pre_merged_mapping_file_id
        self.post_merged_mapping_file = post_merged_mapping_file
        self.raw_data_parquet = raw_data_parquet

    def _do_work(self, context: "Resources") -> None:
        merged_entries = context[self.merged_entries_file_id].read_full()
        merged_entries = update_pk(merged_entries)
        self.raw_data_parquet.save(merged_entries, logger=self.logger)

        pre_merged_mapping = context[self.pre_merged_mapping_file_id].read_full()
        post_merged_mapping = pd.concat(
            [pre_merged_mapping, extract_names(merged_entries)]
        )
        self.post_merged_mapping_file.save(post_merged_mapping, logger=self.logger)


class Spec(ETLSpec):
    class Extract:
        rid = Parquet.link(meta.name, "merged_entries")

    class Raw:
        applicants = Simple()
        atc_code_description = Simple()
        availability_type = Simple()
        biotherapeutic = Simple()
        black_box = Simple()
        black_box_warning = Simple()
        chirality = Simple()
        development_phase = Simple()
        drug_type = Simple()
        first_approval = Simple()
        first_in_class = Simple()
        helm_notation = Simple()
        indication_class = Simple()
        molecule_chembl_id = Simple(terms=[ChEMBLid()])
        molecule_properties = Simple()
        molecule_structures = Simple()
        molecule_synonyms = Simple()
        ob_patent = Simple()
        oral = Simple()
        parenteral = Simple()
        prodrug = Simple()
        research_codes = Simple()
        rule_of_five = Simple()
        sc_patent = Simple()
        synonyms = Simple()
        topical = Simple()
        usan_stem_definition = Simple()
        usan_stem_substem = Simple()
        usan_year = Simple()
        withdrawn_flag = Simple()
        withdrawn_class = Simple()
        withdrawn_country = Simple()
        withdrawn_reason = Simple()
        withdrawn_year = Simple()
        # {'molecule_structures', 'chebi_par_id', 'polymer_flag', 'indication_class', 'atc_classifications',
        # 'rule_of_five', 'usan_year', 'parenteral', 'applicants', 'inorganic_flag', 'usan_substem', 'chirality', 'withdrawn_year', 'helm_notation', 'molecule_properties', 'withdrawn_reason', 'development_phase', 'withdrawn_flag', 'sc_patent', 'research_codes', 'withdrawn_class', 'dosed_ingredient', 'prodrug', 'atc_code_description', 'pref_name', 'withdrawn_country', 'molecule_type', 'structure_type', 'black_box_warning', 'first_approval', 'biotherapeutic', 'availability_type', 'first_in_class', 'usan_stem_substem', 'black_box', 'natural_product', 'molecule_synonyms', 'topical', 'ob_patent', 'cross_references', 'drug_type', 'atc_classification', 'max_phase', 'usan_stem_definition', 'therapeutic_flag', 'molecule_hierarchy', 'molecule_chembl_id', 'synonyms', 'oral', 'usan_stem'
