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

from typing import TYPE_CHECKING

import re

import pandas as pd
import requests
from pandas import DataFrame
from tqdm import tqdm

from i_vis.core.version import Default as DefaultVersion
from i_vis.core.utils import StatusCode200Error

from . import meta, OTHER_MAPPINGS_FNAME
from ...config_utils import get_config
from ...df_utils import (
    sort_prefixed,
    add_pk,
    check_new_columns,
    check_missing_column,
    json_io,
    tsv_io,
    DataFrameIO,
)
from ...etl import ETLSpec, Simple
from ...plugin import DataSource
from ...resource import File, Parquet, ResourceId, ResourceDesc
from ...task.extract import Extract
from ...task.transform import Process
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
        from ...core_types.drug.plugin import add_chembl_mapping_rid

        self.etl_manager.init_part2etl()
        etl = self.etl_manager.part2etl[meta.name]

        # retrieve ChEMBL drugs from ChEMBL
        chembl_drugs_file = self.task_builder.res_builder.file(
            fname="drugs.json",
            io=json_io,
            desc=ResourceDesc(etl.raw_columns.cols),
        )
        self.task_builder.add_task(RetrieveDrugs(out_file=chembl_drugs_file))

        # retrieve and merge other entries with ChEMBL drugs
        other_entries_file = self.task_builder.res_builder.file(
            fname="_other_entries.json",
            io=json_io,
            desc=ResourceDesc(etl.raw_columns.cols),
        )
        self.task_builder.add_task(
            RetrieveOther(
                chembl_drugs_file_id=chembl_drugs_file.rid,
                chembl_mapping_file_id=File.link(drug_meta.name, "chembl_mapping.tsv"),
                out_file=other_entries_file,
            )
        )

        # convert JSON to chembl mappings
        other_mappings_file = self.task_builder.res_builder.file(
            OTHER_MAPPINGS_FNAME, io=tsv_io
        )
        self.task_builder.add_task(
            ConvertJSON(
                in_file_rid=other_entries_file.rid, out_file=other_mappings_file
            )
        )
        add_chembl_mapping_rid(other_mappings_file.rid)

        out_res, harm_desc2files = self.task_builder.harmonize(
            other_entries_file.rid, etl
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
    """TODO
    Retrieves Information for entries in ChEMBL beyond the ones that are listed as drugs
    """

    def __init__(
        self,
        chembl_drugs_file_id: ResourceId,
        chembl_mapping_file_id: ResourceId,
        out_file: File,
    ) -> None:
        super().__init__(
            offers=[out_file],
            requires=[chembl_drugs_file_id, chembl_mapping_file_id],
        )

        # input
        # file with existing chembl drug ids
        self.chembl_drugs_file_id = chembl_drugs_file_id

        # file with existing other chembl ids
        # expected format: "chembl_id"\t"name"\t"data_source
        self.chembl_mapping_file_id = chembl_mapping_file_id

    # pylint: disable=too-many-locals
    def _do_work(self, context: "Resources") -> None:
        from chembl_webresource_client.new_client import new_client

        known_drugs = context[self.chembl_drugs_file_id].read_full()
        known_chembl_ids = set(known_drugs["molecule_chembl_id"].to_list())

        # expected format: file with one column called "chembl_id"
        other = context[self.chembl_mapping_file_id].read_full()

        # container for chembl ids from other sources
        other_chembl_ids = set(other["chembl_id"].unique())

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
    def __init__(self, in_file_rid: "ResourceId", out_file: "File") -> None:
        super().__init__(in_file_rid=in_file_rid, out_res=out_file)

    def _process(self, df: "AnyDataFrame", context: "Resources") -> "AnyDataFrame":
        from ...core_types.drug.models import CHEMBL_PREFIX

        df = DataFrameIO.to_full(df)
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
        new_df["plugins"] = "chembl"

        return new_df


class Spec(ETLSpec):
    class Extract:
        rid = Parquet.link(meta.name, "merged_entries")

    class Raw:
        applicants = Simple()
        atc_code_description = Simple()
        atc_classifications = Simple()
        atc_classification = Simple()
        availability_type = Simple()
        biotherapeutic = Simple()
        black_box = Simple()
        black_box_warning = Simple()
        chebi_par_id = Simple()
        chirality = Simple()
        cross_references = Simple()
        development_phase = Simple()
        dosed_ingredient = Simple()
        drug_type = Simple()
        first_approval = Simple()
        first_in_class = Simple()
        helm_notation = Simple()
        indication_class = Simple()
        inorganic_flag = Simple()
        max_phase = Simple()
        molecule_chembl_id = Simple(terms=[ChEMBLid()])
        molecule_hierarchy = Simple()
        molecule_properties = Simple()
        molecule_structures = Simple()
        molecule_synonyms = Simple()
        molecule_type = Simple()
        natural_product = Simple()
        ob_patent = Simple()
        oral = Simple()
        parenteral = Simple()
        polymer_flag = Simple()
        pref_name = Simple()
        prodrug = Simple()
        research_codes = Simple()
        rule_of_five = Simple()
        sc_patent = Simple()
        structure_type = Simple()
        synonyms = Simple()
        therapeutic_flag = Simple()
        topical = Simple()
        usan_stem = Simple()
        usan_stem_definition = Simple()
        usan_substem = Simple()
        usan_stem_substem = Simple()
        usan_year = Simple()
        withdrawn_flag = Simple()
        withdrawn_class = Simple()
        withdrawn_country = Simple()
        withdrawn_reason = Simple()
        withdrawn_year = Simple()
