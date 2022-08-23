"""
Clinical Trials GOV
-------------------

TODO index data

:name: ctg
:url: `<https://aact.ctti-clinicaltrials.org>`_
:required: no
:entities: gene, cancer-type, drug, variant
:access: file download
:credentials: none
"""

from typing import Any, Callable, MutableSequence, cast

import requests
from lxml import etree
from sqlalchemy import Column, ForeignKey, String

from i_vis.core.utils import StatusCode200Error
from i_vis.core.version import Date as DateVersion

from ... import terms as t
from ...config_utils import get_config
from ...db_utils import internal_fk
from ...df_utils import DaskDataFrameIO
from ...etl import ETLSpec, Exposed, ExposeInfo, Simple
from ...plugin import DataSource
from ...resource import File
from . import meta

_URL_PREFIX_VAR = meta.register_variable(
    name="URL_PREFIX",
    default="https://aact.ctti-clinicaltrials.org/pipe_files/",
)
_XPATH_VAR = meta.register_variable(
    name="XPATH",
    default='((//table[@class="file-archive"])[1]//tr)[1]//a/@href',
)
_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="https://aact.ctti-clinicaltrials.org/snapshots",
)
_VERSION_XPATH_VAR = meta.register_variable(
    name="VERSION_XPATH",
    default="/html/body/div[1]/section/div/div[4]/table/tr[2]/td[2]",
)
_VERSION_FORMAT_VAR = meta.register_variable(
    name="VERSION_FORMAT",
    default="%m/%d/%Y",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[
                Study,
                BriefSummary,
                DetailedDescription,
                Eligibility,
                Condition,
                Intervention,
                StudyReference,
                Facility,
            ],
            str_to_version=DateVersion.from_str,
        )

    @property
    def _latest_version(self) -> DateVersion:
        return DateVersion.from_xpath(
            get_config()[_VERSION_URL_VAR],
            get_config()[_VERSION_XPATH_VAR],
            get_config()[_VERSION_FORMAT_VAR],
        )

    def _init_tasks(self) -> None:
        pass
        # TODO
        # archive_file, extract_task = self._extract(self.get_url("archive"))
        # self._add_task(extract_task)

        # unpack archive_file
        # components = (
        #    "brief_summaries",
        #    "detailed_descriptions",
        #    "eligibilities",
        #    "conditions",
        #    "interventions",
        #    "study_references",
        #    "facilities",
        # )

        # TODO
        # self._unpack(
        #    in_rid=archive_file.rid,
        #    out_fnames=tuple(f"{name}.txt" for name, in components),
        #    readers=tuple(secure_reader(sep="|") for _ in components),
        #    descs=tuple(model.get_raw_desc() for name, model in components),
        # )


#     def _init_tasks(self):
#         archive_file = self._extract(self.get_url("archive"))
#
#         # unpack archive_file
#         components = (
#             ("brief_summaries", RawBriefSummaryModel),
#             # (
#             #    "detailed_descriptions",
#             #    RawDetailedDescriptionModel,
#             # ),
#             # ("eligibilities", RawEligibilityModel),
#             ("conditions", RawConditionModel),
#             ("interventions", RawInterventionModel),
#             # ("study_references", RawStudyReferenceModel),
#             ("facilities", RawFacilityModel),
#         )
#         unzipped_files = unpack_files(
#             plugin=self,
#             out_fnames=tuple(name + ".txt" for name, _ in components),
#             readers=tuple(partial(read_df, sep="|") for _ in components),
#             descriptions=tuple(model.get_raw_desc() for _, model in components),
#         )
#         unpack_task = Unpack(pname=meta.name, archive=archive_file, out_files=unzipped_files)
#         tasks.append(unpack_task)
#         name2file = {
#             component[0]: file for component, file in zip(components, unzipped_files)
#         }
#
#         # map and load interventions
#         self._map_load(
#             in_res=name2file["intervention"],
#             data_model=RawInterventionModel,
#             mapping_models=MappedInterventionModel,
#             mapping_task=MapRawData,
#         )
#
#         # load some components
#         for component in components:
#             name = component[0]
#             model = component[1]
#             # some components need special care
#             if name in ("interventions"):
#                 continue
#
#             table = Table(
#                 plugin=self,
#                 tname=model.__tablename__,  # type: ignore
#                 description=model.get_tbl_desc(),
#             )
#             tasks.append(
#                 Load(
#                     pname=self.name,
#                     file_rid=name2file[name].rid,
#                     table=table,
#                     jsonify=True,
#                 )
#             )


def fetch_url() -> Callable[[], str]:
    def helper() -> str:
        url_prefix = str(get_config()[_URL_PREFIX_VAR])
        r = requests.get(url_prefix, timeout=100)
        if r.status_code != 200:
            raise StatusCode200Error(response=r)

        parser = etree.HTMLParser()
        tree = etree.fromstring(r.text, parser=parser)  # type: ignore
        if not tree:
            raise ValueError("Parser result empty.")

        xpath = get_config()[_VERSION_XPATH_VAR]
        result = cast(MutableSequence["etree._Element"], tree.xpath(xpath))
        if not result:
            raise ValueError("No element found by xpath.")

        if len(result) > 1:
            raise ValueError("More than one element found by xpath.")

        e = result.pop()
        name = str(e.text) if e.text else ""
        if not name:
            raise ValueError("Element reference by xpath is empty.")

        return url_prefix + name

    return helper


NCT_ID_LEN = 11
ctg_io = DaskDataFrameIO(read_callback="read_csv", sep="|")


class Study(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "study.txt")
        io = ctg_io
        add_id = True

        class Raw:
            nct_id = Exposed(
                dtype=str,
                terms=[t.NCTid],
                exposed_info=ExposeInfo(
                    db_column=Column(
                        String(NCT_ID_LEN),
                        nullable=False,
                        index=True,
                        unique=True,
                    )
                ),
            )
            nlm_download_date_description = Simple()
            study_first_submitted_date = Simple()
            results_first_submitted_date = Simple()
            disposition_first_submitted_date = Simple()
            last_update_submitted_date = Simple()
            study_first_submitted_qc_date = Simple()
            study_first_posted_date = Simple()
            study_first_posted_date_type = Simple()
            results_first_submitted_qc_date = Simple()
            results_first_posted_date = Simple()
            results_first_posted_date_type = Simple()
            disposition_first_submitted_qc_date = Simple()
            disposition_first_posted_date = Simple()
            disposition_first_posted_date_type = Simple()
            last_update_submitted_qc_date = Simple()
            last_update_posted_date = Simple()
            last_update_posted_date_type = Simple()
            start_month_year = Simple()
            start_date_type = Simple()
            start_date = Simple()
            verification_month_year = Simple()
            verification_date = Simple()
            completion_month_year = Simple()
            completion_date_type = Simple()
            completion_date = Simple()
            primary_completion_month_year = Simple()
            primary_completion_date_type = Simple()
            primary_completion_date = Simple()
            target_duration = Simple()
            study_type = Simple()
            acronym = Simple()
            baseline_population = Simple()
            brief_title = Simple()
            official_title = Simple()
            overall_status = Simple()
            last_known_status = Simple()
            phase = Simple()
            enrollment = Simple()
            enrollment_type = Simple()
            source = Simple()
            limitations_and_caveats = Simple()
            number_of_arms = Simple()
            number_of_groups = Simple()
            why_stopped = Simple()
            has_expanded_access = Simple()
            expanded_access_type_individual = Simple()
            expanded_access_type_intermediate = Simple()
            expanded_access_type_treatment = Simple()
            has_dmc = Simple()
            is_fda_regulated_drug = Simple()
            is_fda_regulated_device = Simple()
            is_unapproved_device = Simple()
            is_ppsd = Simple()
            is_us_export = Simple()
            biospec_retention = Simple()
            biospec_description = Simple()
            ipd_time_frame = Simple()
            ipd_access_criteria = Simple()
            ipd_url = Simple()
            plan_to_share_ipd = Simple()
            plan_to_share_ipd_description = Simple()
            created_at = Simple()
            updated_at = Simple()


def nct_id_fk_col() -> Column[Any]:
    return Column(
        ForeignKey(
            internal_fk(
                pname=meta.name,
                part_name=Study.part_name,
                fk="nct_id",
            ),
        ),
        nullable=False,
        index=True,
    )


class BriefSummary(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "brief_summary.txt")
        io = ctg_io
        add_id = True

        class Raw:
            nct_id = Exposed(
                terms=[t.NCTid],
                exposed_info=ExposeInfo(db_column=nct_id_fk_col()),
            )


class DetailedDescription(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "detailed_description.txt")
        io = ctg_io
        add_id = True

        class Raw:
            nct_id = Exposed(
                terms=[t.NCTid],
                exposed_info=ExposeInfo(db_column=nct_id_fk_col()),
            )


class Eligibility(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "eligibility.txt")
        io = ctg_io
        add_id = True

        class Raw:
            nct_id = Exposed(
                terms=[t.NCTid],
                exposed_info=ExposeInfo(db_column=nct_id_fk_col()),
            )


class Condition(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "condition.txt")
        io = ctg_io
        add_id = True

        class Raw:
            id = Simple()
            nct_id = Exposed(
                terms=[t.NCTid],
                exposed_info=ExposeInfo(db_column=nct_id_fk_col()),
            )
            name = Simple(terms=[t.Disease, t.CancerType()])
            downcase_name = Simple()


class Intervention(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "intervention.txt")
        io = ctg_io
        add_id = True

        class Raw:
            id = Simple()
            nct_id = Exposed(
                terms=[t.NCTid],
                exposed_info=ExposeInfo(db_column=nct_id_fk_col()),
            )
            intervention_type = Simple()
            name = Simple(terms=[t.Drug])
            description = Simple()  # Treatment


class StudyReference(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "study_reference.txt")
        io = ctg_io
        add_id = True

        class Raw:
            id = Simple()
            nct_id = Exposed(
                terms=[t.NCTid],
                exposed_info=ExposeInfo(db_column=nct_id_fk_col()),
            )
            pmid = Simple(terms=[t.PMID])
            reference_type = Simple()
            citation = Simple(terms=[t.Reference])


class Facility(ETLSpec):
    class Extract:
        rid = File.link(meta.name, "facility.txt")
        io = ctg_io
        add_id = True

        class Raw:
            id = Simple()
            nct_id = Exposed(
                terms=[t.NCTid],
                exposed_info=ExposeInfo(db_column=nct_id_fk_col()),
            )
            status = Simple()
            name = Simple()
            city = Simple()
            state = Simple()
            zip = Simple()
            country = Simple()
