"""
Disease Ontology
----------------

:name: disease_ontology
:url: `<https://disease-ontology.org>`_
:required: no
:entities: cancer-type
:access: file download
:credentials: none
"""

from typing import MutableSet, TYPE_CHECKING

import pronto
from pandas import DataFrame
from pronto import Ontology
from tqdm import tqdm

from i_vis.core.version import Date as DateVersion

from . import meta
from ...config_utils import get_config
from ...df_utils import tsv_io
from ...plugin import DataSource
from ...resource import File
from ...task.transform import Transform
from ...utils import tqdm_desc

if TYPE_CHECKING:
    from ...resource import ResourceId, Resources

do_file_id = File.link(meta.name, "human_do.obo")

_URL = meta.register_variable(
    name="URL",
    default="https://github.com/DiseaseOntology/HumanDiseaseOntology/raw/main/src/ontology/HumanDO.obo",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(meta=meta, str_to_version=DateVersion.from_str)

    @property
    def _latest_version(self) -> DateVersion:
        doid = pronto.Ontology(get_config()[_URL])
        date = doid.metadata.date
        if date is None:
            raise ValueError("Could not retrieve date from metadata.")

        return DateVersion(date)

    def _init_tasks(self) -> None:
        from ...core_types.cancer_type.models import cancer_type_name_res_desc
        from ...core_types.cancer_type.plugin import add_do_mapping_rid

        self.task_builder.extract(url=get_config()[_URL], out_fname=do_file_id.name)

        mappings_file = self.task_builder.res_builder.file(
            "_mappings.tsv",
            io=tsv_io,
            desc=cancer_type_name_res_desc,
        )
        add_do_mapping_rid(mappings_file.rid)
        self.task_builder.add_task(
            FilterDOIDMapping(in_file_id=do_file_id, out_file=mappings_file),
        )


class FilterDOIDMapping(Transform):
    CANCER_DOID = "DOID:162"

    def __init__(
        self,
        in_file_id: "ResourceId",
        out_file: "File",
        expand_doids: bool = False,
    ) -> None:
        super().__init__(offers=[out_file], requires=[in_file_id])
        self.expand_doids = expand_doids

    def _do_work(self, context: "Resources") -> None:
        in_file = context[self.in_rid]
        ontology = Ontology(in_file.qname)

        cancer_types = []
        doids = []
        # xrefs = []

        # TODO currently disabled
        #         # build nci synonyms
        #         nci_types, nci_codes = load_nci()
        #         nci_type_codes = {}
        #
        #         for nci_type, nci_code in zip(nci_types, nci_codes):
        #             if nci_code not in nci_type_codes:
        #                 nci_type_codes[nci_code] = set()
        #             nci_type_codes[nci_code].add(nci_type)

        # pylint: disable=E1101
        cancer_ids = [
            term.id
            for term in ontology.get_term(FilterDOIDMapping.CANCER_DOID).subclasses()
        ]

        for cancer_id in tqdm(cancer_ids, desc=tqdm_desc(self.tid), leave=False):
            if cancer_id == FilterDOIDMapping.CANCER_DOID:
                continue

            term = ontology.get_term(cancer_id)
            names: MutableSet[str] = set()

            if term.name:
                names.add(term.name)

            # add doid synonyms
            for synonym in term.synonyms:
                if synonym.scope == "EXACT":
                    names.add(synonym.description)

            # add nci synonyms (disabled)
            # for xref in term.xrefs:
            #    x_ref_id = xref.id

            # TODO currently disabled
            #    if x_ref_it.startswith("NCI:"):
            #        x_ref_id = x_ref_it.replace("NCI:", "")
            #        if x_ref_id in nci_type_codes:
            #            for nci_synonym in nci_type_codes[x_ref_id]:
            #                names.append(nci_synonym)
            # xrefs.append(list(xref for xref in term.xrefs))

            for name in names:
                cancer_types.append(name)
                doids.append(term.id)

            if self.expand_doids is False:
                continue

            # add subclasses
            for subclass in term.subclasses():
                for name in names:
                    cancer_types.append(name)
                    doids.append(subclass.id)

            # add superclasses
            for superclass in term.superclasses():
                for name in names:
                    cancer_types.append(name)
                    doids.append(superclass.id)

        df = DataFrame(
            {
                "do_id": doids,
                "name": cancer_types,
                "data_sources": meta.name,
            }
        )
        df = df.explode("name")
        self.out_res.save(df, logger=self.logger)
