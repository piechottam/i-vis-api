"""
1000 Genomes Project
--------------------

:name: gp1000
:url: `<https://www.internationalgenome.org/>`_
:required: no
:entities: gene, variant
:access: mysql
:credentials: none
"""

from i_vis.core.version import Default as DefaultVersion

from . import meta
from ...config_utils import get_config
from ... import db
from ... import terms as t
from ...df_utils import tsv_io
from ...db_utils import internal_fk
from ...etl import ETLSpec, Exposed, Simple, ExposeInfo
from ...plugin import DataSource
from ...utils import VariableUrl as Url

_URL_PREFIX = (
    "mysql://anonymous:@mysql-db.1000genomes.org:4272/homo_sapiens_variation_73_37"
)


_SEQ_REGION_URL_VAR = meta.register_variable(
    name="SEQ_REGION",
    default=_URL_PREFIX + "/seq_region",
)
_VARIATION_FEATURE_URL_VAR = meta.register_variable(
    name="VARIATION_FEATURE",
    default=_URL_PREFIX + "/variation_feature",
)
_VERSION_VAR = meta.register_variable(name="VERSION", default="73.37")


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta,
            etl_specs=[SeqRegion, Variation],
            str_to_version=DefaultVersion.from_str,
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        major, minor = get_config()[_VERSION_VAR].split(".")
        return DefaultVersion(int(major), int(minor))

    #     def _init_tasks(self):
    #         from .models import (
    #             RawSeqRegionModel,
    #             RawVariationFeatureModel,
    #         )
    #
    #         # download seq region
    #         extract_seq_region_task, seq_region_file = build_extract_task(
    #             plugin=self,
    #             url=self.get_url("seq_region"),
    #             out_fname="seq_region.tsv",
    #             description=RawSeqRegionModel.get_raw_desc(),
    #             reader=secure_read_df,
    #         )
    #         tasks.append(extract_seq_region_task)
    #
    #         # load seq region
    #         seq_region_table = Table(
    #             plugin=self,
    #             tname=RawSeqRegionModel.__tablename__,
    #             description=RawSeqRegionModel.get_tbl_desc(),
    #         )
    #         load_seq_region_task = Load(
    #             pname=self.name,
    #             file_rid=seq_region_file.rid,
    #             table=seq_region_table,
    #             jsonify=True,
    #         )
    #         tasks.append(load_seq_region_task)
    #
    #         # extract variation feature
    #         extract_variation_task, variation_file = build_extract_task(
    #             plugin=self,
    #             url=self.get_url("variation_feature"),
    #             out_fname="variation.tsv",
    #             description=RawVariationFeatureModel.get_raw_desc(),
    #             reader=secure_read_df,
    #         )
    #         tasks.append(extract_variation_task)
    #
    #         # load variation feature
    #         variation_table = Table(
    #             plugin=self,
    #             tname=RawVariationFeatureModel.__tablename__,
    #             description=RawVariationFeatureModel.get_tbl_desc(),
    #         )
    #         tasks.append(
    #             Load(
    #                 pname=self.name,
    #                 file_rid=variation_file.rid,
    #                 table=variation_table,
    #                 jsonify=True,
    #             )
    #         )
    #
    #         return tasks


class SeqRegion(ETLSpec):
    class Extract:
        url = Url(_SEQ_REGION_URL_VAR)
        out_fname = "seq_region.tsv"
        io = tsv_io
        add_id = True

        class Raw:
            seq_region_id = Exposed(
                exposed_info=ExposeInfo(
                    db_column=db.Column(
                        db.Integer,
                        nullable=False,
                        index=True,
                        unique=True,
                    )
                )
            )
            name = Simple()
            coord_system_id = Simple()


class Variation(ETLSpec):
    class Extract:
        url = Url(_VARIATION_FEATURE_URL_VAR)
        out_fname = "variation_feature.tsv"  # TODO zip
        io = tsv_io
        add_id = True

        class Raw:
            variation_feature_id = Simple()
            seq_region_id = Exposed(
                exposed_info=ExposeInfo(
                    db_column=db.Column(
                        db.ForeignKey(
                            internal_fk(
                                pname=meta.name,
                                part_name=SeqRegion.part_name,
                                fk="seq_region_id",
                            ),
                        ),
                        nullable=False,
                    )
                )
            )
            seq_region_start = Simple()
            seq_region_end = Simple()
            seq_region_strand = Simple()
            variation_id = Simple()
            allele_string = Simple()
            variation_name = Simple(terms=[t.VariantName])
            map_weight = Simple()
            flags = Simple()
            source_id = Simple()
            validation_status = Simple()
            consequence_types = Simple()
            variation_set_id = Simple()
            class_attrib_id = Simple()
            somatic = Simple()
            minor_allele = Simple()
            minor_allele_freq = Simple()
            minor_allele_count = Simple()
            alignment_quality = Simple()
            evidence = Simple()
