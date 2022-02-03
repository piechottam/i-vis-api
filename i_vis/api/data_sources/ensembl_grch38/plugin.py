"""
Ensembl
-------

:name: ensembl_grch38
:url: `<https://www.ensembl.org/index.html>`_
:required: no
:entities: gene
:access: biomart
:credentials: none
"""

from i_vis.core.file_utils import prefix_fname

from . import GENOME_ASSEMBLY
from .ensembl import EnsemblPlugin, get_url
from ...core_types.gene.plugin import add_hgnc_mapping_rid
from ... import terms as t
from ...df_utils import tsv_io
from ...etl import ETLSpec, Simple
from ...file_utils import qualify_fname
from ...utils import DefaultUrl as Url
from ...resource import File


class Plugin(EnsemblPlugin):
    def __init__(self) -> None:
        super().__init__(
            GENOME_ASSEMBLY,
            etl_specs=[
                GeneInfo,
                EnsemblGeneId,
                EnsemblTranscriptId,
                EnsemblProteinId,
            ],
        )

    def _init_tasks(self) -> None:
        from .ensembl import FilterHgncMapping

        super()._init_tasks()
        for etl in self.etl_manager.part2etl.values():
            if "hgnc_id" not in etl.raw_columns.cols:
                continue

            assert etl.extract_opts is not None
            fname = etl.extract_opts.out_fname
            mapping_file = self.task_builder.res_builder.file(
                fname=prefix_fname(fname, "_hgnc_id-dict"),
                io=tsv_io,
            )
            add_hgnc_mapping_rid(mapping_file.rid)
            self.task_builder.add_task(
                FilterHgncMapping(
                    in_rid=File.link(self.name, fname),
                    out_res=mapping_file,
                )
            )


class GeneInfo(ETLSpec):
    class Extract:
        url = Url(
            get_url(
                fname=qualify_fname(__file__, "gene_info.xml"),
                genome_assembly=GENOME_ASSEMBLY,
            ),
        )
        out_fname = "gene_info.tsv"
        io = tsv_io

        class Raw:
            chromosome_scaffold_name = Simple(terms=[t.Chromosome])
            gene_start_bp = Simple(terms=[t.GenomePosition])
            gene_end_bp = Simple(terms=[t.GenomePosition])
            strand = Simple(terms=[t.GenomePosition])
            gene_type = Simple()
            hgnc_symbol = Simple(terms=[t.HGNCsymbol()])


class EnsemblGeneId(ETLSpec):
    class Extract:
        url = Url(
            get_url(
                fname=qualify_fname(__file__, "ensembl_gene_id.xml"),
                genome_assembly=GENOME_ASSEMBLY,
            ),
        )
        out_fname = "ensembl_gene_id.tsv"
        io = tsv_io

        class Raw:
            hgnc_id = Simple(terms=[t.HGNCid()])
            gene_stable_id = Simple(terms=[t.EnsemblGeneID])
            gene_stable_id_version = Simple(
                terms=[t.EnsemblGeneID]
            )  # FUTURE add version


class EnsemblTranscriptId(ETLSpec):
    class Extract:
        url = Url(
            get_url(
                fname=qualify_fname(__file__, "ensembl_transcript_id.xml"),
                genome_assembly=GENOME_ASSEMBLY,
            ),
        )
        out_fname = "ensembl_transcript_id.tsv"
        io = tsv_io

        class Raw:
            hgnc_id = Simple(terms=[t.HGNCid()])
            transcript_stable_id = Simple(terms=[t.EnsemblTranscripID])
            transcript_stable_id_version = Simple(
                terms=[t.EnsemblTranscripID]
            )  # FUTURE add version


class EnsemblProteinId(ETLSpec):
    class Extract:
        url = Url(
            get_url(
                fname=qualify_fname(__file__, "ensembl_peptide_id.xml"),
                genome_assembly=GENOME_ASSEMBLY,
            ),
        )
        out_fname = "ensembl_peptide_id.tsv"
        io = tsv_io

        class Raw:
            hgnc_id = Simple(terms=[t.HGNCid()])
            protein_stable_id = Simple(terms=[t.EnsemblProteinID])
            protein_stable_id_version = Simple(
                terms=[t.EnsemblProteinID]
            )  # FUTURE add version


# FUTURE
# class EntrezGeneId(ETLSpec):
#    class Extract:
#        url = Url(
#            get_url(
#                fname=qualify_fname(__file__, "entrez_gene_id.xml"),
#                genome_assembly=GENOME_ASSEMBLY,
#            ),
#        )
#        out_fname = "entrez_gene_id.tsv"
#        io = secure_reader()
#
#        class Raw:
#            hgnc_id = JSON(terms=[t.HGNCid()])
#            entrezgene_id = JSON(terms=[t.EntrezGeneID])
