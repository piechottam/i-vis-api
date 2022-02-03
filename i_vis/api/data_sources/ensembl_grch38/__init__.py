"""Ensembl grch38 plugin."""

from i_vis.core.constants import GenomeAssembly
from ...plugin import Meta


def _genome_prefix(genome_assembly: GenomeAssembly) -> str:
    if genome_assembly == GenomeAssembly.GRCH37:
        return f"{GenomeAssembly.GRCH37.value}."

    return ""


GENOME_ASSEMBLY = GenomeAssembly.GRCH38

meta = Meta.create(
    package=__package__,
    fullname=f"Ensembl {GENOME_ASSEMBLY.value}",
    url=f"https://{_genome_prefix(GENOME_ASSEMBLY)}ensembl.org",
)
