"""Plugin for core data type "gene"."""

from ...plugin import Meta

meta = Meta(
    name="i-vis-gene",
    api_prefix="genes",
    fullname="I-VIS - gene",
    url="https://predict.hu-berlin.de",
)
TNAME = "genes"
NAMES_TNAME = "gene_names"
NAME_PLUGIN_TNAME = "gene_name_plugin"
