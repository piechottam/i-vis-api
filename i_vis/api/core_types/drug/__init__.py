"""Plugin for core data type "drug"."""

from ...plugin import Meta

meta = Meta(
    name="i-vis-drug",
    fullname="I-VIS - drug harmonization",
    url="https://predict.hu-berlin.de",
    api_prefix="drugs",
)

TNAME = "drugs"
NAMES_TNAME = "drug_names"
NAMES_PLUGIN_TNAME = "drug_name_plugin"
