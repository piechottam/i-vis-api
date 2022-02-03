"""Plugin for core data type "drug"."""

from ...plugin import Meta

meta = Meta(
    name="i-vis-drug",
    fullname="I-VIS - drug harmonization",
    url="https://predict.hu-berlin.de",
    api_prefix="drugs",
)

DRUG_TNAME = "drugs"
DRUG_NAMES_RAW_TNAME = "drug_names_raw"
DRUG_NAMES_TRANSFORMED_TNAME = "drug_names_transformed"
DRUG_NAMES_PLUGIN_TNAME = "drug_name_plugin"
DRUG_NAMES_SECONDARY_TNAME = "drug_names_secondary"
