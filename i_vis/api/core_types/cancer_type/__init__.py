"""Plugin for core data type "cancer-type"."""

from ...plugin import Meta

meta = Meta(
    name="i-vis-cancer-type",
    fullname="I-VIS - cancer type harmonization",
    url="https://predict.hu-berlin.de",
    api_prefix="cancer-types",
)

TNAME = "cancer_types"
NAMES_RAW_TNAME = "cancer_type_names_raw"
