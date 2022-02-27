"""ChEMBL plugin."""

from ...plugin import Meta

meta = Meta.create(
    package=__package__,
    fullname="ChEMBL webresource client",
    url="https://github.com/chembl/chembl_webresource_client",
)

POST_MERGED_MAPPING_FNAME = "_post_merged_mappings.tsv"
