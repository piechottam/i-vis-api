"""ChEMBL plugin."""

from ...plugin import Meta

meta = Meta.create(
    package=__package__,
    fullname="ChEMBL webresource client",
    url="https://github.com/chembl/chembl_webresource_client",
)

OTHER_MAPPINGS_FNAME = "_other_mappings.tsv"
