mypy options --ignore-missing-imports --warn-unused-configs --disallow-any-generics --disallow-untyped-calls
--disallow-untyped-defs --disallow-incomplete-defs --check-untyped-defs --no-implicit-optional --warn-redundant-casts --warn-unused-ignores --warn-return-any --strict-equality


# TODO

## Entities
![entities](entities.png)

### IDs for entities

Variant:
* HGVS notation: DNA, RNA, protein
* genomic coordinates (planned)
* dbSNP (planned)

Gene:
* HGNC id

Drug:
* CHEMBL ID

Pathway:
* Reactome(planned)

Cancer (Disease):
* Disease Ontology (Cancer subtree DOID:162 and only first two levels)

### Variants
![variant_type](variant_types.png)

Ontology (TODO):
* Expression
* Amplification
* deletion
* missense
* fusion
* loss of funciton
* Copy number alteration (CNA)
* Single nucleotide variation
* Multi nucleotide variation
* insertion (Ins)
* deletion (Del)

# Design

## Version management
Data sources and I-VIS data integration. API will change in the future. 
Version tracking is necessary for:

* I-VIS data sources and
* I-VIS code base.
* I-VIS API changes

## I-VIS plugin versions

* Installed (currently installed), 
* Pending (next version to be installed), 
* or Latest (latest remove available version).

## Task class 
Python Class that captures a task in VIS KB.

The subprocesses from ETL and the following task will be represented by the Class:

* extract (ONLY download raw file(s))

- ftp, sftp download
- http, https download
- Xpath parsing TODO is this extract or transform?

* transform (unzip, unrar, and other file transformations(s))
* load (load transformed files to VIS KB)
* ETL container


## Integration, mapping, normalization

* Gene (Type: HGNC, What about synonyms, What about genes from other Species)
* Variant Type (Type: Variant Ontology from Manuela; Aggregate from all sources)
* Variantion ID (Type: HGVS, What with other types, e.g.: BRAF 600 ProteinPositionProtein)

* Drug:

- Name (Normalize against ChEMBL)
- Status (Values: [biomarkers: Approved, Clinical Trials, FDA approved, Pre-clinical]; is this necessary?)
- Family (Values: [biomarkers: free text]):

* Association (Values: [biomarkers: Resistant, Responsive)
* Evidence level (Values: [biomarkers: check file])
* Assay type: 

* source (Values: [biomarkers: PMID:26924578, ASCO 2015 (abstr 11010), ENA 2014 (abstr 428), FDA, EMA, NCT02186821, etc.])

* tumor type -> diesase ontology

- name
- primary
- metastatic

# TODO
class GenomePositionMixin:
contig = db.Column(db.String(255), nullable=False)
start = db.Column(db.Integer, nullable=False)
end = db.Column(db.Integer, nullable=False)
strand = db.Column(db.Integer, nullable=False)
assembly = db.Column(db.Enum(GenomeAssembly), nullable=False)

# TODO
# pathway
# evidence
# tissue
# oncotree


# Planned Features
## Guess Raw data
Given a fiel try to identify and assign columns to entities: gene, variant, etc...
::
    @app_group.command("guess", short_help="Guess mapping.")
    @click.argument("res-id", nargs=1, callback=validate_res_id)
    def guess(res_id: "ResId"):
        res = get_registered(res_id)
        try:
            df = res.read()
        except FileNotFoundError:
            print(f"File '{res.qfname}' does not exist.")
            exit(1)
        df = norm_columns(df)
        print("{")
        for col in df.columns:
            print(f"    '{col}': a.Unknown,")
        print("}")


# Search

Search raw and normalized data.
Howto aggregate? 

Search by:
* entity and
* exposed


Plugins
-------

.. toctree::

    plugins/core
    plugins/sources


.. cli:

CLI App
-------

CLI TODO

.. toctree::

    cli_app/plugin

.. _extending:

Extending
---------

.. toctree::

    extend/core
    extend/sources


