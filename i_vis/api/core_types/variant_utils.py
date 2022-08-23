from typing import cast
import re
from enum import Enum, IntFlag

import pandas as pd
from bioutils.accessions import infer_namespace
from bioutils.sequences import aa1_to_aa3_lut
from pandas import Series

VARIANT_REF_LEN = 30
VARIANT_DESC_LEN = 100
VARIANT_LEN = (
    VARIANT_REF_LEN + VARIANT_DESC_LEN + 1
)  # 1 corresnponds to ":" in ref:desc


class RefDesc(Enum):
    ENSEMBL = "ensembl"
    REFSEQ = "refseq"
    HGNC = "hgnc"
    UNKNOWN = "unknown"


class DescType(Enum):
    GENOMIC = "g"
    CODING = "c"
    PROTEIN = "p"
    UNKNOWN = ""


class VariantType(Enum):
    SNV = "snv"
    GENE_FUSION = "gene_fusion"
    CNA = "cna"  # gain, loss
    EXP = "expression"  # over, under - expression
    UNKNOWN = "unknown"


class ModifyFlag(IntFlag):
    NONE = 0
    GUESSED_DESC_TYPE = 1
    AA1_TO_AA3 = 2
    MAP_REF_TO_HGNC_ID = 4


class MatchFlag(IntFlag):
    UNKNOWN = 1
    NONE = 2
    RAW = 4
    REF = 8
    REF_VERSION = 16
    DESC = 32
    DESC_TYPE = 64
    HGVS1 = REF | DESC
    HGVS2 = HGVS1 | REF_VERSION
    HGVS3 = HGVS2 | DESC_TYPE


HGVS_LIKE_REGEX = re.compile(r"^([^:]+):([^:]+)$")
HGVS_DESC_TYPE = re.compile(r"^([cgmnopr]?)\.?(.+)$")
# amino acid change?
AAC_REGEX = re.compile(r"(p.)?(\D{1,3})(\d+)(\D{1,3})")

AA1_REGEX = re.compile(r"(p.)?(\D?)(\d+)(\D?)")
AA3_REGEX = re.compile(r"(p.)?(\D{0, 3})(\d+)(\D{0, 3})")

REF_VERSION_REGEX = re.compile(r".+\.[0-9]+$")


def parse_ref(ref: str) -> str:
    return str(infer_namespace(ref))


def harmonize_aa1_to_aa3(desc: Series) -> Series:
    df: pd.DataFrame = desc.str.extract(AA1_REGEX)

    na1: pd.Series = ~df[1].isna()
    df.loc[na1, 1] = df.loc[na1, 1].map(aa1_to_aa3_lut)

    na3: pd.Series = ~df[3].isna()
    df.loc[na3, 3] = df.loc[na3, 3].map(aa1_to_aa3_lut)
    df = df.fillna("")

    return cast(Series, df.agg("".join, axis=1))
