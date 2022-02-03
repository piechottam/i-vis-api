import re
from typing import TYPE_CHECKING

from ..terms.general import Term

if TYPE_CHECKING:
    pass


class Variant(Term):
    pass


class VariantName(Variant):
    pass


class DbSNP(VariantName):
    REGEX = re.compile(r"^rs\d+")

    @classmethod
    def isinstance(cls, s: str) -> bool:
        return cls.REGEX.match(s) is not None


class DbVar(VariantName):
    @classmethod
    def isinstance(cls, s: str) -> bool:
        raise NotImplementedError


class RCV(VariantName):
    @classmethod
    def isinstance(cls, s: str) -> bool:
        raise NotImplementedError


class HGVS(VariantName):
    REGEX = re.compile(r"[^:]+:[^:]+")

    @classmethod
    def isinstance(cls, s: str) -> bool:
        return cls.REGEX.match(s) is not None


class HGVSg(HGVS):
    REGEX = re.compile(r"[^:]+:g.[^:]+")


class HGVSc(HGVS):
    REGEX = re.compile(r"[^:]+:c.[^:]+")


class HGVSp(HGVS):
    REGEX = re.compile(r"[^:]+:p.[^:]+")


class VariantType(Variant):
    pass


__all__ = [
    "Variant",
    "VariantName",
    "DbSNP",
    "DbVar",
    "RCV",
    "HGVS",
    "HGVSg",
    "HGVSc",
    "HGVSp",
    "VariantType",
]
