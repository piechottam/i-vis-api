import re
from typing import TYPE_CHECKING

from .general import Term

if TYPE_CHECKING:
    pass


class Gene(Term):
    pass


class GeneType(Gene):
    pass


class GeneName(Gene):
    pass


class EntrezGeneID(GeneName):
    @classmethod
    def isinstance(cls, s: str) -> bool:
        return s.isdigit()


class EnsemblGeneID(GeneName):
    # from https://www.ensembl.org/info/genome/stable_ids/prefixes.html
    REGEX = re.compile(r"^ENS([^\d]{3,})?G\d{11}")

    @classmethod
    def isinstance(cls, s: str) -> bool:
        return cls.REGEX.match(s) is not None


class EnsemblTranscripID(GeneName):
    REGEX = re.compile(r"^ENST\d{11}")

    @classmethod
    def isinstance(cls, s: str) -> bool:
        return cls.REGEX.match(s) is not None


class EnsemblProteinID(GeneName):
    REGEX = re.compile(r"^ENSP\d{11}")

    @classmethod
    def isinstance(cls, s: str) -> bool:
        return cls.REGEX.match(s) is not None


class HGNCid(GeneName):
    REGEX = re.compile(r"^HGNC:\d+")

    @classmethod
    def isinstance(cls, s: str) -> bool:
        return cls.REGEX.match(s) is not None


class HGNCsymbol(GeneName):
    @classmethod
    def isinstance(cls, s: str) -> bool:
        raise NotImplementedError


class HGNCname(GeneName):
    @classmethod
    def isinstance(cls, s: str) -> bool:
        raise NotImplementedError


__all__ = [
    "Gene",
    "GeneType",
    "GeneName",
    "EntrezGeneID",
    "EnsemblGeneID",
    "EnsemblTranscripID",
    "EnsemblProteinID",
    "HGNCid",
    "HGNCsymbol",
    "HGNCname",
]
