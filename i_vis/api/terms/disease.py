from typing import TYPE_CHECKING

from .general import Term

if TYPE_CHECKING:
    pass


class Disease(Term):
    pass


class CancerType(Disease):
    pass


class PrimaryCancer(CancerType):
    pass


class SecondaryCancer(CancerType):
    pass


__all__ = ["Disease", "CancerType", "PrimaryCancer", "SecondaryCancer"]
