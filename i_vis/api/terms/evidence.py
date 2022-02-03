from .general import Term


class Evidence(Term):
    pass


class EvidenceScore(Evidence):
    pass


class EvidenceType(Evidence):
    pass


class EvidenceDirection(Evidence):
    pass


class EvidenceLevel(Evidence):
    pass


__all__ = [
    "Evidence",
    "EvidenceScore",
    "EvidenceType",
    "EvidenceDirection",
    "EvidenceLevel",
]
