from typing import Any

from .general import Term


class Date(Term):
    def __init__(self, date_format: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.date_format = date_format


class UpdatedDate(Date):
    pass


class CreatedDate(Date):
    pass


class Ignore(Term):
    pass


class Assembly(Term):
    pass


class Tissue(Term):
    pass


class Pathway(Term):
    pass


class PathwayName(Pathway):
    pass


class ReactomeId(PathwayName):
    pass


class Pathogenicity(Term):
    pass


class PathogenicityScore(Pathogenicity):
    pass


class PathogenicityPrediction(Pathogenicity):
    pass


class FATHMM(PathogenicityScore):
    pass


class Source(Term):
    pass


class ClinicalTrial(Term):
    pass


class ClinicalTrialID(ClinicalTrial):
    pass


class NCTid(ClinicalTrialID):
    pass


class Phenotype(Term):
    pass


class ClinicalSignificance(Term):
    pass


class EnsemblTranscript(Term):
    pass


class RefSeq(Term):
    pass


class RefSeqId(RefSeq):
    pass


class TranscriptId(Term):
    pass


class Species(Term):
    pass


class GenomePosition(Term):
    def __init__(self, position_format: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.position_format = position_format


class Chromosome(GenomePosition):
    pass


__all__ = [
    "Assembly",
    "Chromosome",
    "ClinicalTrial",
    "ClinicalTrialID",
    "ClinicalSignificance",
    "CreatedDate",
    "Date",
    "FATHMM",
    "GenomePosition",
    "Ignore",
    "NCTid",
    "Pathway",
    "PathwayName",
    "Pathogenicity",
    "PathogenicityScore",
    "PathogenicityPrediction",
    "Phenotype",
    "ReactomeId",
    "RefSeqId",
    "Source",
    "Species",
    "Tissue",
    "TranscriptId",
    "UpdatedDate",
]
