from typing import TYPE_CHECKING

from .general import Term

if TYPE_CHECKING:
    pass


class Drug(Term):
    pass


class DrugName(Drug):
    pass


class ChEMBLid(DrugName):
    pass


class DrugFamily(Drug):
    pass


class DrugStatus(Drug):
    pass


class DrugResponse(Drug):
    pass


__all__ = ["Drug", "DrugName", "ChEMBLid", "DrugFamily", "DrugStatus", "DrugResponse"]
