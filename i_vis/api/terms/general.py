from inspect import isclass
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    MutableSequence,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

if TYPE_CHECKING:
    from ..plugin import CoreType


class Term:
    _core_type: Optional["CoreType"] = None

    def __init__(self, harmonize: bool = True) -> None:
        self._harmonize = harmonize

    @classmethod
    def get_parent(cls) -> type:
        return cast(type, cls).__base__  # pylint: disable=no-member

    @classmethod
    def get_children(cls) -> Sequence[Type["Term"]]:
        return cast(Sequence[Type["Term"]], getattr(cls, "__subclasses__")())

    @classmethod
    def is_parent(cls, other: Any) -> bool:
        if not isclass(other):
            other = other.__class__
        return issubclass(cls, other)

    @classmethod
    def get_name(cls) -> str:
        return cast(type, cls).__name__  # pylint: disable=no-member

    @classmethod
    # pylint: disable=unused-argument
    def isinstance(cls, s: str) -> bool:
        return False

    @classmethod
    def get_core_type(cls) -> "CoreType":
        if not cls._core_type:
            raise AttributeError

        return cls._core_type

    @classmethod
    # pylint: disable=no-self-argument
    def normalized_to(cls) -> str:
        if not cls._core_type:
            return ""
        return cls.get_core_type().normalized_to

    @staticmethod
    def register_core_type(term: Type["Term"], core_type: "CoreType") -> None:
        setattr(term, "_core_type", core_type)

    @staticmethod
    def harmonize(term: "TermType") -> bool:
        if isinstance(term, type):
            return False

        return getattr(term, "_core_type", None) is not None


TermType = Union[Term, Type[Term]]
TermTypes = Sequence[TermType]


def find_terms(
    term: TermType,
    root: Optional[TermType] = None,
    terms: Optional[MutableSequence[TermType]] = None,
    fast: bool = True,
) -> Sequence[TermType]:
    if root is None:
        root = Term

    if terms is None:
        terms = []

    try:
        if root:
            terms.append(root)
            if fast:
                return terms
    except NotImplementedError:
        pass

    if isinstance(root, Term):
        classes = root.__class__.__subclasses__()
    else:
        classes = root.__subclasses__()

    for child_term in classes:
        find_terms(child_term, term, terms)

    return terms


def term_to_dict(term: Type[Term]) -> Mapping[str, Any]:
    return {
        "name": "Root" if term == Term else term.get_name(),
        "subterms": [term_to_dict(term_) for term_ in term.__subclasses__()],
    }


NORMALIZER_ATTR = "NORMALIZER"


__all__ = [
    "find_terms",
    "term_to_dict",
    "Term",
    "TermType",
    "TermTypes",
]
