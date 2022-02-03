from typing import TYPE_CHECKING

from . import meta

from ...config_utils import get_config

if TYPE_CHECKING:
    pass

_CHUNKSIZE = meta.register_variable(
    name="CHUNKSIZE",
    default=5 * 10 ** 5,
)

_COLS = [
    i - 1
    for i in [
        1,
        2,
        3,
        4,
        5,
        6,
        12,
        13,
        14,
        15,
        16,
        21,
        22,
        29,
        49,
        50,
        51,
        52,
        69,
        70,
        71,
        72,
        73,
        74,
        75,
        96,
        97,
        98,
        99,
        100,
        101,
    ]
]

# chunk.convert_dtypes().dropna(subset=["aaref", "aaalt"])
kwargs = {
    "compression": "gzip",
    "sep": "\t",
    "usecols": _COLS,
    "low_memory": False,
    "chunksize": get_config()[_CHUNKSIZE],
    "dtype": {
        "#chr": "category",
        "ref": "category",
        "alt": "category",
        "aaref": "category",
        "aaalt": "category",
    },
}
