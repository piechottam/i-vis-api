import pytest
import pandas as pd

from i_vis.api.core_types import variant_utils

# desc = pd.Series(["p.A1P", "A1P"])


@pytest.mark.parametrize(
    "desc,expected",
    [
        (pd.Series(["p.A1P", "A1P"]), pd.Series(["p.Ala1Pro", "Ala1Pro"])),
    ],
)
def test_harmonize_aa1_to_aa3(desc: pd.Series, expected: pd.Series):
    pd.testing.assert_series_equal(variant_utils.harmonize_aa1_to_aa3(desc), expected)
