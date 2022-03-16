"""
Exac
----

:name: exac
:url: `<http://exac.broadinstitute.org/>`_
:required: no
:entities: gene, variant
:access: file download
:credentials: none
"""

# from operator import itemgetter
from typing import Any, TYPE_CHECKING, Sequence, Tuple, cast
import re

from pandas import DataFrame

from i_vis.core.version import Default as DefaultVersion

from . import meta
from ...etl import Simple, ETLSpec, Modifier
from ...plugin import DataSource
from ...task.transform import VCF2Dataframe, Parser
from ...utils import VariableUrl as Url

if TYPE_CHECKING:
    pass

_URL_VAR = meta.register_variable(
    name="URL",
    default="ftp://anonymous:@ftp.broadinstitute.org/pub/ExAC_release/release1/ExAC.r1.sites.vep.vcf.gz",
)
_THREADS_VAR = meta.register_variable(name="THREADS", default=1)


REGEX = re.compile('Format: ([^"]+)"$')


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DefaultVersion.from_str
        )

    @property
    def _latest_version(self) -> DefaultVersion:
        return DefaultVersion(1)


def to_csq_allele(ref: str, alt: str) -> str:
    s = ""
    last = 0
    for i in range(min(len(ref), len(alt))):
        if ref[i] != alt[i]:
            s += alt[i]
        else:
            last += 1

    if s == "":
        if len(ref) > len(alt):
            return "-"

        return alt[last:]

    return s


def process_csv(df: DataFrame, _: Parser) -> DataFrame:
    df["info_csq"] = df[["ref", "info_csq", "i_vis_alt"]].apply(filter_csq, axis=1)
    return df


def filter_csq(df: DataFrame, _: Parser) -> Tuple[Any, ...]:
    allele = to_csq_allele(str(df["ref"]), str(df["i_vis_alt"]))
    new_csq = []
    for info_csq in df["info_csq"]:
        csq = info_csq.split("|")
        if allele == csq[0]:
            new_csq.append("|".join(csq))
    return tuple(new_csq)


def csq_columns(parser: "Parser") -> Sequence[str]:
    match = re.search(REGEX, parser.header_info["CSQ"]["Description"])
    if not match:
        raise ValueError

    return cast(Sequence[str], match.group(1).split("|"))


class Spec(ETLSpec):
    class Extract:
        url = Url(_URL_VAR)

    class Transform:
        task = Modifier(VCF2Dataframe, task_opts={"process": filter_csq})

        class Raw:
            chrom = Simple()
            pos = Simple()
            ref = Simple()
            alt = Simple()
            qual = Simple()
            filter = Simple()
            # TODO
            # symbol = Simple(terms=[t.HGNCsymbol()])
            # gene = Simple(terms=[t.EnsemblGeneID])
            # hgvsc = Simple(terms=[t.HGVSc()])
            # hgvsp = Simple(terms=[t.HGVSp()])
            # TODO FUTURE remaining columns
