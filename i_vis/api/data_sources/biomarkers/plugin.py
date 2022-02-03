"""
Biomarkers
----------

:name: biomarkers
:url: `<https://www.cancergenomeinterpreter.org/biomarkers>`_
:required: no
:entities: gene, cancer-type, drug, variant
:access: file download
:credentials: none
"""

from typing import Any, TYPE_CHECKING, Mapping, cast, Optional
import os
from pandas import DataFrame

from i_vis.core.file_utils import prefix_fname, change_suffix
from i_vis.core.version import Date as DateVersion

from . import meta
from ... import terms as t
from ...config_utils import get_config
from ...df_utils import i_vis_col, tsv_io
from ...etl import Simple, ETLSpec, Modifier, HarmonizerModifier
from ...plugin import DataSource
from ...task.transform import Process
from ...utils import VariableUrl as Url
from ...resource import Parquet

if TYPE_CHECKING:
    from ...resource import ResourceId, Resources, ResourceDesc
    from ...df_utils import DataFrameIO

_DATA_URL_VAR = meta.register_variable(
    name="URL",
    default="https://www.cancergenomeinterpreter.org/data/cgi_biomarkers_20180117.zip",
)
_VERSION_URL_VAR = meta.register_variable(
    name="VERSION_URL",
    default="https://www.cancergenomeinterpreter.org/biomarkers",
)
_VERSION_XPATH_VAR = meta.register_variable(
    name="VERSION_XPATH",
    default="/html/body/div/div[2]/div[2]/div[2]/div/h2/div/i/b",
)
_VERSION_FORMAT_VAR = meta.register_variable(
    name="VERSION_FORMAT",
    default="%Y/%m/%d",
)


class Plugin(DataSource):
    def __init__(self) -> None:
        super().__init__(
            meta=meta, etl_specs=[Spec], str_to_version=DateVersion.from_str
        )

    @property
    def _latest_version(self) -> DateVersion:
        return DateVersion.from_xpath(
            str(get_config()[_VERSION_URL_VAR]),
            str(get_config()[_VERSION_XPATH_VAR]),
            str(get_config()[_VERSION_FORMAT_VAR]),
        )


# WARNING!
# Some of raw data are expanded and some are not!
# def clean_alteration(df: DataFrame, col: str, sep: str = ",") -> Series:
#    tqdm.pandas(desc="Clean alteration", unit="alteration", leave=False)
#    return df[["gene", col]].progress_apply(
#        lambda x: x[col]
#        .replace(str(x["gene"]) + "::" + "consequence", str(x.loc["gene"]))
#        .replace(sep, sep + str(x.loc["gene"]) + ":")
#        .replace(":::", "::")
#        .split(sep),
#        axis=1,
#    )


# pylint: disable=too-many-arguments
class FilterUnknown(Process):
    def __init__(
        self,
        in_rid: "ResourceId",
        io: "DataFrameIO",
        out_fname: str = "",
        desc: Optional["ResourceDesc"] = None,
        **kwargs: Any,
    ) -> None:
        out_res = Parquet(
            pname=in_rid.pname,
            path=out_fname or prefix_fname(in_rid.name, "_filtered"),
            io=io,
            desc=desc,
        )
        super().__init__(
            in_rid=in_rid,
            out_res=out_res,
            **kwargs,
        )
        self.modifier = cast(
            Mapping[str, Any], Spec.Extract.Raw.alteration_type.modifier
        )

    def _do_work(self, context: "Resources") -> None:
        df = context[self.in_rid].read_full()
        # rows with ";" in "alteration_type" need expansion
        unknown_mask = df["alteration_type"].str.contains(self.modifier["pat"])
        if unknown_mask.any():
            fname = change_suffix(self.in_rid.name, ".tsv")
            fname = prefix_fname(fname, "_unknown")
            qfname = os.path.join(self.plugin.dir.working, fname)
            df[unknown_mask].to_csv(qfname, sep="\t", index=False)
            df = df[~unknown_mask]
            self.logger.warning(
                f"{sum(unknown_mask)} records have been removed and stored in {fname}"
            )
        self.out_res.save(df)


def clean_drug(df: DataFrame, col: str) -> DataFrame:
    df[i_vis_col(col)] = (
        df[col].str.replace(r"[\[\]]", "", regex=True).str.split(pat=";|,")
    )
    return df


def add_hgvs_c(df: DataFrame) -> DataFrame:
    df[i_vis_col("hgvs_c")] = df["transcript"] + ":" + df["c_dna"]
    return df


class Spec(ETLSpec):
    class Extract:
        url = Url(_DATA_URL_VAR, latest=True)
        unpack = "cgi_biomarkers_per_variant.tsv"
        io = tsv_io
        add_id = True

        class Raw:
            alteration = Simple(
                terms=[t.VariantName],
                modifier={"pat": ";"},
            )
            gene = Simple(
                terms=[t.HGNCsymbol()],
                modifier={"pat": ";"},
            )
            alteration_type = Simple(
                terms=[t.VariantType],
                modifier={"pat": ";"},
            )
            assay_type = Simple()
            association = Simple(terms=[t.DrugResponse])
            biomarker = Simple()
            comments = Simple()
            curation_date = Simple(terms=[t.UpdatedDate])
            curator = Simple()
            drug = Simple(
                terms=[t.DrugName()],
                modifier=clean_drug,
            )
            drug_family = Simple(
                terms=[t.DrugFamily],
                modifier={"pat": ";"},
            )
            drug_full_name = Simple(
                terms=[t.DrugName],
                modifier={"pat": "+"},
            )
            drug_status = Simple(
                terms=[t.DrugStatus],
                modifier={"pat": ";"},
            )
            evidence_level = Simple(terms=[t.Evidence])
            metastatic_tumor_type = Simple(terms=[t.SecondaryCancer()])
            primary_tumor_acronym = Simple(terms=[t.PrimaryCancer()])
            primary_tumor_type_full_name = Simple(terms=[t.PrimaryCancer()])
            source = Simple(
                terms=[t.Source],
                modifier={"pat": ";"},
            )
            targeting = Simple(terms=[t.Pathway])
            tcgi_included = Simple()
            c_dna = Simple(terms=[t.HGVSc()])
            g_dna = Simple(terms=[t.HGVSg()])
            individual_mutation = Simple(terms=[t.HGVSp()])
            info = Simple(terms=[t.Variant])
            region = Simple()
            strand = Simple()
            transcript = Simple()
            primary_tumor_type = Simple(
                terms=[t.PrimaryCancer()],
                modifier={"pat": ";"},
            )

    class Transform:
        task = Modifier(FilterUnknown)
        variant = HarmonizerModifier(add_hgvs_c, i_vis_hgvs_c=Simple(terms=[t.HGVSc()]))


# TODO
# verify HarmonizerModifier i_vis + col exist
# convert add_id to _raw_etl
# store raw_value in _raw_etl.tsv
