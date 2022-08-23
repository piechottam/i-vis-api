import time
from typing import MutableMapping, MutableSequence, MutableSet

import numpy as np
from dask import dataframe as dd
from dask import delayed
from pandas import DataFrame


def collect(df: DataFrame) -> DataFrame:
    id2names: MutableMapping[int, MutableSet[str]] = {}
    name2id: MutableMapping[str, int] = {}
    for variant_id, hgvs in df[["variant_id", "hgvs"]].itertuples(index=False):
        if variant_id == name2id.setdefault(hgvs, variant_id):
            id2names.setdefault(variant_id, set()).add(hgvs)
        else:
            known_variant_id = name2id[hgvs]
            min_variant_id = min(known_variant_id, variant_id)
            max_variant_id = max(known_variant_id, variant_id)
            if max_variant_id in id2names:
                id2names[min_variant_id] |= id2names[max_variant_id]
                for name in id2names[max_variant_id]:
                    name2id[name] = min_variant_id
            id2names[max_variant_id] = id2names[min_variant_id]

    pseudo_variant_ids = list(set(name2id.values()))
    pseudo_variant_ids.sort()
    id2names = {
        variant_id + 1: set(id2names[pseudo_variant_id])
        for variant_id, pseudo_variant_id in enumerate(pseudo_variant_ids)
    }
    name2id = {name: id_ for id_, names in id2names.items() for name in names}
    data = ((name, id_) for name, id_ in name2id.items())
    return DataFrame(data, columns=["variant_id", "name"])


@delayed
def merge(df1: DataFrame, df2: DataFrame) -> DataFrame:
    last_variant_id = df1["variant_id"].max()
    df2["variant_id"] = df2["variant_id"] + last_variant_id
    dfs = dd.concat([df1, df2])
    return collect(dfs)


def merge_dfs(dfs: MutableSequence[DataFrame]) -> DataFrame:
    while len(dfs) != 1:
        df1 = dfs.pop(0)
        df2 = dfs.pop(0)
        df = merge(df1, df2)
        dfs.append(df)
    return dfs[0]


if __name__ == "__main__":
    fnames = [
        "/fast-storage/i_vis/biomarkers/2018_01_17/_norm-variant.tsv",
        "/fast-storage/i_vis/clinvar/2021_10_16/_norm-variant_variant_summary.tsv",
    ]

    dtype = {
        "id": np.int64,
        "raw_data_id": np.int64,
        "hgvs": "string",
        "ref": "string",
        "ref_version": "string",
        "desc": "string",
        "desc_type": "string",
        "modify_flags": np.int16,
    }

    start = time.time()
    _dfs = []
    for fname in fnames:
        _df = dd.read_csv(fname, sep="\t", dtype=dtype)
        _df = _df[["raw_data_id", "hgvs"]]
        _df["variant_id"] = _df["raw_data_id"]
        _dfs.append(_df)
    end = time.time()
    _df = merge_dfs(_dfs)
    print("Finished after: ", (end - start))
