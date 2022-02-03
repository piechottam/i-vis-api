from typing import (
    cast,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
    Any,
    MutableSequence,
)
import os
import re
import gzip
from io import StringIO

import pandas as pd
from pandas import DataFrame

from i_vis.core.file_utils import change_suffix

from .resource import File

if TYPE_CHECKING:
    from .df_utils import DataFrameIO
    from .resource import ResourceDesc
    from .plugin import CoreType


def local_url(file: "File") -> str:
    return f"file://{file.qname}"


def mark_fname(fname: str, prefix: str = "_") -> str:
    if not fname.startswith(prefix):
        fname = f"{prefix}{fname}"
    return fname


def rmmark_fname(fname: str, prefix: str = "_") -> str:
    return re.sub(f"^{prefix}+", "", fname)


def qualify_fname(file: str, fname: str) -> str:
    return os.path.join(os.path.dirname(file), fname)


def convert_file(
    file: File,
    new_suffix: str,
    io: "DataFrameIO",
    desc: "ResourceDesc",
    old_suffix: str = "",
) -> File:
    fname = mark_fname(file.name)
    return File(
        file.plugin.name,
        fname=change_suffix(fname, new_suffix, old_suffix),
        io=io,
        desc=desc,
    )


def unpack_files(
    pname: "str",
    out_fnames: Sequence[str],
    ios: Optional[Sequence[Optional["DataFrameIO"]]] = None,
    descs: Optional[Sequence[Optional["ResourceDesc"]]] = None,
) -> Tuple[File, ...]:
    n = len(out_fnames)
    if not ios:
        ios = [None] * n
    if not descs:
        descs = [None] * n

    return tuple(
        File(
            pname=pname,
            fname=out_fname,
            io=reader,
            desc=desc,
        )
        for out_fname, reader, desc in zip(out_fnames, ios, descs)
    )


def rawcount(handle: Any) -> int:
    # from https://stackoverflow.com/questions/845058/how-to-get-line-count-of-a-large-file-cheaply-in-python
    lines_count = 0
    buf_size = 1024 * 1024
    read_f = handle.raw.read

    buf = read_f(buf_size)
    while buf:
        lines_count += buf.count(b"\n")
        buf = read_f(buf_size)

    return lines_count


# pylint: disable=too-many-locals
def merge_files(
    in_fnames: Sequence[str],
    out_fname: str,
    id_col: str = "",
    deduplicate: Optional[Sequence[str]] = None,
) -> int:
    line_count = 0
    last_lines: MutableSequence[bytes] = []
    name2int = {}

    if id_col or deduplicate:
        out_fname = out_fname + ".tmp"

    with open(out_fname, "wb") as out_fd:
        in_fname = in_fnames[0]
        with gzip.open(in_fname, "rb") as in_fd:
            lines = in_fd.readlines()
            header = lines[0].decode().split()
            name2int.update({name: i for i, name in enumerate(header)})
            if deduplicate:
                last_lines.clear()
                last_lines.extend(_last_lines(lines))
            if id_col:
                for i, line in enumerate(lines[1:], start=1):
                    lines[i] = f"{i}\t{line.decode()}".encode()
            out_fd.writelines(lines)
            line_count += len(lines)
        for in_fname in in_fnames[1:]:
            with gzip.open(in_fname, "rb") as in_fd:
                lines = in_fd.readlines()
                if deduplicate:
                    first_lines = _first_lines(lines)
                    columns = list(name2int.keys())
                    last_df = _data_frame(last_lines, columns, "last")
                    first_df = _data_frame(first_lines, columns, "first")
                    df = pd.concat([last_df, first_df], ignore_index=True)
                    duplicated = df.duplicated(subset=deduplicate)
                    if duplicated.any():
                        df = df.loc[~duplicated & df.loc["type"] == "first"]
                        lines = df.values.tolist()

                    last_lines.clear()
                    last_lines.extend(_last_lines(lines))
                if id_col:
                    for i, line in enumerate(lines, start=1):
                        lines[i] = f"{line_count + i}\t{line.decode()}".encode()
                out_fd.writelines(lines)
                line_count += len(lines)
    os.rename(out_fname + ".tmp", out_fname)
    return line_count


def _data_frame(
    lines: Sequence[bytes], columns: Sequence[str], type_: str
) -> DataFrame:
    out = StringIO()
    out.writelines([line.decode() for line in lines])
    out.seek(0)
    df = cast(DataFrame, pd.read_csv(out, sep="\t", names=columns))
    df["type"] = type_
    return df


def _first_lines(lines: Sequence[bytes]) -> Sequence[bytes]:
    i = 0
    pk = lines[i].decode().split("\t")[0]
    while i < len(lines):
        current_pk = lines[i].decode().split("\t")[0]
        if current_pk != pk:
            break

        i += 1
    return lines[:i]


def _last_lines(lines: Sequence[bytes]) -> Sequence[bytes]:
    start = -1
    pk = lines[start].decode().split("\t")[0]
    offset = 0
    while offset < len(lines):
        current_pk = lines[start - offset].decode().split("\t")[0]
        if current_pk != pk:
            break

        offset += 1
    return lines[(start - offset + 1) :]


def harmonized_fname(core_type: "CoreType", part_name: str = "") -> str:
    fname = f"_harmonized-{core_type.short_name}.tsv"
    if part_name:
        fname = f"_harmonized-{core_type.short_name}_{part_name}.tsv"
    return fname


def not_harmonized_fname(core_type: "CoreType", part_name: str = "") -> str:
    fname = f"_not-harmonized-{core_type.short_name}.tsv"
    if part_name:
        fname = f"_not-harmonized-{core_type.short_name}_{part_name}.tsv"
    return fname
