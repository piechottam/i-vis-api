from typing import (
    Any,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TYPE_CHECKING,
)
import ftplib
import logging
import os
from abc import ABC
from datetime import datetime
from urllib.parse import urlparse, urlunparse
import time

import pandas as pd
import requests
from sqlalchemy import create_engine
from tqdm import tqdm

from i_vis.core.file_utils import modified as file_modified, size as file_size

from .base import Task, TaskType
from ..df_utils import normalize_columns
from ..utils import BaseUrl, tqdm_desc

if TYPE_CHECKING:
    from ..resource import Resources, File


# TODO
# * one output tasks
# *

logger = logging.getLogger("task")


class ByPackage:
    def __init__(self, desc: str, version: str) -> None:
        self.desc = desc
        self.version = version


# TODO remove
# def urls2desc(urls: Union[Sequence[str], str]) -> str:
#    if isinstance(urls, str):
#        urls = [urls]
#    schemes = set(urlparse(url).scheme for url in urls)
#    return f'Get data: {len(urls)} URL(s) via {",".join(schemes)}'
#
#
# def urls2name(urls: Union[Sequence[str], str]) -> str:
#    if isinstance(urls, str):
#        urls = [urls]
#    files = set(os.path.basename(urlparse(url).path) for url in urls)
#    return f'{",".join(files)}'


class Extract(Task, ABC):
    @property
    def type(self) -> TaskType:
        return TaskType.EXTRACT


# TODO exception handling
class ExtractFailed(Exception):
    def __init__(self, task: Extract) -> None:
        Exception.__init__(self)
        self.task = task


# pylint: disable=too-many-arguments
class Download(Extract):
    def __init__(
        self,
        urls: Sequence[str],
        out_files: Sequence["File"],
        eq_callback: Optional[Callable[[Any], bool]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(offers=out_files, **kwargs)

        self.urls = urls
        self.eq_callback = eq_callback

    def __len__(self) -> int:
        return len(self.offered)

    def total_size(self) -> int:
        total = 0
        for url in self.urls:
            total += self.server_size(url)
        return total

    def server_size(self, url: str) -> int:
        raise NotImplementedError

    def _server_modified(self, url: str) -> Optional[time.struct_time]:
        raise NotImplementedError

    def _equal(self, url: str, dest: str) -> bool:
        if not os.path.exists(dest) or file_size(dest) == 0:
            self.logger.debug("Does not exist or has zero size: %s", dest)
            return False

        if self.eq_callback:
            self.logger.debug("Call eq_callback")
            return self.eq_callback(self)

        server_mtime = self._server_modified(url)
        if server_mtime is None:
            self.logger.warning("Modified time could not be retrieved from server")
            return False

        local_mtime = time.gmtime(file_modified(dest))
        return server_mtime < local_mtime

    def _do_work(self, context: "Resources") -> None:
        for url, offered in zip(self.urls, self.offered):
            dest = offered.qname
            parsed = urlparse(url)
            short_url = urlunparse(
                (parsed.scheme, parsed.netloc, parsed.path, "", "", "")
            )
            short_url = f"{short_url} [...]"

            if not os.path.exists(dest) or not self._equal(url, dest):
                self.logger.info("Download %s", short_url)
                self._store(url, dest)
                offered.update_db()
                offered.dirty = True
            else:
                self.logger.info("Omit Download %s", short_url)
                offered.update_db()

    def _store(self, url: str, dest: str) -> None:
        raise NotImplementedError


# pylint: disable=too-many-arguments
class FTP(Download):
    def __init__(
        self,
        urls: Sequence[str],
        out_files: Sequence["File"],
        eq_callback: Optional[Callable[["Download"], bool]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            urls=urls,
            out_files=out_files,
            eq_callback=eq_callback,
            **kwargs,
        )
        parsed = urlparse(urls[0])
        self.host = parsed.hostname
        self.port = parsed.port
        self.user = parsed.username
        self.password = parsed.password
        self.ftp: Optional[ftplib.FTP] = None

    def _start(self) -> None:
        super()._start()
        assert (
            self.host is not None
            and self.user is not None
            and self.password is not None
        )
        self.logger.debug("Connect to host=%s", self.host)
        self.ftp = ftplib.FTP(self.host, self.user, self.password)

    def __del__(self) -> None:
        if self.ftp is not None:
            self.ftp.close()
            self.ftp = None

    @staticmethod
    def server_dir(url: str) -> str:
        return os.path.dirname(urlparse(url).path)

    @staticmethod
    def server_fname(url: str) -> str:
        return os.path.basename(urlparse(url).path)

    def server_size(self, url: str) -> int:
        assert self.ftp is not None
        server_fname = self.server_fname(url)
        self.ftp.cwd(self.server_dir(url))
        try:
            size = self.ftp.size(server_fname)
            if size is None:
                size = 0
            return size
        except Exception as e:  # pylint: disable=W0702
            self.ftp.voidcmd("TYPE I")
            size = self.ftp.size(server_fname)
            if size:
                return size
            raise ExtractFailed(self) from e

    def _server_modified(self, url: str) -> Optional[time.struct_time]:
        directory = self.server_dir(url)
        fname = self.server_fname(url)
        assert self.ftp is not None
        gen = self.ftp.mlsd(directory, facts=["modify"])
        if gen is None:
            return None
        for name, facts in gen:
            if name == fname:
                mdate = facts["modify"]
                return time.strptime(mdate, "%Y%m%d%H%M%S")
        return None

    def _store(self, url: str, dest: str) -> None:
        assert self.ftp is not None
        self.ftp.cwd(self.server_dir(url))
        server_fname = self.server_fname(url)

        kwargs: MutableMapping["str", Any] = {
            "unit": "B",
            "unit_scale": True,
            "unit_divisor": 1024,
        }
        server_size = self.server_size(url)
        if server_size:
            kwargs["total"] = server_size

        with open(dest, "wb") as file:
            with tqdm(
                desc=tqdm_desc(self.tid),
                leave=False,
                **kwargs,
            ) as pb:

                def callback(data: Any) -> None:
                    pb.update(len(data))
                    file.write(data)

                self.ftp.retrbinary("RETR {}".format(server_fname), callback)


# pylint: disable=too-many-arguments
class HTTP(Download):
    def __init__(
        self,
        urls: Sequence[str],
        out_files: Sequence["File"],
        eq_callback: Optional[Callable[["Download"], bool]] = None,
        requests_opts: Optional[Mapping[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            urls=urls,
            out_files=out_files,
            eq_callback=eq_callback,
            **kwargs,
        )
        self.requests_opts = requests_opts or {}

    def server_size(self, url: str) -> int:
        requests_opts = dict(self.requests_opts)
        requests_opts.setdefault("headers", {}).update({"Accept-Encoding": "identity"})
        request = requests.get(url, stream=True, **requests_opts)
        key = "Content-length"
        if key in request.headers:
            return int(request.headers[key])

        return 0

    def _store(self, url: str, dest: str) -> None:
        chunk_size = 4 * 1024
        with requests.get(url, stream=True, **self.requests_opts) as request:
            if request.status_code != 200:
                logger.error(
                    "Download failed for: %s. (Status code: %s)",
                    url,
                    request.status_code,
                )
                with open(dest + ".err", "w") as err_out:
                    err_out.write(request.text)
                raise ExtractFailed(self)
            with open(dest, "wb") as file:
                kwargs: MutableMapping["str", Any] = {
                    "unit": "B",
                    "unit_scale": True,
                    "unit_divisor": 1024,
                }
                server_size = self.server_size(url)
                if server_size:
                    kwargs["total"] = server_size

                with tqdm(
                    desc=tqdm_desc(self.tid),
                    leave=False,
                    **kwargs,
                ) as pb:
                    for data in request.iter_content(chunk_size=chunk_size):
                        file.write(data)
                        pb.update(len(data))

    def _server_modified(self, url: str) -> Optional[time.struct_time]:
        request = requests.head(url, **self.requests_opts)
        key = "Last-Modified"
        if key not in request.headers:
            return None

        return datetime.strptime(
            request.headers[key], "%a, %d %b %Y %H:%M:%S GMT"
        ).timetuple()


# pylint: disable=too-many-arguments
class Delayed(Extract):
    def __init__(
        self,
        url: BaseUrl,
        out_file: "File",
        **kwargs: Any,
    ) -> None:
        self._delayed_opts = kwargs.copy()
        super().__init__(offers=[out_file], requires=[])

        self.url = url

    def _do_work(self, context: "Resources") -> None:
        task = create_task(str(self.url), self.out_file, **self._delayed_opts)
        # pylint: disable=protected-access
        task._do_work(context)


class DumpTbl(Extract):
    """Task to dump table

    Args:
        url (str): url encoded database connection
        out_file (File): destination of dumb


    Returns:
        task (bash.Cmd): Task to dump a table from some database
    """

    def __init__(self, url: str, out_file: "File", **kwargs: Any) -> None:
        super().__init__(offers=[out_file], **kwargs)

        self.url = url

    def _do_work(self, context: "Resources") -> None:
        self.dump_table(self.url, out_fname=self.out_res.qname)
        self.out_res.update_db()
        self.out_res.dirty = True

    def dump_table(self, url: str, out_fname: str, chunksize: int = 10000) -> None:
        # extract tname from url, e.g.: mysql://user:pass@host:port/dbname/tname
        parsed = urlparse(url)
        tname = os.path.basename(parsed.path)
        # url without tname
        parsed_url = parsed._replace(path=os.path.basename(parsed.path)).geturl()

        engine = create_engine(parsed_url, server_side_cursors=True)
        with engine.connect() as conn:
            # total number of expected rows
            rows = pd.read_sql(f"SELECT COUNT(*) AS rows FROM {tname}", con=conn).at[
                0, "rows"
            ]
            pb = tqdm(
                total=rows,
                desc=tqdm_desc(self.tid),
                unit=" rows",
                unit_scale=True,
                leave=False,
            )
            gen = pd.read_sql_table(table_name=tname, con=conn, chunksize=chunksize)
            # delete existing file
            if os.path.exists(out_fname):
                os.remove(out_fname)

            for df in gen:
                # switch between write and append
                if os.path.exists(out_fname):
                    mode = "a"
                    header = False
                else:
                    mode = "w"
                    header = True
                df = normalize_columns(df)
                df.to_csv(out_fname, sep="\t", index=False, mode=mode, header=header)
                pb.update(n=chunksize)


def create_http(url: str, out_file: "File", **kwargs: Any) -> HTTP:
    return HTTP(urls=[url], out_files=[out_file], **kwargs)


def create_ftp(url: str, out_file: "File", **kwargs: Any) -> FTP:
    return FTP(urls=[url], out_files=[out_file], **kwargs)


def create_dump_tbl(url: str, out_file: "File", **kwargs: Any) -> DumpTbl:
    return DumpTbl(url=url, out_file=out_file, **kwargs)


scheme2create: Mapping[str, Callable[[str, "File", Any], Extract]] = {
    "HTTP": create_http,  # type: ignore
    "HTTPS": create_http,  # type: ignore
    "FTP": create_ftp,  # type: ignore
    "SFTP": create_ftp,  # type: ignore
    "MYSQL": create_dump_tbl,  # type: ignore
}


def create_task(url: str, out_file: "File", **kwargs: Any) -> Extract:
    scheme = urlparse(url).scheme.upper()
    try:
        create = scheme2create[scheme]
    except KeyError as e:
        raise Exception(f"Unknown scheme: '{scheme}' in url: '{url}'") from e

    return create(url, out_file, **kwargs)  # type: ignore
