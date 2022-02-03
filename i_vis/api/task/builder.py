from typing import (
    cast,
    Any,
    Tuple,
    Optional,
    Sequence,
    TYPE_CHECKING,
    Mapping,
    MutableMapping,
    MutableSequence,
    Set,
)

from i_vis.core.file_utils import url2fname

from ..resource import (
    ResourceId,
    Resources,
    ResourceDesc,
    File,
    Table,
    Parquet,
    Resource,
)
from .extract import Delayed, create_task as create_extract_task
from .load import Load
from .transform import Unpack, HarmonizeRawData, ConvertToParquet, MergedRawData
from ..plugin_exceptions import WrongPlugin
from ..df_utils import DataFrameIO, parquet_io, tsv_io, ParquetIO, TsvIO
from ..file_utils import unpack_files, harmonized_fname, not_harmonized_fname
from ..utils import BaseUrl
from ..resource import res_registry, ResourceBuilder

if TYPE_CHECKING:
    from .base import Task, TaskType
    from ..plugin import BasePlugin, CoreType
    from ..etl import ETL, CoreTypeHarmDesc, ColumnContainer, ExtractOpts


class TaskBuilder:
    def __init__(self, plugin: "BasePlugin") -> None:
        self.res_builder = ResourceBuilder(plugin.name)
        self._plugin = plugin
        # container for all tasks
        self._tasks: MutableSequence["Task"] = []

    @property
    def tasks_done(self) -> bool:
        if not self.tasks():
            return True

        tasks = tuple(task for task in self.tasks() if not task.done)
        return not tasks

    def init_etl(self, etl: "ETL") -> None:
        # extract
        rid = self._process_extract_opts(etl)

        # transform
        if hasattr(etl, "transform_opts"):
            rid = self._process_transform_opts(etl=etl, rid=rid)

        if not rid:
            raise Exception("Missing input")

        out_res, harm_desc2files = self.harmonize(rid, etl)
        if not etl.load_opts or etl.load_opts.create_table:
            self.load_raw(out_res, etl)
            self.load_harmonized(harm_desc2files, etl)

    def required_resources(self, context: Resources) -> Resources:
        """Container of required resources by this task builder and its tasks"""
        try:
            needed_res = tuple(
                context[rid] for task in self.tasks() for rid in task.required_rids
            )
            return Resources(set(needed_res))
        except KeyError as e:
            # DEBUG code:
            print(f"Plugin: {self._plugin.name} requires {e.args[0]}")
            from .. import pp

            pp.pprint({str(res.rid): res for res in context})
            raise e

    def offered_resources(
        self, task_types: Optional[Set["TaskType"]] = None
    ) -> Resources:
        """Container of provided resources by this plugin and its tasks"""

        offered = set(
            offered for task in self.tasks(task_types) for offered in task.offered
        )
        return Resources(offered)

    def requires_work(self, context: Resources) -> bool:
        """Check if tasks require to run do_work()"""
        for task in self.tasks():
            if task.requires_work(context):
                return True

        return False

    def tasks(self, task_types: Optional[Set["TaskType"]] = None) -> Sequence["Task"]:
        """Returns a list of tasks"""
        if not task_types:
            return list(self._tasks)
        return [task for task in self._tasks if task.type in task_types]

    @property
    def tid2task(self) -> Mapping[str, "Task"]:
        return {str(task.tid): task for task in self.tasks()}

    def add_task(self, task: "Task") -> Resources:
        for offered in task.offered:
            # check meta info of resources
            # ensure that rid is correct and
            # references the correct plugin
            if offered.rid.pname != self._plugin.name:
                raise WrongPlugin(
                    f"'{offered.rid}' CANNOT be offered by '{str(task.tid)}'."
                )

        self._tasks.append(task)
        return task.offered

    def add_id(
        self, in_file: File, etl: "ETL", add_id_opts: Mapping[str, Any]
    ) -> Tuple[ConvertToParquet, Parquet]:
        col = add_id_opts["col"]
        path = etl.raw_data_path
        out_parquet = self.res_builder.parquet(
            path,
            io=ParquetIO(read_opts=add_id_opts["read_opts"]),
            desc=in_file.desc,
        )

        task = ConvertToParquet(
            in_file_id=in_file.rid,
            out_parquet=out_parquet,
            col=col,
        )
        return task, out_parquet

    def unpack(
        self,
        in_rid: ResourceId,
        out_fnames: Sequence[str],
        ios: Optional[Sequence[DataFrameIO]] = None,
        descs: Optional[Sequence[ResourceDesc]] = None,
    ) -> Tuple[Unpack, Tuple[File, ...]]:
        unpacked_files = unpack_files(
            pname=self._plugin.name,
            out_fnames=out_fnames,
            ios=ios,
            descs=descs,
        )
        task = Unpack(in_file_id=in_rid, out_files=unpacked_files)
        self.add_task(task)
        return task, unpacked_files

    def harmonize(
        self,
        in_rid: ResourceId,
        etl: "ETL",
    ) -> Tuple["File", Mapping["CoreTypeHarmDesc", Tuple["File", "File"]]]:
        harm_desc2files: MutableMapping["CoreTypeHarmDesc", Tuple["File", "File"]] = {}
        for core_type in etl.core_types:
            harm_file = self.res_builder.file(
                fname=harmonized_fname(core_type, etl.part_name),
                io=tsv_io,
                # TODO remove ID from requirement
                desc=ResourceDesc(
                    cols=[
                        column.name
                        for column in etl.core_type2model[core_type].__table__.c
                        if etl.core_type2model.get(core_type)
                    ],
                ),
            )
            not_harm_file = self.res_builder.file(
                fname=not_harmonized_fname(core_type, etl.part_name),
                io=tsv_io,
            )
            harm_desc2files[etl.core_type2harm_desc[core_type]] = (
                harm_file,
                not_harm_file,
            )

        fname = etl.raw_data_fname
        out_res = self.res_builder.file(fname, io=TsvIO(to_opts={"index": True}))
        if etl.split_harm:
            in_res_ids: MutableSequence["ResourceId"] = list()
            for harm_desc, files in harm_desc2files.items():
                path = "_" + etl.part + "_" + harm_desc.core_type.short_name
                path = Parquet.format_fname(path)
                out_parquet = self.res_builder.parquet(path, io=parquet_io)
                in_res_ids.append(out_parquet.rid)
                self.harmonize_raw_data(
                    in_rid=in_rid,
                    harm_desc2files={
                        harm_desc: files,
                    },
                    out_res=out_parquet,
                    raw_columns=etl.all_raw_columns,
                )
            task = MergedRawData(in_res_ids=in_res_ids, out_file=out_res)
            self.add_task(task)
        else:
            self.harmonize_raw_data(
                in_rid=in_rid,
                harm_desc2files=harm_desc2files,
                out_res=out_res,
                raw_columns=etl.all_raw_columns,
            )

        return out_res, harm_desc2files

    def harmonize_raw_data(
        self,
        in_rid: ResourceId,
        harm_desc2files: Mapping["CoreTypeHarmDesc", Tuple["File", "File"]],
        out_res: Resource,
        raw_columns: "ColumnContainer",
    ) -> HarmonizeRawData:
        task = HarmonizeRawData(
            in_rid=in_rid,
            harm_desc2files=harm_desc2files,
            out_res=out_res,
            raw_columns=raw_columns,
        )
        self.add_task(task)
        return task

    def load(
        self,
        in_rid: "ResourceId",
        table: "Table",
        **kwargs: Any,
    ) -> None:
        self._tasks.append(
            Load(
                in_rid=in_rid,
                table=table,
                **kwargs,
            )
        )

    def delayed(
        self,
        url: BaseUrl,
        out_fname: str,
        io: Optional[DataFrameIO] = None,
        desc: Optional["ResourceDesc"] = None,
        **kwargs: Any,
    ) -> Tuple[File, "Task"]:
        out_file = self.res_builder.file(fname=out_fname, io=io, desc=desc)
        task = Delayed(url, out_file, **kwargs)
        return out_file, task

    # pylint: disable=too-many-arguments
    def extract(
        self,
        url: str,
        io: Optional[DataFrameIO] = None,
        desc: Optional["ResourceDesc"] = None,
        out_fname: str = "",
        add_task: bool = True,
        **kwargs: Any,
    ) -> Tuple[File, "Task"]:
        if not out_fname:
            out_fname = url2fname(url)
        out_file = self.res_builder.file(
            fname=out_fname,
            io=io,
            desc=desc,
        )
        task = create_extract_task(url=url, out_file=out_file, **kwargs)
        if add_task:
            self.add_task(task)
        return out_file, task

    def load_raw(self, in_file: "File", etl: "ETL") -> "Table":
        data_table = self.res_builder.table_from_model(model=etl.raw_model)
        self.load(in_rid=in_file.rid, table=data_table)
        return data_table

    def load_harmonized(
        self,
        harm_desc2files: Mapping["CoreTypeHarmDesc", Tuple["File", "File"]],
        etl: "ETL",
    ) -> Tuple[
        Mapping["CoreTypeHarmDesc", Tuple["File", "File"]],
        Mapping["CoreType", "Table"],
    ]:
        core_type2table: MutableMapping["CoreType", Table] = {}
        for harm_desc, (harm_file, _) in harm_desc2files.items():
            harmonized_model = etl.harmonized_model(harm_desc.core_type)
            assert harmonized_model is not None
            table = self.res_builder.table_from_model(model=harmonized_model)
            self.load(
                in_rid=harm_file.rid,
                table=table,
            )
            core_type2table[harm_desc.core_type] = table
        return harm_desc2files, core_type2table

    def _process_extract_opts(self, etl: "ETL") -> ResourceId:
        opts = getattr(etl, "extract_opts")
        rid: ResourceId = getattr(opts, "rid", None)
        if rid:
            if opts.unpack:
                rid = self._process_unpack(opts, rid)
            else:
                if opts.add_id:
                    in_file = cast(File, res_registry.get(File.type)[rid])
                    task, out_res = self.add_id(in_file, etl, opts.add_id)
                    rid = out_res.rid
                    self.add_task(task)
        else:
            io = None
            desc = None
            if opts.io and not opts.unpack:
                io = opts.io
                if hasattr(opts, "raw_columns") and opts.raw_columns:
                    desc = ResourceDesc(opts.raw_columns.cols)

            url: BaseUrl = opts.url
            assert url is not None
            if not url.static:
                extracted_file, extract_task = self.delayed(
                    url=url,
                    io=io,
                    out_fname=opts.out_fname,
                    requests_opts=url.args,
                )
                self.add_task(extract_task)
            else:
                out_fname = opts.out_fname or url2fname(str(url))
                extracted_file, extract_task = self.extract(
                    url=str(url),
                    io=io,
                    desc=desc,
                    out_fname=out_fname,
                    **url.args,
                )
            if not opts.unpack and opts.add_id:
                task, out_res = self.add_id(extracted_file, etl, opts.add_id)
                rid = out_res.rid
                self.add_task(task)
            else:
                rid = extracted_file.rid
        if opts.unpack:
            rid = self._process_unpack(opts, rid)
            if opts.add_id:
                in_file = cast(File, res_registry.get(File.type)[rid])
                task, out_res = self.add_id(in_file, etl, opts.add_id)
                rid = out_res.rid
                self.add_task(task)
        return rid

    def _process_unpack(self, opts: "ExtractOpts", rid: ResourceId) -> ResourceId:
        io = opts.io
        descs = None
        if hasattr(opts, "raw_columns") and opts.raw_columns:
            descs = (ResourceDesc(opts.raw_columns.cols),)

        _, unpacked_files, = self.unpack(
            in_rid=rid,
            out_fnames=(opts.unpack,),
            ios=(io,),
            descs=descs,
        )
        rid = unpacked_files[0].rid
        return rid

    def _process_transform_opts(self, rid: ResourceId, etl: "ETL") -> ResourceId:
        opts = getattr(etl, "transform_opts")

        if hasattr(opts, "task") and opts.task:
            task = opts.task

            desc: Optional[ResourceDesc] = None
            if opts.raw_columns and opts.raw_columns.cols:
                cols: MutableSequence[str] = []
                cols.extend(opts.raw_columns.cols)
                if task.columns.cols and task.columns.cols:
                    cols.extend(task.columns.cols)
                desc = ResourceDesc(cols)

            task_opts = task.task_opts or {}
            io = getattr(opts, "io")
            if io is None:
                io = rid.get().io
            task_opts.setdefault("io", io)
            task_opts["desc"] = desc

            transform_task = task.task(in_rid=rid, **task_opts)
            rid = transform_task.out_res.rid
            self.add_task(transform_task)
        return rid
