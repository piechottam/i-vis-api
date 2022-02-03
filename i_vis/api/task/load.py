from typing import (
    Any,
    Mapping,
    Sequence,
    TYPE_CHECKING,
)
import pandas as pd

from i_vis.core.file_utils import lines

from .base import Task, TaskType
from .. import db
from ..db_utils import recreate_table
from ..resource import (
    Table,
    ResourceId,
    res_registry,
)

if TYPE_CHECKING:
    from ..resource import Resources


CHUNKSIZE = 10 ** 5


# pylint: disable=W0223
# noinspection PyAbstractClass
class _Load(Task):
    @property
    def type(self) -> TaskType:
        return TaskType.LOAD


class Load(_Load):
    TO_CSV_OPTS: Mapping[str, Any] = {
        "sep": "\t",
        "index": False,
        "doublequote": False,
        "escapechar": "\\",
        "header_first_partition_only": True,
        "compression": "gzip",
    }

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        in_rid: ResourceId,
        table: Table,
        **kwargs: Any,
    ) -> None:
        super().__init__(offers=[table], requires=[in_rid], **kwargs)
        self._in_rid = in_rid

    def post_register(self) -> None:
        if self.out_table.desc:
            for fk in getattr(self.out_table.model, "__table__").foreign_keys:
                ref_tname = fk.target_fullname.split(".")[0]
                res = res_registry.get_table(ref_tname)
                if res is not None:
                    # why equal possible?
                    if res.rid != self.out_table.rid:
                        self.required_rids.add(res.rid)
                    else:
                        breakpoint()

    def _do_work(self, context: "Resources") -> None:
        recreate_table(self.out_table)

        # read and store data
        file = context[self.in_rid]
        fname = file.qname
        rows = lines(fname)
        cols = pd.read_csv(fname, sep="\t", nrows=0).columns.tolist()
        query = self._build_query(self.out_table, fname, cols)
        self._load(self.out_table, query, rows)

    @staticmethod
    def _build_query(
        table: Table,
        fname: str,
        cols: Sequence[str],
    ) -> str:
        fields = []
        nullable_fields = []
        model = table.model
        for col in cols:

            if getattr(model, col).nullable:
                nullable_fields.append(col)
                col = "@" + col
                fields.append(col)
            else:
                fields.append(f"`{col}`")
        q = (
            f" LOAD DATA LOCAL INFILE '{fname}' INTO TABLE {table.tname}"
            + r" FIELDS TERMINATED BY '\t'"
            + r" LINES TERMINATED BY '\n' IGNORE 1 LINES ("
            + ", ".join(fields)
            + ")\n"
        )
        if nullable_fields:
            q += "SET\n"
            q += ",\n".join([f"{col} = nullif(@{col},'')" for col in nullable_fields])
        q += ";"

        return q

    def _load(self, table: Table, query: str, rows: int) -> None:
        db.session.commit()
        db.engine.execute(f"ALTER TABLE `{table.tname}` DISABLE KEYS;")
        db.engine.execute(query)
        warnings = list(db.engine.execute("SHOW WARNINGS;"))
        errors = list(db.engine.execute("SHOW ERRORS;"))
        db.engine.execute(f"ALTER TABLE `{table.tname}` ENABLE KEYS;")

        # set row count manually - we just counted it
        table.update_db(row_count=rows - 1)  # ignore header
        # working_update = self.out_res.plugin.updater.working
        # assert working_update is not None
        # table_updates = working_update.table_updates
        # table_update = table_updates.get(self.out_res.name)
        # if table_update is None:
        #    assert working_update.id is not None
        #    working_update.
        #    table_update = self.out_res.create_update(working_update.id)
        # else:
        #    make_transient(table_update)
        # table_update.updated_at = func.now()
        # table_update.row_count = rows - 1  # ignore header

        db.session.commit()
        working_update = table.working_update
        assert working_update is not None
        if working_update.row_count == 0:
            self.logger.warning("No data was loaded to DB")
        elif not working_update.check()[0]:
            self.logger.error("Some data could not be added to the database")
            print(f"Warnings: {warnings}")
            print(f"Errors: {errors}")
            breakpoint()
            raise Exception
