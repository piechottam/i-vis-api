"""Database utils"""

from functools import cached_property
from typing import cast, TYPE_CHECKING, Mapping, Any, Sequence
from logging import getLogger
from dask import dataframe as dd

from sqlalchemy.orm import declared_attr, Mapped
import pandas as pd

from i_vis.core.db_utils import i_vis_col

from . import db
from .resource import Parquet

if TYPE_CHECKING:
    from .etl import ETL
    from .resource import Table, ResourceDesc
    from .plugin import CoreType

logger = getLogger()

# primary key column name
RAW_DATA_PK = i_vis_col("id")


def get_part(pname: str, part_name: str = "") -> str:
    return _name(pname, part_name)


def internal_fk(pname: str, part_name: str = "", fk: str = "id") -> str:
    """Format PK for table"""
    part = get_part(pname, part_name)
    return f"raw_{part}.{fk}"


def raw_tname(pname: str, part_name: str = "") -> str:
    """Format table name for raw data of part of plugin"""
    if pname == part_name:
        part_name = ""
    return _name("raw", pname, part_name)


def harmonized_tname(pname: str, core_type: "CoreType", part_name: str = "") -> str:
    """Format table name for harmonized data of part of plugin"""
    if pname == part_name:
        part_name = ""
    return _name("harmonized", core_type.short_name, pname, part_name).replace("-", "_")


def _name(*args: str, sep: str = "_") -> str:
    return sep.join(arg for arg in args if arg)


def recreate_table(table: "Table") -> None:
    """Reset table"""

    db.session.commit()
    with db.engine.connect() as con:
        trans = con.begin()
        con.execute("SET FOREIGN_KEY_CHECKS = 0;")
        con.execute(f"TRUNCATE `{table.name}`")
        con.execute("SET FOREIGN_KEY_CHECKS = 1;")
        trans.commit()

    #
    if table.working_update is not None:
        db.session.delete(table.working_update)
        db.session.commit()
    #
    msg = f"Table {table.name} has been reset"
    logger.debug(msg)


class ResDescMixin:
    @classmethod
    def get_res_desc(cls) -> "ResourceDesc":
        from .resource import ResourceDesc

        return ResourceDesc.from_model(cast(db.Model, cls))


class CoreTypeMixin(ResDescMixin):
    """CoreType Model"""

    id = db.Column(db.Integer, primary_key=True)

    @property
    def related_data(self) -> pd.DataFrame:
        breakpoint()
        raise NotImplementedError


class CoreTypeModel(db.Model, CoreTypeMixin):
    __abstract__ = True


class RawDataMixin(ResDescMixin):
    """Mixin for raw data"""

    i_vis_id = db.Column(db.Integer, primary_key=True)

    @classmethod
    def get_etl(cls) -> "ETL":
        """Associated ETL"""
        raise NotImplementedError

    # read header from parquet
    @cached_property
    def header(self) -> Sequence[str]:
        parquet = self.parquet
        df = cast(dd.DataFrame, parquet.read())
        return cast(Sequence[str], df.columns.tolist())

    @cached_property
    def parquet(self) -> Parquet:
        etl = self.get_etl()
        path = etl.raw_data_path
        pname = etl.pname
        rid = Parquet.link(pname=pname, name=path)
        parquet = cast(Parquet, rid.get())
        return parquet

    @property
    def raw_data(self) -> Mapping[str, str]:
        df = cast(dd.DataFrame, self.parquet.read()).loc[self.i_vis_id]
        return cast(Mapping[str, str], df.to_dict())

    def get_raw_data(self, ids: Sequence[int]) -> pd.DataFrame:
        df = cast(dd.DataFrame, self.parquet.read()).loc[ids]
        return cast(pd.DataFrame, df)

    @property
    def harmonized_data(self) -> Mapping[str, Any]:
        raise NotImplementedError

    @property
    def processed_data(self) -> Mapping[str, Any]:
        raise NotImplementedError


# pylint: disable=W0223
class RawData(db.Model, RawDataMixin):
    __abstract__ = True


class HarmonizedDataMixin(ResDescMixin):
    """Mixin for mapped data"""

    id = db.Column(db.Integer, primary_key=True)

    @declared_attr
    def i_vis_raw_data_id(self) -> Mapped[int]:
        """FK to raw data"""
        raise NotImplementedError

    @declared_attr
    def raw_data(self) -> Mapped[RawData]:
        """Relationship to raw_data"""
        raise NotImplementedError

    @classmethod
    def get_core_type(cls) -> "CoreType":
        """Associated core type"""
        raise NotImplementedError

    @classmethod
    def get_etl(cls) -> "ETL":
        """Associated ETL"""
        raise NotImplementedError


class HarmonizedData(db.Model, HarmonizedDataMixin):
    __abstract__ = True
