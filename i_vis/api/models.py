import datetime
import enum
import os
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, declarative_mixin, declared_attr, relationship
from sqlalchemy.sql import func

from i_vis.core.config import get_ivis
from i_vis.core.db_utils import row_count as table_row_count
from i_vis.core.file_utils import file_count, latest_modified_datetime
from i_vis.core.file_utils import md5 as file_md5
from i_vis.core.file_utils import modified_datetime as file_modified_datetime
from i_vis.core.file_utils import path_md5, path_size
from i_vis.core.file_utils import size as file_size
from i_vis.core.models import User, load_user, wrap_func
from i_vis.core.utils import EnumMixin
from i_vis.core.version import MAX_VERSION_LENGTH

from . import Base, login, session
from .resource import File, Parquet, Table
from .utils import register_backup

# pylint: disable=import-error
if TYPE_CHECKING:
    from .plugin import BasePlugin
    from .resource import Resource, ResourceType

logger = getLogger()
login.user_loader(load_user)


class UpdateStatus(EnumMixin, enum.Enum):
    ONGOING = "ONGOING"
    FAILED = "FAILED"
    INSTALLED = "INSTALLED"
    ARCHIVED = "ARCHIVED"
    DELETED = "DELETED"
    TMP = "TEMPORARY"


@register_backup
class PluginUpdate(Base):
    __tablename__ = "plugin_updates"

    __mapper_args__ = {
        "confirm_deleted_rows": False,
    }

    id: Mapped[int] = Column(ForeignKey("plugin_versions.id"), primary_key=True)
    status: Mapped[UpdateStatus] = Column(
        Enum(UpdateStatus), nullable=False, default=UpdateStatus.ONGOING
    )
    installed_at: Mapped[datetime.datetime] = Column(
        DateTime, nullable=False, default=func.now(), onupdate=func.now()
    )

    resource_updates: Mapped[MutableSequence["ResourceUpdate"]] = relationship(
        "ResourceUpdate",
        cascade="all, delete-orphan",
        lazy="subquery",
    )
    plugin_version: Mapped["PluginVersion"] = relationship(
        "PluginVersion", uselist=False, back_populates="plugin_update", viewonly=True
    )

    def resource_updates_by_type(
        self, rtype: "ResourceType"
    ) -> MutableMapping[str, "ResourceUpdate"]:
        return {
            res_update.name: res_update
            for res_update in self.resource_updates
            if res_update.type == rtype
        }

    @property
    def file_updates(self) -> Mapping[str, "FileUpdate"]:
        return cast(Mapping[str, "FileUpdate"], self.resource_updates_by_type("File"))

    @property
    def parquet_updates(self) -> Mapping[str, "ParquetUpdate"]:
        return cast(
            Mapping[str, "ParquetUpdate"], self.resource_updates_by_type("Parquet")
        )

    @property
    def table_updates(self) -> Mapping[str, "TableUpdate"]:
        return cast(Mapping[str, "TableUpdate"], self.resource_updates_by_type("Table"))

    def add(self, resource: "Resource", **kwargs: Any) -> "ResourceUpdate":
        name = resource.name
        rtype = resource.get_type()
        if self.status in (UpdateStatus.INSTALLED,):
            msg = f"Fixing {rtype} '{name}'"
            logger.warning(msg)
        res_updates = self.resource_updates_by_type(rtype)
        res_update = res_updates.get(name)  # pylint: disable=no-member

        if not res_update:
            res_update = ResourceUpdate.create(resource, self)
            res_update.update(kwargs)
            res_updates.update({name: res_update})
            session.add(res_update)
            session.commit()
        else:
            uptodate, cache = res_update.check()
            if not uptodate:
                res_update.update(cache)
            else:
                res_update.updated_at = func.now()
            session.add(res_update)
            session.commit()
        return res_update


@register_backup
class Plugin(Base):
    __tablename__ = "plugins"

    name: Mapped[str] = Column(String(30), primary_key=True)
    current_update_id: Mapped[Optional[int]] = Column(
        ForeignKey("plugin_updates.id"), nullable=True
    )
    pending_update_id: Mapped[Optional[int]] = Column(
        ForeignKey("plugin_updates.id"), nullable=True
    )
    frozen: Mapped[bool] = Column(Boolean, nullable=False, default=False)

    current_update: Mapped[Optional[PluginUpdate]] = relationship(
        PluginUpdate, uselist=False, foreign_keys=[current_update_id]
    )
    pending_update: Mapped[Optional[PluginUpdate]] = relationship(
        PluginUpdate, uselist=False, foreign_keys=[pending_update_id]
    )
    available_versions: Mapped[Sequence["PluginVersion"]] = relationship(
        "PluginVersion", back_populates="plugin"
    )

    def create_update(self, plugin_version: "PluginVersion") -> PluginUpdate:
        if not self.pending_update:
            # create new
            self.pending_update = PluginUpdate(
                id=plugin_version.id, status=UpdateStatus.ONGOING
            )
        else:
            if self.pending_update.id == plugin_version.id:
                raise ValueError("Cannot overwrite update.")

            # update old
            self.pending_update.status = UpdateStatus.FAILED
            session.add(self.pending_update)
            # create new
            self.pending_update = PluginUpdate(
                id=plugin_version.id, status=UpdateStatus.ONGOING
            )
        session.commit()

        return self.pending_update

    @property
    def updates(self) -> Sequence["PluginUpdate"]:
        return [v.plugin_update for v in self.available_versions if v.plugin_update]


@register_backup
class ResourceUpdate(Base):
    __tablename__ = "resource_updates"
    __table_args__ = (UniqueConstraint("plugin_update_id", "type", "name"),)

    id: Mapped[int] = Column(Integer, primary_key=True)
    name: Mapped[str] = Column(String(100), nullable=False)
    type: Mapped[str] = Column(String(10), nullable=False)
    plugin_update_id: Mapped[int] = Column(
        Integer,
        ForeignKey("plugin_updates.id"),
        nullable=False,
    )
    updated_at: Mapped[datetime.datetime] = Column(
        DateTime,
        nullable=False,
        default=func.now(),
        onupdate=func.now(),
    )

    plugin_update: Mapped[PluginUpdate] = relationship(
        PluginUpdate, uselist=False, back_populates="resource_updates"
    )

    __mapper_args__ = {
        "polymorphic_on": "type",
        "polymorphic_identity": "resource_updates",
    }

    @staticmethod
    def create(resource: "Resource", plugin_update: "PluginUpdate") -> "ResourceUpdate":
        for class_ in ResourceUpdate.__subclasses__():
            if class_.__name__ == resource.get_type() + "Update":
                return class_(
                    name=resource.name,
                    type=resource.get_type(),
                    plugin_update_id=plugin_update.id,
                )

        raise TypeError

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        raise NotImplementedError

    def update(self, cache: Optional[Mapping[str, Any]] = None) -> None:
        raise NotImplementedError


def qname(resource_update: Union["FileUpdate", "ParquetUpdate"]) -> str:
    from .plugin import BasePlugin

    plugin_update = resource_update.plugin_update or session.get(
        PluginUpdate, resource_update.plugin_update_id
    )
    assert plugin_update is not None
    plugin_version = plugin_update.plugin_version
    plugin = BasePlugin.get(plugin_version.plugin_name)
    return str(os.path.join(plugin.dir.by_update(plugin_update), resource_update.name))


@register_backup
class ParquetUpdate(ResourceUpdate):
    __tablename__ = "parquet_updates"

    id: Mapped[int] = Column(ForeignKey("resource_updates.id"), primary_key=True)
    size: Mapped[int] = Column(
        BigInteger,
        nullable=False,
        default=wrap_func("qname", path_size),
    )
    files: Mapped[int] = Column(
        Integer,
        nullable=False,
        default=wrap_func("qname", file_count),
    )
    md5: Mapped[str] = Column(
        String(32),
        nullable=False,
        default=wrap_func("qname", path_md5),
    )
    modified_at: Mapped[datetime.datetime] = Column(
        DateTime,
        nullable=False,
        default=wrap_func("qname", latest_modified_datetime),
    )

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        if not os.path.exists(self.qname):
            return False, {}

        md5 = path_md5(self.qname)
        return md5 == self.md5, {"md5": md5}

    @property
    def qname(self) -> str:
        return qname(self)

    def update(self, cache: Optional[Mapping[str, Any]] = None) -> None:
        cache = cache or {}
        for attr, callback in (("md5", path_md5),):
            if attr in cache:
                value = cache[attr]
            else:
                value = callback(self.qname)
            setattr(self, attr, value)

        self.size = file_size(self.qname)
        self.files = file_count(self.qname)
        self.modified_at = latest_modified_datetime(self.qname)
        self.updated_at = func.now()

    __mapper_args__ = {
        "polymorphic_identity": Parquet.get_type(),
    }


@register_backup
class FileUpdate(ResourceUpdate):
    __tablename__ = "file_updates"

    id: Mapped[int] = Column(ForeignKey("resource_updates.id"), primary_key=True)
    size: Mapped[int] = Column(
        BigInteger,
        nullable=False,
        default=wrap_func("qname", file_size),
    )
    md5: Mapped[str] = Column(
        String(32),
        nullable=False,
        default=wrap_func("qname", file_md5),
    )
    modified_at: Mapped[datetime.datetime] = Column(
        DateTime,
        nullable=False,
        default=wrap_func("qname", file_modified_datetime),
    )

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        if not os.path.exists(self.qname):
            return False, {}

        md5 = file_md5(self.qname)
        return md5 == self.md5, {"md5": md5}

    @property
    def qname(self) -> str:
        return qname(self)

    def update(self, cache: Optional[Mapping[str, Any]] = None) -> None:
        cache = cache or {}
        for attr, callback in (("md5", file_md5),):
            if attr in cache:
                value = cache[attr]
            else:
                value = callback(self.qname)
            setattr(self, attr, value)

        self.size = file_size(self.qname)
        self.modified_at = file_modified_datetime(self.qname)
        self.updated_at = func.now()

    __mapper_args__ = {
        "polymorphic_identity": File.get_type(),
    }


@register_backup
class TableUpdate(ResourceUpdate):
    __tablename__ = "table_updates"

    id: Mapped[int] = Column(ForeignKey("resource_updates.id"), primary_key=True)
    row_count: Mapped[int] = Column(
        Integer,
        nullable=False,
        default=wrap_func("name", table_row_count),
    )

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        if not get_ivis("PERFORM_TABLE_CHECK", True):
            return True, {"row_count": self.row_count}

        db_count = table_row_count(self.name)
        is_zero = db_count == 0
        is_equal = self.row_count == db_count
        allow_empty_tables = get_ivis("ALLOW_EMPTY_TABLES", False)
        res = (
            is_zero
            and is_equal
            and allow_empty_tables
            or is_equal
            and not is_zero
            and not allow_empty_tables
        )
        return res, {"row_count": db_count}

    def update(self, cache: Optional[Mapping[str, Any]] = None) -> None:
        cache = cache or {}
        for attr, callback in (("row_count", table_row_count),):
            if attr in cache:
                value = cache[attr]
            else:
                value = callback(self.name)
            setattr(self, attr, value)

        self.updated_at = func.now()

    __mapper_args__ = {
        "polymorphic_identity": Table.get_type(),
    }


@declarative_mixin
class PluginMixin:
    PLUGIN_FK = "plugin_fk"

    @declared_attr
    def plugin_fk(self) -> Mapped[str]:
        return Column(ForeignKey(Plugin.name), nullable=False, name=self.PLUGIN_FK)

    @declared_attr
    def plugin(self) -> Mapped[Plugin]:
        return relationship(Plugin, uselist=False)


@register_backup
class PluginVersion(Base):
    __tablename__ = "plugin_versions"
    __table_args__ = (UniqueConstraint("plugin_name", "version_str"),)

    id: Mapped[int] = Column(Integer, primary_key=True)
    plugin_name: Mapped[str] = Column(ForeignKey(Plugin.name), nullable=False)
    version_str: Mapped[str] = Column(String(MAX_VERSION_LENGTH), nullable=False)
    created_at: Mapped[datetime.datetime] = Column(
        DateTime, nullable=False, default=func.now()
    )
    installable: Mapped[bool] = Column(Boolean, nullable=False, default=True)

    plugin: Mapped[Plugin] = relationship(
        Plugin,
        uselist=False,
        back_populates="available_versions",
        lazy="subquery",
    )
    plugin_update: Mapped[PluginUpdate] = relationship(
        PluginUpdate,
        uselist=False,
        back_populates="plugin_version",
        lazy="subquery",
        cascade="all, delete-orphan",
    )


@register_backup
class Request(Base):
    __tablename__ = "requests"

    id: Mapped[int] = Column(Integer, primary_key=True)
    user_id: Mapped[int] = Column(ForeignKey(User.id), nullable=False)
    ip: Mapped[str] = Column(String(14), nullable=False)
    request: Mapped[str] = Column(String(255), nullable=False)
    accessed_at: Mapped[datetime.datetime] = Column(
        DateTime,
        nullable=False,
        onupdate=func.now(),
    )
