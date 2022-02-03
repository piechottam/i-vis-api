from typing import (
    cast,
    Sequence,
    Mapping,
    MutableSequence,
    MutableMapping,
    Optional,
    TYPE_CHECKING,
    Tuple,
    Any,
    Union,
)
import os
import enum
from logging import getLogger
from secrets import token_urlsafe
import datetime

from sqlalchemy.orm import (
    declared_attr,
    declarative_mixin,
    Mapped,
)
from sqlalchemy.sql import func

from i_vis.core.file_utils import (
    size as file_size,
    md5 as file_md5,
    modified_datetime as file_modified_datetime,
    path_size,
    file_count,
    latest_modified_date,
    path_md5,
)
from i_vis.core.constants import API_TOKEN_LENGTH
from i_vis.core.models import load_user, User as CommonUser, wrap_func
from i_vis.core.utils import EnumMixin
from i_vis.core.version import MAX_VERSION_LENGTH
from i_vis.core.config import get_ivis
from i_vis.core.db_utils import row_count as table_row_count

from . import db, login
from .utils import register_backup
from .resource import File, Parquet, Table

# pylint: disable=import-error
if TYPE_CHECKING:
    from .plugin import BasePlugin
    from .resource import (
        Resource,
        ResourceType,
    )

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
class PluginUpdate(db.Model):
    __tablename__ = "plugin_updates"

    __mapper_args__ = {
        "confirm_deleted_rows": False,
    }

    id = db.Column(db.ForeignKey("plugin_versions.id"), primary_key=True)
    status = db.Column(
        db.Enum(UpdateStatus), nullable=False, default=UpdateStatus.ONGOING
    )
    installed_at = db.Column(
        db.DateTime, nullable=False, default=func.now(), onupdate=func.now()
    )

    resource_updates: Mapped[MutableSequence["ResourceUpdate"]] = db.relationship(
        "ResourceUpdate",
        cascade="all, delete-orphan",
        lazy="subquery",
    )
    plugin_version: Mapped["PluginVersion"] = db.relationship(
        "PluginVersion", uselist=False, back_populates="plugin_update", viewonly=True
    )

    def resource_updates_by_type(
        self, rtype: "ResourceType"
    ) -> MutableMapping[str, "ResourceUpdate"]:
        return {
            res_update.name: res_update
            for res_update in self.resource_updates  # pylint: disable=E1133
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
        rtype = resource.type
        if self.status in (UpdateStatus.INSTALLED,):
            msg = f"Fixing {rtype} '{name}'"
            logger.warning(msg)
        res_updates = self.resource_updates_by_type(rtype)
        res_update = res_updates.get(name)  # pylint: disable=no-member

        if not res_update:
            assert self.id is not None
            res_update = ResourceUpdate.create(resource, self)
            res_update.update(kwargs)
            res_updates.update({name: res_update})
            db.session.add(res_update)
            db.session.commit()
        else:
            uptodate, cache = res_update.check()
            if not uptodate:
                res_update.update(cache)
            else:
                res_update.updated_at = func.now()
            db.session.add(res_update)
            db.session.commit()
        return res_update


@register_backup
class Plugin(db.Model):
    __tablename__ = "plugins"

    name = db.Column(db.String(30), primary_key=True)
    current_update_id = db.Column(db.ForeignKey("plugin_updates.id"), nullable=True)
    pending_update_id = db.Column(db.ForeignKey("plugin_updates.id"), nullable=True)
    frozen = db.Column(db.Boolean, nullable=False, default=False)

    current_update: Optional[Mapped["PluginUpdate"]] = db.relationship(
        PluginUpdate, uselist=False, foreign_keys=[current_update_id]
    )
    pending_update: Optional[Mapped["PluginUpdate"]] = db.relationship(
        PluginUpdate, uselist=False, foreign_keys=[pending_update_id]
    )
    available_versions: Mapped[Sequence["PluginVersion"]] = db.relationship(
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
            db.session.add(self.pending_update)
            # create new
            self.pending_update = PluginUpdate(
                id=plugin_version.id, status=UpdateStatus.ONGOING
            )
        db.session.commit()

        return self.pending_update

    @property
    def updates(self) -> Sequence["PluginUpdate"]:
        # pylint: disable=E1133
        return [v.plugin_update for v in self.available_versions if v.plugin_update]

    @property
    def controller(self) -> "BasePlugin":
        from .plugin import BasePlugin

        return BasePlugin.get(self.name)


@register_backup
class ResourceUpdate(db.Model):
    __tablename__ = "resource_updates"
    __table_args__ = (db.UniqueConstraint("plugin_update_id", "type", "name"),)

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.String(10), nullable=False)
    plugin_update_id = db.Column(
        db.Integer,
        db.ForeignKey("plugin_updates.id"),
        nullable=False,
    )
    updated_at = db.Column(
        db.DateTime, nullable=False, default=func.now(), onupdate=func.now()
    )

    plugin_update: Mapped["PluginUpdate"] = db.relationship(
        "PluginUpdate", uselist=False, back_populates="resource_updates"
    )

    __mapper_args__ = {
        "polymorphic_on": "type",
        "polymorphic_identity": "resource_updates",
    }

    @staticmethod
    def create(resource: "Resource", plugin_update: "PluginUpdate") -> "ResourceUpdate":
        for class_ in ResourceUpdate.__subclasses__():
            if class_.__name__ == resource.type + "Update":
                return cast(
                    "ResourceUpdate",
                    class_(
                        name=resource.name,
                        type=resource.type,
                        plugin_update_id=plugin_update.id,
                    ),
                )

        raise TypeError

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        raise NotImplementedError

    def update(self, cache: Optional[Mapping[str, Any]] = None) -> None:
        raise NotImplementedError


def qname(resource_update: Union["FileUpdate", "ParquetUpdate"]) -> str:
    plugin_update = resource_update.plugin_update
    if not plugin_update:
        plugin_update = db.session.query(PluginUpdate).get(
            resource_update.plugin_update_id
        )
    plugin_version = plugin_update.plugin_version  # pylint: disable=no-member
    plugin = plugin_version.plugin.controller
    return str(os.path.join(plugin.dir.by_update(plugin_update), resource_update.name))


@register_backup
class ParquetUpdate(ResourceUpdate):
    __tablename__ = "parquet_updates"

    id = db.Column(db.ForeignKey("resource_updates.id"), primary_key=True)
    size = db.Column(
        db.BigInteger, nullable=False, default=wrap_func("qname", path_size)
    )
    files = db.Column(
        db.Integer, nullable=False, default=wrap_func("qname", file_count)
    )
    md5 = db.Column(db.String(32), nullable=False, default=wrap_func("qname", path_md5))
    modified_at = db.Column(
        db.DateTime, nullable=False, default=wrap_func("qname", latest_modified_date)
    )

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        if not os.path.exists(self.qname):
            return False, {}

        md5 = path_md5(self.qname)
        return cast(bool, md5 == self.md5), {"md5": md5}

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
        self.modified_at = latest_modified_date(self.qname)
        self.updated_at = func.now()

    __mapper_args__ = {
        "polymorphic_identity": Parquet.type,
    }


@register_backup
class FileUpdate(ResourceUpdate):
    __tablename__ = "file_updates"

    id = db.Column(db.ForeignKey("resource_updates.id"), primary_key=True)
    size = db.Column(
        db.BigInteger, nullable=False, default=wrap_func("qname", file_size)
    )
    md5 = db.Column(db.String(32), nullable=False, default=wrap_func("qname", file_md5))
    modified_at = db.Column(
        db.DateTime, nullable=False, default=wrap_func("qname", file_modified_datetime)
    )

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        if not os.path.exists(self.qname):
            return False, {}

        md5 = file_md5(self.qname)
        return cast(bool, md5 == self.md5), {"md5": md5}

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
        "polymorphic_identity": File.type,
    }


@register_backup
class TableUpdate(ResourceUpdate):
    __tablename__ = "table_updates"

    id = db.Column(db.ForeignKey("resource_updates.id"), primary_key=True)
    row_count = db.Column(
        db.Integer, nullable=False, default=wrap_func("name", table_row_count)
    )

    def check(self) -> Tuple[bool, Mapping[str, Any]]:
        assert self.row_count is not None

        if not get_ivis("PERFORM_TABLE_CHECK", True):
            return True, {"row_count": self.row_count}

        assert self.name is not None
        db_count = table_row_count(self.name)
        is_zero = db_count == 0
        is_equal = cast(bool, self.row_count == db_count)
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
        "polymorphic_identity": Table.type,
    }


@declarative_mixin
class PluginMixin:
    PLUGIN_FK = "plugin_fk"

    @declared_attr
    def plugin_fk(self) -> Mapped[str]:
        return db.Column(
            db.ForeignKey(Plugin.name), nullable=False, name=self.PLUGIN_FK
        )

    @declared_attr
    def plugin(self) -> Mapped[Plugin]:
        return db.relationship(Plugin, uselist=False)


@register_backup
class PluginVersion(db.Model):
    __tablename__ = "plugin_versions"
    __table_args__ = (db.UniqueConstraint("plugin_name", "version_str"),)

    id = db.Column(db.Integer, primary_key=True)
    plugin_name = db.Column(db.ForeignKey(Plugin.name), nullable=False)
    version_str = db.Column(db.String(MAX_VERSION_LENGTH), nullable=False)
    created_at: Mapped[datetime.datetime] = db.Column(
        db.DateTime, nullable=False, default=func.now()
    )
    installable = db.Column(db.Boolean, nullable=False, default=True)

    plugin: Mapped["Plugin"] = db.relationship(
        Plugin,
        uselist=False,
        back_populates="available_versions",
        lazy="subquery",
    )
    plugin_update: Optional[Mapped["PluginUpdate"]] = db.relationship(
        PluginUpdate,
        uselist=False,
        back_populates="plugin_version",
        lazy="subquery",
        cascade="all, delete-orphan",
    )


def create_token() -> str:
    for _ in range(1, 5):
        token = token_urlsafe(API_TOKEN_LENGTH)
        user = User.load_by_token(token)
        if not user:
            return token[:API_TOKEN_LENGTH]
    raise Exception()


@register_backup
class User(CommonUser):
    token = db.Column(
        db.String(API_TOKEN_LENGTH),
        nullable=False,
        unique=True,
        index=True,
        default=create_token,
    )

    @classmethod
    def load_by_token(cls, token: str) -> Optional["User"]:
        user = cls.query.filter_by(token=token).first()
        if not user:
            return None
        return cast("User", user)


@register_backup
class Request(db.Model):
    __tablename__ = "requests"

    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.ForeignKey(User.id), nullable=False)
    ip = db.Column(db.String(14), nullable=False)
    request = db.Column(db.String(255), nullable=False)
    accessed_at = db.Column(db.DateTime, nullable=False, onupdate=func.now())
