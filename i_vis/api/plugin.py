from typing import (
    Any,
    Callable,
    cast,
    Sequence,
    Mapping,
    MutableMapping,
    MutableSequence,
    Optional,
    Set,
    Tuple,
    TYPE_CHECKING,
    Type,
    Union,
)
import os
from functools import cached_property
from importlib import import_module
from pkgutil import iter_modules
from abc import ABC
from logging import getLogger, LoggerAdapter
from datetime import datetime
from types import ModuleType
from sqlalchemy.orm.decl_api import declared_attr
from marshmallow.base import FieldABC

from i_vis.core.config import MissingVariable, get_ivis
from i_vis.core.file_utils import clean_fname
from i_vis.core.version import (
    Unknown as UnknownVersion,
    Version,
    UnknownVersionError,
)

from . import ma
from .config_utils import disabled_pnames, disable_pname
from .models import (
    Plugin as PluginModel,
    PluginVersion,
    PluginUpdate,
    UpdateStatus,
)
from .plugin_exceptions import (
    MissingPluginModel,
    UnknownPlugin,
    MissingAnyVersion,
    MissingCurrentVersion,
    MissingPendingVersion,
    MissingWorkingUpdateError,
    NewPluginVersion,
    WrongPluginType,
)
from .terms import TermType
from .task.builder import TaskBuilder
from .task.workflow import UpdatePlugin
from .resource import File

if TYPE_CHECKING:
    from . import db as db_
    from .task.base import Task
    from .etl import ETLSpec, ETL
    from .harmonizer import Harmonizer
    from .task.transform import HarmonizeRawData

logger = getLogger()


def import_plugin_module(*names: str) -> ModuleType:
    return import_module(".".join(names))


class Meta:
    def __init__(
        self,
        name: str,
        fullname: Optional[str] = None,
        url: Optional[str] = None,
        api_prefix: str = "",
    ) -> None:
        self.name = name
        self.fullname = fullname if fullname else name.title()
        self.url = url
        self.api_prefix = api_prefix

    def register_variable(self, **kwargs: Any) -> str:
        from . import config_meta

        kwargs["pname"] = self.name
        return config_meta.register_plugin_variable(**kwargs)

    @staticmethod
    def create(
        package: str,
        fullname: Optional[str] = None,
        url: Optional[str] = None,
        api_prefix: str = "",
    ) -> "Meta":
        name = package.rsplit(".", maxsplit=1)[-1]
        return Meta(
            name=name,
            fullname=fullname,
            url=url,
            api_prefix=api_prefix,
        )


# pylint: disable=too-many-instance-attributes
class BasePlugin:
    # restrict Plugins to only on Instance per Class and
    # enable selective choice of Classes -> enable|disable specifc plugins

    # container for ALL registered plugin names
    NAME2PLUGIN: MutableMapping[str, Type["BasePlugin"]] = {}

    # container plugin instances
    _INSTANCES: MutableMapping[str, "BasePlugin"] = {}

    def __init__(
        self,
        meta: Meta,
        str_to_version: Callable[[str], Version],
        _register: bool = True,
    ) -> None:
        # register plugin globally...
        self.meta = meta

        # module path e.g.: i_vis.api.plugins.PLUGIN
        self.path = ""  # will be set in import_plugin

        self.updater = UpdateManager(self.meta.name, str_to_version=str_to_version)
        self.version = VersionInfo(self.updater)
        self.dir = DirectoryManager(self.version)

        # logging specific
        self.logger = LoggerAdapter(getLogger("i_vis.api"), {"detail": meta.name})
        self.task_builder = TaskBuilder(self)

        if _register:
            BasePlugin.register_instance(instance=self)

    def __eq__(self, other: Any) -> bool:
        return other and isinstance(other, BasePlugin) and self.meta.name == other.name

    def __hash__(self) -> int:
        return hash(self.meta.name)

    @property
    def routes(self) -> Sequence[Any]:
        return []

    @property
    def latest_version(self) -> Version:
        try:
            return self._latest_version
        except Exception as e:  # pylint: disable = broad-except
            self.logger.exception(e)
        return UnknownVersion()

    @property
    def _latest_version(self) -> Version:
        """Latest version of data provided by this plugin"""
        raise NotImplementedError

    @property
    def name(self) -> str:
        """Plugin name."""
        return self.meta.name

    @classmethod
    def type(cls) -> str:
        return cls.__name__

    def register_tasks(self, force: bool = False) -> Sequence["Task"]:
        """Register all tasks of this plugin."""

        tasks = self.task_builder.tasks()
        if not tasks or force:
            self._init_tasks()
            self.task_builder.add_task(UpdatePlugin(self.name))
            tasks = self.task_builder.tasks()
        return tasks

    @property
    def installed(self) -> bool:
        try:
            current = self.updater.current
            if not current:
                return False
        except MissingPluginModel:
            return False

        return cast(bool, current.status == UpdateStatus.INSTALLED)

    def _init_tasks(self) -> None:
        pass

    def check_upgrade(self) -> bool:
        # pending must be latest - data retrieval always works on latest_version
        local_latest = self.version.local_latest
        if not local_latest.is_known:
            return False

        if not self.version.current.is_known:
            return True

        return self.version.current < local_latest

    def status(
        self, ignore_upgrade_check: bool, ignore_consistency_check: bool
    ) -> Mapping[str, Any]:
        return status(self.name, ignore_upgrade_check, ignore_consistency_check)

    @classmethod
    def import_plugins(cls) -> None:
        raise NotImplementedError

    @classmethod
    def pnames(cls, include_disabled: bool = False) -> Sequence[str]:
        """All or only enabled plugin names"""

        names = [
            pname
            for pname in BasePlugin.NAME2PLUGIN
            if include_helper(
                pname, include_disabled=include_disabled, plugin_class=cls
            )
        ]
        names.sort()
        return names

    @classmethod
    def get(cls, name: str) -> "BasePlugin":
        """Get plugin instance"""

        try:
            plugin = BasePlugin._INSTANCES[name]
            if not isinstance(plugin, cls):
                raise UnknownPlugin(pname=plugin.name)

            return plugin
        except KeyError as e:
            raise UnknownPlugin(pname=name) from e

    @classmethod
    def instances(cls) -> Sequence["BasePlugin"]:
        """Instances of all plugins"""

        return tuple(BasePlugin._INSTANCES[pname] for pname in cls.pnames())

    @staticmethod
    def register_pname(pname: str, plugin: Type["BasePlugin"]) -> None:
        """Register a plugin name"""

        if pname in BasePlugin.NAME2PLUGIN:
            raise ValueError(f"Already registered plugin: {pname}")

        BasePlugin.NAME2PLUGIN[pname] = plugin

    @staticmethod
    def is_registered(name: str) -> bool:
        """Check if plugin name is registered"""

        return name in BasePlugin.NAME2PLUGIN

    @staticmethod
    def register_instance(instance: "BasePlugin") -> str:
        name = instance.meta.name
        if name not in BasePlugin.NAME2PLUGIN:
            raise UnknownPlugin(name)

        if name in BasePlugin._INSTANCES:
            raise ValueError(f"Already registered plugin instance: {name}")

        BasePlugin._INSTANCES.update({name: instance})
        return name

    @staticmethod
    def exists_instance(name: str) -> bool:
        """Check if known plugin instance"""

        return name in BasePlugin._INSTANCES

    @staticmethod
    def _find_pnames(package: ModuleType) -> Sequence[Tuple[str, str]]:
        names = []
        for _, name, ispkg in iter_modules(getattr(package, "__path__")):
            if not ispkg:
                continue

            module = import_module(".".join([package.__name__, name]))
            meta = getattr(module, "meta")
            if not meta:
                continue

            names.append((meta.name, name))
        return names

    @classmethod
    def _import_plugins(
        cls,
        package_name: str,
        additional: Optional[Sequence[str]] = None,
    ) -> None:
        from . import config_meta

        additional = additional or []

        for pname, name in BasePlugin._find_pnames(import_module(package_name)):
            try:
                module = import_plugin_module(package_name, name, "plugin")
            except ModuleNotFoundError:
                continue
            if not hasattr(module, "Plugin"):
                continue

            # register name
            BasePlugin.register_pname(pname, getattr(module, "Plugin"))

            # try to create instance
            try:
                plugin = getattr(module, "Plugin")()
                # register path where plugin was found
                plugin.path = package_name + "." + name

                config_meta.set_plugin_defaults(plugin.name)
                # check if required variables are set
                config_meta.check_plugin_config(plugin.name)
            except MissingVariable as e:
                msg = f"Plugin '{pname}' disabled - missing variable '{e.variable}'"
                logger.warning(msg)
                disable_pname(pname)

            for module_name in additional:
                try:
                    _ = import_plugin_module(package_name, name, module_name)
                except ModuleNotFoundError:
                    pass


def status(
    pname: str,
    ignore_upgrade_check: bool = False,
    ignore_consistency_check: bool = False,
) -> Mapping[str, Any]:
    ptype = "unknown"
    current = str(None)
    pending = str(None)
    latest = str(None)
    plugin = None

    try:
        plugin = BasePlugin.get(pname)
        ptype = plugin.type()
    except UnknownPlugin:
        pass

    info = []
    if not plugin or pname in disabled_pnames():
        info.append("disabled")
    else:
        try:
            register_tasks([plugin], not ignore_upgrade_check)
        except (MissingAnyVersion, MissingPluginModel):
            pass
        except NewPluginVersion:
            info.append("NEW LATEST!!!(deep-reset plugin)")

        if not ignore_consistency_check and plugin.task_builder.tasks():
            if not plugin.task_builder.offered_resources().consistent(
                desc=f"Consistency of {pname}", unit="resources", leave=False
            ):
                info.append("inconsistent")

        try:
            if plugin.version.current.is_known:
                current = str(plugin.version.current)
            pending = str(plugin.version.pending)
            latest = str(plugin.version.local_latest)
            if plugin.updater.model.frozen:
                info.append("frozen")
        except MissingPluginModel:
            info.append("no model")

    return {
        "type": ptype,
        "pname": pname,
        "current": current,
        "pending": pending,
        "latest": latest,
        "info": info,
    }


def post_register() -> None:
    for plugin in BasePlugin.instances():
        for task in plugin.task_builder.tasks():
            task.post_register()


def register_tasks(plugins: Sequence["BasePlugin"], check_upgrade: bool = True) -> None:
    tasks: MutableSequence["Task"] = []
    for plugin in plugins:
        if plugin.updater.pending is not None:
            if check_upgrade and not plugin.check_upgrade():
                # local and remote latest version changed
                raise NewPluginVersion(plugin.name)
        elif plugin.updater.current is not None:
            pass
        else:
            raise MissingAnyVersion(plugin.name)

        plugin_tasks = plugin.register_tasks()
        if plugin_tasks:
            tasks.extend(plugin_tasks)


def register_upgrade_tasks(
    plugins: Sequence["BasePlugin"], check_upgrade: bool = True
) -> None:
    tasks: MutableSequence["Task"] = []
    for plugin in plugins:
        if not plugin.updater.pending:
            raise MissingPendingVersion(plugin.name)
        if check_upgrade and not plugin.check_upgrade():
            # local and remote latest version changed
            # local latest link points to ambiguous
            raise NewPluginVersion(plugin.name)
        plugin_tasks = plugin.register_tasks()
        if plugin_tasks:
            tasks.extend(plugin_tasks)


def register_update_tasks(plugins: Sequence["BasePlugin"]) -> None:
    tasks: MutableSequence["Task"] = []
    for plugin in plugins:
        if not plugin.updater.current:
            raise MissingCurrentVersion(plugin.name)
        plugin_tasks = plugin.register_tasks()
        if plugin_tasks:
            tasks.extend(plugin_tasks)


def unknown_pnames(pnames: Sequence[str]) -> Sequence[str]:
    """Filter unknown plugin names"""

    known_pnames = BasePlugin.pnames()
    return tuple(pname for pname in pnames if pname not in known_pnames)


def is_core_type(name: str) -> bool:
    try:
        _ = CoreType.get(name)
    except WrongPluginType:
        return False

    return True


def is_data_source(name: str) -> bool:
    try:
        _ = DataSource.get(name)
    except WrongPluginType:
        return False

    return True


def is_plugin(name: str) -> bool:
    try:
        _ = BasePlugin.get(name)
    except UnknownPlugin:
        return False

    return True


def include_helper(
    pname: str,
    include_disabled: bool,
    plugin_class: Optional[Type["BasePlugin"]] = None,
) -> bool:
    if pname not in BasePlugin.NAME2PLUGIN:
        return False

    if plugin_class and not issubclass(BasePlugin.NAME2PLUGIN[pname], plugin_class):
        return False

    if include_disabled:
        return True

    if pname in disabled_pnames():
        return False

    try:
        _ = BasePlugin.get(pname)
    except UnknownPlugin:
        return False

    return True


class DirectoryManager:
    def __init__(self, version_info: "VersionInfo") -> None:
        self._version_info = version_info
        self._updater = self._version_info.updater

    @property
    def current(self) -> str:
        """Absolute working path for current version"""
        return self.by_version(self._version_info.current)

    @property
    def pending(self) -> str:
        """Absolute working path for pending version"""
        if self._updater.pending is not None:
            raise MissingPendingVersion(self._updater.pname)

        assert self._version_info.pending is not None
        return self.by_version(self._version_info.pending)

    @property
    def working(self) -> str:
        if not self._updater.working:
            raise MissingWorkingUpdateError()

        assert self._version_info.working is not None
        return self.by_version(self._version_info.working)

    def by_update(self, plugin_update: PluginUpdate) -> str:
        version_str = plugin_update.plugin_version.version_str
        version = self._version_info.str_to_version(version_str)
        return self.by_version(version)

    def by_version(self, version: Version) -> str:
        """Version specific absolute working path for plugin."""
        if not version.is_known:
            raise UnknownVersionError()
        data_dir = get_ivis("DATA_DIR")
        clean_version = clean_fname(str(version))
        return str(os.path.join(data_dir, self._updater.pname, clean_version))


class UpdateManager:
    def __init__(
        self,
        pname: str,
        str_to_version: Callable[[str], Version],
    ) -> None:
        self.pname = pname
        self.logger = LoggerAdapter(getLogger("i_vis.api"), {"detail": pname})
        self.str_to_version = str_to_version
        self._working: Optional[PluginUpdate] = None

    @property
    def _db(self) -> Any:
        from . import db

        return db

    def add_latest_version(self, latest_version: Version) -> PluginVersion:
        # create new latest version
        plugin_version = PluginVersion(
            plugin_name=self.pname, version_str=str(latest_version)
        )
        db = self._db
        db.session.add(plugin_version)
        return plugin_version

    @property
    def has_model(self) -> bool:
        try:
            _ = self.model
        except MissingPluginModel:
            return False
        return True

    @property
    def model(self) -> PluginModel:
        db = self._db
        plugin_model = db.session.query(PluginModel).get(self.pname)
        if not plugin_model:
            raise MissingPluginModel(self.pname)

        return cast(PluginModel, plugin_model)

    @property
    def working(self) -> Optional[PluginUpdate]:
        if self.model.pending_update:
            return self.model.pending_update

        if self.model.current_update:
            return self.model.current_update

        return None

    @property
    def current(self) -> Optional[PluginUpdate]:
        if not self.model.current_update:
            return None

        return self.model.current_update

    @property
    def pending(self) -> Optional[PluginUpdate]:
        if not self.model.pending_update:
            return None

        return self.model.pending_update

    def remove_files(self, version: "Version") -> None:
        """Remove files from file system and DB"""
        db = self._db
        plugin_version = (
            db.session.query(PluginVersion)
            .filter_by(plugin_name=self.pname, version_str=str(version))
            .join(PluginUpdate)
            .first()
        )
        if not plugin_version:
            return

        file_updates = plugin_version.plugin_update.file_updates
        for file_update in file_updates.values():
            try:
                os.remove(file_update.qname)
            except FileNotFoundError:
                self.logger.debug(f"File not found: {file_update.qname}")
            db.session.delete(file_update)
        db.session.commit()

    def remove_tables(self, version: "Version") -> None:
        db = self._db
        plugin_version = (
            db.session.query(PluginVersion)
            .filter_by(plugin_name=self.pname, version_str=str(version))
            .join(PluginUpdate)
            .first()
        )
        if not plugin_version:
            return

        table_updates = plugin_version.plugin_update.table_updates
        db = self._db
        for table_update in table_updates.values():
            db.engine.execute("SET FOREIGN_KEY_CHECKS = 0")
            db.engine.execute(f"TRUNCATE TABLE {table_update.name}")
            db.engine.execute(f"ALTER TABLE {table_update.name} AUTO_INCREMENT = 1")
            db.engine.execute("SET FOREIGN_KEY_CHECKS = 1")

            db.session.commit()
            db.session.delete(table_update)

    @property
    def tnames(self) -> Sequence[str]:
        if not self.model or not self.model.current_update:
            return ()

        current_update = self.model.current_update
        return tuple(
            table_update.name
            for table_update in current_update.table_updates.values()
            if table_update.name is not None
        )


class VersionInfo:
    def __init__(
        self,
        updater: "UpdateManager",
    ) -> None:
        self.updater = updater
        self.str_to_version = self.updater.str_to_version

    @property
    def current(self) -> Version:
        try:
            if not self.updater.current:
                return UnknownVersion()

            return self.str_to_version(self.updater.current.plugin_version.version_str)
        except MissingPluginModel:
            return UnknownVersion()

    @property
    def pending(self) -> Optional[Version]:
        if not self.updater.pending:
            return None

        return self.str_to_version(self.updater.pending.plugin_version.version_str)

    @property
    def working(self) -> Optional[Version]:
        if not self.updater.working:
            return None
        return self.str_to_version(self.updater.working.plugin_version.version_str)

    @property
    def available(self) -> Sequence[PluginVersion]:
        try:
            return self.updater.model.available_versions
        except MissingPluginModel:
            return tuple()

    @property
    def local_latest(self) -> Version:
        if not self.available:
            return UnknownVersion()

        def helper(plugin_version: "PluginVersion") -> datetime:
            return plugin_version.created_at

        latest = sorted(
            self.available,
            key=helper,
            reverse=True,
        )[0]

        assert latest.version_str is not None
        return self.str_to_version(latest.version_str)

    @property
    def is_uptodate(self) -> bool:
        try:
            return (
                self.current.is_known
                and self.local_latest.is_known
                and self.current == self.local_latest
            )
        except MissingPluginModel:
            return False

    @property
    def updateable(self) -> bool:
        return not self.updater.model.frozen and (
            not self.current.is_known
            and self.local_latest.is_known
            or self.current.is_known
            and self.local_latest.is_known
            and self.current < self.local_latest
        )

    def update_to_version(self, plugin_update: "PluginUpdate") -> Version:
        assert plugin_update.plugin_version.plugin_name == self.updater.pname
        return self.str_to_version(plugin_update.plugin_version.version_str)


class ETLManager:
    def __init__(
        self,
        updater: UpdateManager,
        etl_specs: Optional[Sequence[Type["ETLSpec"]]] = None,
    ) -> None:
        self._updater = updater

        self._etl_specs: Set[Type["ETLSpec"]] = set()
        if etl_specs is not None:
            self._etl_specs = set(etl_specs)

        self._part2etl: MutableMapping[str, "ETL"] = {}
        self.init_part2etl()
        for etl in self._part2etl.values():
            _ = etl.raw_model
            _ = etl.core_type2model.values()

    @property
    def pname(self) -> str:
        return self._updater.pname

    @property
    def part_names(self) -> Sequence[str]:
        return tuple(etl.part_name for etl in self._part2etl.values())

    def init_part2etl(self) -> None:
        if self._part2etl:
            return

        for etl_spec in self._etl_specs:
            etl = etl_spec.create(self.pname)
            self._part2etl[etl.part] = etl

    @property
    def part2etl(self) -> Mapping[str, "ETL"]:
        return dict(self._part2etl)

    def parts_by_term(self, *terms: TermType) -> Sequence[str]:
        filtered = set()
        for name, etl in self._part2etl.items():
            for term in terms:
                if etl.all_raw_columns.col_by_closest_term(term):
                    filtered.add(name)
        return list(filtered)

    def parts_by_core_type_short_name(
        self, *core_type_short_names: str
    ) -> Sequence[str]:
        parts: Set[str] = set()
        for part, etl in self._part2etl.items():
            for core_type in etl.core_types:
                if core_type.short_name in core_type_short_names:
                    parts.add(part)

        return list(parts)

    def parts_by_core_types(self, *core_types: "CoreType") -> Sequence[str]:
        return self.parts_by_core_type_short_name(
            *[core_type.short_name for core_type in core_types]
        )

    @cached_property
    def part2core_types(self) -> Mapping[str, Set["CoreType"]]:
        return {name: etl.core_types for name, etl in self._part2etl.items()}


class DataSource(BasePlugin, ABC):
    def __init__(
        self,
        etl_specs: Optional[Sequence[Type["ETLSpec"]]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.etl_specs = etl_specs

    @cached_property
    def etl_manager(self) -> ETLManager:
        return ETLManager(self.updater, self.etl_specs)

    @property
    def parts(self) -> Mapping[str, "ETL"]:
        return dict(self.etl_manager.part2etl)

    @classmethod
    def get(cls, name: str) -> "DataSource":
        return cast("DataSource", super().get(name))

    def _init_tasks(self) -> None:
        self.etl_manager.init_part2etl()
        for etl in self.etl_manager.part2etl.values():
            self.task_builder.init_etl(etl)

    @classmethod
    def import_plugins(cls) -> None:
        if cls.instances():
            return

        BasePlugin._import_plugins("i_vis.api.data_sources")
        for data_source in cls.instances():
            data_source.etl_manager.init_part2etl()

    @classmethod
    def instances(cls) -> Sequence["DataSource"]:
        return cast(Sequence["DataSource"], super().instances())


class CoreType(BasePlugin, ABC):
    @property
    def short_name(self) -> str:
        return CoreType.get_short_name(self.meta)

    @property
    def blueprint_name(self) -> str:
        return CoreType.get_blueprint_name(self.meta)

    @property
    def clean_name(self) -> str:
        return self.blueprint_name.replace("-", "_")

    @staticmethod
    def get_short_name(meta: "Meta") -> str:
        return meta.name.removeprefix("i-vis-")

    @staticmethod
    def get_blueprint_name(meta: "Meta") -> str:
        s = CoreType.get_short_name(meta)
        if s.endswith("s"):
            return s
        return s + "s"

    @classmethod
    def get(cls, name: str) -> "CoreType":
        return cast("CoreType", super().get(name))

    def register_with_terms(self) -> Sequence[TermType]:
        raise NotImplementedError

    @cached_property
    def harmonizer(self) -> "Harmonizer":
        raise NotImplementedError

    @cached_property
    def harm_meta(self) -> "CoreTypeMeta":
        raise NotImplementedError

    @property
    def normalized_to(self) -> str:
        return self.harm_meta.target

    @classmethod
    def get_by_target(cls, target: str) -> "CoreType":
        for core_type in cls.instances():
            if core_type.harm_meta.target == target:
                return core_type

        raise KeyError(f"Unknown target: {target}")

    @classmethod
    def instances(cls) -> Sequence["CoreType"]:
        return cast(Sequence["CoreType"], super().instances())

    @classmethod
    def import_plugins(cls) -> None:
        if cls.instances():
            return

        BasePlugin._import_plugins("i_vis.api.core_types")
        for core_type in cls.instances():
            try:
                core_type.register_with_terms()
            except NotImplementedError:
                core_type.logger.warning("Could not register harmonizer.")

    @property
    def model(self) -> "db_.Model":
        raise NotImplementedError

    def register_harmonize_raw_data_task(self, task: "HarmonizeRawData") -> None:
        if self.harmonizer:
            plugin_info = File.link(self.name, "plugin_info.json")
            task.required_rids.add(plugin_info)


class CoreTypeField(ma.Str):
    @classmethod
    def core_type(cls) -> CoreType:
        return CoreType.get(cls.core_type_name())

    # FUTURE JSON Query
    # @classmethod
    # def ops(cls) -> Set[Callable[[Any], str]]:
    #    raise NotImplementedError

    @classmethod
    def core_type_name(cls) -> str:
        raise NotImplementedError


def default_field(field: Union[ma.Field, ma.Field]) -> ma.Field:
    if isinstance(field, type) and issubclass(field, FieldABC):
        return field()

    return field


class CoreTypeMeta:
    # pylint: disable=too-many-arguments
    def __init__(
        self,
        db_mixin: Type[Any],
        fields: Mapping[
            str, Union[ma.Field, Type[ma.Field], CoreTypeField, Type[CoreTypeField]]
        ],
        schema: "ma.Schema",
        targets: Optional[Sequence[str]] = None,
        target: Optional[str] = None,
    ) -> None:
        self.db_mixin = db_mixin
        self.fields = fields
        self.schema = schema
        if targets:
            self.targets = targets
        else:
            self.targets = [
                var
                for var, value in vars(db_mixin).items()
                if isinstance(value, declared_attr)
            ]
        # self.targets = list(fields.keys())
        if target is not None:
            self.target = target
        elif len(self.targets) == 1:
            self.target = self.targets[0]
        else:
            raise ValueError("target cannot be 'None'")

    @cached_property
    def default_fields(self) -> Mapping[str, Union[CoreTypeField, ma.Field]]:
        return {name: default_field(field) for name, field in self.fields.items()}
