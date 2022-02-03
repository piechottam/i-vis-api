class WrongPlugin(Exception):
    pass


class MissingLatestUrls(Exception):
    pass


class MissingWorkingUpdateError(Exception):
    pass


class PluginError(Exception):
    """Generic plugin exception"""

    def __init__(self, pname: str) -> None:
        Exception.__init__(self)
        self.pname = pname

    def __str__(self) -> str:
        return self.message

    @property
    def message(self) -> str:
        raise NotImplementedError


class LatestUrlRetrievalError(PluginError):
    """URL could not be retrieved exception"""

    @property
    def message(self) -> str:
        return f"Latest url could not be retrieved for plugin: {self.pname}"

    @property
    def msg(self) -> str:
        return "Latest url could not be retrieved"


class MissingPluginModel(PluginError):
    """Plugin hasn't been initialized exception"""

    @property
    def message(self) -> str:
        return f"Missing plugin model for plugin: {self.pname}."

    @property
    def msg(self) -> str:
        return "Missing plugin model."


class MissingPendingVersion(PluginError):
    """Missing pending version exception"""

    @property
    def message(self) -> str:
        return f"There is no pending version for plugin: {self.pname}"


class MissingCurrentVersion(PluginError):
    """Missing current version exception"""

    @property
    def message(self) -> str:
        return f"There is no current version for plugin: {self.pname}"

    @property
    def msg(self) -> str:
        return "There is no current version"


class MissingAnyVersion(PluginError):
    """Missing current or pending version exception"""

    @property
    def message(self) -> str:
        return f"There is no (current or pending) version for plugin: {self.pname}"

    @property
    def msg(self) -> str:
        return "There is no (current or pending) version"


class NewPluginVersion(PluginError):
    """Newer plugin version available exception"""

    @property
    def message(self) -> str:
        return f"Must install newer version of plugin: {self.pname}"


class UnknownPlugin(PluginError):
    """Unknown plugin exception"""

    @property
    def message(self) -> str:
        return f"Unknown plugin: {self.pname}"

    @property
    def msg(self) -> str:

        return "Unknown plugin"


class WrongPluginType(PluginError):
    def __init__(self, pname: str, ptype: str) -> None:
        super().__init__(pname)
        self.ptype = ptype

    @property
    def message(self) -> str:
        return f"Plugin: {self.pname} is not of type: {self.ptype}"

    @property
    def msg(self) -> str:
        return "Wrong plugin type"
