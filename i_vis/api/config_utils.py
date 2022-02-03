"""Config related."""

from typing import Any, Mapping, MutableMapping, Set, MutableSet
from flask import current_app, has_app_context

from i_vis.core.config import variable_name

_DISABLED = variable_name("DISABLED_PLUGINS")

_CONFIG: MutableMapping[str, Any] = {}


def register_config(config: Mapping[str, Any]) -> None:
    _CONFIG.update(config)


def get_config() -> MutableMapping[str, Any]:
    if has_app_context():
        return current_app.config

    return _CONFIG


def disable_pname(name: str) -> None:
    """disable plugin by name.

    Args:
        name: Plugin name to be disabled.
    """

    pnames = _unpack(_DISABLED)
    if name in pnames:
        pnames.add(name)
        _pack(_DISABLED, pnames)


def disabled_pnames() -> Set[str]:
    """Returns disabled plugin names.

    Returns:
        Set of disabled plugins.
    """

    return set(_unpack(_DISABLED))


def _unpack(var: str) -> MutableSet[str]:
    return set(get_config().setdefault(var, "").split(","))


def _pack(var: str, s: MutableSet[str]) -> None:
    get_config()[var] = ",".join(s)
