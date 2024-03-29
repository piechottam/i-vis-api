"""
I-VIS api app factories
=======================

Contains flask :doc:`flask:patterns/appfactories` for server and CLI tools.

Check details at `PREDICT <https://predict.informatik.hu-berlin.de>`_

:author: michael.piechotta@gmail.com
:copyright: 2021, PREDICT
:license: MIT, see LICENSE.txt for more details
"""

import importlib
import logging.config
import os
from logging import getLogger
from pprint import PrettyPrinter
from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple

# noinspection PyUnresolvedReferences
import colorlog  # pylint: disable=unused-import
import yaml
from flask import Flask, jsonify
from flask.typing import ResponseReturnValue
from flask_smorest import Api
from pkg_resources import resource_filename

from i_vis.core.app import create as create_mini_app
from i_vis.core.config import ConfigMeta
from i_vis.core.config import init_app as i_vis_init_app
from i_vis.core.config import variable_name

# noinspection PyUnresolvedReferences
from i_vis.core.db import (  # pylint: disable=unused-import
    Base,
    engine,
    metadata,
    session,
)
from i_vis.core.file_utils import create_dir
from i_vis.core.login import login
from i_vis.core.ma import ma
from i_vis.core.version import Default as DefaultVersion

from .__version__ import __MAJOR__, __MINOR__, __PATCH__, __SUFFIX__

#  from werkzeug.exceptions import HTTPException


if TYPE_CHECKING:
    from .plugin import CoreType, DataSource

pp = PrettyPrinter(indent=4)

VERSION = DefaultVersion(
    major=__MAJOR__, minor=__MINOR__, patch=__PATCH__, suffix=__SUFFIX__
)

config_meta = ConfigMeta()

# Create an APISpec -> swagger and redoc
api_spec = Api(
    spec_kwargs={
        "title": "Integrated Variant Information System (I-VIS)",
        "version": "v0",  # change API version here
        "openapi_version": "3.0.2",
        "openapi_url_prefix": "/",
        "openapi_json_path": "api-spec.json",
        "openapi_redoc_path": "/",
        "openapi_redoc_url": "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js",
    },
)


def _create_default_app(config: Optional[type] = None) -> Flask:
    """Create an :class:`Flask` app object.

    Read configuration form :class:`config` or from a file identified by :envvar:`I_VIS_CONF` and init Flask plugins.

    Args:
        config: (optional) config for the new :class:`Flask` app object.

    Returns:
       :class:`Flask <Flask>` app object

    Raises:
        MissingConfig if :envvar:`I_VIS_CONF` and :class:`config` are not set.
    """

    app = create_mini_app(__name__, config)
    # OPENAPI specific
    app.config.setdefault("OPENAPI_URL_PREFIX", "/api")
    app.config.setdefault("OPENAPI_JSON_PATH", "spec.json")
    app.config.setdefault("OPENAPI_SWAGGER_UI_PATH", "/swagger")
    app.config.setdefault(
        "OPENAPI_SWAGGER_UI_URL",
        "https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/3.19.5/",
    )
    app.config.setdefault("OPENAPI_REDOC_PATH", "/redoc")
    app.config.setdefault(
        "OPENAPI_REDOC_URL",
        "https://cdn.jsdelivr.net/npm/redoc@next/bundles/redoc.standalone.js",
    )

    # when NOT testing ignore warnings from other packages
    if app.config.get("TESTING", False):
        import warnings

        # ignore warnings from combining tqdm and pandas
        warnings.filterwarnings("ignore", category=FutureWarning)

    # set default for secure cookies...
    app.config.update(
        SECRET_KEY=os.urandom(24),
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_NAME=app.config.get("SESSION_COOKIE_NAME", "PREDICT-I-VIS"),
        WTF_CSRF_TIME_LIMIT=None,
    )

    # register required variables
    config_meta.register_core_variable("DATA_DIR")
    config_meta.register_core_variable("LOG_DIR")

    # load default (logging.yaml) or
    # custom logging(I_VIS_LOGGING_CONF) configuration
    logging_conf = app.config.get(
        variable_name("LOGGING_CONF"), resource_filename(__name__, "logging.yaml")
    )
    with open(logging_conf, encoding="utf8") as file:
        dict_config = yaml.safe_load(file.read())
        logging.config.dictConfig(dict_config)

    # init flask plugins
    setattr(app, "session", session)
    login.init_app(app)
    ma.init_app(app)
    api_spec.init_app(app)
    i_vis_init_app(app)

    @app.teardown_appcontext
    def remove_session(*_args: Any, **_kwargs: Any) -> None:
        getattr(app, "session").remove()

    return app


def _init_plugins(
    app: Flask,
) -> Tuple[Sequence["CoreType"], Sequence["DataSource"]]:
    """Initialize plugins.

    Args:
        app: class:`Flask` app object

    """
    from . import core_types, data_sources  # pylint: disable=unused-import
    from .plugin import BasePlugin, CoreType, DataSource

    # register core and plugins - order important!
    CoreType.import_plugins()
    DataSource.import_plugins()

    # create default directories
    create_dir(app.config[variable_name("DATA_DIR")])
    create_dir(app.config[variable_name("LOG_DIR")])

    config_meta.set_defaults()

    # verify that all required variables are set
    config_meta.check_core_config()
    config_meta.check_plugin_config(BasePlugin.pnames())

    for core_type in CoreType.instances():
        core_type.register_tasks()

    return CoreType.instances(), DataSource.instances()


def _init_blueprints(app: Flask, core_types: Sequence["CoreType"]) -> None:
    """Init blueprints."""

    # remove not needed default flask_smorest schemas
    # for schema in ():
    #    if schema in api_spec.spec.components.schemas:
    #        del api_spec.spec.components.schemas[schema]

    # add core type services
    for core_type in core_types:
        path = ".".join([core_type.path, "routes"])
        try:
            module = importlib.import_module(path)
        except ModuleNotFoundError:
            continue
        blp = getattr(module, "blp", None)
        getattr(module, "register")()
        if blp:
            api_spec.register_blueprint(blp)

    from .routes.api import api_blp, ds_bp, list_parts

    api_spec.register_blueprint(api_blp)

    app.register_blueprint(ds_bp)
    list_parts()

    # remove not needed intermediate i-vis schemas
    # for schema in ():
    #    if schema in api_spec.spec.components.schemas:
    #        del api_spec.spec.components.schemas[schema]


def create_cli_app(config: Optional[type] = None) -> Flask:
    """Create CLI :class:`Flask` app.

    Args:
        config: (optional) config for the new :class:`Flask` app object.

    Returns:
        :class:`Flask <Flask>` app object
    """

    app = _create_default_app(config)

    with app.app_context():
        core_types, _ = _init_plugins(app)
        metadata.create_all(engine)

        from .cli import groups as cli_groups

        # register CLI commands
        for cli_group in cli_groups:
            app.cli.add_command(cli_group)

        _init_blueprints(app, core_types)

    return app


def create_api_app(config: Optional[type] = None) -> Flask:
    """Create API :class:`Flask` app.

    Args:
        config: (optional) config for the new :class:`Flask` app object.

    Returns:
        :class:`Flask <Flask>` app object
    """

    app = _create_default_app(config)

    with app.app_context():
        core_types, data_sources = _init_plugins(app)
        _init_blueprints(app, core_types)

        from .plugin import register_tasks

        register_tasks(data_sources)

        # Return validation errors as JSON
        # app.register_error_handler(422, handle_error)
        # app.register_error_handler(400, handle_error)

    return app


def create_mock_app(config: Optional[type] = None) -> Flask:
    """Create MOCK :class:`Flask` app for server maintenance.

    Args:
        config: (optional) config for the new :class:`Flask` app object.

    Returns:
        :class:`Flask <Flask>` app object
    """
    app = _create_default_app(config)

    with app.app_context():

        def handle_404(error: Exception) -> ResponseReturnValue:
            getLogger().exception(error)

            msg = "The server is currently unable to handle the request due to maintenance of the server."
            return jsonify({"error": msg}), 503

    app.register_error_handler(404, handle_404)

    return app


# def handle_error(error: HTTPException) -> ResponseReturnValue:
#    if isinstance(error, HTTPException):
#        description = error.description or ["Invalid request."]
#        code = error.code
#        headers = error.get_headers()
#        return jsonify({"error": description}), code, headers
#
#    description = "Invalid request."
#    code = 503
#    return jsonify({"error": description}), code
