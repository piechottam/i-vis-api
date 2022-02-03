"""CLI groups.
"""

import sys
from click.core import BaseCommand

from .file import app_group as file_cli
from .harmonization import app_group as harm_cli
from .plugin import app_group as plugin_cli
from .reset import app_group as reset_cli
from .task import app_group as task_cli
from .user import app_group as user_cli


groups = [
    getattr(sys.modules[__name__], name)
    for name in dir()
    if isinstance(getattr(sys.modules[__name__], name), BaseCommand)
]
