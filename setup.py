#!/usr/bin/env python

from typing import Any, Dict
import os

from setuptools import setup, find_namespace_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "requirements.txt"), "r") as f:
    requires = f.read().splitlines()

with open(os.path.join(here, "test-requirements.txt"), "r") as f:
    test_require = f.read().splitlines()

version: Dict[str, Any] = {}
with open(os.path.join(here, "i_vis", "api", "__version__.py"), "r") as f:
    # pylint: disable=exec-used
    exec(f.read(), version)

setup(
    version=version["__VERSION__"],
    packages=find_namespace_packages(include=["i_vis.api.*"]),
    package_data={"": ["LICENSE.txt"], "i_vis.api": ["py.typed"]},
    install_requires=requires,
    tests_require=test_require,
)
