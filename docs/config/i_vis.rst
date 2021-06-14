.. _config-i-vis:

I-VIS Config
============

I-VIS plugins may add further configuration keys.
Some plugins require a username and password to access data.

.. seealso:: Check _`Plugins list` for further configuration keys.

If you are interested in all plugin specific configuration keys for an `i-vis-api` instance check CLI
:ref:`cli-plugin-variables`.

.. _config-general:

General
-------
The following configuration values are used internally:

.. py:data:: I_VIS_DATA_DIR

    The directory where `i-vis-app` will store files.
    This includes downloaded and temporary files.
    Plugins will store their data in a subdirectory of :data:`I_VIS_DATA_DIR`.

    **Make sure there is enough free space available.**

.. py:data:: I_VIS_LOD_DIR

    The directory where log file we stored.

.. py:data:: I_VIS_PLUGINS_IGNORE::

    A ","-separated lists of plugins to ignore.
    Those plugins will be ignore during `Plugin Update <cli-plugin-update>`_ and `Plugin upgrade <cli-plugin-upgrade>`_
    and their respective API routes won't be available.

    **Typically, `i-vis-app` must be restarted.**
