.. _install-repository:

Install from Repository
=======================

Python Version
--------------

I-VIS requires Python 3.9+.

Dependencies
------------

* `i_vis_core`_ provides general functionality.
* see `requirements.txt` for details

We strongly recommend to use virtual environments to manage python and package dependencies

Requirements
------------

I-VIS requires a MySQL database server. Installation and setup of a MySQL database server is beyond the scope of this
manual.
For details, check 'Installing and upgrading MySQL <https://dev.mysql.com/doc/refman/8.0/en/installing.html>`_.

Create I-VIS database
~~~~~~~~~~~~~~~~~~~~~

Use your favorite User interface to connecto MySQL server and create a database that will be used by I-VIS, e.g.:
`i_vis`.

Connect to your MySQL server by replacing the placeholders with your values:
.. code-block:: console

    $ mysql -h <HOST>-u <USER> -P<PASSWORD>

If everything worked, a simple SQL shell should opened.

.. note: Make sure you have the proper permission to create and modify databases.

Create a new I-VIS database with the following command:
.. code-block:: console

    mysql> CREATE DATABASE <DATABASE-NAME>;

TODO adjust config of SQLALCHEMY

.. note: Database setup is *only* required for `install-repository'_ - we strongly recommend to install `install-docker`_.


Install
~~~~~~~

Clone the `i_vis_api`_:
.. code-block:: console

   $ git clone `i_vis_api`_

We recommend to use a virtual environment to manage package dependencies.
Use :mod:`virtualenc` module to create virtual environments.
.. code-block:: console

   $ cd i_vis_api
   $ virtualenv venv
   $ pip install i_vis_api

I-VIS API is now installed. But :ref:`config` is required.
