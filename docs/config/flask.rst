.. config-other:

Flask
=====

The default configuration of I-VIS has already some flask specific variables set.

.. seealso:: For more details check :doc:`flask:config`.

.. config-sqlalchemy:

SQLALCHEMY
~~~~~~~~~~

The most important configuration key for the database is `SQLALCHEMY_DATABASE_URI`.

The format is self explanatory - replace the placeholders according to your environment:

    SQLALCHEMY = <driver>://<username>:<password>@<db-host>:<db-port>/<database>

.. seealso:: For more details check :doc:`flask-sqlalchemy:config`.