=====================
gpsdio vectortile plugin
=====================


.. image:: https://travis-ci.org/SkyTruth/gpsdio-vectortile.svg?branch=master
    :target: https://travis-ci.org/SkyTruth/gpsdio-vectortile


.. image:: https://coveralls.io/repos/SkyTruth/gpsdio-vectortile/badge.svg?branch=master
    :target: https://coveralls.io/r/SkyTruth/gpsdio-vectortile


A CLI plugin for `gpsdio <https://github.com/skytruth/gpdsio/>`_ that generates tilesets suitable for Pelagos Client.


Examples
--------

See ``gpsdio vectortile --help`` for info.

.. code-block:: console

    $ gpsdio vectortile input.msg output_dir \


Installing
----------

Via pip:

.. code-block:: console

    $ pip install gpsdio-vectortile

From master:

.. code-block:: console

    $ git clone https://github.com/SkyTruth/gpsdio-vectortile
    $ cd gpsdio-vectortile
    $ pip install .


Developing
----------

.. code-block::

    $ git clone https://github.com/SkyTruth/gpsdio-vectortile
    $ cd gpsdio-vectortile
    $ virtualenv venv && source venv/bin/activate
    $ pip install -e .[test]
    $ py.test tests --cov gpsdio_vectortile --cov-report term-missing
