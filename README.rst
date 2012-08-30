=====
slurp
=====

.. image:: https://secure.travis-ci.org/bninja/slurp.png?branch=dev
    :target: http://travis-ci.org/bninja/slurp

slurp is a block parser. It iterates over "entries" in append only sources
(aka files) and sends them along to a sink for further processing. A source
file is something that:

- is created
- has uniformly delimited strings appended to it
- is then possibly deleted.

If a file does not conform to this lifestyle it is not suitable as a slurp source.

In the slurp world sources are mapped to channels. Channels:

- have once or more sources associates with them
- can tweak source parsing bahvior (e.g. struct, read size, etc)
- has a single sink  
- controls sink throttling (e.g. if a sink fails or takes a long time)

The motivating use-case for slurp is feeding entries streamed to centralized
syslog spool(s) to elastic search and other data mining tools.

Issues
------

Please use tagged github `issues <https://github.com/bninja/slurp/issues>`_ to request features or report bugs.

Dependencies
------------

Required:

- `Python <http://python.org/>`_ >= 2.5, < 3.0
- `pyinotify <https://github.com/seb-m/pyinotify>`_ >= 0.9.3

Optional:

- `lockfile <http://code.google.com/p/pylockfile/>`_  >= 1.9
- `python-daemon <pypi.python.org/pypi/python-daemon/>`_ >= 1.5

Install
-------

Simply::

    $ pip install slurp
    
or if you prefer::
    
    $ easy_install slurp

Usage
-----

Slurp has both programming and command-line interfaces.

To use the programming interface import it and read doc strings::

    $ python
    >>> import slurp

To use the command-line interface run the slurp script::

    $ slurp --help

Another common use case is to run the slurp script as a monitor daemon. See
extras/slurp.init for an example init script.


Configuration
-------------

**TODO**


Examples
--------

**TODO**


Contributing
------------

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Write your code **and tests**
4. Ensure all tests still pass (`nosetests -svx tests`)
5. Commit your changes (`git commit -am 'Add some feature'`)
6. Push to the branch (`git push origin my-new-feature`)
7. Create new pull request
