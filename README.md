Testing
=======

Get the code:

```bash
$ git clone git@github.com:bninja/slurp.git
```
    
Setup environment:

```bash
$ mkvirtualenv slurp
(slurp)$ cd slurp/src
(slurp)$ python setup.py test
```
    
And run the tests:

```bash
(slurp)$ nosetests --with-coverage --cover-package=slurp
```

Packaging
=========

You'll probably want to package off a particular tag, so:

```bash
$ cd ~/code/slurp
$ git checkout tags/v0.1.0
```

PyPI
----

Package **and** publish to [PyPI](https://pypi.python.org/pypi/) like this:

```bash
$ cd src
$ python setup.py sdist upload
```

Debian
------

Get these these:

- [bdist-venv](https://github.com/bninja/bdist-venv2)
- build-essential
- devsripts
- debhelper

Make sure you are **not** in a virtualenv:

```bash
(slurp)$ deactivate
$ 
```

and then:

```bash
$ cd packages/deb
$ mkdir build
$ cp -R ../../src/* debian files Makefile build/
$ cd build
$ debuild -uc -us 
```

which generates e.g. these:
    
```bash
$ ls .
package/deb/slurp_0.1.0_amd64.build
package/deb/slurp_0.1.0_amd64.changes
package/deb/slurp_0.1.0_amd64.deb
package/deb/slurp_0.1.0.dsc
package/deb/slurp_0.1.0.tar.gz
```
