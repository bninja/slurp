[![Build Status](https://travis-ci.org/bninja/slurp.png?branch=master)](https://travis-ci.org/bninja/slurp)

Installing
==========

From [PyPI](https://pypi.python.org/pypi/):

```bash
$ sudo pip install slurp[watch,stats,elasticsearch]
```

which will drop all the code, but for system integration use a package:

```bash
$ sudo apt-get install slurp
```

or

```bash
$ sudo yum install slurp
```

Testing
=======

Get the code:

```bash
$ git clone git@github.com:bninja/slurp.git ~/code/slurp
```
    
Setup environment:

```bash
$ mkvirtualenv slurp
(slurp)$ cd ~/code/slurp
(slurp)$ python setup.py develop
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
$ git checkout tags/v0.6.0
```

PyPI
----

Package **and** publish to [PyPI](https://pypi.python.org/pypi/) like this:

```bash
$ cd src
$ sudo python setup.py sdist upload
```

Debian
------

Get these:

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
$ cd deb
$ mkdir build
$ cp -R ../src/* debian files Makefile build/
$ cd build
$ debuild -uc -us 
```

which, e.g. for `0.6.0`, generates these:
    
```
deb/slurp_0.6.0_amd64.build
deb/slurp_0.6.0_amd64.changes
deb/slurp_0.6.0_amd64.deb
deb/slurp_0.6.0.dsc
deb/slurp_0.6.0.tar.gz
```

Install it like:

```bash
$ sudo dpkg -i deb/slurp_0.6.0_amd64.deb
```

and verify the install:

```bash
$ slurp -v
0.6
```
