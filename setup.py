import re

try:
    import setuptools
except ImportError:
    import distutils.core
    setup = distutils.core.setup
else:
    setup = setuptools.setup


setup(
    name='slurp',
    version=(re
        .compile(r".*__version__ = '(.*?)'", re.S)
        .match(open('slurp.py').read())
        .group(1)),
    description='Log file slurper',
    long_description=(
        open('README.rst').read() + '\n\n' +
        open('HISTORY.rst').read()
        ),
    url='https://github.com/bninja/slurp',
    author='slurp',
    author_email='slurp@tbd.com',
    install_requires=[
        'lockfile==0.9.1',
        'pyinotify==0.9.3',
        'python-daemon==1.6',
        'setproctitle==1.1.6',
        ],
    extras_require={
        },
    tests_require=[
        'nose==1.1.2',
        'mock==0.8',
        ],
    py_modules=[
        'slurp',
        ],
    scripts=[
        'slurp',
        ],
    package_data={'': ['LICENSE']},
    include_package_data=True,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        ],
    )
