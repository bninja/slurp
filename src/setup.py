import re
import setuptools

tests_require = [
    'nose==1.1.2',
    'mock==0.8',
    'unitest2 >= 0.5.1',
]

setuptools.setup(
    name='slurp',
    version=(re
        .compile(r".*__version__ = '(.*?)'", re.S)
        .match(open('slurp/__init__.py').read())
        .group(1)
    ),
    description='Slurper',
    long_description=open('README.rst').read(),
    url='https://github.com/bninja/slurp',
    author='slurp',
    author_email='slurp@egon.gb',
    install_requires=[
        'arrow >=0.4.2,<0.5',
    ],
    extras_require={
        'es': 'pyes >=0.90.1,<0.91',
        'sentry': 'raven',
        'newrelic': 'newrelic >=1.13.1.31',
        'watch': ['pyinotify ==0.9.3', 'setproctitle ==1.1.6'],
        'test': tests_require,
    },
    tests_require=tests_require,
    packages=[
        'slurp',
        'slurp.ext',
    ],
    scripts=[
        'bin/slurp',
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
