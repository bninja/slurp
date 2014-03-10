import re
import setuptools


extras_require = {
    'elasticsearch': ['pyes >=0.90.1,<0.91'],
    'sentry': ['raven'],
    'email': ['Mako >=0.9,<1.0'],
    'stats': ['newrelic >=1.13.1.31'],
    'watch': ['pyinotify ==0.9.3', 'setproctitle ==1.1.6'],
}

tests_require = [
    'nose >=1.1.0',
    'mock ==0.8',
    'unittest2 >=0.5.1',
    'coverage',
] + [v for v in extras_require.itervalues()]

extras_require['test'] = tests_require

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
    author_email='egon@gb.com',
    install_requires=[
        'arrow >=0.4.2,<0.5',
        'pilo ==0.2.1',
    ],
    extras_require=extras_require,
    packages=[
        'slurp',
        'slurp.ext',
    ],
    scripts=[
        'bin/slurp',
    ],
    package_data={'': ['LICENSE']},
    include_package_data=True,
    tests_require=tests_require,
    test_suite='nose.collector',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
    ],
)
