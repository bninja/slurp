from setuptools import setup


__version__ = '0.0.1'


setup(
    name='slurp',
    version=__version__,
    description='Log file slurper',
    author='noone',
    author_email='noone@nowhere.com',
    url='https://github.com/bninja/slurp',
    keywords=[
        'slurp',
        ],
    install_requires=[
        'lockfile==0.9.1',
        'pyinotify==0.9.3',
        'python-daemon==1.6',
        ],
    py_modules=[
        'slurp',
        ],
    scripts=[
        'slurp',
        ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    )
