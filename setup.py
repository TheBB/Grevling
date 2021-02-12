#!/usr/bin/env python3

from distutils.core import setup

setup(
    name='badger',
    version='0.1.0',
    description='A batch runner tool',
    author='Eivind Fonn',
    author_email='eivind.fonn@sintef.no',
    license='AGPL3',
    url='https://github.com/TheBB/Badger',
    packages=['badger'],
    install_requires=[
        'bidict',
        'click',
        'fasteners',
        'mako',
        'numpy',
        'ruamel.yaml',
        'simpleeval',
        'strictyaml',
        'treelog',
    ],
    extras_require={
        'testing': ['pytest'],
        'deploy': ['twine', 'cibuildwheel==1.1.0'],
        'matplotlib': ['matplotlib'],
        'plotly': ['plotly>=4'],
    },
    entry_points={
        'console_scripts': ['badger=badger.__main__:main'],
    },
)
