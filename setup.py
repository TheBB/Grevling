#!/usr/bin/env python3

from distutils.core import setup

setup(
    name='Grevling',
    version='1.0.2',
    description='A batch runner tool',
    author='Eivind Fonn',
    author_email='eivind.fonn@sintef.no',
    license='AGPL3',
    url='https://github.com/TheBB/Grevling',
    packages=['grevling'],
    install_requires=[
        'bidict',
        'click',
        'fasteners',
        'mako',
        'multiprocessing-logging',
        'numpy',
        'pandas',
        'pyarrow',
        'rich',
        'ruamel.yaml',
        'simpleeval',
        'strictyaml',
        'typing-inspect',
    ],
    extras_require={
        'testing': ['pytest'],
        'deploy': ['twine', 'cibuildwheel==1.1.0'],
        'matplotlib': ['matplotlib'],
        'plotly': ['plotly>=4'],
    },
    entry_points={
        'console_scripts': [
            'badger=grevling.__main__:main',
            'grevling=grevling.__main__:main',
        ],
    },
)
