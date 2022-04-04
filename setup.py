#!/usr/bin/env python3

import site, sys
from setuptools import setup, find_packages

site.ENABLE_USER_SITE = '--user' in sys.argv[1:]

setup(
    name='Grevling',
    version='2.0.0',
    description='A batch runner tool',
    author='Eivind Fonn',
    author_email='eivind.fonn@sintef.no',
    license='AGPL3',
    url='https://github.com/TheBB/Grevling',
    packages=find_packages(),
    package_data={'grevling': ['grevling.gold']},
    python_requires='>=3.7',
    install_requires=[
        'asteval',
        'bidict',
        'cached_property',
        'click',
        'fasteners',
        'goldpy>=1.3.9',
        'jsonschema',
        'jsonpath-ng',
        'mako',
        'numpy',
        'pandas',
        'peewee',
        'pyarrow',
        'pydantic',
        'pyyaml',
        'requests',
        'rich',
        'tqdm',
        'typing-inspect',
        'xdg',
    ],
    extras_require={
        'testing': ['pytest'],
        'deploy': ['twine', 'cibuildwheel==2.0.0'],
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
