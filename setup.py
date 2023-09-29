#!/usr/bin/env python3

import site, sys
from setuptools import setup, find_packages

site.ENABLE_USER_SITE = '--user' in sys.argv[1:]

setup(
    name='Grevling',
    version='2.0.2',
    description='A batch runner tool',
    author='Eivind Fonn',
    author_email='eivind.fonn@sintef.no',
    license='AGPL3',
    url='https://github.com/TheBB/Grevling',
    packages=find_packages(),
    package_data={'grevling': ['grevling.gold']},
    python_requires='>=3.8',
    install_requires=[
        'asteval',
        'bidict',
        'click',
        'fasteners',
        'goldpy>=2.1',
        'mako>=1.2',
        'numpy',
        'pandas',
        'pyarrow',
        'pydantic<2',
        'pyyaml',
        'rich',
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
