#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name='Grevling',
    version='1.2.1',
    description='A batch runner tool',
    author='Eivind Fonn',
    author_email='eivind.fonn@sintef.no',
    license='AGPL3',
    url='https://github.com/TheBB/Grevling',
    packages=find_packages(),
    python_requires='>=3.7',
    install_requires=[
        'asteval',
        'bidict',
        'click',
        'fasteners',
        'goldpy>=0.4.2',
        'mako',
        'numpy',
        'pandas',
        'pyarrow',
        'pydantic',
        'rich',
        'ruamel.yaml',
        'strictyaml>=1.4',
        'typing-inspect',
    ],
    extras_require={
        'testing': ['pytest'],
        'deploy': ['twine', 'cibuildwheel==1.2.1'],
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
