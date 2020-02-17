#!/usr/bin/env python
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("VERSION") as version_file:
    version = version_file.read().strip()

requires = [
    'cerberus==1.2',
    'click==7.0',
    'click-default-group==1.2.1',
    'pyyaml==3.13',
    'snowflake-connector-python==1.6.10',
    'snowflake-sqlalchemy==1.1.2',
    'sqlalchemy==1.2.12',
]

dev_requires = [
    'black==18.9b0',
    'bumpversion==0.5.3',
    'changelog-cli==0.6.2',
    'coverage==4.5.4',
    'pytest==4.3.1',
    'pytest-cov==2.6.1',
]

setup(
    name="meltano-permissions",
    version=version,
    author='Meltano Team & Contributors',
    author_email="meltano@gitlab.com",
    description="Meltano Permissions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://gitlab.com/meltano/meltano-permissions",
    package_dir={'': 'src'},
    packages=find_packages(where="src"),
    include_package_data=True,
    py_modules=['meltano_permissions'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    setup_requires=['pytest-runner'],
    tests_require=dev_requires,
    # run `make requirements.txt` after editing
    install_requires=requires,
    extras_require={
        'dev': dev_requires,
    },
    entry_points={
        'console_scripts': [
            "meltano_permissions = meltano_permissions.cli:main"
        ]
    }
)
