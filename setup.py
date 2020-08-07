#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

setup_requirements = [
    "pytest-runner>=5.2",
]

test_requirements = [
    "black>=19.10b0",
    "codecov>=2.1.4",
    "flake8>=3.8.3",
    "flake8-debugger>=3.2.1",
    "pytest>=5.4.3",
    "pytest-cov>=2.9.0",
    "pytest-raises>=0.11",
    "quilt3>=3.1.10",
]

dev_requirements = [
    *setup_requirements,
    *test_requirements,
    "bumpversion>=0.6.0",
    "coverage>=5.1",
    "ipython>=7.15.0",
    "m2r>=0.2.1",
    "Sphinx>=2.0.0b1,<3",
    "sphinx_rtd_theme>=0.4.3",
    "tox>=3.15.2",
    "twine>=3.1.1",
    "wheel>=0.34.2",
]

step_workflow_requirements = [
    "aics_dask_utils==0.2.0",
    "bokeh>=2.1.0",
    "dask[bag]>=2.19.0",
    "dask_jobqueue>=0.7.0",
    "datastep>=0.1.6",
    "distributed>=2.19.0",
    "docutils==0.15.2",  # needed for botocore (quilt dependency)
    "fire",
    "prefect>=0.12.1",
    "psutil",
    "python-dateutil<=2.8.0",  # need <=2.8.0 for quilt3 in step
]

requirements = [
    *step_workflow_requirements,
    # project requires
    "aicsfeature==0.2.1",
    "aicsimageio==3.2.3",
    "aicsimageprocessing==0.7.4",
    "numpy==1.18.2",
    "pandas==1.0.3",
]

aics_data_requirements = [
    "lkaccess",
]

extra_requirements = {
    "setup": setup_requirements,
    "test": test_requirements,
    "dev": dev_requirements,
    "aics": aics_data_requirements,
    "all": [
        *requirements,
        *dev_requirements,
    ]
}

setup(
    author="Jackson Maxfield Brown",
    author_email="jacksonb@alleninstitute.org",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: Free for non-commercial use",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    description="Automated Cell Toolkit",
    entry_points={"console_scripts": ["actk=actk.bin.cli:cli"]},
    install_requires=requirements,
    license="Allen Institute Software License",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="actk, computational biology, workflow, cell, microscopy",
    name="actk",
    packages=find_packages(exclude=["tests", "*.tests", "*.tests.*"]),
    python_requires=">=3.6",
    setup_requires=setup_requirements,
    test_suite="actk/tests",
    tests_require=test_requirements,
    extras_require=extra_requirements,
    url="https://github.com/AllenCellModeling/actk",
    # Do not edit this string manually, always use bumpversion
    # Details in CONTRIBUTING.rst
    version="0.1.0",
    zip_safe=False,
)
