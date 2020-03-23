# ack

[![Build Status](https://github.com/AllenCellModeling/ack/workflows/Build%20Master/badge.svg)](https://github.com/AllenCellModeling/ack/actions)
[![Documentation](https://github.com/AllenCellModeling/ack/workflows/Documentation/badge.svg)](https://AllenCellModeling.github.io/ack)
[![Code Coverage](https://codecov.io/gh/AllenCellModeling/ack/branch/master/graph/badge.svg)](https://codecov.io/gh/AllenCellModeling/ack)

Automated Cell Kit

---

A pipeline and individual steps to process field-of-view (FOV) microscopy images and
generate data for the cells in each field. Of note, the data produced by this pipeline
is used for the [Cell Feature Explorer](https://cfe.allencell.org/).

## Features
All steps and functionality in this package can be run as single steps or all together
by using the command line.

In general, all commands for this package will follow the format: `ack {step} {command}`

* `step` is the name of the step, such as "StandardizeFOVArray" or "SingleCellFeatures"
* `command` is what you want that step to do, such as "run" or "push"

### Pipeline
To run the entire pipeline from start to finish you can simply run:

```bash
ack all run --dataset {path to dataset}
```

Step specific parameters can additionally be passed by simply appending them.
The step `SingleCellFeatures` has a parameter for `cell_ceiling_adjustment` and this
can be set on both the individual step run level but also for the entire pipeline with:

```bash
ack all run --dataset {path to dataset} --cell_celing_adjustment {(float,float,float)}
```

### Individual Steps
* `ack standardizefovarray run --dataset {path to dataset}`, Generate standardized,
ordered, and normalized FOV images
* `ack singlecellfeatures run --dataset {path to dataset}`, Generate a features JSON
file for each cell in the dataset

## Installation
**Install Requires:** The python package, `numpy`, must be installed prior to the
installation of this package: `pip install numpy`

**Stable Release:** `pip install ack`<br>
**Development Head:** `pip install git+https://github.com/AllenCellModeling/ack.git`

## Development
See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.

For more details on how this pipeline is constructed please see
[dataset](https://github.com/AllenCellModeling/datastep) and
[cookiecutter-stepworkflow](https://github.com/AllenCellModeling/cookiecutter-stepworkflow).

To add new steps to this pipeline, run `make_new_step` and follow the instructions in
[CONTRIBUTING.md](CONTRIBUTING.md)

### Developer Installation
The following two commands will install the package with dev dependencies in editable
mode and download all resources required for testing.

```bash
pip install -e .[dev]
python scripts/download_test_data.py
```

### AICS Developer Instructions
If you want to run this pipeline with the Pipeline Integrated Cell dataset
(`pipeline 4.*`) run the following commands:

```bash
pip install -e .[all]
python scripts/download_aics_dataset.py
```

Options for this script are available and can be viewed with:
`python scripts/download_aics_dataset.py --help`

***Free software: Allen Institute Software License***
