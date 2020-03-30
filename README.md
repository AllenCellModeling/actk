# actk

[![Build Status](https://github.com/AllenCellModeling/actk/workflows/Build%20Master/badge.svg)](https://github.com/AllenCellModeling/actk/actions)
[![Documentation](https://github.com/AllenCellModeling/actk/workflows/Documentation/badge.svg)](https://AllenCellModeling.github.io/actk)
[![Code Coverage](https://codecov.io/gh/AllenCellModeling/actk/branch/master/graph/badge.svg)](https://codecov.io/gh/AllenCellModeling/actk)

Automated Cell Toolkit

---

A pipeline to process field-of-view (FOV) microscopy images and generate data and
render-ready products for the cells in each field. Of note, the data produced by this
pipeline is used for the [Cell Feature Explorer](https://cfe.allencell.org/).

## Features
All steps and functionality in this package can be run as single steps or all together
by using the command line.

In general, all commands for this package will follow the format:
`actk {step} {command}`

* `step` is the name of the step, such as "StandardizeFOVArray" or "SingleCellFeatures"
* `command` is what you want that step to do, such as "run" or "push"

Each step will check that the dataset provided contains the required fields prior to
processing. For details and definitions on each field, see our
[dataset fields documentation](https://AllenCellModeling.github.io/actk/dataset_fields.html).

An example dataset can be seen [here](https://open.quiltdata.com/b/aics-modeling-packages-test-resources/tree/actk/test_data/data/example_dataset.csv).

### Pipeline
To run the entire pipeline from start to finish you can simply run:

```bash
actk all run --dataset {path to dataset}
```

Step specific parameters can additionally be passed by simply appending them.
For example: the step `SingleCellFeatures` has a parameter for
`cell_ceiling_adjustment` and this can be set on both the individual step run level and
also for the entire pipeline with:

```bash
actk all run --dataset {path to dataset} --cell_ceiling_adjustment {integer}
```

### Individual Steps
* `actk standardizefovarray run --dataset {path to dataset}`, Generate standardized,
ordered, and normalized FOV images as OME-Tiffs.
* `actk singlecellfeatures run --dataset {path to dataset}`, Generate a features JSON
file for each cell in the dataset.

## Installation
**Install Requires:** The python package, `numpy`, must be installed prior to the
installation of this package: `pip install numpy`

**Stable Release:** `pip install actk`<br>
**Development Head:** `pip install git+https://github.com/AllenCellModeling/actk.git`

## Documentation
For full package documentation please visit
[allencellmodeling.github.io/actk](https://allencellmodeling.github.io/actk/index.html).

## Development
See
[CONTRIBUTING.md](https://github.com/AllenCellModeling/actk/blob/master/CONTRIBUTING.md)
for information related to developing the code.

For more details on how this pipeline is constructed please see
[cookiecutter-stepworkflow](https://github.com/AllenCellModeling/cookiecutter-stepworkflow)
and [datastep](https://github.com/AllenCellModeling/datastep).

To add new steps to this pipeline, run `make_new_step` and follow the instructions in
[CONTRIBUTING.md](https://github.com/AllenCellModeling/actk/blob/master/CONTRIBUTING.md)

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
