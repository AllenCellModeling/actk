# ack

[![Build Status](https://github.com/AllenCellModeling/ack/workflows/Build%20Master/badge.svg)](https://github.com/AllenCellModeling/ack/actions)
[![Documentation](https://github.com/AllenCellModeling/ack/workflows/Documentation/badge.svg)](https://AllenCellModeling.github.io/ack)
[![Code Coverage](https://codecov.io/gh/AllenCellModeling/ack/branch/master/graph/badge.svg)](https://codecov.io/gh/AllenCellModeling/ack)

Automated Cell Kit

---

## Installation
**Install Requires:** The python package, `numpy`, must be installed prior to the
installation of this package: `pip install numpy`

**Stable Release:** `pip install ack`<br>
**Development Head:** `pip install git+https://github.com/AllenCellModeling/ack.git`

## Development
See [CONTRIBUTING.md](CONTRIBUTING.md) for information related to developing the code.

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
pip install -e .[aics]
python scripts/download_aics_dataset.py
```

Options for this script are available and can be viewed with:
`python scripts/download_aics_dataset.py --help`

***Free software: Allen Institute Software License***
