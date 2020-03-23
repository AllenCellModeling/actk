#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import dask.dataframe as dd

from ack.constants import DatasetFields
from ack.steps import StandardizeFOVArray

#######################################################################################


def test_run(data_dir):
    # Initialize step
    step = StandardizeFOVArray()

    # Ensure that it still runs
    output_manifest = step.run(data_dir / "example_dataset.csv")
    output_manifest = dd.read_csv(output_manifest)

    # Read input dataset
    input_dataset = dd.read_csv(data_dir / "example_dataset.csv")

    # Run asserts
    # Check expected columns
    assert all(
        expected_col in output_manifest.columns for expected_col in [
            *input_dataset.columns,
            DatasetFields.StandardizedFOVPath,
        ]
    )
    # Check output length
    assert len(output_manifest) == len(input_dataset)
    # Check all expected files exist
    assert all(
        Path(f).resolve(strict=True) for f in output_manifest[
            DatasetFields.StandardizedFOVPath
        ]
    )
