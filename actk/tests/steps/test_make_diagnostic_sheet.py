#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import dask.dataframe as dd

from actk.constants import DatasetFields
from actk.steps import MakeDiagnosticSheet

#######################################################################################


def test_run(data_dir):
    # Initialize step
    step = MakeDiagnosticSheet()

    # Ensure that it still runs
    output_manifest = step.run(
        data_dir / "example_single_cell_images_dataset.csv",
        group_by="FOVId",
        feature_display="imsize_orig",
        overwrite=True,
    )
    output_manifest = dd.read_csv(output_manifest)

    # Read input dataset
    input_dataset = dd.read_csv(data_dir / "example_single_cell_images_dataset.csv")

    # Run asserts
    # Check expected columns
    assert all(
        expected_col in output_manifest.columns
        for expected_col in [*input_dataset.columns, DatasetFields.DiagnosticSheetPath]
    )
    # Check output length
    assert len(output_manifest) == len(input_dataset)
    # Check all expected files exist
    assert all(
        Path(f).resolve(strict=True)
        for f in output_manifest[DatasetFields.DiagnosticSheetPath]
    )
