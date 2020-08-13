#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path

import dask.dataframe as dd

from actk.constants import DatasetFields
from actk.steps import MakeDiagnosticSheet
import ast

#######################################################################################


def test_run(data_dir):
    # Initialize step
    step = MakeDiagnosticSheet()

    # Ensure that it still runs
    output_manifest = step.run(
        data_dir / "example_single_cell_images_dataset.csv",
        metadata="FOVId",
        feature="imsize_orig",
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


def test_catch_no_all_proj_image_path(data_dir):

    # Initialize step
    step = MakeDiagnosticSheet()

    output_manifest = step.run(
        data_dir / "example_single_cell_features_dataset.csv", overwrite=True
    )

    output_manifest = dd.read_csv(output_manifest)

    # Read input dataset
    input_dataset = dd.read_csv(data_dir / "example_single_cell_features_dataset.csv")

    # Run asserts
    # Check expected columns
    assert all(
        expected_col in output_manifest.columns
        for expected_col in [*input_dataset.columns]
    )
    # Check output length
    assert len(output_manifest) == len(input_dataset)

    # Check that diagnostic sheet path doesnt exist
    assert [DatasetFields.DiagnosticSheetPath not in output_manifest.columns]


def test_max_num_cells_per_sheet(data_dir):

    # Initialize step
    step = MakeDiagnosticSheet()

    # Ensure that it still runs
    output_manifest = step.run(
        data_dir / "example_single_cell_images_dataset.csv",
        max_cells=2,
        metadata="FOVId",
        feature="imsize_orig",
        overwrite=True,
    )

    output_manifest = dd.read_csv(output_manifest)

    # Read input dataset
    input_dataset = dd.read_csv(data_dir / "example_single_cell_images_dataset.csv")

    # Run asserts
    # Check expected columns
    assert all(
        expected_col in output_manifest.columns
        for expected_col in [*input_dataset.columns]
    )
    # Check output length
    assert len(output_manifest) == len(input_dataset)

    # Check all expected files exist
    assert all(
        Path(f).resolve(strict=True)
        for f in output_manifest[DatasetFields.DiagnosticSheetPath]
    )


def test_multiple_metadata_and_fig_size(data_dir):

    # Initialize step
    step = MakeDiagnosticSheet()

    # Ensure that it still runs
    output_manifest = step.run(
        data_dir / "example_single_cell_images_dataset.csv",
        max_cells=2,
        metadata=["FOVId", "ChannelIndexDNA"],
        feature="imsize_orig",
        overwrite=True,
        fig_width=27,
        fig_height=27,
    )

    output_manifest = dd.read_csv(output_manifest)

    # Read input dataset
    input_dataset = dd.read_csv(data_dir / "example_single_cell_images_dataset.csv")

    # Run asserts
    # Check expected columns
    assert all(
        expected_col in output_manifest.columns
        for expected_col in [*input_dataset.columns]
    )
    # Check output length
    assert len(output_manifest) == len(input_dataset)

    # Check all expected files exist
    for f in output_manifest[DatasetFields.DiagnosticSheetPath]:
        assert all(Path(ff).resolve(strict=True) for ff in ast.literal_eval(f))
