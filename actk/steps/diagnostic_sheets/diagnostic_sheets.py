#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import List, NamedTuple, Optional, Union

import aicsimageio
import dask.dataframe as dd
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from aics_dask_utils import DistributedHandler
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils
from ..single_cell_images import SingleCellImages

plt.style.use("dark_background")

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.CellId,
    DatasetFields.CellImage2DAllProjectionsPath,
]


class DiagnosticSheetResult(NamedTuple):
    cell_id: Union[int, str]
    save_path: Optional[Path] = None


class DiagnosticSheetError(NamedTuple):
    cell_id: Union[int, str]
    error: str


###############################################################################


class DiagnosticSheets(Step):
    def __init__(
        self,
        direct_upstream_tasks: List["Step"] = [SingleCellImages],
        filepath_columns=[DatasetFields.DiagnosticSheetPath],
        **kwargs,
    ):
        super().__init__(
            direct_upstream_tasks=direct_upstream_tasks,
            filepath_columns=filepath_columns,
            **kwargs,
        )

    @staticmethod
    def _save_plot(
        dataset: pd.DataFrame,
        metadata: str,
        metadata_value: str,
        number_of_subplots: int,
        feature: Optional[str] = None,
        fig_width: Optional[int] = None,
        fig_height: Optional[int] = None,
    ):

        log.info(f"Beginning diagnostic sheet generation for {metadata_value}")

        # Choose columns and rows
        columns = int(np.sqrt(number_of_subplots) + 0.5)
        rows = columns + 1

        # Set figure size
        if not fig_width:
            fig_width = columns * 7
        if not fig_height:
            fig_height = rows * 5

        # Set subplots
        fig, ax_array = plt.subplots(
            rows,
            columns,
            squeeze=False,
            figsize=(fig_height, fig_width),
        )

        for row_index, row in dataset.iterrows():
            this_axes = ax_array.flatten()[row_index]

            # Load feature to plot if feature
            if feature:
                with open(row[DatasetFields.CellFeaturesPath]) as f:
                    cell_features = json.load(f)
                title = "CellId: {0}, {1} {2}: {3}".format(
                    row[DatasetFields.CellId],
                    "\n",
                    feature,
                    cell_features[feature],
                )
                this_axes.set_title(title)
            else:
                this_axes.set_title(f"CellID: {row[DatasetFields.CellId]}")

            # Read AllProjections Image
            img = mpimg.imread(row[DatasetFields.CellImage2DAllProjectionsPath])
            this_axes.imshow(img)
            this_axes.set_aspect(1)

        # Need to do this outside the loop because sometimes number
        # of rows < number of axes subplots
        [ax.axis("off") for ax in ax_array.flatten()]

        # Save figure
        ax_array.flatten()[0].get_figure().savefig(
            dataset[DatasetFields.DiagnosticSheetPath + str(metadata)][0]
        )

        # Close figure, otherwise clogs memory
        plt.close(fig)
        log.info(f"Completed diagnostic sheet generation for" f"{metadata_value}")

    @staticmethod
    def _collect_group(
        row_index: int,
        row: pd.Series,
        diagnostic_sheet_dir: Path,
        overwrite: bool,
        metadata: str,
        max_cells: int,
    ) -> Union[DiagnosticSheetResult, DiagnosticSheetError]:
        # Don't use dask for image reading
        aicsimageio.use_dask(False)

        try:
            # Get the ultimate end save paths for grouped plot
            if row[str(metadata)] or row[str(metadata)] == 0:
                assert DatasetFields.CellImage2DAllProjectionsPath in row.index
                save_path_index = int(
                    np.ceil((row["SubplotNumber" + str(metadata)] + 1) / max_cells)
                )
                # np ceil for 0 = 0
                if save_path_index == 0:
                    save_path_index = 1

                # Clean metadata name of spaces
                cleaned_metadata_name = str(row[str(metadata)]).replace(" ", "-")
                save_path = (
                    diagnostic_sheet_dir / f"{metadata}"
                    f"_{cleaned_metadata_name}"
                    f"_{save_path_index}.png"
                )

                log.info(
                    f"Collecting diagnostic sheet path for cell ID: {row.CellId}, "
                    f"{metadata}: {row[str(metadata)]}"
                )
            else:
                # else no path to save
                save_path = None

            # Check skip
            if not overwrite and save_path.is_file():
                log.info(
                    f"Skipping diagnostic sheet path for cell ID: {row.CellId}, "
                    f"{metadata}: {row[str(metadata)]}"
                )
                return DiagnosticSheetResult(row.CellId, None)

            # Return ready to save image
            return DiagnosticSheetResult(row.CellId, str(save_path))
        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed to retrieve the CellImage2DAllProjectionsPath"
                f"for cell ID: {row.CellId},"
                f"{metadata} {row[str(metadata)]}"
                f"Error: {e}"
            )
            return DiagnosticSheetError(row.CellId, str(e))

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        max_cells: int = 200,
        metadata: Optional[Union[list, str]] = DatasetFields.FOVId,
        feature: Optional[str] = None,
        fig_width: Optional[int] = None,
        fig_height: Optional[int] = None,
        distributed_executor_address: Optional[str] = None,
        batch_size: Optional[int] = None,
        overwrite: bool = False,
        **kwargs,
    ):
        """
        Provided a dataset of single cell all projection images, generate a diagnostic
        sheet grouped by desired metadata and feature

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The primary cell dataset to use for generating
            diagnistic sheet for a group of cells.

            **Required dataset columns:** *["CellId", "CellImage2DAllProjectionsPath"]*

        max_cells: int
            The maximum number of cells to display on a single diagnostic sheet.
            Deafult: 200

        metadata: Optional[Union[list, str]]
            The metadata to group cells and generate a diagnostic sheet.
            For example, "FOVId" or "["FOVId", "ProteinDisplayName"]"
            Default: "FOVId"

        feature: Optional[str]
            The name of the single cell feature to display. For example, "imsize_orig".

        fig_width: Optional[int]
            Width of the diagnostic sheet figure.

        fig_height: Optional[int]
            Height of the diagnostic sheet figure.

        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
            Default: None

        batch_size: Optional[int]
            An optional batch size to process n features at a time.
            Default: None (Process all at once)

        overwrite: bool
            If this step has already partially or completely run, should it overwrite
            the previous files or not.
            Default: False (Do not overwrite or regenerate files)

        Returns
        -------
        manifest_save_path: Path
            Path to the produced manifest with the DiagnosticSheetPath column added.
        """
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Read dataset
            dataset = pd.read_csv(dataset)

        # Check dataset and manifest have required fields
        dataset_utils.check_required_fields(
            dataset=dataset,
            required_fields=REQUIRED_DATASET_FIELDS,
        )

        # Create save directories
        diagnostic_sheet_dir = self.step_local_staging_dir / "diagnostic_sheets"
        diagnostic_sheet_dir.mkdir(exist_ok=True)

        # Create empty manifest
        manifest = {
            DatasetFields.DiagnosticSheetPath: [],
        }

        # Check for metadata
        if metadata:
            # Make metadata a list
            metadata = metadata if isinstance(metadata, list) else [metadata]

            # Make an empty list of grouped_datasets to collect and
            # then distribute via Dask for plotting
            all_grouped_datasets = []
            all_metadata = []
            all_metadata_values = []
            all_subplot_numbers = []

            # Process each row
            for j, this_metadata in enumerate(metadata):

                # Add some helper columns for subsequent analysis
                helper_dataset = pd.DataFrame()

                for unique_metadata_value in dataset[this_metadata].unique():
                    dataset_subgroup = dataset.loc[
                        dataset[this_metadata] == unique_metadata_value
                    ]
                    # "SubplotNumber" + str(this_metadata) + "/MaxCells" is a new column
                    # which will help iterate through subplots to add to a figure
                    dataset_subgroup.insert(
                        2,
                        "SubplotNumber" + str(this_metadata) + "/MaxCells",
                        dataset_subgroup.groupby(this_metadata)["CellId"].transform(
                            lambda x: ((~x.duplicated()).cumsum() - 1) % max_cells
                        ),
                        True,
                    )

                    # "SubplotNumber" + str(this_metadata) is a new column
                    # which will help in the _collect group method to identify
                    # diagnostic sheet save paths per CellId
                    dataset_subgroup.insert(
                        2,
                        "SubplotNumber" + str(this_metadata),
                        dataset_subgroup.groupby(this_metadata)["CellId"].transform(
                            lambda x: ((~x.duplicated()).cumsum() - 1)
                        ),
                        True,
                    )

                    helper_dataset = helper_dataset.append(dataset_subgroup)

                dataset = helper_dataset
                # Done creating helper columns

                # Create empty diagnostic sheet result dataset and errors
                diagnostic_sheet_result_dataset = []
                errors = []

                with DistributedHandler(distributed_executor_address) as handler:
                    # First, lets collect all the diagnostic sheet save paths
                    # per CellId. These are collected based on this_metadata
                    # and max_cells
                    diagnostic_sheet_result = handler.batched_map(
                        self._collect_group,
                        # Convert dataframe iterrows into two lists of items to iterate
                        # One list will be row index
                        # One list will be the pandas series of every row
                        *zip(*list(dataset.iterrows())),
                        [diagnostic_sheet_dir for i in range(len(dataset))],
                        [overwrite for i in range(len(dataset))],
                        [this_metadata for i in range(len(dataset))],
                        [max_cells for i in range(len(dataset))],
                    )
                    # Generate diagnostic sheet dataset rows
                    for r in diagnostic_sheet_result:
                        if isinstance(r, DiagnosticSheetResult):
                            diagnostic_sheet_result_dataset.append(
                                {
                                    DatasetFields.CellId: r.cell_id,
                                    DatasetFields.DiagnosticSheetPath
                                    + str(this_metadata): r.save_path,
                                }
                            )
                        else:
                            errors.append(
                                {DatasetFields.CellId: r.cell_id, "Error": r.error}
                            )

                    # Convert diagnostic sheet paths rows to dataframe
                    diagnostic_sheet_result_dataset = pd.DataFrame(
                        diagnostic_sheet_result_dataset
                    )

                    # Drop the various diagnostic sheet columns if they already exist
                    # Check at j = 0 because the path will exist at j > 1 if
                    # multiple metadata
                    drop_columns = []
                    if (
                        DatasetFields.DiagnosticSheetPath + str(this_metadata)
                        in dataset.columns
                    ):
                        drop_columns.append(
                            DatasetFields.DiagnosticSheetPath + str(this_metadata)
                        )

                    dataset = dataset.drop(columns=drop_columns)

                    # Update manifest with these paths if there is data
                    if len(diagnostic_sheet_result_dataset) > 0:

                        # Join original dataset to the fov paths
                        dataset = dataset.merge(
                            diagnostic_sheet_result_dataset,
                            on=DatasetFields.CellId,
                        )

                    # Reset index in dataset
                    if j == 0:
                        dataset.dropna().reset_index(inplace=True)

                    # Update manifest with these saved paths
                    this_metadata_paths = dataset[
                        DatasetFields.DiagnosticSheetPath + str(this_metadata)
                    ].unique()

                    for this_path in this_metadata_paths:
                        if this_path not in manifest[DatasetFields.DiagnosticSheetPath]:
                            manifest[DatasetFields.DiagnosticSheetPath].append(
                                this_path
                            )

                    # Save errored cells to JSON
                    with open(
                        self.step_local_staging_dir / "errors.json", "w"
                    ) as write_out:
                        json.dump(errors, write_out)

                    # Group the dataset by this metadata and the saved
                    # diagnostic sheet paths (there can be many different save paths)
                    # per metadata value (if max_cells < number of items of
                    # this_metadata)
                    grouped_dataset = dataset.groupby(
                        [
                            str(this_metadata),
                            DatasetFields.DiagnosticSheetPath + str(this_metadata),
                        ]
                    )["SubplotNumber" + str(this_metadata) + "/MaxCells"]

                    # Get maximum values of the subplot numbers in this
                    # grouped dataset. This will tell us the shape of the figure
                    # to make
                    grouped_max = grouped_dataset.max()

                    # Loop through metadata value and max number of subplots
                    for metadata_value, number_of_subplots in grouped_max.items():

                        # Total num of subplots = subplots + 1
                        number_of_subplots = number_of_subplots + 1

                        # Get this metadata group from the original dataset
                        this_metadata_value_dataset = grouped_dataset.get_group(
                            metadata_value, dataset
                        )

                        # reset index
                        this_metadata_value_dataset.reset_index(inplace=True)

                        # Append to related lists for Dask distributed plotting
                        # of all groups
                        all_grouped_datasets.append(this_metadata_value_dataset)
                        all_metadata.append(this_metadata)
                        all_metadata_values.append(metadata_value)
                        all_subplot_numbers.append(number_of_subplots)

            # Plot each diagnostic sheet
            with DistributedHandler(distributed_executor_address) as handler:
                # Start processing. This will add subplots to the current fig
                # axes via dask
                handler.batched_map(
                    self._save_plot,
                    # Convert dataframe iterrows into two lists of items to
                    # iterate. One list will be row index
                    # One list will be the pandas series of every row
                    [dataset for dataset in all_grouped_datasets],
                    [metadata for metadata in all_metadata],
                    [metadata_value for metadata_value in all_metadata_values],
                    [number_of_subplots for number_of_subplots in all_subplot_numbers],
                    [feature for i in range(len(all_grouped_datasets))],
                    [fig_width for i in range(len(all_grouped_datasets))],
                    [fig_height for i in range(len(all_grouped_datasets))],
                )

            self.manifest = pd.DataFrame(manifest)

        else:
            # If no metadata, just return input manifest
            self.manifest = dataset

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
