#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import List, NamedTuple, Optional, Union

import aicsimageio
import dask.dataframe as dd
import numpy as np
import pandas as pd
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import matplotlib

from aics_dask_utils import DistributedHandler
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils
from ..single_cell_images import SingleCellImages

plt.style.use("dark_background")

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [DatasetFields.CellId]


class DiagnosticSheetResult(NamedTuple):
    cell_id: Union[int, str]
    save_path: Optional[Path] = None


class DiagnosticSheetError(NamedTuple):
    cell_id: Union[int, str]
    error: str


def flatten(x):
    result = []
    for el in x:
        if hasattr(el, "__iter__") and not isinstance(el, str):
            result.extend(flatten(el))
        else:
            result.append(el)
    return result


###############################################################################


class MakeDiagnosticSheet(Step):
    def __init__(self, direct_upstream_tasks: List["Step"] = [SingleCellImages]):
        super().__init__(direct_upstream_tasks=direct_upstream_tasks)

    def _make_group_plot(
        self,
        dataset: pd.DataFrame,
        metadata: str,
        all_metadata: Union[list, str],
        max_cells: int = 1000,
        feature: Optional[str] = None,
        fig_width: Optional[int] = None,
        fig_height: Optional[int] = None,
    ):
        # Get figure numbers, subplot numbers, and figure paths
        # Number of unique metadata values
        figure_numbers = dataset[metadata].value_counts().index.to_numpy()
        # Number of cells that have each unique metadata value
        subplot_numbers = dataset[metadata].value_counts().values
        # Paths to save
        figure_paths = []
        for fig_num in figure_numbers:
            figure_paths.append(
                dataset.loc[dataset[metadata] == fig_num][
                    DatasetFields.DiagnosticSheetPath
                ].values
            )

        # Index to help loop through figure numbers
        index = 0

        # Set font size
        font = {"weight": "bold", "size": 11}
        matplotlib.rc("font", **font)

        # Loop through figure numbers
        for i in range(len(figure_numbers)):

            this_figure_number = figure_numbers[i]
            this_figure_subplots = subplot_numbers[i]
            # figure_paths[i] is a list of fig paths for all cell IDs
            # that share the same metadata value. Lets choose
            # figure_paths[i][0] as one of those paths (all are same)
            this_figure_path = figure_paths[i][0]

            # Get correct figure path if multiple metadata, i.e.
            # len(figure_paths[i][0]) > 1
            if metadata != all_metadata:
                if isinstance(this_figure_path, list) and len(this_figure_path) > 1:
                    multiple_paths = True
                    this_figure_path = this_figure_path[all_metadata.index(metadata)]
                else:
                    multiple_paths = False

            if this_figure_subplots < max_cells:
                index = self._save_single_figure(
                    dataset,
                    this_figure_number,
                    this_figure_subplots,
                    index,
                    metadata,
                    all_metadata,
                    multiple_paths,
                    this_figure_path,
                    feature,
                    fig_width,
                    fig_height,
                )
            else:
                # If subplot numbers are more than a max cell number (1000),
                # then make separate figures
                subplot_split = [
                    max_cells for i in range(int(this_figure_subplots / max_cells))
                ] + [
                    (
                        this_figure_subplots
                        - max_cells * int(this_figure_subplots / max_cells)
                    )
                ]

                # pop last element if 0 (figure subplots =
                # max_cells * int(subplots/max_cells))
                if subplot_split[-1] == 0:
                    subplot_split.pop()

                for j, sublot in enumerate(subplot_split):
                    # If split, add a "_{number}" before ".png" in path
                    split_fig_path = Path(
                        str(this_figure_path)[:-4] + "_" + str(j) + ".png"
                    )
                    log.info(f"Splitting this figure and saving to: {split_fig_path}")

                    index, dataset = self._save_single_figure(
                        dataset,
                        this_figure_number,
                        sublot,
                        index,
                        metadata,
                        all_metadata,
                        multiple_paths,
                        split_fig_path,
                        feature,
                        fig_width,
                        fig_height,
                    )

            log.info(
                f"Completed diagnostic sheet for: {metadata} {this_figure_number} "
            )

        return dataset

    @staticmethod
    def _save_single_figure(
        dataset: pd.DataFrame,
        figure_number: Union[int, str],
        number_of_subplots: int,
        index: int,
        metadata: str,
        all_metadata: Union[str, list],
        multiple_paths: bool,
        figure_path: Union[str, Path],
        feature: Optional[str] = None,
        fig_width: Optional[int] = None,
        fig_height: Optional[int] = None,
    ):
        # Get rows and columns of figure
        columns = int(np.sqrt(number_of_subplots) + 0.5)
        if np.sqrt(number_of_subplots) != int(np.sqrt(number_of_subplots)):
            rows = columns + 1
        else:
            rows = columns

        # Set figure size
        if not fig_width:
            fig_width = columns * 7
        if not fig_height:
            fig_height = rows * 5

        # Set subplots
        fig, ax_array = plt.subplots(
            rows, columns, squeeze=False, figsize=(fig_height, fig_width)
        )

        # Set title
        fig.suptitle(f"{metadata}:{figure_number}")

        # Second index to check subplots
        index2 = 0

        for k, ax_row in enumerate(ax_array):
            for j, axes in enumerate(ax_row):
                if index2 < number_of_subplots:
                    # Load feature to plot if feature
                    if feature:
                        with open(dataset[DatasetFields.CellFeaturesPath][index]) as f:
                            this_cell_features = json.load(f)
                        title = "CellId: {0}, {1} {2}: {3}".format(
                            dataset[DatasetFields.CellId][index],
                            "\n",
                            feature,
                            this_cell_features[feature],
                        )
                        axes.set_title(title)
                    else:
                        axes.set_title(f"CellID: {[DatasetFields.CellId][index]}")
                    axes.axis("off")
                    # Read AllProjections Image
                    img = mpimg.imread(
                        dataset[DatasetFields.CellImage2DAllProjectionsPath][index]
                    )
                    axes.imshow(img)
                    axes.set_aspect(1)

                    # Update fig save path in dataset
                    # If multiple paths, choose the correct index for
                    # figure path
                    if metadata != all_metadata and multiple_paths:
                        dataset.loc[index, DatasetFields.DiagnosticSheetPath][
                            all_metadata.index(metadata)
                        ] = str(figure_path)
                    else:
                        dataset.loc[index, DatasetFields.DiagnosticSheetPath] = str(
                            figure_path
                        )

                    index += 1
                    index2 += 1
                else:
                    axes.axis("off")

        # Savefig
        fig.savefig(figure_path)

        return index, dataset

    @staticmethod
    def _collect_group(
        row_index: int,
        row: pd.Series,
        diagnostic_sheet_dir: Path,
        overwrite: bool,
        metadata: str,
    ) -> Union[DiagnosticSheetResult, DiagnosticSheetError]:
        # Don't use dask for image reading
        aicsimageio.use_dask(False)

        try:
            # Get the ultimate end save paths for grouped plot
            if row[str(metadata)] or row[str(metadata)] == 0:
                assert DatasetFields.CellImage2DAllProjectionsPath in row.index
                save_path = (
                    diagnostic_sheet_dir / f"{metadata}_{row[str(metadata)]}.png"
                )
                log.info(
                    f"Generating diagnostic sheet path for cell ID: {row.CellId},"
                    f"{metadata} {row[str(metadata)]}"
                )
            else:
                # else no path to save
                save_path = None
                # diagnostic_sheet_save_path = (
                #     diagnostic_sheet_dir / f"{metadata}_{row[str(metadata)]}.png"
                # )

            # Check skip
            if not overwrite and save_path.is_file():
                log.info(
                    f"Skipping diagnostic sheet path for cell ID: {row.CellId},"
                    f"{metadata} {row[str(metadata)]}"
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
        max_cells: int = 1000,
        distributed_executor_address: Optional[str] = None,
        batch_size: Optional[int] = None,
        overwrite: bool = False,
        feature: Optional[str] = None,
        metadata: Optional[Union[list, str]] = None,
        fig_width: Optional[int] = None,
        fig_height: Optional[int] = None,
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

            **Required dataset columns:** *["CellId"]*

        batch_size: Optional[int]
            An optional batch size to process n features at a time.
            Default: None (Process all at once)

        metadata: Union[str, list, None]
            The metadata to group cells and generate a diagnostic sheet. 
            For example, "FOVId" or "["FOVId", "ProteinDisplayName"]"

        feature: str
            The name of the single cell feature to display. For example, "imsize_orig"

        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
            Default: None

        clean: bool
            Should the local staging directory be cleaned prior to this run.
            Default: False (Do not clean)

        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc.
            Default: False (Do not debug)

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
            dataset=dataset, required_fields=REQUIRED_DATASET_FIELDS,
        )

        # Create save directories
        diagnostic_sheet_dir = self.step_local_staging_dir
        diagnostic_sheet_dir.mkdir(exist_ok=True)

        # Check for metadata
        if metadata:
            # Make metadata a list
            metadata = metadata if isinstance(metadata, list) else [metadata]

            # Process each row
            for j, this_metadata in enumerate(metadata):

                # Create empty diagnostic sheet result dataset and errors
                diagnostic_sheet_result_dataset = []
                errors = []

                with DistributedHandler(distributed_executor_address) as handler:
                    # Start processing
                    diagnostic_sheet_result = handler.batched_map(
                        self._collect_group,
                        # Convert dataframe iterrows into two lists of items to iterate
                        # One list will be row index
                        # One list will be the pandas series of every row
                        *zip(*list(dataset.iterrows())),
                        [diagnostic_sheet_dir for i in range(len(dataset))],
                        [overwrite for i in range(len(dataset))],
                        [this_metadata for i in range(len(dataset))],
                        batch_size=batch_size,
                    )
                # Generate diagnostic sheet dataset rows
                for r in diagnostic_sheet_result:
                    if isinstance(r, DiagnosticSheetResult):
                        diagnostic_sheet_result_dataset.append(
                            {
                                DatasetFields.CellId: r.cell_id,
                                DatasetFields.DiagnosticSheetPath: r.save_path,
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
                if DatasetFields.DiagnosticSheetPath in dataset.columns and j == 0:
                    drop_columns.append(DatasetFields.DiagnosticSheetPath)

                dataset = dataset.drop(columns=drop_columns)

                # Update manifest with these paths if there is data
                if len(diagnostic_sheet_result_dataset) > 0:

                    # Join original dataset to the fov paths
                    dataset = dataset.merge(
                        diagnostic_sheet_result_dataset, on=DatasetFields.CellId
                    )

                    # If j > 0 (i.e. multiple metadata), we will append new paths
                    # to the same DiagnosticSheetPath column
                    if j > 0:
                        PathList = dataset[
                            [
                                DatasetFields.DiagnosticSheetPath + "_x",
                                DatasetFields.DiagnosticSheetPath + "_y",
                            ]
                        ].values.tolist()

                        # Flatten pathlist
                        PathList = [list(flatten(ThisPath)) for ThisPath in PathList]
                        dataset[DatasetFields.DiagnosticSheetPath] = PathList

                        # Delete the _x and _y columns that are auto generated
                        del dataset[DatasetFields.DiagnosticSheetPath + "_x"]
                        del dataset[DatasetFields.DiagnosticSheetPath + "_y"]

                if j == 0:
                    dataset.dropna().reset_index(inplace=True)

                # Call the group plotting function
                dataset = self._make_group_plot(
                    dataset,
                    this_metadata,
                    metadata,
                    max_cells,
                    feature,
                    fig_width,
                    fig_height,
                )

                # Update manifest
                self.manifest = dataset

                # Save errored cells to JSON
                with open(
                    self.step_local_staging_dir / "errors.json", "w"
                ) as write_out:
                    json.dump(errors, write_out)
        else:
            # If no metadata, just return input manifest
            self.manifest = dataset

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
