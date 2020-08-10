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
    diagnostic_sheet_save_path: Optional[Path] = None


class DiagnosticSheetError(NamedTuple):
    cell_id: Union[int, str]
    error: str


###############################################################################


class MakeDiagnosticSheet(Step):
    def __init__(self, direct_upstream_tasks: List["Step"] = [SingleCellImages]):
        super().__init__(direct_upstream_tasks=direct_upstream_tasks)

    @staticmethod
    def _make_group_plot(
        dataset: pd.DataFrame, metadata: str, feature: Optional[str] = None,
    ):

        # Get figure numbers, subplot numbers, and figure paths
        figure_numbers = dataset[metadata].value_counts().index.to_numpy()
        subplot_numbers = dataset[metadata].value_counts().values
        figure_paths = dataset["DiagnosticSheetPath"].value_counts().index

        # Index to help loop through figure numbers
        index = 0

        # Set font size
        font = {"weight": "bold", "size": 11}
        matplotlib.rc("font", **font)

        # Loop through figure numbers
        for i in range(len(figure_numbers)):

            # Get rows and columns of figure
            columns = int(np.sqrt(subplot_numbers[i]) + 0.5)
            if np.sqrt(subplot_numbers[i]) != int(np.sqrt(subplot_numbers[i])):
                rows = columns + 1
            else:
                rows = columns

            # Set figure size
            fig_size_width = columns * 7
            fig_size_height = rows * 5

            # Set subplots
            fig, ax_array = plt.subplots(
                rows, columns, squeeze=False, figsize=(fig_size_height, fig_size_width)
            )

            # Set title
            fig.suptitle(f"{metadata}:{figure_numbers[i]}")

            # Second index to check subplots
            index2 = 0

            for k, ax_row in enumerate(ax_array):
                for j, axes in enumerate(ax_row):
                    if index2 < subplot_numbers[i]:
                        # Load feature to plot if feature
                        if feature:
                            with open(dataset["CellFeaturesPath"][index]) as f:
                                this_cell_features = json.load(f)
                            title = "CellId: {0}, {1} {2}: {3}".format(
                                dataset["CellId"][index],
                                "\n",
                                feature,
                                this_cell_features[feature],
                            )
                            axes.set_title(title)
                        else:
                            axes.set_title(f"CellID: {dataset['CellId'][index]}")
                        axes.axis("off")
                        # Read AllProjections Image
                        img = mpimg.imread(
                            dataset["CellImage2DAllProjectionsPath"][index]
                        )
                        axes.imshow(img)
                        axes.set_aspect(1)
                        index += 1
                        index2 += 1
                    else:
                        axes.axis("off")

            log.info(
                f"Completed diagnostic sheet for : {metadata} {figure_numbers[i]} "
            )

            # Savefig
            fig.savefig(figure_paths[i])

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
            if row[str(metadata)]:
                diagnostic_sheet_save_path = (
                    diagnostic_sheet_dir / f"{metadata}_{row[metadata]}.png"
                )
                log.info(f"Generating diagnostic sheet for cell ID: {row.CellId}")
            else:
                # else no path to save
                diagnostic_sheet_save_path = None

            # Check skip
            if not overwrite and diagnostic_sheet_save_path.is_file():
                log.info(f"Skipping diagnostic sheet for Cell Id: {row.CellId}")
                return DiagnosticSheetResult(row.CellId, None)

            # Return ready to save image
            return DiagnosticSheetResult(row.CellId, diagnostic_sheet_save_path)
        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed to retrieve the diagnostic sheet path"
                f"for cell ID: {row.CellId}"
                f"Error: {e}"
            )
            return DiagnosticSheetError(row.CellId, str(e))

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        distributed_executor_address: Optional[str] = None,
        overwrite: bool = False,
        metadata: Optional[str] = None,
        feature: Optional[str] = None,
        **kwargs,
    ):
        """
        Run a pure function.

        Protected Parameters
        --------------------
        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
        clean: bool
            Should the local staging directory be cleaned prior to this run.
            Default: False (Do not clean)
        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc.
            Default: False (Do not debug)

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The primary cell dataset to use for generating 
            diagnistic sheet for a group of cells.

            **Required dataset columns:** *["CellId"]*

        metadata: str
            The metadata to group cells and generate a diagnostic sheet. 
            For example, "FOVId"

        feature: str
            The name of the single cell feature to display. For example, "imsize_orig"

        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
            Default: None

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

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            diagnostic_sheet_futures = handler.client.map(
                self._collect_group,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
                [diagnostic_sheet_dir for i in range(len(dataset))],
                [overwrite for i in range(len(dataset))],
                [metadata for i in range(len(dataset))],
            )

            # Block until all complete
            diagnostic_sheet_result = handler.gather(diagnostic_sheet_futures)

        # Generate diagnostic sheet dataset rows
        diagnostic_sheet_result_dataset = []
        errors = []
        for r in diagnostic_sheet_result:
            if isinstance(r, DiagnosticSheetResult):
                diagnostic_sheet_result_dataset.append(
                    {
                        DatasetFields.CellId: r.cell_id,
                        DatasetFields.DiagnosticSheetPath: r.diagnostic_sheet_save_path,
                    }
                )
            else:
                errors.append({DatasetFields.CellId: r.cell_id, "Error": r.error})

        # Convert diagnostic sheet paths rows to dataframe
        diagnostic_sheet_result_dataset = pd.DataFrame(diagnostic_sheet_result_dataset)

        # Drop the various diagnostic sheet columns if they already exist
        drop_columns = []
        if DatasetFields.DiagnosticSheetPath in dataset.columns:
            drop_columns.append(DatasetFields.DiagnosticSheetPath)

        dataset = dataset.drop(columns=drop_columns)

        if metadata and len(diagnostic_sheet_result_dataset) > 0:
            # Join original dataset to the fov paths
            self.manifest = dataset.merge(
                diagnostic_sheet_result_dataset, on=DatasetFields.CellId
            )

            # Drop cell rows that dont have a saved path for the diagnostic sheet
            self._make_group_plot(
                self.manifest.dropna().reset_index(), metadata, feature
            )
        else:
            self.manifest = dataset

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        # Save errored cells to JSON
        with open(self.step_local_staging_dir / "errors.json", "w") as write_out:
            json.dump(errors, write_out)

        return manifest_save_path
