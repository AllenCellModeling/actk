#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import List, NamedTuple, Optional, Union

import aicsimageio
import aicsimageprocessing as proc
import dask.dataframe as dd
import numpy as np
import pandas as pd
import matplotlib.image as mpimg
import matplotlib.pyplot as plt
from aics_dask_utils import DistributedHandler
from aicsimageio import AICSImage, transforms
from aicsimageio.writers import OmeTiffWriter
from datastep import Step, log_run_params
from imageio import imwrite

from ...constants import Channels, DatasetFields
from ...utils import dataset_utils, image_utils
from ..single_cell_features import SingleCellFeatures
from ..single_cell_images import SingleCellImages

plt.style.use('dark_background')
###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.CellId
]

class DiagnosticSheetResult(NamedTuple):
    cell_id: Union[int, str]
    diagnostic_sheet_save_path: Optional[Path] = None


class DiagnosticSheetError(NamedTuple):
    cell_id: Union[int, str]
    error: str


###############################################################################


class MakeDiagnosticSheet(Step):
    def __init__(
        self,
        direct_upstream_tasks: List["Step"] = [SingleCellImages]
    ):
        super().__init__(direct_upstream_tasks=direct_upstream_tasks)


    @staticmethod
    def _make_group_plot(
        dataset: pd.DataFrame,
        group_by: str,
        group_by_value: str,
        feature_display: Optional[str] = None,
        ):

        # Get rows and columns of figure
        columns = int(np.sqrt(len(dataset)) + 0.5)
        if np.sqrt(len(dataset)) != int(np.sqrt(len(dataset))):
            rows = columns + 1
        else:
            rows = columns

        # Set figure size
        fig_size_width =  columns*5
        fig_size_height = rows*3

        fig, ax_array = plt.subplots(rows, columns,squeeze=False, figsize = (fig_size_height, fig_size_width))
        index = 0

        try:
            for i,ax_row in enumerate(ax_array):   
                for j,axes in enumerate(ax_row):
                    if index < len(dataset):
                        # Load feature to plot if feature_display
                        if feature_display:
                            with open(dataset[ "CellFeaturesPath"][index]) as f:
                                this_cell_features = json.load(f)
                            axes.set_title(f"{group_by}:{group_by_value} \n {feature_display}:{this_cell_features[feature_display]} ")
                        else:
                            axes.set_title(f"{group_by}:{group_by_value}")
                        axes.axis('off')
                        # Read AllProjections Image
                        img = mpimg.imread(dataset[ "CellImage2DAllProjectionsPath"][index])
                        axes.imshow(img)
                        axes.set_aspect(1)
                        index += 1
                    else:
                        axes.axis('off')
            log.info(f"Made diagnostic sheet for group: {group_by} and value: {group_by_value}")
        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed to make diagnositc sheet for group: {group_by} and value: {group_by_value}. Error: {e}"
            )
        # Savefig, all rows will have the same diagnostic sheet path since they are merged, so choose any index
        fig.savefig(dataset[ "DiagnosticSheetPath"][0])

    @staticmethod
    def _collect_group(
        row_index: int, 
        row: pd.Series,
        diagnostic_sheet_dir: Path,
        overwrite: bool,
        group_by: str,
        group_by_value: str
        ) -> Union[DiagnosticSheetResult, DiagnosticSheetError]:
        # Don't use dask for image reading
        aicsimageio.use_dask(False)

        try:
            # Get the ultimate end save paths for grouped plot
            if str(row[group_by]) == str(group_by_value):    
                diagnostic_sheet_save_path = (
                    diagnostic_sheet_dir / f"{group_by}:{group_by_value}.png"
                )
            else:
                # else no path to save
                diagnostic_sheet_save_path = None

            log.info(f"Retrieved the path for this group: {group_by}")

            # Return ready to save image
            return DiagnosticSheetResult(
                row.CellId,
                diagnostic_sheet_save_path
            )
        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed to retrieve the path for this group: {group_by} "
                "Error: {e}"
            )
            return DiagnosticSheetError(row.CellId, str(e))    


    @log_run_params
    def run(
        self, 
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame], 
        distributed_executor_address: Optional[str] = None,
        overwrite: bool = False,
        group_by: Optional[str] = None,
        group_by_value: Optional[str] = None,
        feature_display: Optional[str] = None,
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
            The primary cell dataset to use for generating diagnistic sheet for a group of cells.

            **Required dataset columns:** *["CellId"]*

        group_by: str
            The column name (metadata) to display information. For example, "FOVId"

        group_by_value: str
            The value of this metadata to display. For example, "1" (For group "FOVId", this is FOVId 1)

        feature_display: str
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
                [group_by for i in range(len(dataset))], 
                [group_by_value for i in range(len(dataset))]
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
                        DatasetFields.DiagnosticSheetPath: r.diagnostic_sheet_save_path
                    }
                )
            else:
                errors.append({DatasetFields.CellId: r.cell_id, "Error": r.error})

        # Convert features paths rows to dataframe
        diagnostic_sheet_result_dataset = pd.DataFrame(diagnostic_sheet_result_dataset)
        
        # Drop the various diagnostic sheet columns if they already exist
        drop_columns = []
        if DatasetFields.DiagnosticSheetPath in dataset.columns:
            drop_columns.append(DatasetFields.DiagnosticSheetPath)

        dataset = dataset.drop(columns=drop_columns)

        # Join original dataset to the fov paths
        try:
            self.manifest = dataset.merge(
                diagnostic_sheet_result_dataset, on=DatasetFields.CellId
            )
            log.info(
                f"Added paths of the saved diagnostic sheet to the manifest "
                "Error: {e}"
            )
        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed to make diagnostic sheet dataset, no group input provided "
                "Error: {e}"
            )
            return

        grouped_manifest = self.manifest.dropna().reset_index()

        if group_by and group_by_value and len(grouped_manifest) > 0:
            self._make_group_plot(grouped_manifest, group_by, group_by_value, feature_display)

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
