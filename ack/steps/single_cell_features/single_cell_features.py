#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import NamedTuple, Optional, Union

import dask.dataframe as dd
import pandas as pd
from aicsimageio import AICSImage
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils, image_utils
from ...utils.dask_utils import DistributedHandler
from ..standardize_fov_array import StandardizeFOVArray

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.CellId, DatasetFields.CellIndex,
    DatasetFields.FOVId, DatasetFields.StandardizedFOVPath,
]


class SingleCellFeaturesResult(NamedTuple):
    cell_id: int
    path: Path

###############################################################################


class SingleCellFeatures(Step):
    def __init__(
        self,
        direct_upstream_tasks=[StandardizeFOVArray],
        filepath_columns=[DatasetFields.CellFeaturesPath],
    ):
        super().__init__(
            direct_upstream_tasks=direct_upstream_tasks,
            filepath_columns=filepath_columns,
        )

    @staticmethod
    def _generate_single_cell_features(
        row_index: int,
        row: pd.Series,
        cell_ceiling_adjustment: int,
        save_dir: Path,
    ) -> Path:
        # Read the standardized FOV
        image = AICSImage(row.StandardizedFOVPath)

        # Select and adjust cell shape ceiling for this cell
        adjusted = image_utils.select_and_adjust_segmentation_ceiling(
            image=image.get_image_data("CYXZ", S=0, T=0),
            cell_index=row.CellIndex,
            cell_ceiling_adjustment=cell_ceiling_adjustment,
        )

        # Crop the FOV to the segmentation portions
        cropped = image_utils.crop_raw_channels_with_segmentation(
            image=adjusted, channels=image.get_channel_names(),
        )

        # Generate features
        features = image_utils.get_features_from_image(cropped)

        # Save to JSON
        save_path = save_dir / f"{row.CellId}.json"
        with open(save_path, "w") as write_out:
            json.dump(features, write_out)

        return SingleCellFeaturesResult(row.CellId, save_path)

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        cell_ceiling_adjustment: int = 7,
        distributed_executor_address: Optional[str] = None,
        clean: bool = False,
        debug: bool = False,
        **kwargs
    ):
        """
        Provided a dataset generate a features JSON file for each cell.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The primary cell dataset to use for generating features JSON for each cell.
            Required dataset columns: [
                "CellId",
                "CellIndex"
                "FOVId",
                "StandardizedFOVPath",
            ]

        cell_ceiling_adjustment: int
            The adjust to use for raising the cell shape ceiling. If <= 0, this will be
            ignored and cell data will be selected but not adjusted.
            Default: 7

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
            Path to the produced manifest with the CellFeaturesPath column added.
        """
        # Handle dataset provided as string or path
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Read dataset
            dataset = pd.read_csv(dataset)

        # Check dataset and manifest have required fields
        dataset_utils.check_required_fields(
            dataset=dataset,
            required_fields=REQUIRED_DATASET_FIELDS,
        )

        # Create features directory
        features_dir = self.step_local_staging_dir / "cell_features"
        features_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            futures = handler.client.map(
                self._generate_single_cell_features(
                    # Convert dataframe iterrows into two lists of items to iterate over
                    # One list will be row index
                    # One list will be the pandas series of every row
                    *zip(*list(dataset.iterrows())),
                    # Pass the other parameters as list of the same thing for each
                    # mapped function call
                    [cell_ceiling_adjustment for i in range(len(dataset))],
                    [features_dir for i in range(len(dataset))],
                )
            )

            # Block until all complete
            results = handler.gather(futures)

        # Generate features paths rows
        cell_features_dataset = []
        for result in results:
            cell_features_dataset.append({
                DatasetFields.CellId: result.cell_id,
                DatasetFields.CellFeaturesPath: result.path
            })

        # Convert features paths rows to dataframe
        cell_features_dataset = pd.DataFrame(cell_features_dataset)

        # Join original dataset to the fov paths
        self.manifest = dataset.merge(
            cell_features_dataset,
            on=DatasetFields.CellId
        )

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / f"manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
