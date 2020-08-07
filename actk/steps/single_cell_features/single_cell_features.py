#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import NamedTuple, Optional, Union

import aicsimageio
import dask.dataframe as dd
import pandas as pd
from aics_dask_utils import DistributedHandler
from aicsimageio import AICSImage
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils, image_utils
from ..standardize_fov_array import StandardizeFOVArray

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.CellId,
    DatasetFields.CellIndex,
    DatasetFields.FOVId,
    DatasetFields.StandardizedFOVPath,
]


class SingleCellFeaturesResult(NamedTuple):
    cell_id: Union[int, str]
    path: Path


class SingleCellFeaturesError(NamedTuple):
    cell_id: int
    error: str


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
        overwrite: bool,
    ) -> Union[SingleCellFeaturesResult, SingleCellFeaturesError]:
        # Don't use dask for image reading
        aicsimageio.use_dask(False)

        # Get the ultimate end save path for this cell
        save_path = save_dir / f"{row.CellId}.json"

        # Check skip
        if not overwrite and save_path.is_file():
            log.info(f"Skipping cell feature generation for Cell Id: {row.CellId}")
            return SingleCellFeaturesResult(row.CellId, save_path)

        # Overwrite or didn't exist
        log.info(f"Beginning cell feature generation for CellId: {row.CellId}")

        # Wrap errors for debugging later
        try:
            # Read the standardized FOV
            image = AICSImage(row.StandardizedFOVPath)

            # Preload image data
            image.data

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
            with open(save_path, "w") as write_out:
                json.dump(features, write_out)

            log.info(f"Completed cell feature generation for CellId: {row.CellId}")
            return SingleCellFeaturesResult(row.CellId, save_path)

        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed cell feature generation for CellId: {row.CellId}. Error: {e}"
            )
            return SingleCellFeaturesError(row.CellId, str(e))

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        cell_ceiling_adjustment: int = 7,
        distributed_executor_address: Optional[str] = None,
        batch_size: Optional[int] = None,
        overwrite: bool = False,
        **kwargs,
    ):
        """
        Provided a dataset generate a features JSON file for each cell.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The primary cell dataset to use for generating features JSON for each cell.

            **Required dataset columns:** *["CellId", "CellIndex", "FOVId",
            "StandardizedFOVPath"]*

        cell_ceiling_adjustment: int
            The adjust to use for raising the cell shape ceiling. If <= 0, this will be
            ignored and cell data will be selected but not adjusted.
            Default: 7

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
            Path to the produced manifest with the CellFeaturesPath column added.
        """
        # Handle dataset provided as string or path
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Check dataset and manifest have required fields
            dataset_utils.check_required_fields(
                dataset=dataset, required_fields=REQUIRED_DATASET_FIELDS,
            )

        # Read dataset
        dataset = pd.read_csv(dataset)

        # Create features directory
        features_dir = self.step_local_staging_dir / "cell_features"
        features_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            results = handler.batched_map(
                self._generate_single_cell_features,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
                # Pass the other parameters as list of the same thing for each
                # mapped function call
                [cell_ceiling_adjustment for i in range(len(dataset))],
                [features_dir for i in range(len(dataset))],
                [overwrite for i in range(len(dataset))],
                batch_size=batch_size,
            )

        # Generate features paths rows
        cell_features_dataset = []
        errors = []
        for result in results:
            if isinstance(result, SingleCellFeaturesResult):
                cell_features_dataset.append(
                    {
                        DatasetFields.CellId: result.cell_id,
                        DatasetFields.CellFeaturesPath: result.path,
                    }
                )
            else:
                errors.append(
                    {DatasetFields.CellId: result.cell_id, "Error": result.error}
                )

        # Convert features paths rows to dataframe
        cell_features_dataset = pd.DataFrame(cell_features_dataset)

        # Drop CellFeaturesPath column if it already exists
        if DatasetFields.CellFeaturesPath in dataset.columns:
            dataset = dataset.drop(columns=[DatasetFields.CellFeaturesPath])

        # Join original dataset to the fov paths
        self.manifest = dataset.merge(cell_features_dataset, on=DatasetFields.CellId)

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        # Save errored cells to JSON
        with open(self.step_local_staging_dir / "errors.json", "w") as write_out:
            json.dump(errors, write_out)

        return manifest_save_path
