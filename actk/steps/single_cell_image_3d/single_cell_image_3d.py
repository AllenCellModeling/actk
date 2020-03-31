#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import List, NamedTuple, Optional, Union

import aicsimageprocessing as proc
import dask.dataframe as dd
import numpy as np
import pandas as pd
from aicsimageio import AICSImage, transforms
from aicsimageio.writers import OmeTiffWriter
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils, image_utils
from ...utils.dask_utils import DistributedHandler
from ..single_cell_features import SingleCellFeatures

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.CellId,
    DatasetFields.StandardizedFOVPath,
    DatasetFields.CellFeaturesPath,
]


class CellImage3DResult(NamedTuple):
    cell_id: Union[int, str]
    path: Path


###############################################################################


class SingleCellImage3D(Step):
    def __init__(
        self,
        direct_upstream_tasks=[SingleCellFeatures],
        filepath_columns=[DatasetFields.CellImage3DPath],
    ):
        super().__init__(
            direct_upstream_tasks=direct_upstream_tasks,
            filepath_columns=filepath_columns,
        )

    @staticmethod
    def _get_registered_image_size(row_index: int, row: pd.Series) -> List[int]:
        # Open cell features JSON
        with open(row.CellFeaturesPath, "r") as read_in:
            cell_features = json.load(read_in)

        # Return registered image size
        return cell_features["imsize_registered"]

    @staticmethod
    def _generate_single_cell_image_3d(
        row_index: int,
        row: pd.Series,
        cell_ceiling_adjustment: int,
        bounding_box: np.ndarray,
        save_dir: Path,
    ) -> CellImage3DResult:
        log.info(f"Beginning 3D single cell image generation for CellId: {row.CellId}")

        # Initialize image object with standardized FOV
        standardized_image = AICSImage(row.StandardizedFOVPath)

        # Select and adjust cell shape ceiling for this cell
        image = image_utils.select_and_adjust_segmentation_ceiling(
            # Unlike most other operations, we can read in normal "CZYX" dimension order
            # here as all future operations are expecting it
            image=standardized_image.get_image_data("CYXZ", S=0, T=0),
            cell_index=row.CellIndex,
            cell_ceiling_adjustment=cell_ceiling_adjustment,
        )

        # Transpose to CZYX for saving and because bounding box is in CZYX order
        image = transforms.transpose_to_dims(image, "CYXZ", "CZYX")

        # Perform a rigid registration on the image
        image, _, _ = proc.cell_rigid_registration(image, bbox_size=bounding_box)

        # Reduce size
        image = image * 255
        image = image.astype(np.uint8)

        # Save to OME-TIFF
        save_path = save_dir / f"{row.CellId}.ome.tiff"
        with OmeTiffWriter(save_path, overwrite_file=True) as writer:
            writer.save(
                image,
                dimension_order="CZYX",
                channel_names=standardized_image.get_channel_names(),
                pixels_physical_size=standardized_image.get_physical_pixel_size(),
            )

        log.info(f"Completed 3D single cell image generation for CellId: {row.CellId}")

        # Return ready to save image
        return CellImage3DResult(row.CellId, save_path)

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        cell_ceiling_adjustment: int = 7,
        bounding_box_percentile: float = 95.0,
        distributed_executor_address: Optional[str] = None,
        **kwargs,
    ):
        """
        Provided a dataset of cell features, generate 3D single cell images.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The primary cell dataset to generate 3D single cell images for.

            **Required dataset columns:** *["CellId", "StandardizedFOVPath",
            "CellFeaturesPath"]*

        cell_ceiling_adjustment: int
            The adjust to use for raising the cell shape ceiling. If <= 0, this will be
            ignored and cell data will be selected but not adjusted.
            Default: 7

        bounding_box_percentile: float
            A float used to generate the actual bounding box for all cells by finding
            provided percentile of all cell image sizes.
            Default: 95.0

        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
            Default: None

        Returns
        -------
        manifest_save_path: Path
            Path to the produced manifest with the CellImage3DPath column added.
        """
        # Handle dataset provided as string or path
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Read dataset
            dataset = pd.read_csv(dataset)

        # Check dataset and manifest have required fields
        dataset_utils.check_required_fields(
            dataset=dataset, required_fields=REQUIRED_DATASET_FIELDS,
        )

        # Create save directories
        cell_images_3d_dir = self.step_local_staging_dir / "cell_images_3d"
        cell_images_3d_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            bounding_box_futures = handler.client.map(
                self._get_registered_image_size,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
            )

            # Block until all complete
            bb_results = handler.gather(bounding_box_futures)

            # Compute bounding box with percentile
            bb_results = np.array(bb_results)
            bounding_box = np.percentile(bb_results, bounding_box_percentile, axis=0)
            bounding_box = np.ceil(bounding_box)

            # Generate bounded arrays
            futures = handler.client.map(
                self._generate_single_cell_image_3d,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
                # Pass the other parameters as list of the same thing for each
                # mapped function call
                [cell_ceiling_adjustment for i in range(len(dataset))],
                [bounding_box for i in range(len(dataset))],
                [cell_images_3d_dir for i in range(len(dataset))],
            )

            # Block until all complete
            results = handler.gather(futures)

        # Generate single cell images dataset rows
        cell_images_3d_dataset = []
        for result in results:
            cell_images_3d_dataset.append(
                {
                    DatasetFields.CellId: result.cell_id,
                    DatasetFields.CellImage3DPath: result.path,
                }
            )

        # Convert features paths rows to dataframe
        cell_images_3d_dataset = pd.DataFrame(cell_images_3d_dataset)

        # Join original dataset to the fov paths
        self.manifest = dataset.merge(cell_images_3d_dataset, on=DatasetFields.CellId)

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / f"manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
