#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import List, NamedTuple, Optional, Union

import aicsimageprocessing as proc
import dask.dataframe as dd
import numpy as np
import pandas as pd
from aicsimageio import AICSImage, transforms
from datastep import Step, log_run_params
from imageio import imwrite

from ...constants import Channels, DatasetFields
from ...utils import dataset_utils, image_utils
from ...utils.dask_utils import DistributedHandler
from ..single_cell_image_3d import SingleCellImage3D

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.CellId,
    DatasetFields.CellImage3DPath,
]


class CellImage2DResult(NamedTuple):
    cell_id: Union[int, str]
    path: Path

###############################################################################


class SingleCellImage2D(Step):
    def __init__(
        self,
        direct_upstream_tasks=[SingleCellImage3D],
        filepath_columns=[DatasetFields.CellImage2DPath],
        **kwargs,
    ):
        super().__init__(
            direct_upstream_tasks=direct_upstream_tasks,
            filepath_columns=filepath_columns,
            **kwargs,
        )

    @staticmethod
    def _generate_single_cell_image_2d(
        row_index: int,
        row: pd.Series,
        colors: List[List[int]],
        project_all_axes: bool,
        projection_method: str,
        save_dir: Path,
    ) -> CellImage2DResult:
        log.info(f"Beginning 2D single cell image generation for CellId: {row.CellId}")

        # Initialize image object with 3D image
        cell_image_3d = AICSImage(row.CellImage3DPath)
        channels = cell_image_3d.get_channel_names()

        # Crop raw channels using segmentations
        image = image_utils.crop_raw_channels_with_segmentation(
            cell_image_3d.get_image_data("CYXZ", S=0, T=0), channels,
        )

        # Transpose to CZYX for projections
        image = transforms.transpose_to_dims(image, "CYXZ", "CZYX")

        # Select the DNA, Membrane, and Structure channels
        image = image[
            [
                channels.index(target) for target in
                [Channels.DNA, Channels.Membrane, Channels.Structure]
            ]
        ]

        # Generate projection
        image = proc.imgtoprojection(
            image,
            proj_all=project_all_axes,
            proj_method=projection_method,
            local_adjust=False,
            global_adjust=True,
            colors=colors,
        )

        # Convert to YXC for PNG writing
        image = transforms.transpose_to_dims(image, "CYX", "YXC")

        # Drop size to uint8
        image = image.astype(np.uint8)

        # Save to PNG
        save_path = save_dir / f"{row.CellId}.png"
        imwrite(save_path, image)

        log.info(f"Completed 2D single cell image generation for CellId: {row.CellId}")

        return CellImage2DResult(row.CellId, save_path)

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        colors: List[List[int]] = [[0, 0, 1], [1, 0, 0], [0, 1, 0]],
        project_all_axes: bool = True,
        projection_method: str = "max",
        distributed_executor_address: Optional[str] = None,
        **kwargs,
    ):
        """
        Provided a dataset of 3D cell images, generate 2D max projections.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The primary cell dataset to generate 3D single cell images for.

            **Required dataset columns:** *["CellId", "CellImage3DPath"]*

        colors: List[List[int]]
            A list of lists containing color multipliers for RGB values.
            Default: [[0, 0, 1], [1, 0, 0], [0, 1, 0]]

            More details:
            https://allencellmodeling.github.io/aicsimageprocessing/aicsimageprocessing.html#aicsimageprocessing.imgToProjection.imgtoprojection

        project_all_axes: bool
            Should all axes be included in the projection.
            Default: True (include all axes projections)

            More details:
            https://allencellmodeling.github.io/aicsimageprocessing/aicsimageprocessing.html#aicsimageprocessing.imgToProjection.imgtoprojection

        project_all_axes: str
            The method to use for generating the flat projection.
            Default: max

            More details:
            https://allencellmodeling.github.io/aicsimageprocessing/aicsimageprocessing.html#aicsimageprocessing.imgToProjection.imgtoprojection

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
        cell_images_2d_dir = self.step_local_staging_dir / "cell_images_2d"
        cell_images_2d_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            futures = handler.client.map(
                self._generate_single_cell_image_2d,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
                # Pass the other parameters as list of the same thing for each
                # mapped function call
                [colors for i in range(len(dataset))],
                [project_all_axes for i in range(len(dataset))],
                [projection_method for i in range(len(dataset))],
                [cell_images_2d_dir for i in range(len(dataset))],
            )

            # Block until all complete
            results = handler.gather(futures)

        # Generate single cell images dataset rows
        cell_images_2d_dataset = []
        for result in results:
            cell_images_2d_dataset.append(
                {
                    DatasetFields.CellId: result.cell_id,
                    DatasetFields.CellImage2DPath: result.path,
                }
            )

        # Convert features paths rows to dataframe
        cell_images_2d_dataset = pd.DataFrame(cell_images_2d_dataset)

        # Join original dataset to the fov paths
        self.manifest = dataset.merge(cell_images_2d_dataset, on=DatasetFields.CellId)

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / f"manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
