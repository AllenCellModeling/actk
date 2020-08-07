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
from aics_dask_utils import DistributedHandler
from aicsimageio import AICSImage, transforms
from aicsimageio.writers import OmeTiffWriter
from datastep import Step, log_run_params
from imageio import imwrite

from ...constants import Channels, DatasetFields
from ...utils import dataset_utils, image_utils
from ..single_cell_features import SingleCellFeatures

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.CellId,
    DatasetFields.StandardizedFOVPath,
    DatasetFields.CellFeaturesPath,
]


class CellImagesResult(NamedTuple):
    cell_id: Union[int, str]
    path_3d: Path
    path_2d_all_proj: Path
    path_2d_yx_proj: Path


class CellImagesError(NamedTuple):
    cell_id: Union[int, str]
    error: str


###############################################################################


class SingleCellImages(Step):
    def __init__(
        self,
        direct_upstream_tasks=[SingleCellFeatures],
        filepath_columns=[
            DatasetFields.CellImage3DPath,
            DatasetFields.CellImage2DAllProjectionsPath,
            DatasetFields.CellImage2DYXProjectionPath,
        ],
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
    def _generate_single_cell_images(
        row_index: int,
        row: pd.Series,
        cell_ceiling_adjustment: int,
        bounding_box: np.ndarray,
        projection_method: str,
        cell_images_3d_dir: Path,
        cell_images_2d_all_proj_dir: Path,
        cell_images_2d_yx_proj_dir: Path,
        overwrite: bool,
    ) -> Union[CellImagesResult, CellImagesError]:
        # Don't use dask for image reading
        aicsimageio.use_dask(False)

        # Get the ultimate end save paths for this cell
        cell_image_3d_save_path = cell_images_3d_dir / f"{row.CellId}.ome.tiff"
        cell_image_2d_all_proj_save_path = (
            cell_images_2d_all_proj_dir / f"{row.CellId}.png"
        )
        cell_image_2d_yx_proj_save_path = (
            cell_images_2d_yx_proj_dir / f"{row.CellId}.png"
        )

        # Check skip
        if (
            not overwrite
            # Only skip if all images exist for this cell
            and all(
                p.is_file()
                for p in [
                    cell_image_3d_save_path,
                    cell_image_2d_all_proj_save_path,
                    cell_image_2d_yx_proj_save_path,
                ]
            )
        ):
            log.info(f"Skipping single cell image generation for CellId: {row.CellId}")
            return CellImagesResult(
                row.CellId,
                cell_image_3d_save_path,
                cell_image_2d_all_proj_save_path,
                cell_image_2d_yx_proj_save_path,
            )

        # Overwrite or didn't exist
        log.info(f"Beginning single cell image generation for CellId: {row.CellId}")

        # Wrap errors for debugging later
        try:
            # Initialize image object with standardized FOV
            standardized_image = AICSImage(row.StandardizedFOVPath)
            channels = standardized_image.get_channel_names()

            # Preload image data
            standardized_image.data

            # Select and adjust cell shape ceiling for this cell
            image = image_utils.select_and_adjust_segmentation_ceiling(
                # Unlike most other operations, we can read in normal "CZYX" dimension
                # order here as all future operations are expecting it
                image=standardized_image.get_image_data("CYXZ", S=0, T=0),
                cell_index=row.CellIndex,
                cell_ceiling_adjustment=cell_ceiling_adjustment,
            )

            # Perform a rigid registration on the image
            image, _, _ = proc.cell_rigid_registration(
                image,
                # Reorder bounding box as image is currently CYXZ
                bbox_size=bounding_box[[0, 2, 3, 1]],
            )

            # Reduce size
            crop_3d = image * 255
            crop_3d = crop_3d.astype(np.uint8)

            # Transpose to CZYX for saving
            crop_3d = transforms.transpose_to_dims(crop_3d, "CYXZ", "CZYX")

            # Save to OME-TIFF
            with OmeTiffWriter(cell_image_3d_save_path, overwrite_file=True) as writer:
                writer.save(
                    crop_3d,
                    dimension_order="CZYX",
                    channel_names=standardized_image.get_channel_names(),
                    pixels_physical_size=standardized_image.get_physical_pixel_size(),
                )

            # Generate 2d image projections
            # Crop raw channels using segmentations
            image = image_utils.crop_raw_channels_with_segmentation(image, channels)

            # Transpose to CZYX for projections
            image = transforms.transpose_to_dims(image, "CYXZ", "CZYX")

            # Select the DNA, Membrane, and Structure channels
            image = image[
                [
                    channels.index(target)
                    for target in [Channels.DNA, Channels.Membrane, Channels.Structure]
                ]
            ]

            # Set RGB colors
            # This will set:
            # DNA to Blue
            # Membrane to Red
            # Structure to Green
            colors = [[0, 0, 1], [1, 0, 0], [0, 1, 0]]

            # Get all axes projection image
            all_proj = proc.imgtoprojection(
                image,
                proj_all=True,
                proj_method=projection_method,
                local_adjust=False,
                global_adjust=True,
                colors=colors,
            )

            # Convert to YXC for PNG writing
            all_proj = transforms.transpose_to_dims(all_proj, "CYX", "YXC")

            # Drop size to uint8
            all_proj = all_proj.astype(np.uint8)

            # Save to PNG

            imwrite(cell_image_2d_all_proj_save_path, all_proj)

            # Get YX axes projection image
            yx_proj = proc.imgtoprojection(
                image,
                proj_all=False,
                proj_method=projection_method,
                local_adjust=False,
                global_adjust=True,
                colors=colors,
            )

            # Convert to YXC for PNG writing
            yx_proj = transforms.transpose_to_dims(yx_proj, "CYX", "YXC")

            # Drop size to uint8
            yx_proj = yx_proj.astype(np.uint8)

            # Save to PNG
            imwrite(cell_image_2d_yx_proj_save_path, yx_proj)

            log.info(f"Completed single cell image generation for CellId: {row.CellId}")

            # Return ready to save image
            return CellImagesResult(
                row.CellId,
                cell_image_3d_save_path,
                cell_image_2d_all_proj_save_path,
                cell_image_2d_yx_proj_save_path,
            )

        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed single cell image generation for CellId: {row.CellId}. "
                "Error: {e}"
            )
            return CellImagesError(row.CellId, str(e))

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        cell_ceiling_adjustment: int = 7,
        bounding_box_percentile: float = 95.0,
        projection_method: str = "max",
        distributed_executor_address: Optional[str] = None,
        batch_size: Optional[int] = None,
        overwrite: bool = False,
        **kwargs,
    ):
        """
        Provided a dataset of cell features and standardized FOV images, generate 3D
        single cell crops and 2D projections.

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

        projection_method: str
            The method to use for generating the flat projection.
            Default: max

            More details:
            https://allencellmodeling.github.io/aicsimageprocessing/aicsimageprocessing.html#aicsimageprocessing.imgToProjection.imgtoprojection

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
            Path to the produced manifest with the various cell image path fields added.
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
        cell_images_2d_all_proj_dir = (
            self.step_local_staging_dir / "cell_images_2d_all_proj"
        )
        cell_images_2d_yx_proj_dir = (
            self.step_local_staging_dir / "cell_images_2d_yx_proj"
        )
        cell_images_3d_dir.mkdir(exist_ok=True)
        cell_images_2d_all_proj_dir.mkdir(exist_ok=True)
        cell_images_2d_yx_proj_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            bbox_results = handler.batched_map(
                self._get_registered_image_size,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
                batch_size=batch_size,
            )

            # Compute bounding box with percentile
            bbox_results = np.array(bbox_results)
            bounding_box = np.percentile(bbox_results, bounding_box_percentile, axis=0)
            bounding_box = np.ceil(bounding_box)

            # Generate bounded arrays
            results = handler.batched_map(
                self._generate_single_cell_images,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
                # Pass the other parameters as list of the same thing for each
                # mapped function call
                [cell_ceiling_adjustment for i in range(len(dataset))],
                [bounding_box for i in range(len(dataset))],
                [projection_method for i in range(len(dataset))],
                [cell_images_3d_dir for i in range(len(dataset))],
                [cell_images_2d_all_proj_dir for i in range(len(dataset))],
                [cell_images_2d_yx_proj_dir for i in range(len(dataset))],
                [overwrite for i in range(len(dataset))],
                batch_size=batch_size,
            )

        # Generate single cell images dataset rows
        single_cell_images_dataset = []
        errors = []
        for r in results:
            if isinstance(r, CellImagesResult):
                single_cell_images_dataset.append(
                    {
                        DatasetFields.CellId: r.cell_id,
                        DatasetFields.CellImage3DPath: r.path_3d,
                        DatasetFields.CellImage2DAllProjectionsPath: r.path_2d_all_proj,
                        DatasetFields.CellImage2DYXProjectionPath: r.path_2d_yx_proj,
                    }
                )
            else:
                errors.append({DatasetFields.CellId: r.cell_id, "Error": r.error})

        # Convert features paths rows to dataframe
        single_cell_images_dataset = pd.DataFrame(single_cell_images_dataset)

        # Drop the various single cell image columns if they already exist
        drop_columns = []
        if DatasetFields.CellImage3DPath in dataset.columns:
            drop_columns.append(DatasetFields.CellImage3DPath)
        if DatasetFields.CellImage2DAllProjectionsPath in dataset.columns:
            drop_columns.append(DatasetFields.CellImage2DAllProjectionsPath)
        if DatasetFields.CellImage2DYXProjectionPath in dataset.columns:
            drop_columns.append(DatasetFields.CellImage2DYXProjectionPath)

        dataset = dataset.drop(columns=drop_columns)

        # Join original dataset to the fov paths
        self.manifest = dataset.merge(
            single_cell_images_dataset, on=DatasetFields.CellId
        )

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        # Save errored cells to JSON
        with open(self.step_local_staging_dir / "errors.json", "w") as write_out:
            json.dump(errors, write_out)

        return manifest_save_path
