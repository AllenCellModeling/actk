#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import NamedTuple, Optional, Tuple, Union

import dask.dataframe as dd
import pandas as pd
from aicsimageio import transforms
from aicsimageio.writers import OmeTiffWriter
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils, image_utils
from ...utils.dask_utils import DistributedHandler

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_FIELDS = [
    DatasetFields.FOVId,
    DatasetFields.SourceReadPath,
    DatasetFields.NucleusSegmentationReadPath,
    DatasetFields.MembraneSegmentationReadPath,
    DatasetFields.ChannelIndexDNA,
    DatasetFields.ChannelIndexMembrane,
    DatasetFields.ChannelIndexStructure,
    DatasetFields.ChannelIndexBrightfield,
]


class StandardizeFOVArrayResult(NamedTuple):
    fov_id: int
    path: Path


###############################################################################


class StandardizeFOVArray(Step):
    def __init__(self, filepath_columns=[DatasetFields.StandardizedFOVPath]):
        super().__init__(filepath_columns=filepath_columns)

    @staticmethod
    def _generate_standardized_fov_array(
        row_index: int,
        row: pd.Series,
        desired_pixel_sizes: Tuple[float],
        save_dir: Path,
        overwrite: bool,
    ) -> StandardizeFOVArrayResult:
        # Get the ultimate end save path for this cell
        save_path = save_dir / f"{row.FOVId}.ome.tiff"

        # Check skip
        if not overwrite and save_path.is_file():
            print(f"Skipping Standardized FOV Generation for FOVId: {row.FOVId}")
            return StandardizeFOVArrayResult(row.FOVId, save_path)

        # Overwrite or didn't exist
        print(f"Beginning Standardized FOV Generation for FOVId: {row.FOVId}")

        # Get normalized image array
        normalized_img, channels, pixel_sizes = image_utils.get_normed_image_array(
            raw_image=row.SourceReadPath,
            nucleus_seg_image=row.NucleusSegmentationReadPath,
            membrane_seg_image=row.MembraneSegmentationReadPath,
            dna_channel_index=row.ChannelIndexDNA,
            membrane_channel_index=row.ChannelIndexMembrane,
            structure_channel_index=row.ChannelIndexStructure,
            brightfield_channel_index=row.ChannelIndexBrightfield,
            desired_pixel_sizes=desired_pixel_sizes,
        )

        # Reshape data for serialization
        reshaped = transforms.transpose_to_dims(normalized_img, "CYXZ", "CZYX")

        # Save array as OME Tiff
        with OmeTiffWriter(save_path, overwrite_file=True) as writer:
            writer.save(
                data=reshaped,
                dimension_order="CZYX",
                channel_names=channels,
                pixels_physical_size=pixel_sizes,
            )

        print(f"Beginning Standardized FOV Generation for FOVId: {row.FOVId}")
        return StandardizeFOVArrayResult(row.FOVId, save_path)

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        desired_pixel_sizes: Tuple[float] = (0.29, 0.29, 0.29),
        distributed_executor_address: Optional[str] = None,
        overwrite: bool = False,
        debug: bool = False,
        **kwargs,
    ) -> Path:
        """
        Convert a dataset of raw FOV images and their nucleus and membrane
        segmentations, into a single, standard order and shape, and normalized image.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The dataset to use for generating standard order, normalized, image arrays.

            **Required dataset columns:** *["FOVId", "SourceReadPath",
            "NucleusSegmentationReadPath", "MembraneSegmentationReadPath",
            "ChannelIndexDNA", "ChannelIndexMembrane", "ChannelIndexStructure",
            "ChannelIndexBrightfield"]*

        desired_pixel_sizes: Tuple[float]
            The desired pixel size for to resize each image to in XYZ order.
            Default: (0.29, 0.29, 0.29)

        distributed_executor_address: Optional[str]
            An optional executor address to pass to some computation engine.
            Default: None

        overwrite: bool
            If this step has already partially or completely run, should it overwrite the previous files or not.
            Default: False (Do not overwrite or regenerate files)

        debug: bool
            A debug flag for the developer to use to manipulate how much data runs,
            how it is processed, etc.
            Default: False (Do not debug)

        Returns
        -------
        manifest_save_path: Path
            Path to the produced manifest with the StandardizedFOVPath column added.
        """
        # Handle dataset provided as string or path
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Read dataset
            dataset = pd.read_csv(dataset)

        # Check the dataset for the required columns
        dataset_utils.check_required_fields(
            dataset=dataset, required_fields=REQUIRED_DATASET_FIELDS,
        )

        # Log original length of cell dataset
        log.info(f"Original dataset length: {len(dataset)}")

        # As there is an assumption that this dataset is for cells,
        # generate the FOV dataset by selecting unique FOV Ids
        fov_dataset = dataset.drop_duplicates(DatasetFields.FOVId)

        # Log produced FOV dataset length
        log.info(f"Unique FOV's found in dataset: {len(fov_dataset)}")

        # Create standardized fovs directory
        fovs_dir = self.step_local_staging_dir / "standardized_fovs"
        fovs_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            futures = handler.client.map(
                self._generate_standardized_fov_array,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(fov_dataset.iterrows())),
                # Pass the other parameters as list of the same thing for each
                # mapped function call
                [desired_pixel_sizes for i in range(len(fov_dataset))],
                [fovs_dir for i in range(len(fov_dataset))],
                [overwrite for i in range(len(dataset))],
                # Chunk the processing in batches of 50
                # See: https://github.com/dask/distributed/issues/2181
                # Why: aicsimageio using dask under the hood generates hundreds of tasks per image
                # this means that when we are running the pipeline with thousands of images, the scheduler
                # may be overloaded
                batch_size=50,
            )

            # Block until all complete
            results = handler.gather(futures)

        # Generate fov paths rows
        standardized_fov_paths_dataset = []
        for result in results:
            standardized_fov_paths_dataset.append(
                {
                    DatasetFields.FOVId: result.fov_id,
                    DatasetFields.StandardizedFOVPath: result.path,
                }
            )

        # Convert fov paths to dataframe
        standardized_fov_paths_dataset = pd.DataFrame(standardized_fov_paths_dataset)

        # Join original dataset to the fov paths
        self.manifest = dataset.merge(
            standardized_fov_paths_dataset, on=DatasetFields.FOVId
        )

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / f"manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
