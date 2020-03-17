#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import NamedTuple, Optional, Tuple, Union

import dask.dataframe as dd
import pandas as pd
from aicsimageio.writers import OmeTiffWriter
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils, image_utils
from ...utils.dask_utils import DistributedHandler

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

REQUIRED_DATASET_COLUMNS = [
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
    ) -> StandardizeFOVArrayResult:
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

        # Save array as OME Tiff
        save_path = save_dir / f"{row.FOVId}.ome.tiff"
        with OmeTiffWriter(save_path, overwrite_file=True) as writer:
            writer.save(
                data=normalized_img,
                dimension_order="CZYX",
                channel_names=channels,
                pixels_physical_size=pixel_sizes
            )

        return StandardizeFOVArrayResult(row.FOVId, save_path)

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        desired_pixel_sizes: Tuple[float] = (0.29, 0.29, 0.29),
        distributed_executor_address: Optional[str] = None,
        clean: bool = False,
        debug: bool = False,
        **kwargs
    ) -> Path:
        """
        Convert a dataset of raw FOV images and their nucleus and membrane
        segmentations, into a single, standard order and shape, and normalized image.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The dataset to use for generating standard order, normalized, image arrays.
            Required dataset columns: [
                "FOVId",
                "SourceReadPath",
                "NucleusSegmentationReadPath",
                "MembraneSegmentationReadPath",
                "ChannelIndexDNA",
                "ChannelIndexMembrane",
                "ChannelIndexStructure",
                "ChannelIndexBrightfield",
            ]

        desired_pixel_sizes: Tuple[float]
            The desired pixel size for to resize each image to in XYZ order.
            Default: (0.29, 0.29, 0.29)

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
        save_dir: Path
            Path to the directory where all the image arrays are stored.
        """
        # Handle dataset provided as string or path
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Read dataset
            dataset = dd.read_csv(dataset)

        # Check the dataset for the required columns
        dataset_utils.check_required_columns(dataset, REQUIRED_DATASET_COLUMNS)

        # Log original length of cell dataset
        log.info(f"Original cell dataset length: {len(dataset)}")

        # As there is an assumption that this dataset is for cells,
        # generate the FOV dataset by selecting unique FOV Ids
        dataset = dataset.drop_duplicates("FOVId")

        # Log produced FOV dataset length
        log.info(f"Unique FOV's found in cell dataset: {len(dataset)}")

        # Create standardized array directory
        arr_dir = self.step_local_staging_dir / "standardized_fovs"
        arr_dir.mkdir(exist_ok=True)

        # Process each row
        with DistributedHandler(distributed_executor_address) as handler:
            # Start processing
            futures = handler.client.map(
                self._generate_standardized_fov_array,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(dataset.iterrows())),
                # Pass the other parameters as list of the same thing for each
                # mapped function call
                [desired_pixel_sizes for i in range(len(dataset))],
                [arr_dir for i in range(len(dataset))]
            )

            # Block until all complete
            results = handler.gather(futures)

        # Generate manifest rows
        self.manifest = []
        for result in results:
            self.manifest.append({
                DatasetFields.FOVId: result.fov_id,
                DatasetFields.StandardizedFOVPath: result.path
            })

        # Convert manifest to dataframe
        self.manifest = pd.DataFrame(self.manifest)

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / f"manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
