#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import NamedTuple, Optional, Tuple, Union

import aicsimageio
import dask.dataframe as dd
import pandas as pd
from aics_dask_utils import DistributedHandler
from aicsimageio import transforms
from aicsimageio.writers import OmeTiffWriter
from datastep import Step, log_run_params
from quilt3 import Bucket, Package

from ...constants import DatasetFields
from ...utils import dataset_utils, image_utils, s3_utils
from ..get_s3_dataset import GetS3Dataset

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
    fov_id: Union[int, str]
    path: Path


class StandardizeFOVArrayError(NamedTuple):
    fov_id: int
    error: str


###############################################################################


class StandardizeFOVArray(Step):
    def __init__(self, filepath_columns=[DatasetFields.StandardizedFOVPath]):
        super().__init__(
            direct_upstream_tasks=[GetS3Dataset], filepath_columns=filepath_columns
        )

    @staticmethod
    def _generate_standardized_fov_array(
        row_index: int,
        row: pd.Series,
        desired_pixel_sizes: Tuple[float],
        save_dir: Path,
        overwrite: bool,
    ) -> Union[StandardizeFOVArrayResult, StandardizeFOVArrayError]:
        # Don't use dask for image reading
        aicsimageio.use_dask(False)

        # Get the ultimate end save path for this cell
        local_save_path = save_dir / f"{row.FOVId}.ome.tiff"
        local_save_path.parent.mkdir(exist_ok=True, parents=True)
        b = Bucket("s3://allencell-internal-quilt")
        bucket_save_path = f"jacksonb/actk/{local_save_path}"

        # Check skip
        if not overwrite and s3_utils.s3_file_exists(b, bucket_save_path):
            log.info(f"Skipping standardized FOV generation for FOVId: {row.FOVId}")
            return StandardizeFOVArrayResult(row.FOVId, local_save_path)

        # Overwrite or didn't exist
        log.info(f"Beginning standardized FOV generation for FOVId: {row.FOVId}")

        # Wrap errors for debugging later
        try:
            # Download the images
            local_base = "local_staging/raw"
            p = Package.browse("aics/pipeline_integrated_cell", "s3://allencell")
            source_path = Path(f"{local_base}/{row.SourceReadPath}")
            nuc_seg_path = Path(f"{local_base}/{row.NucleusSegmentationReadPath}")
            memb_seg_path = Path(f"{local_base}/{row.MembraneSegmentationReadPath}")
            if not source_path.exists():
                p[row.SourceReadPath].fetch(source_path)
            if not nuc_seg_path.exists():
                p[row.NucleusSegmentationReadPath].fetch(nuc_seg_path)
            if not memb_seg_path.exists():
                p[row.MembraneSegmentationReadPath].fetch(memb_seg_path)

            # Get normalized image array
            normalized_img, channels, pixel_sizes = image_utils.get_normed_image_array(
                raw_image=source_path,
                nucleus_seg_image=nuc_seg_path,
                membrane_seg_image=memb_seg_path,
                dna_channel_index=row.ChannelIndexDNA,
                membrane_channel_index=row.ChannelIndexMembrane,
                structure_channel_index=row.ChannelIndexStructure,
                brightfield_channel_index=row.ChannelIndexBrightfield,
                desired_pixel_sizes=desired_pixel_sizes,
            )

            # Reshape data for serialization
            reshaped = transforms.transpose_to_dims(normalized_img, "CYXZ", "CZYX")

            # Save array as OME Tiff
            with OmeTiffWriter(local_save_path, overwrite_file=True) as writer:
                writer.save(
                    data=reshaped,
                    dimension_order="CZYX",
                    channel_names=channels,
                    pixels_physical_size=pixel_sizes,
                )

            # Upload FOV to Bucket
            b.put_file(bucket_save_path, local_save_path)

            log.info(f"Completed standardized FOV generation for FOVId: {row.FOVId}")
            return StandardizeFOVArrayResult(row.FOVId, local_save_path)

        # Catch and return error
        except Exception as e:
            log.info(
                f"Failed standardized FOV generation for FOVId: {row.FOVId}. Error: {e}"
            )
            return StandardizeFOVArrayError(row.FOVId, str(e))

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        desired_pixel_sizes: Tuple[float] = (0.29, 0.29, 0.29),
        distributed_executor_address: Optional[str] = None,
        overwrite: bool = False,
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
            If this step has already partially or completely run, should it overwrite
            the previous files or not.
            Default: False (Do not overwrite or regenerate files)

        Returns
        -------
        manifest_save_path: Path
            Path to the produced manifest with the StandardizedFOVPath column added.
        """
        # Handle dataset provided as string or path
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Check the dataset for the required columns
            dataset_utils.check_required_fields(
                dataset=dataset, required_fields=REQUIRED_DATASET_FIELDS,
            )

            # Read dataset
            dataset = pd.read_csv(dataset)

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
            )
            results = handler.gather(futures)

        # Generate fov paths rows
        standardized_fov_paths_dataset = []
        errors = []
        for result in results:
            if isinstance(result, StandardizeFOVArrayResult):
                standardized_fov_paths_dataset.append(
                    {
                        DatasetFields.FOVId: result.fov_id,
                        DatasetFields.StandardizedFOVPath: result.path,
                    }
                )
            else:
                errors.append(
                    {DatasetFields.FOVId: result.fov_id, "Error": result.error}
                )

        # Convert fov paths to dataframe
        standardized_fov_paths_dataset = pd.DataFrame(standardized_fov_paths_dataset)

        # Drop StandardizedFOVPath column if it already exists
        if DatasetFields.StandardizedFOVPath in dataset.columns:
            dataset = dataset.drop(columns=[DatasetFields.StandardizedFOVPath])

        # Join original dataset to the fov paths
        self.manifest = dataset.merge(
            standardized_fov_paths_dataset, on=DatasetFields.FOVId
        )

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        b = Bucket("s3://allencell-internal-quilt")
        b.put_file(f"jacksonb/actk/{manifest_save_path}", manifest_save_path)

        # Save errored FOVs to JSON
        errors_save_path = self.step_local_staging_dir / "errors.json"
        with open(errors_save_path, "w") as write_out:
            json.dump(errors, write_out)

        b.put_file(f"jacksonb/actk/{errors_save_path}", errors_save_path)

        return manifest_save_path
