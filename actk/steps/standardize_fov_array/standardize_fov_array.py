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

from ...constants import DatasetFields
from ...utils import dataset_utils, image_utils

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
        super().__init__(filepath_columns=filepath_columns)

    @staticmethod
    def _generate_standardized_fov_array(
        row_index: int,
        row: pd.Series,
        current_pixel_sizes: Optional[Tuple[float]],
        desired_pixel_sizes: Optional[Tuple[float]],
        save_dir: Path,
        overwrite: bool,
    ) -> Union[StandardizeFOVArrayResult, StandardizeFOVArrayError]:
        # Don't use dask for image reading
        aicsimageio.use_dask(False)

        # Get the ultimate end save path for this cell
        save_path = save_dir / f"{row.FOVId}.ome.tiff"

        # Check skip
        if not overwrite and save_path.is_file():
            log.info(f"Skipping standardized FOV generation for FOVId: {row.FOVId}")
            return StandardizeFOVArrayResult(row.FOVId, save_path)

        # Overwrite or didn't exist
        log.info(f"Beginning standardized FOV generation for FOVId: {row.FOVId}")

        # Wrap errors for debugging later
        try:
            # Get normalized image array
            normalized_img, channels, pixel_sizes = image_utils.get_normed_image_array(
                raw_image=row.SourceReadPath,
                nucleus_seg_image=row.NucleusSegmentationReadPath,
                membrane_seg_image=row.MembraneSegmentationReadPath,
                dna_channel_index=row.ChannelIndexDNA,
                membrane_channel_index=row.ChannelIndexMembrane,
                structure_channel_index=row.ChannelIndexStructure,
                brightfield_channel_index=row.ChannelIndexBrightfield,
                current_pixel_sizes=current_pixel_sizes,
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

            log.info(f"Completed standardized FOV generation for FOVId: {row.FOVId}")
            return StandardizeFOVArrayResult(row.FOVId, save_path)

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
        current_pixel_sizes: Optional[Tuple[float]] = (
            0.10833333333333332,
            0.10833333333333332,
            0.29,
        ),
        desired_pixel_sizes: Tuple[float] = (0.29, 0.29, 0.29),
        distributed_executor_address: Optional[str] = None,
        batch_size: Optional[int] = None,
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


        current_pixel_sizes: Optional[Tuple[float]]
            The current physical pixel sizes as a tuple of the raw image.
            Default: (0.10833333333333332, 0.10833333333333332, 0.29), though if None,
            uses (`aicsimageio.AICSImage.get_physical_pixel_size` on the raw image)


        desired_pixel_sizes: Tuple[float]
            The desired pixel size for to resize each image to in XYZ order.
            Default: (0.29, 0.29, 0.29)

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

        # Check assumption: all fields per FOV are constant
        # except CellID and CellIndex
        const_cols_per_fov = [
            c for c in dataset.columns if c not in ["CellId", "CellIndex"]
        ]
        df_const_cols = dataset.groupby("FOVId")[const_cols_per_fov].nunique().eq(1)

        for col_name, is_const in df_const_cols.all().iteritems():
            try:
                assert is_const
            except AssertionError:
                example = df_const_cols[~df_const_cols[col_name]].sample()
                message = (
                    f"{col_name} has multiple values per FOV. "
                    f"Example: FOV {example.index.item()}"
                )
                logging.error(message, exc_info=True)
                raise ValueError(message)

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
            results = handler.batched_map(
                self._generate_standardized_fov_array,
                # Convert dataframe iterrows into two lists of items to iterate over
                # One list will be row index
                # One list will be the pandas series of every row
                *zip(*list(fov_dataset.iterrows())),
                # Pass the other parameters as list of the same thing for each
                # mapped function call
                [current_pixel_sizes for i in range(len(fov_dataset))],
                [desired_pixel_sizes for i in range(len(fov_dataset))],
                [fovs_dir for i in range(len(fov_dataset))],
                [overwrite for i in range(len(dataset))],
                batch_size=batch_size,
            )

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

        # Save errored FOVs to JSON
        with open(self.step_local_staging_dir / "errors.json", "w") as write_out:
            json.dump(errors, write_out)

        return manifest_save_path
