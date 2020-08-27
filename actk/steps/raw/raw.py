#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from typing import Union

import dask.dataframe as dd
import pandas as pd
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils

###############################################################################

log = logging.getLogger(__name__)

###############################################################################

# This is the merge of all other steps required fields.
# Reasoning here is that the user will only want to upload the raw
# if the user is doing a full pipeline run
REQUIRED_DATASET_FIELDS = DatasetFields.AllExpectedInputs

###############################################################################


class Raw(Step):
    def __init__(
        self,
        filepath_columns=[
            DatasetFields.SourceReadPath,
            DatasetFields.NucleusSegmentationReadPath,
            DatasetFields.MembraneSegmentationReadPath,
        ],
        metadata_columns=[DatasetFields.FOVId],
        **kwargs,
    ):
        super().__init__(
            filepath_columns=filepath_columns,
            metadata_columns=metadata_columns,
            **kwargs,
        )

    @log_run_params
    def run(self, dataset: Union[str, Path, pd.DataFrame, dd.DataFrame], **kwargs):
        """
        Simple passthrough to store the dataset in local_staging/raw.
        This does not copy any the image files to local_staging/raw, only the manifest.
        This is an optional step that will only run if you want to upload the raw data.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The dataset to use for the rest of the pipeline run.

            **Required dataset columns:** *["CellId", "CellIndex", "FOVId",
            "SourceReadPath", "NucleusSegmentationReadPath",
            "MembraneSegmentationReadPath", "ChannelIndexDNA", "ChannelIndexMembrane",
            "ChannelIndexStructure", "ChannelIndexBrightfield"]*

        Returns
        -------
        manifest_save_path: Path
            The path to the manifest in local_staging with the raw data.
        """
        if isinstance(dataset, (str, Path)):
            dataset = Path(dataset).expanduser().resolve(strict=True)

            # Read dataset
            dataset = pd.read_csv(dataset)

        # Check dataset and manifest have required fields
        dataset_utils.check_required_fields(
            dataset=dataset,
            required_fields=REQUIRED_DATASET_FIELDS,
        )

        # Save manifest to CSV
        self.manifest = dataset
        manifest_save_path = self.step_local_staging_dir / "manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return manifest_save_path
