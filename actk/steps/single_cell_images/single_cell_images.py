#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
from pathlib import Path
from typing import List, Optional, Union

import dask.dataframe as dd
import numpy as np
import pandas as pd
from datastep import Step, log_run_params

from ...constants import DatasetFields
from ...utils import dataset_utils
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

###############################################################################


class SingleCellImages(Step):
    def __init__(
        self,
        direct_upstream_tasks=[SingleCellFeatures],
        filepath_columns=[DatasetFields.CellImage3DPath, DatasetFields.CellImage2DPath],
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

    @log_run_params
    def run(
        self,
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame],
        bounding_box_percentile: float = 95.0,
        distributed_executor_address: Optional[str] = None,
        **kwargs,
    ):
        """
        Provided a dataset of cell features, generate single cell images in both 3D and
        2D max projections.

        Parameters
        ----------
        dataset: Union[str, Path, pd.DataFrame, dd.DataFrame]
            The primary cell dataset to generate 2D max projections and 3D crops for
            each cell.

            **Required dataset columns:** *["CellFeaturesPath", "StandardizedFOVPath",
            "CellFeaturesPath"]*

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
            Path to the produced manifest with the CellImage2DPath and CellImage3DPath
            columns added.
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

        # Save to manifest
        self.manifest = pd.DataFrame()

        # Save manifest to CSV
        manifest_save_path = self.step_local_staging_dir / f"manifest.csv"
        self.manifest.to_csv(manifest_save_path, index=False)

        return bounding_box
